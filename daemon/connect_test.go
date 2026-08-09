package daemon

import (
	"context"
	"testing"
	"time"

	"github.com/devgianlu/go-librespot/dealer"
	connectpb "github.com/devgianlu/go-librespot/proto/spotify/connectstate"
	"google.golang.org/protobuf/proto"
)

// A slow PUT response must not overwrite a fresher dealer update that arrived
// while it was in flight; the cluster's own change timestamp is the ordering.
func TestStoreClusterOrdering(t *testing.T) {
	s := &State{}

	s.storeCluster(nil) // must not panic or store
	if s.lastCluster != nil {
		t.Fatal("expected nil cluster ignored")
	}

	s.storeCluster(&connectpb.Cluster{ChangedTimestampMs: 100, ActiveDeviceId: "new"})
	if s.lastCluster.ActiveDeviceId != "new" {
		t.Fatal("expected first cluster stored")
	}
	firstAt := s.lastClusterAt

	s.storeCluster(&connectpb.Cluster{ChangedTimestampMs: 50, ActiveDeviceId: "old"})
	if s.lastCluster.ActiveDeviceId != "new" {
		t.Fatal("expected older cluster dropped")
	}
	if s.lastClusterAt != firstAt {
		t.Fatal("expected timestamp untouched by a dropped cluster")
	}

	time.Sleep(time.Millisecond)
	s.storeCluster(&connectpb.Cluster{ChangedTimestampMs: 200, ActiveDeviceId: "newer"})
	if s.lastCluster.ActiveDeviceId != "newer" || !s.lastClusterAt.After(firstAt) {
		t.Fatal("expected newer cluster stored with fresh timestamp")
	}
}

func testClusterFixture() *connectpb.Cluster {
	return &connectpb.Cluster{
		ActiveDeviceId:     "self-id",
		ChangedTimestampMs: 100,
		Device: map[string]*connectpb.DeviceInfo{
			"self-id":  {Name: "stale self name", Volume: 1},
			"phone-id": {Name: "Phone", Brand: "Apple", Model: "iPhone", CanPlay: true, Volume: 30000, Capabilities: &connectpb.Capabilities{VolumeSteps: 16}},
			"tv-id":    {Name: "TV", CanPlay: true},
		},
	}
}

// The listing is a pure view of the snapshot: self upserted from live local
// state, deterministic order (self first, then name), active flag computed
// from the cluster's active id.
func TestClusterToApiDevicesMapping(t *testing.T) {
	self := &connectpb.DeviceInfo{Name: "tunebox", Volume: 3276, Capabilities: &connectpb.Capabilities{VolumeSteps: 100}}
	at := time.Now()

	resp := clusterToApiDevices(testClusterFixture(), self, "self-id", at)

	if resp.ActiveDeviceId == nil || *resp.ActiveDeviceId != "self-id" {
		t.Fatalf("expected active device id, got %v", resp.ActiveDeviceId)
	}
	if resp.UpdatedAt == nil || *resp.UpdatedAt != at.UnixMilli() {
		t.Fatalf("expected updated_at %d, got %v", at.UnixMilli(), resp.UpdatedAt)
	}
	if len(resp.Devices) != 3 {
		t.Fatalf("expected 3 devices, got %d", len(resp.Devices))
	}

	if d := resp.Devices[0]; !d.Self || d.Name != "tunebox" || !d.Active || d.Volume != 3276 || d.VolumeSteps != 100 {
		t.Fatalf("expected self first, upserted from live state, got %+v", d)
	}
	if resp.Devices[1].Name != "Phone" || resp.Devices[2].Name != "TV" {
		t.Fatalf("expected name order after self, got %s, %s", resp.Devices[1].Name, resp.Devices[2].Name)
	}
	if d := resp.Devices[1]; d.Self || d.Active || d.Brand != "Apple" || d.VolumeSteps != 16 || !d.CanPlay {
		t.Fatalf("unexpected phone mapping: %+v", d)
	}
}

// The startup case: no cluster yet must still list this device, so a UI never
// renders an empty picker on a healthy daemon.
func TestClusterToApiDevicesNilCluster(t *testing.T) {
	self := &connectpb.DeviceInfo{Name: "tunebox"}

	resp := clusterToApiDevices(nil, self, "self-id", time.Time{})

	if len(resp.Devices) != 1 || !resp.Devices[0].Self {
		t.Fatalf("expected exactly the self device, got %+v", resp.Devices)
	}
	if resp.ActiveDeviceId != nil || resp.UpdatedAt != nil {
		t.Fatal("expected null active id and updated_at before any cluster")
	}
}

// The local rejections that must fire before any network call: self-transfer
// (would bounce a transfer command back and reload the playing stream),
// not-active (unverified from/to semantics upstream), unknown target.
func TestValidateTransferTarget(t *testing.T) {
	cluster := testClusterFixture()

	cases := []struct {
		name    string
		cluster *connectpb.Cluster
		target  string
		active  bool
		want    error
	}{
		{"valid target", cluster, "phone-id", true, nil},
		{"self is rejected", cluster, "self-id", true, ErrBadRequest},
		{"not active is rejected", cluster, "phone-id", false, ErrBadRequest},
		{"unknown device", cluster, "nope", true, ErrNotFound},
		{"no cluster yet", nil, "phone-id", true, ErrNotFound},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := validateTransferTarget(tc.cluster, "self-id", tc.target, tc.active); got != tc.want {
				t.Fatalf("validateTransferTarget() = %v, want %v", got, tc.want)
			}
		})
	}
}

// Regression guard for the wiring itself: a merge that drops the storeCluster
// call in handleDealerMessage must fail this test, not a field session.
func TestClusterUpdateIsStored(t *testing.T) {
	p, _ := newSettleTestPlayer(t, 0)

	cluster := testClusterFixture()
	payload, err := proto.Marshal(&connectpb.ClusterUpdate{Cluster: cluster})
	if err != nil {
		t.Fatal(err)
	}

	if err := p.handleDealerMessage(context.Background(), dealer.Message{
		Uri:     "hm://connect-state/v1/cluster",
		Payload: payload,
	}); err != nil {
		t.Fatalf("handleDealerMessage failed: %v", err)
	}

	if p.state.lastCluster == nil || p.state.lastCluster.ActiveDeviceId != "self-id" {
		t.Fatal("expected the cluster update to be stored")
	}

	// A nil-cluster update must be ignored, not panic (latent deref fixed).
	empty, _ := proto.Marshal(&connectpb.ClusterUpdate{})
	if err := p.handleDealerMessage(context.Background(), dealer.Message{
		Uri:     "hm://connect-state/v1/cluster",
		Payload: empty,
	}); err != nil {
		t.Fatalf("nil-cluster update failed: %v", err)
	}
	if p.state.lastCluster.ActiveDeviceId != "self-id" {
		t.Fatal("expected snapshot untouched by nil-cluster update")
	}
}
