package spclient

import (
	"testing"

	librespot "github.com/devgianlu/go-librespot"
	connectpb "github.com/devgianlu/go-librespot/proto/spotify/connectstate"
	"google.golang.org/protobuf/proto"
)

// The connect-state PUT response being a Cluster is observed behavior, not a
// documented contract, so the parser must round-trip a real cluster and shrug
// off anything else without an error escaping into the state-put path.
func TestParseClusterResponse(t *testing.T) {
	log := &librespot.NullLogger{}

	cluster := &connectpb.Cluster{
		ActiveDeviceId:     "abc",
		ChangedTimestampMs: 42,
		Device: map[string]*connectpb.DeviceInfo{
			"abc": {Name: "phone"},
		},
	}
	body, err := proto.Marshal(cluster)
	if err != nil {
		t.Fatal(err)
	}

	got := parseClusterResponse(log, body)
	if got == nil || got.ActiveDeviceId != "abc" || got.Device["abc"].Name != "phone" {
		t.Fatalf("expected the cluster back, got %v", got)
	}

	if parseClusterResponse(log, nil) != nil {
		t.Fatal("expected nil for an empty body")
	}
	if parseClusterResponse(log, []byte{0xff, 0xff, 0xff, 0xff}) != nil {
		t.Fatal("expected nil for garbage")
	}
}
