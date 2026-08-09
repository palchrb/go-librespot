package daemon

import (
	"testing"
	"time"

	connectpb "github.com/devgianlu/go-librespot/proto/spotify/connectstate"
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
