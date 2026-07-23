package daemon

import (
	"fmt"
	"testing"

	librespot "github.com/devgianlu/go-librespot"
	metadatapb "github.com/devgianlu/go-librespot/proto/spotify/metadata"
)

func mediaFixture(name string) *librespot.Media {
	return librespot.NewMediaFromTrack(&metadatapb.Track{Name: &name})
}

func TestTrackMetaCachePutGet(t *testing.T) {
	c := newTrackMetaCache()

	if c.get("spotify:track:a") != nil {
		t.Fatal("expected miss on empty cache")
	}

	m := mediaFixture("A")
	c.put("spotify:track:a", m)
	if got := c.get("spotify:track:a"); got != m {
		t.Fatalf("expected cached media, got %v", got)
	}

	// nil / empty are ignored
	c.put("", m)
	c.put("spotify:track:b", nil)
	if c.get("spotify:track:b") != nil {
		t.Fatal("expected nil media not to be cached")
	}
}

func TestTrackMetaCacheMissing(t *testing.T) {
	c := newTrackMetaCache()
	c.put("spotify:track:a", mediaFixture("A"))

	missing := c.missing([]string{"spotify:track:a", "spotify:track:b", "spotify:track:b", "", "spotify:track:c"})
	if len(missing) != 2 || missing[0] != "spotify:track:b" || missing[1] != "spotify:track:c" {
		t.Fatalf("expected deduplicated misses [b c], got %v", missing)
	}
}

func TestTrackMetaCacheEviction(t *testing.T) {
	c := newTrackMetaCache()

	for i := 0; i <= trackMetaCacheLimit; i++ {
		c.put(fmt.Sprintf("spotify:track:%d", i), mediaFixture("x"))
	}

	if c.get("spotify:track:0") != nil {
		t.Fatal("expected the oldest entry to be evicted")
	}
	if c.get(fmt.Sprintf("spotify:track:%d", trackMetaCacheLimit)) == nil {
		t.Fatal("expected the newest entry to be cached")
	}
	if len(c.entries) != trackMetaCacheLimit || len(c.order) != trackMetaCacheLimit {
		t.Fatalf("expected cache size clamped to %d, got %d/%d", trackMetaCacheLimit, len(c.entries), len(c.order))
	}

	// Re-putting an existing key must not grow the order slice.
	c.put(fmt.Sprintf("spotify:track:%d", trackMetaCacheLimit), mediaFixture("y"))
	if len(c.order) != trackMetaCacheLimit {
		t.Fatalf("expected re-put not to grow the cache, got %d", len(c.order))
	}
}
