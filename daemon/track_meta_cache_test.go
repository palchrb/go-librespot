package daemon

import (
	"fmt"
	"testing"
	"time"

	librespot "github.com/devgianlu/go-librespot"
	extmetadatapb "github.com/devgianlu/go-librespot/proto/spotify/extendedmetadata"
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

// A client polling a filling sweep asks for the same context every second or
// two; enumerating pages over the network, so those polls must be served from
// memory rather than re-paging the whole playlist each time.
func TestContextListCacheServesRepeatLookups(t *testing.T) {
	c := newContextListCache()
	now := time.Now()

	if _, ok := c.get("spotify:playlist:a"); ok {
		t.Fatal("expected miss on empty cache")
	}

	c.put("spotify:playlist:a", []string{"spotify:track:1", "spotify:track:2"}, now)

	uris, ok := c.get("spotify:playlist:a")
	if !ok || len(uris) != 2 {
		t.Fatalf("expected the enumerated listing back, got %v (ok=%t)", uris, ok)
	}
}

// The listing carries no revision, so a stale entry is the only way a client
// could miss an edit; the TTL bounds how long that can last.
func TestContextListCacheExpires(t *testing.T) {
	c := newContextListCache()

	c.put("spotify:playlist:a", []string{"spotify:track:1"}, time.Now().Add(-contextListTTL-time.Second))

	if _, ok := c.get("spotify:playlist:a"); ok {
		t.Fatal("expected an entry older than the TTL to be treated as a miss")
	}
}

func TestContextListCacheEviction(t *testing.T) {
	c := newContextListCache()
	now := time.Now()

	for i := 0; i < contextListCacheLimit+3; i++ {
		c.put(fmt.Sprintf("spotify:playlist:%d", i), []string{"spotify:track:1"}, now)
	}

	if len(c.entries) != contextListCacheLimit {
		t.Fatalf("expected the cache bounded to %d entries, got %d", contextListCacheLimit, len(c.entries))
	}
	if _, ok := c.get("spotify:playlist:0"); ok {
		t.Fatal("expected the oldest context evicted")
	}
	if _, ok := c.get(fmt.Sprintf("spotify:playlist:%d", contextListCacheLimit+2)); !ok {
		t.Fatal("expected the newest context retained")
	}
}

// A client polls this endpoint while the enumeration runs; each poll must find
// the job already claimed rather than start another one.
func TestContextListCacheSingleFlightsEnumeration(t *testing.T) {
	c := newContextListCache()

	if !c.beginFetch("spotify:playlist:a") {
		t.Fatal("expected the first caller to claim the enumeration")
	}
	if c.beginFetch("spotify:playlist:a") {
		t.Fatal("expected a concurrent caller to be turned away")
	}

	c.endFetch("spotify:playlist:a")
	if !c.beginFetch("spotify:playlist:a") {
		t.Fatal("expected the job claimable again once the previous one finished")
	}
}

// A cached listing needs no enumeration at all, however often it is asked for.
func TestContextListCacheSkipsEnumerationWhenCached(t *testing.T) {
	c := newContextListCache()
	c.put("spotify:playlist:a", []string{"spotify:track:1"}, time.Now())

	if c.beginFetch("spotify:playlist:a") {
		t.Fatal("expected a cached listing to need no enumeration")
	}

	c.put("spotify:playlist:a", []string{"spotify:track:1"}, time.Now().Add(-contextListTTL-time.Second))
	if !c.beginFetch("spotify:playlist:a") {
		t.Fatal("expected an expired listing to be re-enumerated")
	}
}

// Each uri resolves under its own extended-metadata kind: a TRACK_V4 query for
// an episode returns nothing, which is how shows used to enumerate to an empty
// listing.
func TestMetaExtensionKind(t *testing.T) {
	cases := []struct {
		uri  string
		kind extmetadatapb.ExtensionKind
		ok   bool
	}{
		{"spotify:track:4cOdK2wGLETKBW3PvgPWqT", extmetadatapb.ExtensionKind_TRACK_V4, true},
		{"spotify:episode:4rOoJ6Egrf8K2IrywzwOMk", extmetadatapb.ExtensionKind_EPISODE_V4, true},
		{"spotify:local:a:b:c:1", 0, false},
		{"spotify:artist:0OdUWJ0sBjDrqHygGUXeCF", 0, false},
		{"", 0, false},
	}

	for _, tc := range cases {
		kind, ok := metaExtensionKind(tc.uri)
		if ok != tc.ok || kind != tc.kind {
			t.Fatalf("metaExtensionKind(%q) = (%v, %t), want (%v, %t)", tc.uri, kind, ok, tc.kind, tc.ok)
		}
	}
}
