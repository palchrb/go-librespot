package daemon

import (
	"context"
	"strings"
	"sync"
	"time"

	librespot "github.com/devgianlu/go-librespot"
	extmetadatapb "github.com/devgianlu/go-librespot/proto/spotify/extendedmetadata"
	metadatapb "github.com/devgianlu/go-librespot/proto/spotify/metadata"
)

// trackMetaCacheLimit bounds the in-memory metadata cache. Entries are a few
// KB each (a metadata proto), so the cap keeps the cache under ~10MB.
const trackMetaCacheLimit = 1000

// trackMetaCache is a bounded in-memory cache of track metadata keyed by URI.
// It is fed by loaded and prefetched streams and by background batch fetches
// of the current context window, and read by /status to describe tracks whose
// stream has not been loaded yet (the pending track of a deferred skip, the
// upcoming track). Reads and writes cross goroutines (the batch fetch runs in
// the background), hence the mutex.
type trackMetaCache struct {
	mu      sync.Mutex
	entries map[string]*librespot.Media
	order   []string // insertion order for FIFO eviction
}

func newTrackMetaCache() *trackMetaCache {
	return &trackMetaCache{entries: map[string]*librespot.Media{}}
}

func (c *trackMetaCache) get(uri string) *librespot.Media {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.entries[uri]
}

func (c *trackMetaCache) put(uri string, media *librespot.Media) {
	if uri == "" || media == nil {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.entries[uri]; !ok {
		c.order = append(c.order, uri)
	}
	c.entries[uri] = media

	for len(c.order) > trackMetaCacheLimit {
		delete(c.entries, c.order[0])
		c.order = c.order[1:]
	}
}

// missing returns the subset of uris not present in the cache, preserving
// order and dropping duplicates.
func (c *trackMetaCache) missing(uris []string) []string {
	c.mu.Lock()
	defer c.mu.Unlock()

	var out []string
	seen := map[string]bool{}
	for _, uri := range uris {
		if uri == "" || seen[uri] {
			continue
		}
		seen[uri] = true
		if _, ok := c.entries[uri]; !ok {
			out = append(out, uri)
		}
	}
	return out
}

// maxMetaBatch caps how many tracks a single background metadata fetch asks
// for; the connect-state window (prev + current + next) fits comfortably.
const maxMetaBatch = 100

// scheduleMetaPrefetch batch-fetches metadata for the tracks in the current
// state window (prev + current + next) that are not cached yet, so /status
// can name pending and upcoming tracks (and their cover art) before their
// streams load. Runs on the Run goroutine; the fetch itself runs in the
// background and is single-flighted — a window that changes while a fetch is
// in flight is picked up by the next call.
func (p *AppPlayer) scheduleMetaPrefetch() {
	if p.metaCache == nil || p.sess == nil {
		return
	}

	var uris []string
	add := func(uri string) {
		// Only tracks: the batch queries TRACK_V4 metadata.
		if strings.HasPrefix(uri, "spotify:track:") {
			uris = append(uris, uri)
		}
	}
	if t := p.state.player.Track; t != nil {
		add(t.Uri)
	}
	for _, t := range p.state.player.PrevTracks {
		add(t.Uri)
	}
	for _, t := range p.state.player.NextTracks {
		add(t.Uri)
	}

	missing := p.metaCache.missing(uris)
	if len(missing) == 0 {
		return
	}
	if len(missing) > maxMetaBatch {
		missing = missing[:maxMetaBatch]
	}

	if !p.metaFetchInFlight.CompareAndSwap(false, true) {
		return
	}
	go p.fetchTrackMetadata(missing)
}

// fetchTrackMetadata resolves TRACK_V4 metadata for the given track URIs in a
// single batched extended-metadata request and fills the cache. Best-effort:
// failures only cost the pending/next-track fields in /status.
func (p *AppPlayer) fetchTrackMetadata(uris []string) {
	defer p.metaFetchInFlight.Store(false)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	req := &extmetadatapb.BatchedEntityRequest{}
	for _, uri := range uris {
		req.EntityRequest = append(req.EntityRequest, &extmetadatapb.EntityRequest{
			EntityUri: uri,
			Query: []*extmetadatapb.ExtensionQuery{{
				ExtensionKind: extmetadatapb.ExtensionKind_TRACK_V4,
			}},
		})
	}

	resp, err := p.sess.Spclient().ExtendedMetadata(ctx, req)
	if err != nil {
		p.app.log.WithError(err).Warnf("failed prefetching metadata for %d tracks", len(uris))
		return
	}

	var cached int
	for _, item := range resp.ExtendedMetadata {
		if item.ExtensionKind != extmetadatapb.ExtensionKind_TRACK_V4 {
			continue
		}
		for _, extData := range item.ExtensionData {
			if extData.Header == nil || extData.Header.StatusCode != 200 || extData.ExtensionData == nil {
				continue
			}

			var trackMeta metadatapb.Track
			if err := extData.ExtensionData.UnmarshalTo(&trackMeta); err != nil {
				continue
			}

			p.metaCache.put(extData.EntityUri, librespot.NewMediaFromTrack(&trackMeta))
			cached++
		}
	}

	p.app.log.Debugf("prefetched metadata for %d/%d tracks", cached, len(uris))
}
