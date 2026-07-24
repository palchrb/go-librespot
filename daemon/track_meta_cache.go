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

	cached, err := p.fetchTrackMetadataBatch(ctx, uris)
	if err != nil {
		p.app.log.WithError(err).Warnf("failed prefetching metadata for %d tracks", len(uris))
		return
	}

	p.app.log.Debugf("prefetched metadata for %d/%d tracks", cached, len(uris))
}

// fetchTrackMetadataBatch performs one batched extended-metadata request for
// the given track URIs and fills the cache, returning how many were cached.
func (p *AppPlayer) fetchTrackMetadataBatch(ctx context.Context, uris []string) (int, error) {
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
		return 0, err
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

	return cached, nil
}

// fullMetaFetchLimit caps how many tracks of a context the full sweep
// resolves, leaving cache headroom for the moving window of other contexts.
const fullMetaFetchLimit = 800

// fullMetaBatchPause spaces the batches of a full-context sweep so it never
// competes with the playback path for the radio or the account budget.
const fullMetaBatchPause = time.Second

// scheduleContextMetaPrefetch resolves metadata for the WHOLE playlist in the
// background: one internal GetPlaylist call for the track URIs, then batched
// extended-metadata requests, paced. After it completes every track in the
// list is known to /status (pending_track/next_track) before the user skips
// anywhere. Only playlists carry a cheap full track listing; other context
// types rely on the window prefetch. Runs on the Run goroutine; the sweep is
// single-flighted and skipped when this context was already swept.
func (p *AppPlayer) scheduleContextMetaPrefetch(contextUri string) {
	if p.metaCache == nil || p.sess == nil {
		return
	}
	if !strings.HasPrefix(contextUri, "spotify:playlist:") {
		return
	}
	if contextUri == p.lastFullMetaContext {
		return
	}
	if !p.fullMetaFetchInFlight.CompareAndSwap(false, true) {
		return
	}

	p.lastFullMetaContext = contextUri
	go p.fetchPlaylistMetadata(contextUri)
}

// fetchPlaylistMetadata is the background half of scheduleContextMetaPrefetch.
func (p *AppPlayer) fetchPlaylistMetadata(contextUri string) {
	defer p.fullMetaFetchInFlight.Store(false)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	spotId, err := librespot.SpotifyIdFromUri(contextUri)
	if err != nil {
		return
	}

	content, err := p.sess.Spclient().GetPlaylist(ctx, *spotId)
	if err != nil {
		p.app.log.WithError(err).Warnf("failed fetching playlist for metadata sweep: %s", contextUri)
		return
	}

	var uris []string
	if content.Contents != nil {
		for _, item := range content.Contents.Items {
			if uri := item.GetUri(); strings.HasPrefix(uri, "spotify:track:") {
				uris = append(uris, uri)
			}
		}
	}
	if len(uris) > fullMetaFetchLimit {
		p.app.log.Debugf("metadata sweep truncated to %d of %d tracks", fullMetaFetchLimit, len(uris))
		uris = uris[:fullMetaFetchLimit]
	}

	p.sweepBatches(ctx, p.metaCache.missing(uris), contextUri)
}

// scheduleMetaSweep resolves metadata for the given track URIs in the
// background (paced batches), for callers that already know the track list —
// e.g. the /playlist/tracks endpoint. Single-flighted together with the
// context sweep; when a sweep is already running the call is dropped and the
// client's next poll picks up whatever has been cached meanwhile.
func (p *AppPlayer) scheduleMetaSweep(uris []string, label string) {
	if p.metaCache == nil || p.sess == nil {
		return
	}

	missing := p.metaCache.missing(uris)
	if len(missing) == 0 {
		return
	}
	if len(missing) > fullMetaFetchLimit {
		missing = missing[:fullMetaFetchLimit]
	}

	if !p.fullMetaFetchInFlight.CompareAndSwap(false, true) {
		return
	}

	go func() {
		defer p.fullMetaFetchInFlight.Store(false)

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()

		p.sweepBatches(ctx, missing, label)
	}()
}

// sweepBatches fetches metadata for the given URIs in paced batches.
func (p *AppPlayer) sweepBatches(ctx context.Context, missing []string, label string) {
	total := len(missing)
	var cached int
	for len(missing) > 0 && ctx.Err() == nil {
		batch := missing
		if len(batch) > maxMetaBatch {
			batch = batch[:maxMetaBatch]
		}
		missing = missing[len(batch):]

		n, err := p.fetchTrackMetadataBatch(ctx, batch)
		if err != nil {
			p.app.log.WithError(err).Warnf("metadata sweep aborted for %s", label)
			return
		}
		cached += n

		if len(missing) > 0 {
			time.Sleep(fullMetaBatchPause)
		}
	}

	if total > 0 {
		p.app.log.Infof("swept metadata for %d/%d tracks in %s", cached, total, label)
	}
}
