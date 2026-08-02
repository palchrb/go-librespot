package daemon

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	librespot "github.com/devgianlu/go-librespot"
	extmetadatapb "github.com/devgianlu/go-librespot/proto/spotify/extendedmetadata"
	metadatapb "github.com/devgianlu/go-librespot/proto/spotify/metadata"
	"github.com/devgianlu/go-librespot/tracks"
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

// fullMetaFetchLimit caps how many tracks of a context are enumerated and
// swept, leaving cache headroom for the moving window of other contexts.
const fullMetaFetchLimit = 800

// fullMetaBatchPause spaces the batches of a full-context sweep so it never
// competes with the playback path for the radio or the account budget.
const fullMetaBatchPause = time.Second

// contextListTTL bounds how long an enumerated context listing is reused.
// Re-polls while a sweep fills in metadata (seconds apart) must not re-page the
// whole context, but a playlist edited between two sittings should be picked up.
const contextListTTL = 5 * time.Minute

// contextListCacheLimit bounds how many enumerated contexts are remembered.
const contextListCacheLimit = 8

type contextListEntry struct {
	uris    []string
	fetched time.Time
}

// contextListCache remembers the track URIs of recently enumerated contexts.
// Enumeration pages over the network, and both the listing endpoint and the
// sweep ask for the same context repeatedly, so without this a client polling
// for a filling sweep would re-page the whole playlist on every poll.
type contextListCache struct {
	mu       sync.Mutex
	entries  map[string]contextListEntry
	order    []string
	inFlight map[string]bool
}

func newContextListCache() *contextListCache {
	return &contextListCache{
		entries:  map[string]contextListEntry{},
		inFlight: map[string]bool{},
	}
}

// beginFetch claims the right to enumerate uri, reporting false when the
// listing is already cached or another goroutine is already enumerating it.
// A client polling every second must not spawn an enumeration per poll.
func (c *contextListCache) beginFetch(uri string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if e, ok := c.entries[uri]; ok && time.Since(e.fetched) <= contextListTTL {
		return false
	}
	if c.inFlight[uri] {
		return false
	}
	c.inFlight[uri] = true
	return true
}

func (c *contextListCache) endFetch(uri string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.inFlight, uri)
}

func (c *contextListCache) get(uri string) ([]string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	e, ok := c.entries[uri]
	if !ok || time.Since(e.fetched) > contextListTTL {
		return nil, false
	}
	return e.uris, true
}

func (c *contextListCache) put(uri string, uris []string, now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.entries[uri]; !ok {
		c.order = append(c.order, uri)
	}
	c.entries[uri] = contextListEntry{uris: uris, fetched: now}

	for len(c.order) > contextListCacheLimit {
		delete(c.entries, c.order[0])
		c.order = c.order[1:]
	}
}

// resolveContextTracks enumerates every track of an arbitrary context URI —
// playlist, album, artist, whatever the context resolver understands. It builds
// a throwaway track list rather than reading the playing one: List is not safe
// for concurrent use and the Run goroutine mutates it on every skip, and a fresh
// list is in the context's own order rather than the shuffled playback order.
//
// Only track URIs are returned; episodes carry no TRACK_V4 metadata, so a show
// context enumerates to nothing.
func (p *AppPlayer) resolveContextTracks(ctx context.Context, uri string) ([]string, error) {
	if uris, ok := p.contextLists.get(uri); ok {
		return uris, nil
	}

	spotCtx, err := p.sess.Spclient().ContextResolve(ctx, uri)
	if err != nil {
		return nil, fmt.Errorf("failed resolving context: %w", err)
	}

	tl, err := tracks.NewTrackListFromContext(ctx, p.app.log, p.sess.Spclient(), spotCtx)
	if err != nil {
		return nil, fmt.Errorf("failed building track list: %w", err)
	}

	var uris []string
	for _, t := range tl.AllTracks(ctx) {
		if strings.HasPrefix(t.Uri, "spotify:track:") {
			uris = append(uris, t.Uri)
		}
		if len(uris) == fullMetaFetchLimit {
			p.app.log.Debugf("context listing truncated to %d tracks: %s", fullMetaFetchLimit, uri)
			break
		}
	}

	p.contextLists.put(uri, uris, time.Now())
	return uris, nil
}

// scheduleContextEnumerate enumerates a context and sweeps metadata for its
// tracks, in the background. Nothing on the playback path waits for it: the
// listing endpoint answers from whatever is already enumerated and cached, and
// a caller that finds neither gets an empty, not-ready listing rather than a
// blocked control loop. Safe to call from the Run goroutine — it only claims
// the job and returns.
func (p *AppPlayer) scheduleContextEnumerate(contextUri string) {
	if p.metaCache == nil || p.sess == nil || contextUri == "" {
		return
	}
	// Already enumerated: the tracks are known, so go straight to the sweep —
	// it may have been aborted, or the listing may have been enumerated by a
	// caller that never swept it.
	if uris, ok := p.contextLists.get(contextUri); ok {
		p.scheduleMetaSweep(uris, contextUri)
		return
	}
	if !p.contextLists.beginFetch(contextUri) {
		return
	}

	go p.enumerateContext(contextUri)
}

// enumerateContext is the background half of scheduleContextEnumerate.
func (p *AppPlayer) enumerateContext(contextUri string) {
	defer p.contextLists.endFetch(contextUri)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	uris, err := p.resolveContextTracks(ctx, contextUri)
	if err != nil {
		p.app.log.WithError(err).Warnf("failed enumerating context: %s", contextUri)
		return
	}

	p.scheduleMetaSweep(uris, contextUri)
}

// scheduleContextMetaPrefetch warms the metadata of the WHOLE context that just
// started playing, so every track in it is known to /status
// (pending_track/next_track) before the user skips anywhere. Skipped when this
// context was already swept.
func (p *AppPlayer) scheduleContextMetaPrefetch(contextUri string) {
	if contextUri == "" || contextUri == p.lastFullMetaContext {
		return
	}

	p.lastFullMetaContext = contextUri
	p.scheduleContextEnumerate(contextUri)
}

type metaSweepJob struct {
	uris  []string
	label string
}

// metaSweepQueue serialises the paced full-context sweeps: one runs at a time,
// and one waits. A sweep takes seconds (batches are spaced to stay polite), so
// a second context arriving mid-sweep is common — switching contexts while one
// runs used to drop the new sweep on the floor, leaving that context with only
// the moving window and nothing to trigger a retry.
//
// The waiting slot holds one job because only the newest matters: if a third
// context arrives, the user has moved on from the second before its metadata
// could have been of any use.
type metaSweepQueue struct {
	mu      sync.Mutex
	running bool
	pending *metaSweepJob
}

// enqueue submits a job, reporting whether the caller must start the worker.
func (q *metaSweepQueue) enqueue(job metaSweepJob) bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.running {
		q.pending = &job
		return false
	}
	q.running = true
	q.pending = &job
	return true
}

// next hands the worker its next job, or reports that it should stop.
func (q *metaSweepQueue) next() (metaSweepJob, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.pending == nil {
		q.running = false
		return metaSweepJob{}, false
	}
	job := *q.pending
	q.pending = nil
	return job, true
}

// scheduleMetaSweep resolves metadata for the given track URIs in the
// background, in paced batches. Sweeps are serialised: one runs while at most
// one waits, and a job submitted while another is waiting replaces it.
func (p *AppPlayer) scheduleMetaSweep(uris []string, label string) {
	if p.metaCache == nil || p.sess == nil || len(uris) == 0 {
		return
	}

	if !p.metaSweeps.enqueue(metaSweepJob{uris: uris, label: label}) {
		return
	}

	go func() {
		for {
			job, ok := p.metaSweeps.next()
			if !ok {
				return
			}

			// Resolve what is missing when the job runs, not when it was
			// queued: a job that waited behind another sweep may have had
			// part of its tracks cached meanwhile.
			missing := p.metaCache.missing(job.uris)
			if len(missing) > fullMetaFetchLimit {
				missing = missing[:fullMetaFetchLimit]
			}
			if len(missing) == 0 {
				continue
			}

			func() {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
				defer cancel()

				p.sweepBatches(ctx, missing, job.label)
			}()
		}
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
