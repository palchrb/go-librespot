package daemon

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	librespot "github.com/devgianlu/go-librespot"
	"golang.org/x/exp/rand"
)

// cacheDownloadFailBreaker aborts the whole run after this many consecutive
// failures — the signature of rate-limiting or a dropped connection.
const cacheDownloadFailBreaker = 4

// precacheTimeout bounds one pre-cache run: enumerating the context and
// downloading everything in it, at the pace cache.download asks for.
const precacheTimeout = 2 * time.Hour

// precacheUris enumerates the tracks and episodes of a context to pre-cache, in
// its own order. The resolver is one of its own rather than the playing track
// list's: that one mutates its pages as it walks and is reachable only from the
// loader lane.
func (p *AppPlayer) precacheUris(ctx context.Context, uri string) ([]string, error) {
	resolver, err := p.precacheResolve(ctx, uri)
	if err != nil {
		return nil, fmt.Errorf("failed resolving context: %w", err)
	}

	// Bounded because a context longer than the cache holds is not what
	// pre-caching is for: its tail would only evict the head it just
	// downloaded.
	return enumerateContextTracks(ctx, resolver, p.precacheMaxTracks())
}

// defaultPrecacheMaxTracks caps a pre-cache when cache.download.max_tracks is
// not set.
const defaultPrecacheMaxTracks = 800

// precacheMaxTracks returns the configured pre-cache cap.
func (p *AppPlayer) precacheMaxTracks() int {
	if n := p.app.cfg.Cache.Download.MaxTracks; n > 0 {
		return n
	}
	return defaultPrecacheMaxTracks
}

// cacheContext downloads every track of the given context (playlist, album,
// artist or a single track/episode) into the cache, without playing it.
//
// It is deliberately paced so a bulk download does not look like abuse to
// Spotify: a small bounded concurrency, a jittered delay between track starts,
// and a circuit breaker that aborts the run after a streak of consecutive
// failures. Already-cached tracks are skipped by CacheTrack, so re-running a
// context is cheap.
func (p *AppPlayer) cacheContext(ctx context.Context, uri string) {
	log := p.app.log.WithField("uri", uri)

	if p.app.audioCache == nil {
		log.Warnf("cannot pre-cache with the audio cache disabled")
		return
	}

	all, err := p.precacheUris(ctx, uri)
	if err != nil {
		log.WithError(err).Warnf("failed enumerating context for pre-caching")
		return
	}
	if len(all) == 0 {
		log.Warnf("no tracks to pre-cache")
		return
	}

	log.Infof("pre-caching %d track(s)", len(all))

	// Pacing (configurable via cache.download.*), with safe fallbacks.
	concurrency := p.app.cfg.Cache.Download.Concurrency
	if concurrency < 1 {
		concurrency = 1
	}
	minDelay := p.app.cfg.Cache.Download.MinDelay
	jitter := p.app.cfg.Cache.Download.Jitter

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		sem        = make(chan struct{}, concurrency)
		wg         sync.WaitGroup
		failStreak atomic.Int32
		cached     atomic.Int32
	)

	for i, trackUri := range all {
		if ctx.Err() != nil {
			break
		}

		// Pace the dispatches: spread the control-plane calls out.
		if i > 0 {
			delay := minDelay
			if jitter > 0 {
				delay += time.Duration(rand.Int63n(int64(jitter)))
			}
			if delay > 0 {
				select {
				case <-ctx.Done():
				case <-time.After(delay):
				}
			}
		}

		select {
		case <-ctx.Done():
		case sem <- struct{}{}:
		}
		if ctx.Err() != nil {
			break
		}

		wg.Add(1)
		go func(trackUri string) {
			defer wg.Done()
			defer func() { <-sem }()

			// The enumeration only yields tracks and episodes, so anything that
			// does not parse here is a malformed uri off the wire.
			spotId, err := librespot.SpotifyIdFromUri(trackUri)
			if err != nil {
				return
			}

			if err := p.player.CacheTrack(ctx, p.app.client, *spotId, p.app.cfg.Bitrate); err != nil {
				log.WithError(err).WithField("track", trackUri).Warnf("failed pre-caching track")
				if failStreak.Add(1) >= cacheDownloadFailBreaker {
					log.Warnf("too many consecutive failures — aborting pre-cache run")
					cancel()
				}
				return
			}
			failStreak.Store(0)
			cached.Add(1)
		}(trackUri)
	}

	wg.Wait()
	log.Infof("pre-caching finished: %d/%d cached", cached.Load(), len(all))
}
