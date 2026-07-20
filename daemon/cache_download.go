package daemon

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	librespot "github.com/devgianlu/go-librespot"
	"github.com/devgianlu/go-librespot/tracks"
	"golang.org/x/exp/rand"
)

// cacheDownloadFailBreaker aborts the whole run after this many consecutive
// failures — the signature of rate-limiting or a dropped connection.
const cacheDownloadFailBreaker = 4

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

	spotCtx, err := p.sess.Spclient().ContextResolve(ctx, uri)
	if err != nil {
		log.WithError(err).Warnf("failed resolving context for pre-caching")
		return
	}

	tl, err := tracks.NewTrackListFromContext(ctx, p.app.log, p.sess.Spclient(), spotCtx)
	if err != nil {
		log.WithError(err).Warnf("failed building track list for pre-caching")
		return
	}

	all := tl.AllTracks(ctx)
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

	for i, pt := range all {
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

			spotId, err := librespot.SpotifyIdFromUri(trackUri)
			if err != nil {
				return
			}
			if spotId.Type() != librespot.SpotifyIdTypeTrack && spotId.Type() != librespot.SpotifyIdTypeEpisode {
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
		}(pt.Uri)
	}

	wg.Wait()
	log.Infof("pre-caching finished: %d/%d cached", cached.Load(), len(all))
}
