//go:build test_unit

package daemon

import (
	"context"
	"errors"
	"io"
	"testing"

	librespot "github.com/devgianlu/go-librespot"
	connectpb "github.com/devgianlu/go-librespot/proto/spotify/connectstate"
	"github.com/devgianlu/go-librespot/tracks"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// newPrecacheTestPlayer builds a player with the metadata feature off — the
// state a headless box pre-caching audio is in — whose context resolver is
// resolve.
func newPrecacheTestPlayer(t *testing.T, resolve func(context.Context, string) (tracks.ContextResolver, error)) *AppPlayer {
	t.Helper()

	app := &App{log: &librespot.NullLogger{}, cfg: &Config{}}
	require.Nil(t, app.metaCache, "pre-caching must not need the metadata caches")
	require.Nil(t, app.contextLists)

	return &AppPlayer{app: app, ctx: t.Context(), precacheResolve: resolve}
}

func staticResolver(resolver tracks.ContextResolver) func(context.Context, string) (tracks.ContextResolver, error) {
	return func(context.Context, string) (tracks.ContextResolver, error) { return resolver, nil }
}

// A playlist worth pre-caching is longer than one page, and AllTracks only ever
// returns the resident ones: the run walks every page to the end.
func TestPrecacheUrisPagesToTheEnd(t *testing.T) {
	resolver := tracks.NewMockContextResolver(t)
	resolver.EXPECT().Type().Return(librespot.SpotifyIdTypeTrack)
	for page := range 4 {
		items := []*connectpb.ContextTrack{
			contextTrack(trackUri(byte(2*page+1)), nil),
			contextTrack("", gid(byte(2*page+2))),
		}
		resolver.EXPECT().Page(mock.Anything, page).Return(items, nil)
	}
	resolver.EXPECT().Page(mock.Anything, 4).Return(nil, io.EOF)

	p := newPrecacheTestPlayer(t, staticResolver(resolver))

	uris, err := p.precacheUris(t.Context(), "spotify:playlist:a")
	require.NoError(t, err)

	var want []string
	for i := byte(1); i <= 8; i++ {
		want = append(want, trackUri(i))
	}
	require.Equal(t, want, uris, "every page, in the context's own order")
	require.Nil(t, p.meta, "no metadata fetcher was constructed")
}

// The enumeration is bounded the same way the metadata listing is, so a
// generated context cannot download for ever.
func TestPrecacheUrisRespectsTheTrackCap(t *testing.T) {
	resolver := tracks.NewMockContextResolver(t)
	resolver.EXPECT().Type().Return(librespot.SpotifyIdTypeTrack)
	resolver.EXPECT().Page(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, page int) ([]*connectpb.ContextTrack, error) {
		require.Less(t, page, maxContextPages, "the page cap still applies")
		return []*connectpb.ContextTrack{contextTrack("", gid(byte(page)))}, nil
	})

	p := newPrecacheTestPlayer(t, staticResolver(resolver))
	p.app.cfg.Cache.Download.MaxTracks = 3

	uris, err := p.precacheUris(t.Context(), "spotify:playlist:a")
	require.NoError(t, err)
	require.Equal(t, []string{trackUri(0), trackUri(1), trackUri(2)}, uris)
}

// Local files and malformed entries carry nothing to download, so they are left
// out rather than failing a track each.
func TestPrecacheUrisSkipsUndownloadableEntries(t *testing.T) {
	resolver := tracks.NewMockContextResolver(t)
	resolver.EXPECT().Type().Return(librespot.SpotifyIdTypeTrack)
	resolver.EXPECT().Page(mock.Anything, 0).Return([]*connectpb.ContextTrack{
		contextTrack(trackUri(0x01), nil),
		contextTrack("spotify:local:a:b:c:1", nil),
		contextTrack(episodeUri(0x02), nil),
		contextTrack("", []byte{0x01, 0x02}),
	}, nil)
	resolver.EXPECT().Page(mock.Anything, 1).Return(nil, io.EOF)

	p := newPrecacheTestPlayer(t, staticResolver(resolver))

	uris, err := p.precacheUris(t.Context(), "spotify:playlist:a")
	require.NoError(t, err)
	require.Equal(t, []string{trackUri(0x01), episodeUri(0x02)}, uris)
}

// A context that will not resolve is reported, not pre-cached from a stale or
// empty listing.
func TestPrecacheUrisReportsResolveFailure(t *testing.T) {
	boom := errors.New("boom")

	p := newPrecacheTestPlayer(t, func(context.Context, string) (tracks.ContextResolver, error) {
		return nil, boom
	})

	_, err := p.precacheUris(t.Context(), "spotify:playlist:a")
	require.ErrorIs(t, err, boom)
}

// Pre-caching without an audio cache would fail a track at a time against
// Spotify; it gives up before the first request instead.
func TestCacheContextWithoutAudioCache(t *testing.T) {
	p := newPrecacheTestPlayer(t, func(context.Context, string) (tracks.ContextResolver, error) {
		t.Fatal("the context must not be resolved with the cache disabled")
		return nil, nil
	})

	p.cacheContext(t.Context(), "spotify:playlist:a")
}
