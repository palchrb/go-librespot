//go:build test_unit

package daemon

import (
	"testing"

	librespot "github.com/devgianlu/go-librespot"
	"github.com/stretchr/testify/require"
)

func snapshotRequest(uri string) ApiRequest {
	return ApiRequest{Type: ApiRequestTypeCacheSnapshot, Data: ApiRequestDataCacheSnapshot{Uri: uri}}
}

// Only playlists carry a revision. Anything else is answered from the uri
// alone, without a request to Spotify, so a client polling a whole library
// costs nothing for the entries that can never change under it.
func TestCacheSnapshotNullForNonPlaylist(t *testing.T) {
	p := &AppPlayer{app: &App{log: &librespot.NullLogger{}, cfg: &Config{}}, ctx: t.Context()}

	for _, uri := range []string{trackUri(0x01), episodeUri(0x02), "spotify:album:2rsrNHPkQg4UCYLU3CGMLN"} {
		resp, err := p.handleApiRequest(snapshotRequest(uri))
		require.NoError(t, err)
		require.Equal(t, &ApiCacheSnapshot{}, resp, "no snapshot and no length for %s", uri)
	}
}

// A uri that is not a Spotify uri at all is a client bug, not an empty answer.
func TestCacheSnapshotRejectsInvalidUri(t *testing.T) {
	p := &AppPlayer{app: &App{log: &librespot.NullLogger{}, cfg: &Config{}}, ctx: t.Context()}

	for _, uri := range []string{"", "not-a-uri", "spotify:playlist:", "https://open.spotify.com/playlist/a"} {
		_, err := p.handleApiRequest(snapshotRequest(uri))
		require.ErrorIs(t, err, ErrBadRequest, "uri %q", uri)
	}
}
