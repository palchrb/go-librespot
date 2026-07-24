package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	librespot "github.com/devgianlu/go-librespot"
	metadatapb "github.com/devgianlu/go-librespot/proto/spotify/metadata"
	"github.com/rs/cors"
)

const timeout = 10 * time.Second

type ApiServer interface {
	Emit(ev *ApiEvent)
	Receive() <-chan ApiRequest
	Close() error
}

type ConcreteApiServer struct {
	log librespot.Logger

	allowOrigin string
	certFile    string
	keyFile     string

	close    bool
	listener net.Listener

	requests chan ApiRequest

	clients     []*websocket.Conn
	clientsLock sync.RWMutex
}

var (
	ErrNoSession        = errors.New("no session")
	ErrBadRequest       = errors.New("bad request")
	ErrForbidden        = errors.New("forbidden")
	ErrNotFound         = errors.New("not found")
	ErrMethodNotAllowed = errors.New("method not allowed")
	ErrTooManyRequests  = errors.New("the app has exceeded its rate limits")
)

type ApiRequestType string

const (
	ApiRequestTypeRoot                ApiRequestType = "root"
	ApiRequestTypeWebApi              ApiRequestType = "web_api"
	ApiRequestTypeStatus              ApiRequestType = "status"
	ApiRequestTypeResume              ApiRequestType = "resume"
	ApiRequestTypePause               ApiRequestType = "pause"
	ApiRequestTypePlayPause           ApiRequestType = "playpause"
	ApiRequestTypeSeek                ApiRequestType = "seek"
	ApiRequestTypePrev                ApiRequestType = "prev"
	ApiRequestTypeNext                ApiRequestType = "next"
	ApiRequestTypePlay                ApiRequestType = "play"
	ApiRequestTypeStop                ApiRequestType = "stop"
	ApiRequestTypeGetVolume           ApiRequestType = "get_volume"
	ApiRequestTypeSetVolume           ApiRequestType = "set_volume"
	ApiRequestTypeSetRepeatingContext ApiRequestType = "repeating_context"
	ApiRequestTypeSetRepeatingTrack   ApiRequestType = "repeating_track"
	ApiRequestTypeSetShufflingContext ApiRequestType = "shuffling_context"
	ApiRequestTypeAddToQueue          ApiRequestType = "add_to_queue"
	ApiRequestTypeToken               ApiRequestType = "token"
	ApiRequestSetDeviceName           ApiRequestType = "set_device_name"
	ApiRequestTypeCacheDownload       ApiRequestType = "cache_download"
	ApiRequestTypeCacheSnapshot       ApiRequestType = "cache_snapshot"
	ApiRequestTypeReopenOutput        ApiRequestType = "reopen_output"
	ApiRequestTypeContextTracks       ApiRequestType = "context_tracks"
)

type ApiEventType string

const (
	ApiEventTypePlaying        ApiEventType = "playing"
	ApiEventTypeNotPlaying     ApiEventType = "not_playing"
	ApiEventTypeWillPlay       ApiEventType = "will_play"
	ApiEventTypePaused         ApiEventType = "paused"
	ApiEventTypeActive         ApiEventType = "active"
	ApiEventTypeInactive       ApiEventType = "inactive"
	ApiEventTypeMetadata       ApiEventType = "metadata"
	ApiEventTypeVolume         ApiEventType = "volume"
	ApiEventTypeSeek           ApiEventType = "seek"
	ApiEventTypeStopped        ApiEventType = "stopped"
	ApiEventTypeRepeatTrack    ApiEventType = "repeat_track"
	ApiEventTypeRepeatContext  ApiEventType = "repeat_context"
	ApiEventTypeShuffleContext ApiEventType = "shuffle_context"
	ApiEventTypePlaybackReady  ApiEventType = "playback_ready"
)

type ApiRequest struct {
	Type ApiRequestType
	Data any

	resp chan apiResponse
}

func (r *ApiRequest) Reply(data any, err error) {
	r.resp <- apiResponse{data, err}
}

// NewApiRequest builds an ApiRequest pre-wired with a reply channel, plus a
// wait function that blocks until the daemon calls Reply (or ctx is done).
func NewApiRequest(t ApiRequestType, data any) (req ApiRequest, wait func(context.Context) (any, error)) {
	ch := make(chan apiResponse, 1)
	req = ApiRequest{Type: t, Data: data, resp: ch}
	wait = func(ctx context.Context) (any, error) {
		select {
		case r := <-ch:
			return r.data, r.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return
}

type ApiRequestDataSeek struct {
	Position int64 `json:"position"`
	Relative bool  `json:"relative"`
}

type ApiRequestDataVolume struct {
	Volume   int32 `json:"volume"`
	Relative bool  `json:"relative"`
}

type ApiRequestDataWebApi struct {
	Method string
	Path   string
	Query  url.Values
}

type ApiRequestDataPlay struct {
	Uri       string `json:"uri"`
	SkipToUri string `json:"skip_to_uri"`
	Paused    bool   `json:"paused"`
	// Position is the position in milliseconds to start playback at within the
	// selected track. Zero starts from the beginning.
	Position int64 `json:"position"`
}

type ApiRequestDataNext struct {
	Uri *string `json:"uri"`
}

type ApiRequestDataCacheDownload struct {
	Uri string `json:"uri"`
}

type ApiRequestDataCacheSnapshot struct {
	Uri string `json:"uri"`
}

type ApiResponseCacheSnapshot struct {
	// SnapshotId is the hex-encoded playlist revision, or null when the URI is
	// not a playlist (albums are immutable, other contexts have no snapshot).
	SnapshotId *string `json:"snapshot_id"`
	// Length is the number of tracks in the playlist, when available.
	Length *int32 `json:"length"`
}

type ApiRequestDataContextTracks struct {
	Uri string `json:"uri"`
}

// ApiResponseContextTrackItem is one entry of a context listing. For
// playlists, Track is null until the daemon's metadata cache knows the track;
// a background sweep is kicked off by the request, so re-polling fills the
// gaps. Album listings are always complete on the first call.
type ApiResponseContextTrackItem struct {
	Uri   string                  `json:"uri"`
	Track *ApiResponseStatusTrack `json:"track"`
}

type ApiResponseContextTracks struct {
	Uri string `json:"uri"`
	// SnapshotId is the hex-encoded playlist revision; clients can cache the
	// listing and skip re-fetching while it is unchanged. Null for albums,
	// which are immutable.
	SnapshotId *string `json:"snapshot_id"`
	// Length is the number of track entries in the listing.
	Length int `json:"length"`
	// Cached is how many entries carry full metadata; when Cached < Length a
	// background sweep is filling the rest — poll again shortly.
	Cached int                           `json:"cached"`
	Tracks []ApiResponseContextTrackItem `json:"tracks"`
}

type apiResponse struct {
	data any
	err  error
}

type ApiResponseStatusTrack struct {
	Uri           string   `json:"uri"`
	Name          string   `json:"name"`
	ArtistNames   []string `json:"artist_names"`
	AlbumName     string   `json:"album_name"`
	AlbumCoverUrl *string  `json:"album_cover_url"`
	Position      int64    `json:"position"`
	Duration      int      `json:"duration"`
	ReleaseDate   string   `json:"release_date"`
	TrackNumber   int      `json:"track_number"`
	DiscNumber    int      `json:"disc_number"`
}

func getBestImageIdForSize(images []*metadatapb.Image, size string) []byte {
	if len(images) == 0 {
		return nil
	}

	imageSize := metadatapb.Image_Size(metadatapb.Image_Size_value[strings.ToUpper(size)])

	dist := func(a metadatapb.Image_Size) int {
		diff := int(a) - int(imageSize)
		if diff < 0 {
			return -diff
		}
		return diff
	}

	// Find an image with the exact requested size.
	// If no exact match, return the closest image to the requested size.
	var bestImage *metadatapb.Image
	for _, img := range images {
		if img.Size == nil {
			continue
		}

		if *img.Size == imageSize {
			return img.FileId
		}

		// Find the image with the closest size. This logic works because the
		// metadatapb.Image_Size enum values are ordered from smallest to largest.
		if bestImage == nil || dist(*img.Size) < dist(*bestImage.Size) {
			bestImage = img
		}
	}

	if bestImage != nil {
		return bestImage.FileId
	}

	// Fallback to the first image if none have size information.
	return images[0].FileId
}

func (p *AppPlayer) newApiResponseStatusTrack(media *librespot.Media, position int64) *ApiResponseStatusTrack {
	if media.IsTrack() {
		track := media.Track()

		var artists []string
		for _, a := range track.Artist {
			artists = append(artists, *a.Name)
		}

		albumCoverId := getBestImageIdForSize(track.Album.Cover, p.app.cfg.ImageSize)
		if albumCoverId == nil && track.Album.CoverGroup != nil {
			albumCoverId = getBestImageIdForSize(track.Album.CoverGroup.Image, p.app.cfg.ImageSize)
		}

		return &ApiResponseStatusTrack{
			Uri:           librespot.SpotifyIdFromGid(librespot.SpotifyIdTypeTrack, track.Gid).Uri(),
			Name:          *track.Name,
			ArtistNames:   artists,
			AlbumName:     *track.Album.Name,
			AlbumCoverUrl: p.prodInfo.ImageUrl(albumCoverId),
			Position:      position,
			Duration:      int(*track.Duration),
			ReleaseDate:   track.Album.Date.String(),
			TrackNumber:   int(*track.Number),
			DiscNumber:    int(*track.DiscNumber),
		}
	} else {
		episode := media.Episode()

		albumCoverId := getBestImageIdForSize(episode.CoverImage.Image, p.app.cfg.ImageSize)

		return &ApiResponseStatusTrack{
			Uri:           librespot.SpotifyIdFromGid(librespot.SpotifyIdTypeEpisode, episode.Gid).Uri(),
			Name:          *episode.Name,
			ArtistNames:   []string{*episode.Show.Name},
			AlbumName:     *episode.Show.Name,
			AlbumCoverUrl: p.prodInfo.ImageUrl(albumCoverId),
			Position:      position,
			Duration:      int(*episode.Duration),
			ReleaseDate:   "",
			TrackNumber:   0,
			DiscNumber:    0,
		}
	}
}

type ApiResponseStatus struct {
	Username       string                  `json:"username"`
	DeviceId       string                  `json:"device_id"`
	DeviceType     string                  `json:"device_type"`
	DeviceName     string                  `json:"device_name"`
	PlayOrigin     string                  `json:"play_origin"`
	Stopped        bool                    `json:"stopped"`
	Paused         bool                    `json:"paused"`
	Buffering      bool                    `json:"buffering"`
	Volume         uint32                  `json:"volume"`
	VolumeSteps    uint32                  `json:"volume_steps"`
	RepeatContext  bool                    `json:"repeat_context"`
	RepeatTrack    bool                    `json:"repeat_track"`
	ShuffleContext bool                    `json:"shuffle_context"`
	Track          *ApiResponseStatusTrack `json:"track"`
	// PendingTrackUri is the track selected by a not-yet-settled skip (its load
	// is deferred while the user is still browsing); null otherwise. The track
	// object still describes the last loaded stream.
	PendingTrackUri *string `json:"pending_track_uri,omitempty"`
	// PendingTrack carries full metadata (name, artists, cover art) for the
	// pending track when it is known from the daemon's metadata cache, so
	// clients can display it while the load is still deferred.
	PendingTrack *ApiResponseStatusTrack `json:"pending_track,omitempty"`
	// NextTrack describes the upcoming track when its metadata is cached, so
	// clients can pre-warm name and cover art before the user skips to it.
	NextTrack *ApiResponseStatusTrack `json:"next_track,omitempty"`
}

type ApiResponseRoot struct {
	PlaybackReady bool `json:"playback_ready"`
}

type ApiResponseVolume struct {
	Value uint32 `json:"value"`
	Max   uint32 `json:"max"`
}

type ApiResponseToken struct {
	Token string `json:"token"`
}

type ApiEvent struct {
	Type ApiEventType `json:"type"`
	Data any          `json:"data"`
}

type ApiEventDataMetadata ApiResponseStatusTrack

type ApiEventDataVolume ApiResponseVolume

type ApiEventDataPlaying struct {
	ContextUri string `json:"context_uri"`
	Uri        string `json:"uri"`
	Resume     bool   `json:"resume"`
	PlayOrigin string `json:"play_origin"`
}

type ApiEventDataWillPlay struct {
	ContextUri string `json:"context_uri"`
	Uri        string `json:"uri"`
	PlayOrigin string `json:"play_origin"`
}

type ApiEventDataNotPlaying struct {
	ContextUri string `json:"context_uri"`
	Uri        string `json:"uri"`
	PlayOrigin string `json:"play_origin"`
}

type ApiEventDataPaused struct {
	ContextUri string `json:"context_uri"`
	Uri        string `json:"uri"`
	PlayOrigin string `json:"play_origin"`
}

type ApiEventDataStopped struct {
	PlayOrigin string `json:"play_origin"`
}

type ApiEventDataSeek struct {
	ContextUri string `json:"context_uri"`
	Uri        string `json:"uri"`
	Position   int    `json:"position"`
	Duration   int    `json:"duration"`
	PlayOrigin string `json:"play_origin"`
}

type ApiEventDataRepeatTrack struct {
	Value bool `json:"value"`
}

type ApiEventDataRepeatContext struct {
	Value bool `json:"value"`
}

type ApiEventDataShuffleContext struct {
	Value bool `json:"value"`
}

func NewApiServer(log librespot.Logger, address string, port int, allowOrigin string, certFile string, keyFile string) (_ ApiServer, err error) {
	s := &ConcreteApiServer{log: log, allowOrigin: allowOrigin, certFile: certFile, keyFile: keyFile}
	s.requests = make(chan ApiRequest)

	s.listener, err = net.Listen("tcp", fmt.Sprintf("%s:%d", address, port))
	if err != nil {
		return nil, fmt.Errorf("failed starting api listener: %w", err)
	}

	log.Infof("api server listening on %s", s.listener.Addr())

	go s.serve()
	return s, nil
}

type StubApiServer struct {
	log librespot.Logger
}

func NewStubApiServer(log librespot.Logger) (ApiServer, error) {
	return &StubApiServer{log: log}, nil
}

func (s *StubApiServer) Emit(ev *ApiEvent) {
	s.log.Tracef("voiding websocket event: %s", ev.Type)
}

func (s *StubApiServer) Receive() <-chan ApiRequest {
	return make(<-chan ApiRequest)
}

func (s *StubApiServer) Close() error {
	return nil
}

func (s *ConcreteApiServer) handleRequest(req ApiRequest, w http.ResponseWriter) {
	req.resp = make(chan apiResponse, 1)
	s.requests <- req
	resp := <-req.resp

	if resp.err != nil {
		switch {
		case errors.Is(resp.err, ErrNoSession):
			w.WriteHeader(http.StatusNoContent)
			return
		case errors.Is(resp.err, ErrForbidden):
			w.WriteHeader(http.StatusForbidden)
			return
		case errors.Is(resp.err, ErrNotFound):
			w.WriteHeader(http.StatusNotFound)
			return
		case errors.Is(resp.err, ErrMethodNotAllowed):
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		case errors.Is(resp.err, ErrTooManyRequests):
			w.WriteHeader(http.StatusTooManyRequests)
			return
		case errors.Is(resp.err, ErrBadRequest):
			w.WriteHeader(http.StatusBadRequest)
			return
		default:
			s.log.WithError(resp.err).Errorf("failed handling request %s", req.Type)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	}

	switch respData := resp.data.(type) {
	case []byte:
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write(respData)
	default:
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(respData)
	}
}

func jsonDecode(r *http.Request, v any) error {
	defer func() { _ = r.Body.Close() }()

	data, err := io.ReadAll(r.Body)
	if err != nil {
		return err
	} else if len(data) == 0 {
		return nil
	}

	return json.Unmarshal(data, v)
}

func (s *ConcreteApiServer) serve() {
	m := http.NewServeMux()
	m.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		s.handleRequest(ApiRequest{Type: ApiRequestTypeRoot}, w)
	})
	m.Handle("/web-api/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.handleRequest(ApiRequest{
			Type: ApiRequestTypeWebApi,
			Data: ApiRequestDataWebApi{
				Method: r.Method,
				Path:   strings.TrimPrefix(r.URL.Path, "/web-api/"),
				Query:  r.URL.Query(),
			},
		}, w)
	}))
	m.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		s.handleRequest(ApiRequest{Type: ApiRequestTypeStatus}, w)
	})
	m.HandleFunc("/player/play", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		var data ApiRequestDataPlay
		if err := jsonDecode(r, &data); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		if len(data.Uri) == 0 {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		s.handleRequest(ApiRequest{Type: ApiRequestTypePlay, Data: data}, w)
	})
	m.HandleFunc("/cache/download", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		var data ApiRequestDataCacheDownload
		if err := jsonDecode(r, &data); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		if len(data.Uri) == 0 {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		s.handleRequest(ApiRequest{Type: ApiRequestTypeCacheDownload, Data: data}, w)
	})
	m.HandleFunc("/cache/snapshot", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		uri := r.URL.Query().Get("uri")
		if len(uri) == 0 {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		s.handleRequest(ApiRequest{Type: ApiRequestTypeCacheSnapshot, Data: ApiRequestDataCacheSnapshot{Uri: uri}}, w)
	})
	contextTracksHandler := func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		uri := r.URL.Query().Get("uri")
		if len(uri) == 0 {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		s.handleRequest(ApiRequest{Type: ApiRequestTypeContextTracks, Data: ApiRequestDataContextTracks{Uri: uri}}, w)
	}
	m.HandleFunc("/context/tracks", contextTracksHandler)
	// Deprecated alias from when the listing was playlist-only.
	m.HandleFunc("/playlist/tracks", contextTracksHandler)
	m.HandleFunc("/player/resume", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		s.handleRequest(ApiRequest{Type: ApiRequestTypeResume}, w)
	})
	m.HandleFunc("/player/pause", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		s.handleRequest(ApiRequest{Type: ApiRequestTypePause}, w)
	})
	m.HandleFunc("/player/stop", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		s.handleRequest(ApiRequest{Type: ApiRequestTypeStop}, w)
	})
	m.HandleFunc("/player/playpause", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		s.handleRequest(ApiRequest{Type: ApiRequestTypePlayPause}, w)
	})
	m.HandleFunc("/player/next", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		var data ApiRequestDataNext
		if err := jsonDecode(r, &data); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		s.handleRequest(ApiRequest{Type: ApiRequestTypeNext, Data: data}, w)
	})
	m.HandleFunc("/player/prev", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		s.handleRequest(ApiRequest{Type: ApiRequestTypePrev}, w)
	})
	m.HandleFunc("/player/seek", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		var data ApiRequestDataSeek
		if err := jsonDecode(r, &data); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		if !data.Relative && data.Position < 0 {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		s.handleRequest(ApiRequest{Type: ApiRequestTypeSeek, Data: data}, w)
	})
	m.HandleFunc("/player/volume", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			s.handleRequest(ApiRequest{Type: ApiRequestTypeGetVolume}, w)
		} else if r.Method == "POST" {
			var data ApiRequestDataVolume
			if err := jsonDecode(r, &data); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			if !data.Relative && data.Volume < 0 {
				w.WriteHeader(http.StatusBadRequest)
				return
			}

			s.handleRequest(ApiRequest{Type: ApiRequestTypeSetVolume, Data: data}, w)
		} else {
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})
	m.HandleFunc("/player/repeat_context", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		var data struct {
			Repeat bool `json:"repeat_context"`
		}
		if err := jsonDecode(r, &data); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		s.handleRequest(ApiRequest{Type: ApiRequestTypeSetRepeatingContext, Data: data.Repeat}, w)
	})
	m.HandleFunc("/player/repeat_track", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		var data struct {
			Repeat bool `json:"repeat_track"`
		}
		if err := jsonDecode(r, &data); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		s.handleRequest(ApiRequest{Type: ApiRequestTypeSetRepeatingTrack, Data: data.Repeat}, w)
	})
	m.HandleFunc("/player/shuffle_context", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		var data struct {
			Shuffle bool `json:"shuffle_context"`
		}
		if err := jsonDecode(r, &data); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		s.handleRequest(ApiRequest{Type: ApiRequestTypeSetShufflingContext, Data: data.Shuffle}, w)
	})
	m.HandleFunc("/player/add_to_queue", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		var data struct {
			Uri string `json:"uri"`
		}
		if err := jsonDecode(r, &data); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		if len(data.Uri) == 0 {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		s.handleRequest(ApiRequest{Type: ApiRequestTypeAddToQueue, Data: data.Uri}, w)
	})
	m.HandleFunc("/token", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		s.handleRequest(ApiRequest{Type: ApiRequestTypeToken}, w)
	})
	m.HandleFunc("/set_device_name", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		var data struct {
			Name string `json:"name"`
		}
		if err := jsonDecode(r, &data); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		if len(data.Name) == 0 {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		s.handleRequest(ApiRequest{Type: ApiRequestSetDeviceName, Data: data.Name}, w)
	})
	m.HandleFunc("/player/output", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		var data struct {
			Device string `json:"device"`
		}
		if err := jsonDecode(r, &data); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		s.handleRequest(ApiRequest{Type: ApiRequestTypeReopenOutput, Data: data.Device}, w)
	})
	m.HandleFunc("/events", func(w http.ResponseWriter, r *http.Request) {
		opts := &websocket.AcceptOptions{}
		if len(s.allowOrigin) > 0 {
			allow := s.allowOrigin
			allow = strings.TrimPrefix(allow, "http://")
			allow = strings.TrimPrefix(allow, "https://")
			allow = strings.TrimSuffix(allow, "/")
			opts.OriginPatterns = []string{allow}
		}

		c, err := websocket.Accept(w, r, opts)
		if err != nil {
			s.log.WithError(err).Error("failed accepting websocket connection")
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		// add the client to the list
		s.clientsLock.Lock()
		s.clients = append(s.clients, c)
		s.clientsLock.Unlock()

		s.log.Debugf("new websocket client")

		for {
			_, _, err := c.Read(context.Background())
			if s.close {
				return
			} else if err != nil {
				s.log.WithError(err).Error("websocket connection errored")

				// remove the client from the list
				s.clientsLock.Lock()
				for i, cc := range s.clients {
					if cc == c {
						s.clients = append(s.clients[:i], s.clients[i+1:]...)
						break
					}
				}
				s.clientsLock.Unlock()
				return
			}
		}
	})

	c := cors.New(cors.Options{
		AllowedOrigins:      []string{s.allowOrigin},
		AllowPrivateNetwork: true,
		AllowCredentials:    true,
	})

	var err error
	if len(s.certFile) > 0 && len(s.keyFile) > 0 {
		err = http.ServeTLS(s.listener, c.Handler(m), s.certFile, s.keyFile)
	} else {
		err = http.Serve(s.listener, c.Handler(m))
	}

	if s.close {
		return
	} else if err != nil {
		s.log.WithError(err).Error("failed serving api")
		_ = s.Close()
	}
}

func (s *ConcreteApiServer) Emit(ev *ApiEvent) {
	s.clientsLock.RLock()
	defer s.clientsLock.RUnlock()

	s.log.Tracef("emitting websocket event: %s", ev.Type)

	for _, client := range s.clients {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		err := wsjson.Write(ctx, client, ev)
		cancel()
		if err != nil {
			// purposely do not propagate this to the caller
			s.log.WithError(err).Error("failed communicating with websocket client")
		}
	}
}

func (s *ConcreteApiServer) Receive() <-chan ApiRequest {
	return s.requests
}

func (s *ConcreteApiServer) Close() error {
	s.close = true

	// close all websocket clients
	s.clientsLock.RLock()
	for _, client := range s.clients {
		_ = client.Close(websocket.StatusGoingAway, "")
	}
	s.clientsLock.RUnlock()

	// close the listener
	_ = s.listener.Close()
	return nil
}
