package daemon

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	librespot "github.com/devgianlu/go-librespot"
	"github.com/devgianlu/go-librespot/audio"
	connectpb "github.com/devgianlu/go-librespot/proto/spotify/connectstate"
	"github.com/devgianlu/go-librespot/tracks"
)

// recordingApiServer is an ApiServer that records emitted events, so tests can
// assert on the event sequence without a websocket.
type recordingApiServer struct {
	mu     sync.Mutex
	events []ApiEventType
}

func (s *recordingApiServer) Emit(ev *ApiEvent) {
	s.mu.Lock()
	s.events = append(s.events, ev.Type)
	s.mu.Unlock()
}

func (s *recordingApiServer) Receive() <-chan ApiRequest { return make(<-chan ApiRequest) }
func (s *recordingApiServer) Close() error               { return nil }

func (s *recordingApiServer) snapshot() []ApiEventType {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]ApiEventType(nil), s.events...)
}

// newSettleTestPlayer builds the minimal AppPlayer needed by the settle
// bookkeeping helpers (shouldDeferSkip, deferSettle, settleNow's no-context
// path, cancelSettle): config, recording event server, state and stopped
// timers. lastStatePut is set to now so updateState defers its PUT to the
// state timer instead of reaching for a live session.
func newSettleTestPlayer(t *testing.T, debounce time.Duration) (*AppPlayer, *recordingApiServer) {
	t.Helper()

	server := &recordingApiServer{}
	p := &AppPlayer{
		app: &App{
			cfg:    &Config{SkipDebounce: debounce},
			server: server,
			log:    &librespot.NullLogger{},
		},
		state: &State{
			player: &connectpb.PlayerState{
				PlayOrigin: &connectpb.PlayOrigin{FeatureIdentifier: "go-librespot"},
				Track:      &connectpb.ProvidedTrack{Uri: "spotify:track:pending"},
			},
		},
		lastStatePut: time.Now(),
	}

	p.settleTimer = time.NewTimer(math.MaxInt64)
	p.settleTimer.Stop()
	p.stateTimer = time.NewTimer(math.MaxInt64)
	p.stateTimer.Stop()

	return p, server
}

func TestShouldDeferSkip(t *testing.T) {
	cases := []struct {
		name          string
		debounce      time.Duration
		hasContext    bool
		settlePending bool
		lastSkipDone  time.Time
		want          bool
	}{
		{"disabled never defers", 0, true, true, time.Now(), false},
		{"no context never defers", 400 * time.Millisecond, false, true, time.Now(), false},
		{"first skip is immediate", 400 * time.Millisecond, true, false, time.Time{}, false},
		{"skip after quiet period is immediate", 400 * time.Millisecond, true, false, time.Now().Add(-time.Second), false},
		{"pending settle defers", 400 * time.Millisecond, true, true, time.Time{}, true},
		{"skip right after previous defers", 400 * time.Millisecond, true, false, time.Now(), true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, _ := newSettleTestPlayer(t, tc.debounce)
			if tc.hasContext {
				p.state.tracks = &tracks.List{}
			}
			p.settlePending = tc.settlePending
			p.lastSkipDone = tc.lastSkipDone

			if got := p.shouldDeferSkip(); got != tc.want {
				t.Fatalf("shouldDeferSkip() = %t, want %t", got, tc.want)
			}
		})
	}
}

func TestDeferSettlePublishesPendingTrack(t *testing.T) {
	p, server := newSettleTestPlayer(t, 400*time.Millisecond)

	p.deferSettle(context.Background())

	if !p.settlePending {
		t.Fatal("expected settlePending after deferSettle")
	}
	if !p.state.player.IsPlaying || !p.state.player.IsBuffering {
		t.Fatalf("expected the pending track to be published as playing+buffering, got playing=%t buffering=%t",
			p.state.player.IsPlaying, p.state.player.IsBuffering)
	}
	if time.Since(p.lastSkipDone) > time.Second {
		t.Fatal("expected lastSkipDone to be refreshed")
	}

	events := server.snapshot()
	if len(events) != 1 || events[0] != ApiEventTypeWillPlay {
		t.Fatalf("expected exactly one will_play event, got %v", events)
	}
}

func TestSettleNowWithoutContextIsNoop(t *testing.T) {
	p, server := newSettleTestPlayer(t, 400*time.Millisecond)
	p.settlePending = true
	p.settleAtEnd = false

	if err := p.settleNow(context.Background()); err != nil {
		t.Fatalf("settleNow without context failed: %v", err)
	}
	if p.settlePending || p.settleAtEnd {
		t.Fatal("expected settle flags cleared")
	}
	if events := server.snapshot(); len(events) != 0 {
		t.Fatalf("expected no events, got %v", events)
	}
}

// TestSettleNowAtEndWithoutContext locks in the no-context end-of-context
// path: a deferred skip past the end with no track list must not panic and
// must tell clients playback stopped.
func TestSettleNowAtEndWithoutContext(t *testing.T) {
	p, server := newSettleTestPlayer(t, 400*time.Millisecond)
	p.state.player.Track = nil // fresh state: no track ever loaded
	p.settlePending = true
	p.settleAtEnd = true

	if err := p.settleNow(context.Background()); err != nil {
		t.Fatalf("settleNow at end without context failed: %v", err)
	}
	if p.settlePending || p.settleAtEnd {
		t.Fatal("expected settle flags cleared")
	}

	events := server.snapshot()
	if len(events) != 1 || events[0] != ApiEventTypeStopped {
		t.Fatalf("expected exactly one stopped event, got %v", events)
	}
}

func TestCancelSettleClearsEverything(t *testing.T) {
	p, _ := newSettleTestPlayer(t, 400*time.Millisecond)
	p.settlePending = true
	p.settleAtEnd = true
	p.keyRetries = 1
	p.settleTimer.Reset(time.Hour)

	p.cancelSettle()

	if p.settlePending || p.settleAtEnd || p.keyRetries != 0 {
		t.Fatalf("expected all settle state cleared, got pending=%t atEnd=%t retries=%d",
			p.settlePending, p.settleAtEnd, p.keyRetries)
	}
}

// TestThrottledKeyErrorDetection locks in the circuit-breaker classification:
// a KeyProviderError with the throttled code must be recognized through the
// wrapping applied by the stream-load path, and other codes must not match.
func TestThrottledKeyErrorDetection(t *testing.T) {
	wrap := func(err error) error {
		// Mirrors NewStream's wrapping: "failed retrieving audio key: %w",
		// then loadCurrentTrack's "failed creating stream for %s: %w".
		return fmt.Errorf("failed creating stream for x: %w", fmt.Errorf("failed retrieving audio key: %w", err))
	}

	var keyErr *audio.KeyProviderError
	if err := wrap(&audio.KeyProviderError{Code: audioKeyThrottledCode}); !errors.As(err, &keyErr) || keyErr.Code != audioKeyThrottledCode {
		t.Fatalf("expected throttled key error to be detected through wrapping, got %v", err)
	}

	keyErr = nil
	if err := wrap(&audio.KeyProviderError{Code: 1}); !errors.As(err, &keyErr) || keyErr.Code == audioKeyThrottledCode {
		t.Fatal("expected code 1 to be a key error but not classified as throttled")
	}

	keyErr = nil
	if err := wrap(errors.New("plain failure")); errors.As(err, &keyErr) {
		t.Fatal("expected a non-key error not to match")
	}
}
