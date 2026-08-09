package daemon

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	connectpb "github.com/devgianlu/go-librespot/proto/spotify/connectstate"
	"github.com/devgianlu/go-librespot/spclient"
)

// connectTransferTimeout bounds the transfer POST. It runs inline on the
// player's goroutine (that is what makes the settle interaction below safe to
// order), so it must stay far below both the 30s API budget and the dealer's
// 40s pong window — a hanging call that long would stall every button on the
// device and can cascade into a dealer reconnect.
const connectTransferTimeout = 8 * time.Second

// clusterToApiDevices maps a cluster snapshot to the API device listing. The
// daemon's own device is upserted from its live DeviceInfo, so the listing is
// never empty and always carries the local name/volume — even before any
// cluster has been seen. Output is self-first then by name: Cluster.Device is
// a map, and a UI wants a stable order.
func clusterToApiDevices(cluster *connectpb.Cluster, self *connectpb.DeviceInfo, selfId string, updatedAt time.Time) *ApiConnectDevices {
	resp := &ApiConnectDevices{Devices: []ApiConnectDevice{}}

	if !updatedAt.IsZero() {
		ms := updatedAt.UnixMilli()
		resp.UpdatedAt = &ms
	}
	if active := cluster.GetActiveDeviceId(); active != "" {
		resp.ActiveDeviceId = &active
	}

	toApi := func(id string, info *connectpb.DeviceInfo) ApiConnectDevice {
		return ApiConnectDevice{
			Id:          id,
			Name:        info.GetName(),
			Type:        info.GetDeviceType().String(),
			Brand:       info.GetBrand(),
			Model:       info.GetModel(),
			Active:      id == cluster.GetActiveDeviceId(),
			Self:        id == selfId,
			CanPlay:     info.GetCanPlay(),
			Volume:      info.GetVolume(),
			VolumeSteps: info.GetCapabilities().GetVolumeSteps(),
		}
	}

	for id, info := range cluster.GetDevice() {
		if info == nil || id == selfId {
			continue
		}
		resp.Devices = append(resp.Devices, toApi(id, info))
	}
	if self != nil {
		resp.Devices = append(resp.Devices, toApi(selfId, self))
	}

	sort.Slice(resp.Devices, func(i, j int) bool {
		if resp.Devices[i].Self != resp.Devices[j].Self {
			return resp.Devices[i].Self
		}
		return resp.Devices[i].Name < resp.Devices[j].Name
	})

	return resp
}

func (p *AppPlayer) apiConnectDevices() *ApiConnectDevices {
	return clusterToApiDevices(p.state.lastCluster, p.state.device, p.app.deviceId, p.state.lastClusterAt)
}

// transferSource picks the {from} device for a transfer. Sending away moves
// our own session; pulling home (target == self) moves the session of
// whichever device the cluster says is active.
func transferSource(cluster *connectpb.Cluster, selfId, target string) string {
	if target == selfId {
		return cluster.GetActiveDeviceId()
	}
	return selfId
}

// validateTransferTarget applies the local rejections that must happen before
// any network call.
//
// Sending away (target elsewhere) requires us to be the active player: the
// from/to semantics of moving a third device's session are unverified, and
// the worst plausible interpretation moves someone else's playback.
//
// Pulling home (target == self) is the mirror: it requires another device to
// be the active player — with nothing playing anywhere there is nothing to
// pull, and with ourselves active the service would bounce a transfer command
// straight back at us, forcing a full reload of the very stream that is
// playing.
//
// An unknown target is a lookup miss, not a bad request.
func validateTransferTarget(cluster *connectpb.Cluster, selfId, target string, active bool) error {
	if target == selfId {
		if active || cluster.GetActiveDeviceId() == "" || cluster.GetActiveDeviceId() == selfId {
			return ErrBadRequest
		}
		return nil
	}
	if !active {
		return ErrBadRequest
	}
	if cluster == nil || cluster.Device[target] == nil {
		return ErrNotFound
	}
	return nil
}

func (p *AppPlayer) apiConnectTransfer(ctx context.Context, data ApiConnectTransfer) error {
	if err := validateTransferTarget(p.state.lastCluster, p.app.deviceId, data.DeviceId, p.state.active && p.primaryStream != nil); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, connectTransferTimeout)
	defer cancel()

	err := p.sess.Spclient().ConnectTransfer(ctx,
		transferSource(p.state.lastCluster, p.app.deviceId, data.DeviceId), data.DeviceId, data.RestorePaused)

	var rateLimited *spclient.RateLimitedError
	if errors.As(err, &rateLimited) {
		return ErrTooManyRequests
	}
	if err != nil {
		return fmt.Errorf("failed transferring playback: %w", err)
	}

	// Only after Spotify accepted the request: abandon any deferred skip, so
	// the settle timer cannot fire a pointless load (audio key included) for
	// a session that is leaving. Never before the POST — a failed transfer
	// with the settle cancelled would strand clients on a forever-buffering
	// pending track. The actual local stop is driven by the cluster update,
	// exactly like a phone-initiated transfer away.
	p.cancelSettle()

	return nil
}
