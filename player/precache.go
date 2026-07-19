package player

import (
	"context"
	"fmt"
	"io"
	"net/http"

	librespot "github.com/devgianlu/go-librespot"
	"github.com/devgianlu/go-librespot/audio"
	downloadpb "github.com/devgianlu/go-librespot/proto/spotify/download"
	extmetadatapb "github.com/devgianlu/go-librespot/proto/spotify/extendedmetadata"
	audiofilespb "github.com/devgianlu/go-librespot/proto/spotify/extendedmetadata/audiofiles"
	metadatapb "github.com/devgianlu/go-librespot/proto/spotify/metadata"
)

// CacheTrack downloads the encrypted audio file for the given track or episode
// into the cache without playing it. It resolves the metadata and the best
// available format exactly like NewStream, then downloads the raw (still
// encrypted) bytes straight into the cache. No audio key is requested and no
// decoding is performed: caching stores the encrypted file, so decryption only
// happens later, at playback time.
//
// It is a no-op (returning nil) when the cache is disabled or the file is
// already cached.
func (p *Player) CacheTrack(ctx context.Context, client *http.Client, spotId librespot.SpotifyId, bitrate int) error {
	if p.cache == nil {
		return fmt.Errorf("cache is not enabled")
	}

	log := p.log.WithField("uri", spotId.Uri())

	var file *metadatapb.AudioFile
	switch spotId.Type() {
	case librespot.SpotifyIdTypeTrack:
		trackMeta, err := p.getUnrestrictedTrack(ctx, spotId)
		if err != nil {
			return err
		}

		spotId = librespot.NewMediaFromTrack(trackMeta).Id()

		var audioFilesResp audiofilespb.AudioFilesExtensionResponse
		if err := p.sp.ExtendedMetadataSimple(ctx, spotId, extmetadatapb.ExtensionKind_AUDIO_FILES, &audioFilesResp); err != nil {
			return fmt.Errorf("failed getting audio files metadata: %w", err)
		}

		var audioFiles []*metadatapb.AudioFile
		for _, f := range audioFilesResp.Files {
			audioFiles = append(audioFiles, f.File)
		}

		file = selectBestMediaFormat(audioFiles, bitrate, p.flacEnabled)
	case librespot.SpotifyIdTypeEpisode:
		var episodeMeta metadatapb.Episode
		if err := p.sp.ExtendedMetadataSimple(ctx, spotId, extmetadatapb.ExtensionKind_EPISODE_V4, &episodeMeta); err != nil {
			return fmt.Errorf("failed getting episode metadata: %w", err)
		}

		if isMediaRestricted(librespot.NewMediaFromEpisode(&episodeMeta), *p.countryCode) {
			return librespot.ErrMediaRestricted
		}

		file = selectBestMediaFormat(episodeMeta.Audio, bitrate, p.flacEnabled)
	default:
		return fmt.Errorf("unsupported spotify type: %s", spotId.Type())
	}

	if file == nil {
		return librespot.ErrNoSupportedFormats
	}

	// Already cached: nothing to do (and no request is made to Spotify).
	if cached, ok := p.cache.File(file.FileId); ok {
		_ = cached.(io.Closer).Close()
		log.Debugf("file %x already cached", file.FileId)
		return nil
	}

	// Use the prefetch storage-resolve endpoint: this is the "downloading
	// ahead" signal Spotify's own clients use, rather than the interactive one.
	storageResolve, err := p.sp.ResolveStorageInteractive(ctx, file.FileId, file.Format, true)
	if err != nil {
		return fmt.Errorf("failed resolving track storage: %w", err)
	}

	if storageResolve.Result != downloadpb.StorageResolveResponse_CDN {
		return fmt.Errorf("unsupported storage resolve result: %s", storageResolve.Result)
	}

	// Build the reader directly from the CDN urls. Unlike NewStream this does
	// not touch the player's cdnQuarantine map, so pre-caching can safely run
	// concurrently with playback.
	var raw *audio.HttpChunkedReader
	var rerr error
	for _, cdnUrl := range storageResolve.Cdnurl {
		raw, rerr = audio.NewHttpChunkedReader(log, client, cdnUrl)
		if rerr == nil {
			break
		}
		log.WithError(rerr).Warnf("failed creating chunked reader for cdn url, trying next")
	}
	if raw == nil {
		return fmt.Errorf("failed creating chunked reader for any cdn url: %w", rerr)
	}
	defer func() { _ = raw.Close() }()

	// Reading the whole reader forces every chunk to download; the encrypted
	// bytes are written straight to the cache.
	if err := p.cache.SaveFile(file.FileId, io.NewSectionReader(raw, 0, raw.Size())); err != nil {
		return fmt.Errorf("failed caching file: %w", err)
	}

	log.Debugf("pre-cached file %x (%d bytes)", file.FileId, raw.Size())
	return nil
}
