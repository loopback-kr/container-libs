package storage

import (
	"bytes"
	"context"
	"sync"
	"testing"

	digest "github.com/opencontainers/go-digest"
	imgspecv1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.podman.io/image/v5/internal/blobinfocache"
	"go.podman.io/image/v5/internal/private"
	"go.podman.io/image/v5/pkg/blobinfocache/memory"
	"go.podman.io/image/v5/types"
	"go.podman.io/storage/pkg/archive"
)

// layerCommitEvents collects private.LayerCommitProgress reports.
type layerCommitEvents struct {
	mu     sync.Mutex
	events []private.LayerCommitProgress
}

func (e *layerCommitEvents) record(p private.LayerCommitProgress) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.events = append(e.events, p)
}

// forLayer returns the events reported for layerIndex.
func (e *layerCommitEvents) forLayer(layerIndex int) []private.LayerCommitProgress {
	e.mu.Lock()
	defer e.mu.Unlock()
	res := []private.LayerCommitProgress{}
	for _, ev := range e.events {
		if ev.LayerIndex == layerIndex {
			res = append(res, ev)
		}
	}
	return res
}

// assertLayerCommitReported checks that extracting a layer from a blob of blobSize was reported, followed by Done.
func assertLayerCommitReported(t *testing.T, events []private.LayerCommitProgress, blobSize int64) {
	t.Helper()
	require.NotEmpty(t, events)
	assert.Equal(t, private.LayerCommitProgress{LayerIndex: events[0].LayerIndex, Offset: 0, Size: blobSize}, events[0])
	for _, ev := range events[1 : len(events)-1] {
		assert.False(t, ev.Done)
		assert.Equal(t, blobSize, ev.Size)
		assert.LessOrEqual(t, ev.Offset, blobSize)
	}
	assert.True(t, events[len(events)-1].Done)
}

func TestLayerCommitProgressInOrder(t *testing.T) {
	ensureTestCanCreateImages(t)

	newStore(t)
	cache := memory.New()
	ref, err := Transport.ParseReference("test")
	require.NoError(t, err)

	layers := []testBlob{makeLayer(t, archive.Gzip), makeLayer(t, archive.Gzip)}
	unreportedLayer := makeLayer(t, archive.Gzip) // Committed after the callback is removed

	publicDest, err := ref.NewImageDestination(context.Background(), nil)
	require.NoError(t, err)
	defer publicDest.Close()
	dest, ok := publicDest.(private.ImageDestination)
	require.True(t, ok)
	reporter, ok := publicDest.(private.LayerCommitProgressReporter)
	require.True(t, ok)
	events := layerCommitEvents{}
	reporter.SetLayerCommitProgressCallback(events.record)

	// With the config known in advance, layers are committed as soon as they (and their parents) are received.
	diffIDs := []digest.Digest{}
	for _, l := range append(layers, unreportedLayer) {
		diffIDs = append(diffIDs, l.uncompressedDigest)
	}
	err = dest.NoteOriginalOCIConfig(&imgspecv1.Image{
		RootFS: imgspecv1.RootFS{Type: "layers", DiffIDs: diffIDs},
	}, nil)
	require.NoError(t, err)

	for i, l := range layers {
		_, err := dest.PutBlobWithOptions(context.Background(), bytes.NewReader(l.data), types.BlobInfo{Size: l.compressedSize, Digest: l.compressedDigest}, private.PutBlobOptions{
			Cache:      blobinfocache.FromBlobInfoCache(cache),
			LayerIndex: &i,
		})
		require.NoError(t, err)
		// The layer was committed within PutBlobWithOptions.
		assertLayerCommitReported(t, events.forLayer(i), l.compressedSize)
	}

	// Removing the callback stops reporting, including for layers committed later.
	reporter.SetLayerCommitProgressCallback(nil)
	reported := len(events.events)
	index := len(layers)
	_, err = dest.PutBlobWithOptions(context.Background(), bytes.NewReader(unreportedLayer.data), types.BlobInfo{Size: unreportedLayer.compressedSize, Digest: unreportedLayer.compressedDigest}, private.PutBlobOptions{
		Cache:      blobinfocache.FromBlobInfoCache(cache),
		LayerIndex: &index,
	})
	require.NoError(t, err)
	assert.Len(t, events.events, reported)
}

func TestLayerCommitProgressAtCommit(t *testing.T) {
	ensureTestCanCreateImages(t)

	newStore(t)
	cache := memory.New()
	ref, err := Transport.ParseReference("test")
	require.NoError(t, err)

	layers := []testBlob{makeLayer(t, archive.Gzip), makeLayer(t, archive.Gzip)}
	// createUncommittedImageDest uses the public PutBlob without layer indices, so layers are only committed in Commit().
	dest, unparsedToplevel := createUncommittedImageDest(t, ref, cache, layers, nil)
	defer dest.Close()
	reporter, ok := dest.(private.LayerCommitProgressReporter)
	require.True(t, ok)
	events := layerCommitEvents{}
	reporter.SetLayerCommitProgressCallback(events.record)

	err = dest.Commit(context.Background(), unparsedToplevel)
	require.NoError(t, err)
	for i, l := range layers {
		assertLayerCommitReported(t, events.forLayer(i), l.compressedSize)
	}
}
