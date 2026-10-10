package copy

import (
	"bytes"
	"io"
	"testing"
	"time"

	digest "github.com/opencontainers/go-digest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vbauerster/mpb/v8"
	"go.podman.io/image/v5/internal/private"
	"go.podman.io/image/v5/types"
)

var testLayerBlobInfo = types.BlobInfo{
	Digest: digest.Digest("sha256:" + "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"),
	Size:   100,
}

// waitForPool fails the test if pool.Wait() does not return, i.e. if some bar was left incomplete.
func waitForPool(t *testing.T, pool *mpb.Progress) {
	done := make(chan struct{})
	go func() {
		pool.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("progress pool did not finish; a progress bar was left incomplete")
	}
}

// eventually waits for an asynchronous mpb state update.
func eventually(t *testing.T, cond func() bool) {
	assert.Eventually(t, cond, 5*time.Second, 10*time.Millisecond)
}

func TestLayerCommitTrackerCommittedAfterBlobReceived(t *testing.T) {
	pool := mpb.New(mpb.WithOutput(io.Discard))
	tracker := newLayerCommitTracker(&copier{reportWriter: io.Discard})

	bar, err := tracker.createProgressBar(pool, testLayerBlobInfo, 0)
	require.NoError(t, err)
	bar.SetCurrent(testLayerBlobInfo.Size) // The whole blob has been received…
	eventually(t, func() bool { return bar.Current() == testLayerBlobInfo.Size })
	assert.False(t, bar.Completed()) // … but that does not complete the bar.

	require.True(t, tracker.blobReceived(0))
	tl := tracker.layers[0]
	assert.Equal(t, layerPhaseWaiting, layerCommitPhase(tl.phase.Load()))

	tracker.report(private.LayerCommitProgress{LayerIndex: 0, Offset: 25, Size: 50})
	assert.Equal(t, layerPhaseCommitting, layerCommitPhase(tl.phase.Load()))
	eventually(t, func() bool { return bar.Current() == 50 }) // Scaled to the blob size
	assert.False(t, bar.Completed())

	tracker.report(private.LayerCommitProgress{LayerIndex: 0, Size: -1, Done: true})
	eventually(t, bar.Completed)

	tracker.finish()
	waitForPool(t, pool)
}

func TestLayerCommitTrackerCommittedBeforeBlobReceived(t *testing.T) {
	pool := mpb.New(mpb.WithOutput(io.Discard))
	tracker := newLayerCommitTracker(&copier{reportWriter: io.Discard})

	bar, err := tracker.createProgressBar(pool, testLayerBlobInfo, 3)
	require.NoError(t, err)
	bar.SetCurrent(testLayerBlobInfo.Size)

	// The layer was committed within the blob copy, which has not returned yet (e.g. because it is committing later layers).
	tracker.report(private.LayerCommitProgress{LayerIndex: 3, Offset: 0, Size: 100})
	tracker.report(private.LayerCommitProgress{LayerIndex: 3, Size: -1, Done: true})
	eventually(t, bar.Completed) // The bar is completed right away…

	require.True(t, tracker.blobReceived(3)) // … and the caller must still not abort it.
	assert.True(t, bar.Completed())

	tracker.finish()
	waitForPool(t, pool)
}

func TestLayerCommitTrackerFinish(t *testing.T) {
	pool := mpb.New(mpb.WithOutput(io.Discard))
	tracker := newLayerCommitTracker(&copier{reportWriter: io.Discard})

	committedLater, err := tracker.createProgressBar(pool, testLayerBlobInfo, 0)
	require.NoError(t, err)
	committedLater.SetCurrent(testLayerBlobInfo.Size)
	require.True(t, tracker.blobReceived(0))

	failed, err := tracker.createProgressBar(pool, testLayerBlobInfo, 1)
	require.NoError(t, err)

	// A layer which is never reported as committed (e.g. committed only when the image is committed)
	// must still have its bar completed by finish().
	tracker.finish()
	eventually(t, committedLater.Completed)

	// Bars which were not handed over to the tracker remain the caller's responsibility.
	assert.False(t, failed.Completed())
	failed.Abort(false)

	// Reports after finish() are ignored.
	tracker.report(private.LayerCommitProgress{LayerIndex: 0, Offset: 10, Size: 100})
	tracker.report(private.LayerCommitProgress{LayerIndex: 1, Size: -1, Done: true})
	waitForPool(t, pool)
	assert.False(t, failed.Completed())
}

func TestLayerCommitTrackerBlobReceivedAfterFinish(t *testing.T) {
	pool := mpb.New(mpb.WithOutput(io.Discard))
	tracker := newLayerCommitTracker(&copier{reportWriter: io.Discard})

	bar, err := tracker.createProgressBar(pool, testLayerBlobInfo, 0)
	require.NoError(t, err)
	tracker.finish()
	// The bar can not be completed by mark100PercentComplete, so the tracker must still take it over.
	require.True(t, tracker.blobReceived(0))
	eventually(t, bar.Completed)
	waitForPool(t, pool)
}

func TestLayerCommitTrackerWithoutBars(t *testing.T) {
	var out bytes.Buffer
	tracker := newLayerCommitTracker(&copier{reportWriter: &out})
	tracker.registerWithoutBar(testLayerBlobInfo, 2)

	// Without a bar, the caller completes its own (discarded) bar.
	assert.False(t, tracker.blobReceived(2))

	tracker.report(private.LayerCommitProgress{LayerIndex: 2, Offset: 0, Size: 100})
	tracker.report(private.LayerCommitProgress{LayerIndex: 2, Offset: 50, Size: 100})
	tracker.report(private.LayerCommitProgress{LayerIndex: 2, Size: -1, Done: true})
	// Unknown layers (e.g. reused ones) are ignored.
	tracker.report(private.LayerCommitProgress{LayerIndex: 7, Offset: 0, Size: 100})
	tracker.finish()

	assert.Equal(t, "Extracting blob "+testLayerBlobInfo.Digest.String()+"\n", out.String())
}
