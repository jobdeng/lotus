package testnode

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/filecoin-project/lotus/api/test"
	sectorstorage "github.com/filecoin-project/lotus/extern/sector-storage"
	"github.com/filecoin-project/lotus/extern/sector-storage/sealtasks"
	"github.com/filecoin-project/lotus/node/modules"
	"github.com/google/uuid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"
	logging "github.com/ipfs/go-log/v2"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/filecoin-project/go-statestore"
	"github.com/filecoin-project/lotus/extern/sector-storage/fsutil"
	"github.com/filecoin-project/lotus/extern/sector-storage/stores"
)

var log = logging.Logger("testnode")

func init() {
	logging.SetAllLoggers(logging.LevelDebug)
}

type testStorage stores.StorageConfig

func (t testStorage) DiskUsage(path string) (int64, error) {
	return 1, nil // close enough
}

func newTestStorage(t *testing.T) *testStorage {
	tp, err := ioutil.TempDir(os.TempDir(), "sector-storage-test-")
	require.NoError(t, err)

	{
		b, err := json.MarshalIndent(&stores.LocalStorageMeta{
			ID:       stores.ID(uuid.New().String()),
			Weight:   1,
			CanSeal:  true,
			CanStore: true,
		}, "", "  ")
		require.NoError(t, err)

		err = ioutil.WriteFile(filepath.Join(tp, "sectorstore.json"), b, 0644)
		require.NoError(t, err)
	}

	return &testStorage{
		StoragePaths: []stores.LocalPath{
			{Path: tp},
		},
	}
}

func (t testStorage) cleanup() {
	for _, path := range t.StoragePaths {
		if err := os.RemoveAll(path.Path); err != nil {
			fmt.Println("Cleanup error:", err)
		}
	}
}

func (t testStorage) GetStorage() (stores.StorageConfig, error) {
	return stores.StorageConfig(t), nil
}

func (t *testStorage) SetStorage(f func(*stores.StorageConfig)) error {
	f((*stores.StorageConfig)(t))
	return nil
}

func (t *testStorage) Stat(path string) (fsutil.FsStat, error) {
	return fsutil.Statfs(path)
}


func NewTestSectorMgr(ctx context.Context, t *testing.T, workers []test.WorkerSpec) *sectorstorage.Manager {
	st := newTestStorage(t)
	si := stores.NewIndex()
	ds := datastore.NewMapDatastore()

	wsts := statestore.New(namespace.Wrap(ds, modules.WorkerCallsPrefix))
	smsts := statestore.New(namespace.Wrap(ds, modules.ManagerWorkPrefix))

	m, err := sectorstorage.New(ctx, st, si, sectorstorage.SealerConfig{
		ParallelFetchLimit: 10,
		AllowAddPiece:      true,
		AllowPreCommit1:    true,
		AllowPreCommit2:    true,
		AllowCommit:        true,
		AllowUnseal:        true,
	}, nil, nil, wsts, smsts)
	require.NoError(t, err)

	if workers != nil {
		for _, w := range workers {
			err := m.AddWorker(ctx, newTestWorker(sectorstorage.WorkerConfig{
				TaskTypes: w.TaskTypes,
			}, m.GetLocalStore(), m, w.Name))
			require.NoError(t, err)
		}
	}

	return m
}

type workerSpec struct {
	name      string
	taskTypes []sealtasks.TaskType
}
