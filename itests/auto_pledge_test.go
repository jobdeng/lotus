package itests

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/filecoin-project/go-statestore"
	"github.com/filecoin-project/lotus/auto"
	sectorstorage "github.com/filecoin-project/lotus/extern/sector-storage"
	"github.com/filecoin-project/lotus/extern/sector-storage/fsutil"
	"github.com/filecoin-project/lotus/extern/sector-storage/sealtasks"
	"github.com/filecoin-project/lotus/extern/sector-storage/stores"
	"github.com/filecoin-project/lotus/extern/storage-sealing/sealiface"
	"github.com/filecoin-project/lotus/itests/kit"
	"github.com/filecoin-project/lotus/node"
	"github.com/filecoin-project/lotus/node/config"
	"github.com/filecoin-project/lotus/node/modules"
	"github.com/filecoin-project/lotus/node/modules/dtypes"
	"github.com/filecoin-project/lotus/node/modules/helpers"
	"github.com/filecoin-project/lotus/node/repo"
	minerstorage "github.com/filecoin-project/lotus/storage"
	"github.com/google/uuid"
	"github.com/ipfs/go-datastore"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func init() {
	_ = os.Setenv("FIL_PROOFS_USE_MULTICORE_SDR", "1")
	_ = os.Setenv("FIL_PROOFS_MULTICORE_SDR_PRODUCERS", "7")
}

func TestAutoSectorsPledge(t *testing.T) {
	var (
		endRunning = make(chan bool, 1)
		autoPledge = true
	)

	kit.QuietMiningLogs()

	blockTime := 50 * time.Millisecond

	extOpts := kit.ConstructorOpts(
		node.Override(new(stores.LocalStorage), func() stores.LocalStorage {
			return newTestStorage(t)
		}),
		node.ApplyIf(node.IsType(repo.StorageMiner),

			node.ApplyIf(func(s *node.Settings) bool { return autoPledge },
				// Auto Sectors Pledge
				node.Override(node.AutoSectorsPledgeKey, func(mctx helpers.MetricsCtx, lc fx.Lifecycle, miner *minerstorage.Miner, storageMgr *sectorstorage.Manager) (*auto.AutoSectorsPledge, error) {
					ctx := helpers.LifecycleCtx(mctx, lc)
					//添加workers
					err := addWorkers(t, ctx, storageMgr, 1, 1, 1, 1)
					if err != nil {
						return nil, err
					}
					return auto.NewAutoSectorsPledge(lc, miner, storageMgr)
				}),
			),
		),
		node.Override(new(sectorstorage.SealerConfig), func() sectorstorage.SealerConfig {
			scfg := config.DefaultStorageMiner()
			scfg.Storage.AllowAddPiece = false
			scfg.Storage.AllowPreCommit1 = false
			scfg.Storage.AllowPreCommit2 = false
			scfg.Storage.AllowCommit = false
			scfg.Storage.AllowUnseal = false
			scfg.Storage.ResourceFiltering = sectorstorage.ResourceFilteringDisabled
			return scfg.Storage
		}),

		node.Override(new(dtypes.GetSealingConfigFunc), func() (dtypes.GetSealingConfigFunc, error) {
			return func() (out sealiface.Config, err error) {
				scfg := config.DefaultStorageMiner()
				scfg.Sealing.BatchPreCommits = false
				scfg.Sealing.AggregateCommits = false
				out = modules.ToSealingConfig(scfg)
				return
			}, nil
		}),
	)

	_, _, ens := kit.EnsembleMinimal(t, extOpts)
	ens.InterconnectAll().BeginMining(blockTime)

	<-endRunning
}

func addWorkers(t *testing.T, ctx context.Context, m *sectorstorage.Manager, ap int, p1 int, p2 int, c2 int) error {

	localStore, err := stores.NewLocal(ctx, newTestSealing(t), m.GetIndex(), []string{})
	require.NoError(t, err)

	remote := stores.NewRemote(localStore, m.GetIndex(), nil, 1, &stores.DefaultPartialFileHandler{})

	requiredTasks := []sealtasks.TaskType{sealtasks.TTFetch, sealtasks.TTCommit1, sealtasks.TTFinalize}

	for i := 0; i < ap; i++ {
		worker := sectorstorage.NewLocalWorker(sectorstorage.WorkerConfig{TaskTypes: append(requiredTasks, sealtasks.TTAddPiece)},
			remote, localStore, m.GetIndex(), m, statestore.New(datastore.NewMapDatastore()), "AP-Worker")
		err := m.AddWorker(ctx, worker)
		if err != nil {
			return err
		}
	}

	for i := 0; i < p1; i++ {
		worker := sectorstorage.NewLocalWorker(sectorstorage.WorkerConfig{TaskTypes: append(requiredTasks, sealtasks.TTPreCommit1)},
			remote, localStore, m.GetIndex(), m, statestore.New(datastore.NewMapDatastore()), "P1-Worker")
		err := m.AddWorker(ctx, worker)
		if err != nil {
			return err
		}
	}

	for i := 0; i < p2; i++ {
		worker := sectorstorage.NewLocalWorker(sectorstorage.WorkerConfig{TaskTypes: append(requiredTasks, sealtasks.TTPreCommit2)},
			remote, localStore, m.GetIndex(), m, statestore.New(datastore.NewMapDatastore()), "P2-Worker")
		err := m.AddWorker(ctx, worker)
		if err != nil {
			return err
		}
	}

	for i := 0; i < c2; i++ {
		worker := sectorstorage.NewLocalWorker(sectorstorage.WorkerConfig{TaskTypes: append(requiredTasks, sealtasks.TTCommit2)},
			remote, localStore, m.GetIndex(), m, statestore.New(datastore.NewMapDatastore()), "C2-Worker")
		err := m.AddWorker(ctx, worker)
		if err != nil {
			return err
		}
	}

	return nil
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
			Weight:   10,
			CanSeal:  false,
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

func newTestSealing(t *testing.T) *testStorage {
	tp, err := ioutil.TempDir(os.TempDir(), "sector-sealing-test-")
	require.NoError(t, err)

	{
		b, err := json.MarshalIndent(&stores.LocalStorageMeta{
			ID:       stores.ID(uuid.New().String()),
			Weight:   10,
			CanSeal:  true,
			CanStore: false,
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
