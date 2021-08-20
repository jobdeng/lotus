package itests

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/auto"
	sectorstorage "github.com/filecoin-project/lotus/extern/sector-storage"
	"github.com/filecoin-project/lotus/extern/sector-storage/fsutil"
	"github.com/filecoin-project/lotus/extern/sector-storage/mock"
	"github.com/filecoin-project/lotus/extern/sector-storage/sealtasks"
	"github.com/filecoin-project/lotus/extern/sector-storage/stores"
	"github.com/filecoin-project/lotus/extern/sector-storage/storiface"
	"github.com/filecoin-project/lotus/itests/kit"
	"github.com/filecoin-project/lotus/node"
	"github.com/filecoin-project/lotus/node/config"
	"github.com/filecoin-project/lotus/node/modules/helpers"
	"github.com/filecoin-project/lotus/node/repo"
	minerstorage "github.com/filecoin-project/lotus/storage"
	"github.com/filecoin-project/specs-storage/storage"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
	"golang.org/x/xerrors"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

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
					err := addWorkers(ctx, storageMgr, 1, 1, 1, 1)
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
	)

	_, _, ens := kit.EnsembleMinimal(t, extOpts)
	ens.InterconnectAll().BeginMining(blockTime)

	<-endRunning
}

func addWorkers(ctx context.Context, m *sectorstorage.Manager, ap int, p1 int, p2 int, c2 int) error {

	mockSeal := mock.NewMockSectorMgr(nil)
	for i := 0; i < ap; i++ {
		err := m.AddWorker(ctx, newTestWorker(
			sectorstorage.WorkerConfig{
				TaskTypes: []sealtasks.TaskType{sealtasks.TTAddPiece},
			},
			m.GetLocalStore(), m, mockSeal))
		if err != nil {
			return err
		}
	}

	for i := 0; i < p1; i++ {
		err := m.AddWorker(ctx, newTestWorker(
			sectorstorage.WorkerConfig{
				TaskTypes: []sealtasks.TaskType{sealtasks.TTPreCommit1},
			},
			m.GetLocalStore(), m, mockSeal))
		if err != nil {
			return err
		}
	}

	for i := 0; i < p2; i++ {
		err := m.AddWorker(ctx, newTestWorker(
			sectorstorage.WorkerConfig{
				TaskTypes: []sealtasks.TaskType{sealtasks.TTPreCommit2},
			},
			m.GetLocalStore(), m, mockSeal))
		if err != nil {
			return err
		}
	}

	for i := 0; i < c2; i++ {
		err := m.AddWorker(ctx, newTestWorker(
			sectorstorage.WorkerConfig{
				TaskTypes: []sealtasks.TaskType{sealtasks.TTCommit2},
			},
			m.GetLocalStore(), m, mockSeal))
		if err != nil {
			return err
		}
	}

	return nil
}

type testWorker struct {
	acceptTasks map[sealtasks.TaskType]struct{}
	lstor       *stores.Local
	ret         storiface.WorkerReturn

	mockSeal *mock.SectorMgr

	pc1s    int
	pc1lk   sync.Mutex
	pc1wait *sync.WaitGroup

	session uuid.UUID

	sectorstorage.Worker
}

func newTestWorker(wcfg sectorstorage.WorkerConfig, lstor *stores.Local, ret storiface.WorkerReturn, mockSeal *mock.SectorMgr) *testWorker {
	acceptTasks := map[sealtasks.TaskType]struct{}{}
	for _, taskType := range wcfg.TaskTypes {
		acceptTasks[taskType] = struct{}{}
	}

	return &testWorker{
		acceptTasks: acceptTasks,
		lstor:       lstor,
		ret:         ret,

		mockSeal: mockSeal,

		session: uuid.New(),
	}
}

func (t *testWorker) asyncCall(sector storage.SectorRef, work func(ci storiface.CallID)) (storiface.CallID, error) {
	ci := storiface.CallID{
		Sector: sector.ID,
		ID:     uuid.New(),
	}

	go work(ci)

	return ci, nil
}

func (t *testWorker) AddPiece(ctx context.Context, sector storage.SectorRef, pieceSizes []abi.UnpaddedPieceSize, newPieceSize abi.UnpaddedPieceSize, pieceData storage.Data) (storiface.CallID, error) {
	return t.asyncCall(sector, func(ci storiface.CallID) {
		p, err := t.mockSeal.AddPiece(ctx, sector, pieceSizes, newPieceSize, pieceData)
		if err := t.ret.ReturnAddPiece(ctx, ci, p, toCallError(err)); err != nil {
			//log.Error(err)
		}
	})
}

func (t *testWorker) SealPreCommit1(ctx context.Context, sector storage.SectorRef, ticket abi.SealRandomness, pieces []abi.PieceInfo) (storiface.CallID, error) {
	return t.asyncCall(sector, func(ci storiface.CallID) {
		t.pc1s++

		if t.pc1wait != nil {
			t.pc1wait.Done()
		}

		t.pc1lk.Lock()
		defer t.pc1lk.Unlock()
		//time.Sleep(1 * time.Minute)
		p1o, err := t.mockSeal.SealPreCommit1(ctx, sector, ticket, pieces)
		if err := t.ret.ReturnSealPreCommit1(ctx, ci, p1o, toCallError(err)); err != nil {
			//log.Error(err)
		}
	})
}

func (t *testWorker) SealPreCommit2(ctx context.Context, sector storage.SectorRef, pc1o storage.PreCommit1Out) (storiface.CallID, error) {
	return t.asyncCall(sector, func(ci storiface.CallID) {
		p2o, err := t.mockSeal.SealPreCommit2(ctx, sector, pc1o)
		if err := t.ret.ReturnSealPreCommit2(ctx, ci, p2o, toCallError(err)); err != nil {
			//log.Error(err)
		}
	})
}

func (t *testWorker) Fetch(ctx context.Context, sector storage.SectorRef, fileType storiface.SectorFileType, ptype storiface.PathType, am storiface.AcquireMode) (storiface.CallID, error) {
	return t.asyncCall(sector, func(ci storiface.CallID) {
		if err := t.ret.ReturnFetch(ctx, ci, nil); err != nil {
			//log.Error(err)
		}
	})
}

func (t *testWorker) TaskTypes(ctx context.Context) (map[sealtasks.TaskType]struct{}, error) {
	return t.acceptTasks, nil
}

func (t *testWorker) Paths(ctx context.Context) ([]stores.StoragePath, error) {
	return t.lstor.Local(ctx)
}

func (t *testWorker) Info(ctx context.Context) (storiface.WorkerInfo, error) {
	res := sectorstorage.ResourceTable[sealtasks.TTPreCommit2][abi.RegisteredSealProof_StackedDrg2KiBV1]

	return storiface.WorkerInfo{
		Hostname: "testworkerer",
		Resources: storiface.WorkerResources{
			MemPhysical: res.MinMemory * 3,
			MemSwap:     0,
			MemReserved: res.MinMemory,
			CPUs:        32,
			GPUs:        nil,
		},
		AcceptTasks: t.acceptTasks,
	}, nil
}

func (t *testWorker) Session(context.Context) (uuid.UUID, error) {
	return t.session, nil
}

func (t *testWorker) Close() error {
	panic("implement me")
}

func toCallError(err error) *storiface.CallError {
	var serr *storiface.CallError
	if err != nil && !xerrors.As(err, &serr) {
		serr = storiface.Err(storiface.ErrUnknown, err)
	}

	return serr
}

var _ sectorstorage.Worker = &testWorker{}

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
