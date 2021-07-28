package testnode

import (
	"context"
	"github.com/filecoin-project/go-state-types/abi"
	sectorstorage "github.com/filecoin-project/lotus/extern/sector-storage"
	"github.com/filecoin-project/lotus/extern/sector-storage/mock"
	"github.com/filecoin-project/lotus/extern/sector-storage/sealtasks"
	"github.com/filecoin-project/lotus/extern/sector-storage/stores"
	"github.com/filecoin-project/lotus/extern/sector-storage/storiface"
	"github.com/filecoin-project/specs-storage/storage"
	"github.com/google/uuid"
	"golang.org/x/xerrors"
)

type testWorker struct {
	name        string
	acceptTasks map[sealtasks.TaskType]struct{}
	lstor       *stores.Local
	ret         storiface.WorkerReturn

	mockSeal *mock.SectorMgr

	session uuid.UUID

	sectorstorage.Worker
}

func newTestWorker(wcfg sectorstorage.WorkerConfig, lstor *stores.Local, ret storiface.WorkerReturn, name string, sectorMgr *mock.SectorMgr) *testWorker {
	acceptTasks := map[sealtasks.TaskType]struct{}{}
	for _, taskType := range wcfg.TaskTypes {
		acceptTasks[taskType] = struct{}{}
	}

	return &testWorker{
		name:        name,
		acceptTasks: acceptTasks,
		lstor:       lstor,
		ret:         ret,

		mockSeal: sectorMgr,

		session: uuid.New(),
	}
}

func toCallError(err error) *storiface.CallError {
	var serr *storiface.CallError
	if err != nil && !xerrors.As(err, &serr) {
		serr = storiface.Err(storiface.ErrUnknown, err)
	}

	return serr
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
	log.Infof("worker name: %s", t.name)
	return t.asyncCall(sector, func(ci storiface.CallID) {
		p, err := t.mockSeal.AddPiece(ctx, sector, pieceSizes, newPieceSize, pieceData)
		if err := t.ret.ReturnAddPiece(ctx, ci, p, toCallError(err)); err != nil {
			log.Error(err)
		}
	})
}

func (t *testWorker) SealPreCommit1(ctx context.Context, sector storage.SectorRef, ticket abi.SealRandomness, pieces []abi.PieceInfo) (storiface.CallID, error) {
	log.Infof("worker name: %s", t.name)
	return t.asyncCall(sector, func(ci storiface.CallID) {
		p1o, err := t.mockSeal.SealPreCommit1(ctx, sector, ticket, pieces)
		if err := t.ret.ReturnSealPreCommit1(ctx, ci, p1o, toCallError(err)); err != nil {
			log.Error(err)
		}
	})
}

func (t *testWorker) SealPreCommit2(ctx context.Context, sector storage.SectorRef, pc1o storage.PreCommit1Out) (storiface.CallID, error) {
	log.Infof("worker name: %s", t.name)
	return t.asyncCall(sector, func(ci storiface.CallID) {
		p2o, err := t.mockSeal.SealPreCommit2(ctx, sector, pc1o)
		if err := t.ret.ReturnSealPreCommit2(ctx, ci, p2o, toCallError(err)); err != nil {
			log.Error(err)
		}
	})
}

func (t *testWorker) Fetch(ctx context.Context, sector storage.SectorRef, fileType storiface.SectorFileType, ptype storiface.PathType, am storiface.AcquireMode) (storiface.CallID, error) {
	log.Infof("worker name: %s", t.name)
	return t.asyncCall(sector, func(ci storiface.CallID) {
		if err := t.ret.ReturnFetch(ctx, ci, nil); err != nil {
			log.Error(err)
		}
	})
}

func (t *testWorker) SealCommit1(ctx context.Context, sector storage.SectorRef, ticket abi.SealRandomness, seed abi.InteractiveSealRandomness, pieces []abi.PieceInfo, cids storage.SectorCids) (storiface.CallID, error) {
	log.Infof("worker name: %s", t.name)
	return t.asyncCall(sector, func(ci storiface.CallID) {
		p, err := t.mockSeal.SealCommit1(ctx, sector, ticket, seed, pieces, cids)
		if err := t.ret.ReturnSealCommit1(ctx, ci, p, toCallError(err)); err != nil {
			log.Error(err)
		}
	})
}

func (t *testWorker) SealCommit2(ctx context.Context, sector storage.SectorRef, c1o storage.Commit1Out) (storiface.CallID, error) {
	log.Infof("worker name: %s", t.name)
	return t.asyncCall(sector, func(ci storiface.CallID) {
		p, err := t.mockSeal.SealCommit2(ctx, sector, c1o)
		if err := t.ret.ReturnSealCommit2(ctx, ci, p, toCallError(err)); err != nil {
			log.Error(err)
		}
	})
}

func (t *testWorker) FinalizeSector(ctx context.Context, sector storage.SectorRef, keepUnsealed []storage.Range) (storiface.CallID, error) {
	log.Infof("worker name: %s", t.name)
	return t.asyncCall(sector, func(ci storiface.CallID) {
		err := t.mockSeal.FinalizeSector(ctx, sector, keepUnsealed)
		if err := t.ret.ReturnFinalizeSector(ctx, ci, toCallError(err)); err != nil {
			log.Error(err)
		}
	})
}

func (t *testWorker) ReleaseUnsealed(ctx context.Context, sector storage.SectorRef, safeToFree []storage.Range) (storiface.CallID, error) {
	log.Infof("worker name: %s", t.name)
	return t.asyncCall(sector, func(ci storiface.CallID) {
		err := t.mockSeal.ReleaseUnsealed(ctx, sector, safeToFree)
		if err := t.ret.ReturnReleaseUnsealed(ctx, ci, toCallError(err)); err != nil {
			log.Error(err)
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
	}, nil
}

func (t *testWorker) Session(context.Context) (uuid.UUID, error) {
	return t.session, nil
}

func (t *testWorker) Close() error {
	panic("implement me")
}

var _ sectorstorage.Worker = &testWorker{}
