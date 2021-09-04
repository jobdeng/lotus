package sectorstorage

import (
	"context"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/extern/sector-storage/sealtasks"
	"github.com/filecoin-project/lotus/extern/sector-storage/stores"
	"github.com/filecoin-project/lotus/extern/sector-storage/storiface"
	"golang.org/x/xerrors"
)

type allocExistingSelector struct {
	index  stores.SectorIndex
	sector abi.SectorID
	alloc  storiface.SectorFileType
	ptype  storiface.PathType
}

func newAllocExistingSelector(index stores.SectorIndex, sector abi.SectorID, alloc storiface.SectorFileType, ptype storiface.PathType) *allocExistingSelector {
	return &allocExistingSelector{
		index:  index,
		sector: sector,
		alloc:  alloc,
		ptype:  ptype,
	}
}

func (s *allocExistingSelector) Ok(ctx context.Context, task sealtasks.TaskType, spt abi.RegisteredSealProof, whnd *workerHandle) (bool, error) {
	tasks, err := whnd.workerRpc.TaskTypes(ctx)
	if err != nil {
		return false, xerrors.Errorf("getting supported worker task types: %w", err)
	}
	if _, supported := tasks[task]; !supported {
		return false, nil
	}

	paths, err := whnd.workerRpc.Paths(ctx)
	if err != nil {
		return false, xerrors.Errorf("getting worker paths: %w", err)
	}

	have := map[stores.ID]struct{}{}
	for _, path := range paths {
		have[path.ID] = struct{}{}
	}

	ssize, err := spt.SectorSize()
	if err != nil {
		return false, xerrors.Errorf("getting sector size: %w", err)
	}

	bestFind, err := s.index.StorageFindSector(ctx, s.sector, s.alloc, ssize, false)
	if err != nil {
		return false, xerrors.Errorf("finding best storage: %w", err)
	}

	bestAlloc, err := s.index.StorageBestAlloc(ctx, s.alloc, ssize, s.ptype)
	if err != nil {
		return false, xerrors.Errorf("finding best alloc storage: %w", err)
	}

	bestFindOK := false
	bestAllocOK := false

	for _, info := range bestFind {
		if _, ok := have[info.ID]; ok {
			bestFindOK = true
			break
			//return true, nil
		}
	}

	for _, info := range bestAlloc {
		if _, ok := have[info.ID]; ok {
			bestAllocOK = true
			break
			//return true, nil
		}
	}

	if bestFindOK && bestAllocOK {
		return true, nil
	}

	return false, nil
}

func (s *allocExistingSelector) Cmp(ctx context.Context, task sealtasks.TaskType, a, b *workerHandle) (bool, error) {
	return a.utilization() < b.utilization(), nil
}

var _ WorkerSelector = &allocExistingSelector{}
