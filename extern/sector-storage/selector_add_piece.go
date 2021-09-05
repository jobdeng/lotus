package sectorstorage

import (
	"context"

	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-state-types/abi"

	"github.com/filecoin-project/lotus/extern/sector-storage/sealtasks"
	"github.com/filecoin-project/lotus/extern/sector-storage/stores"
	"github.com/filecoin-project/lotus/extern/sector-storage/storiface"
)

type addPieceSelector struct {
	index          stores.SectorIndex
	sector         abi.SectorID
	alloc          storiface.SectorFileType
	ptype          storiface.PathType
	existingPieces bool
}

func newAddPieceSelector(index stores.SectorIndex, sector abi.SectorID, alloc storiface.SectorFileType, ptype storiface.PathType, existingPieces bool) *addPieceSelector {
	return &addPieceSelector{
		index:          index,
		sector:         sector,
		alloc:          alloc,
		ptype:          ptype,
		existingPieces: existingPieces,
	}
}

func (s *addPieceSelector) Ok(ctx context.Context, task sealtasks.TaskType, spt abi.RegisteredSealProof, whnd *workerHandle) (bool, error) {
	// check hostname is equal ctx.Value("hostname")
	hostname := whnd.info.Hostname
	targetHostname, ok := ctx.Value("pledgeHostname").(string)

	if ok && len(targetHostname) > 0 {
		if hostname != targetHostname {
			return false, xerrors.Errorf("new sector expect hostname: %s, not equal: %s", targetHostname, hostname)
		}
	}

	tasks, err := whnd.workerRpc.TaskTypes(ctx) //worker是否支持任务类型
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

	if s.existingPieces {
		best, err := s.index.StorageFindSector(ctx, s.sector, s.alloc, ssize, false)
		if err != nil {
			return false, xerrors.Errorf("finding best storage: %w", err)
		}

		for _, info := range best {
			if _, ok := have[info.ID]; ok {
				return true, nil
			}
		}
	} else {
		best, err := s.index.StorageBestAlloc(ctx, s.alloc, ssize, s.ptype)
		if err != nil {
			return false, xerrors.Errorf("finding best alloc storage: %w", err)
		}

		for _, info := range best {
			if _, ok := have[info.ID]; ok {
				return true, nil
			}
		}
	}

	return false, nil
}

func (s *addPieceSelector) Cmp(ctx context.Context, task sealtasks.TaskType, a, b *workerHandle) (bool, error) {
	return a.utilization() < b.utilization(), nil
}

var _ WorkerSelector = &addPieceSelector{}
