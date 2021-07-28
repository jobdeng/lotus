package sectorstorage

import (
	"context"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/extern/sector-storage/sealtasks"
	"github.com/filecoin-project/lotus/extern/sector-storage/stores"
	"github.com/filecoin-project/lotus/extern/sector-storage/storiface"
	"golang.org/x/xerrors"
)

type SealTaskStatus struct {
	Task sealtasks.TaskType
	Completed bool
}

type fixSelector struct {
	index  stores.SectorIndex
	alloc  storiface.SectorFileType
	ptype  storiface.PathType
	sector abi.SectorID
}

func newFixSelector(index stores.SectorIndex, sector abi.SectorID, alloc storiface.SectorFileType, ptype storiface.PathType) *fixSelector {
	return &fixSelector{
		index:  index,
		sector: sector,
		alloc:  alloc,
		ptype:  ptype,
	}
}

func (s *fixSelector) Ok(ctx context.Context, task sealtasks.TaskType, spt abi.RegisteredSealProof, whnd *workerHandle) (bool, error) {

	tasks, err := whnd.workerRpc.TaskTypes(ctx) //worker是否支持任务类型
	if err != nil {
		return false, xerrors.Errorf("getting supported worker task types: %w", err)
	}
	if _, supported := tasks[task]; !supported {
		return false, nil
	}

	//processTask := whnd.sectorProcessStatus[s.sector]
	//wid, _ := whnd.workerRpc.Session(ctx)
	//
	//if processTask != nil {
	//	log.Debugf("task: %s selector workerid: %v", task, wid)
	//	log.Debugf("worker processTask: %v, completed: %v", processTask.Task, processTask.Completed)
	//}

	switch task {
	case sealtasks.TTPreCommit1:
		// worker有在做P1，没有完成就不接新的P1任务
		for _, status := range whnd.sectorProcessStatus {
			if status.Task == sealtasks.TTPreCommit1 {
				if !status.Completed {
					return false, nil
				}
			}
		}
	default:
		break
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

	best, err := s.index.StorageBestAlloc(ctx, s.alloc, ssize, s.ptype)
	if err != nil {
		return false, xerrors.Errorf("finding best alloc storage: %w", err)
	}

	for _, info := range best {
		if _, ok := have[info.ID]; ok {
			return true, nil
		}
	}

	return false, nil
}

func (s *fixSelector) Cmp(ctx context.Context, task sealtasks.TaskType, a, b *workerHandle) (bool, error) {
	return a.utilization() < b.utilization(), nil
}

var _ WorkerSelector = &fixSelector{}
