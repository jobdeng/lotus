package auto

import (
	"context"
	"fmt"
	"github.com/filecoin-project/lotus/extern/sector-storage"
	"github.com/filecoin-project/lotus/extern/sector-storage/sealtasks"
	"github.com/filecoin-project/lotus/extern/sector-storage/storiface"
	"github.com/filecoin-project/lotus/storage"
	"github.com/google/uuid"
	logging "github.com/ipfs/go-log/v2"
	"go.uber.org/fx"
	"time"
)

var log = logging.Logger("auto")

var AutoSectorsPledgeInterval = 10 * time.Second

type AutoSectorsPledge struct {
	storageMgr     *sectorstorage.Manager
	miner          *storage.Miner
	heartbeatTimer *time.Ticker
}

func NewAutoSectorsPledge(lc fx.Lifecycle, miner *storage.Miner, storageMgr *sectorstorage.Manager) (*AutoSectorsPledge, error) {
	asp := &AutoSectorsPledge{
		storageMgr:     storageMgr,
		miner:          miner,
		heartbeatTimer: time.NewTicker(AutoSectorsPledgeInterval),
	}
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			go asp.runAutoSectorsPledge()
			return nil
		},
	})
	return asp, nil
}

func (asp *AutoSectorsPledge) runAutoSectorsPledge() {
	if asp.miner.GetSealing() == nil {
		log.Errorf("run auto sectors pledge failed, error: miner sealing is nil")
		return
	}

	defer func() {
		asp.heartbeatTimer.Stop()
		asp.heartbeatTimer = nil
	}()
	for {
		select {
		case <-asp.heartbeatTimer.C:
			err := asp.executeSectorsPledge()
			if err != nil {
				log.Warnf("execute sectors pledge failed, error: %v", err)
			} else {
				log.Infof("execute sectors pledge successfully. ")
			}
		}
	}
}

func (asp *AutoSectorsPledge) executeSectorsPledge() error {

	sealing := asp.miner.GetSealing()
	if sealing == nil {
		return fmt.Errorf("miner sealing is nil")
	}

	ctx := context.Background()
	jobs := asp.storageMgr.WorkerJobs()

	proofType, err := sealing.CurrentSealProof(ctx)
	if err != nil {
		return err
	}

	ap_workers := make(map[uuid.UUID]storiface.WorkerStats)
	p1_workers := make(map[uuid.UUID]storiface.WorkerStats)
	c2_workers := make(map[uuid.UUID]storiface.WorkerStats)

	doPledge := false

	wst := asp.storageMgr.WorkerStats()

	totalAPTasks := 0
	totalC2Tasks := 0

	for wid, st := range wst {
		if _, ok := st.Info.AcceptTasks[sealtasks.TTAddPiece]; ok {
			totalAPTasks += len(jobs[wid])
			ap_workers[wid] = st
		}
		if _, ok := st.Info.AcceptTasks[sealtasks.TTCommit2]; ok {
			totalC2Tasks += len(jobs[wid])
			c2_workers[wid] = st
		}
		if _, ok := st.Info.AcceptTasks[sealtasks.TTPreCommit1]; ok {
			p1_workers[wid] = st
		}

	}

	// 1. 检查是否有空闲的AP-worker。
	if totalAPTasks >= len(ap_workers) {
		return fmt.Errorf("ap workers are busy")
	}

	// 2. 检查C2-worker是否有空闲。少于4 * C2 worker的待处理任务数量，则可以做pledge。
	if totalC2Tasks >= len(c2_workers)*4 {
		return fmt.Errorf("c2 workers are busy")
	}

	// 3. 检查P1-worker是否有空闲资源可以压盘。
	needRes := sectorstorage.ResourceTable[sealtasks.TTPreCommit1][proofType]
	for wid, st := range p1_workers {
		doPledge = sectorstorage.WorkerCanHandleRequest(needRes, sectorstorage.WorkerID(wid), "executeSectorsPledge", st)
		if doPledge {
			_, err := sealing.PledgeSector(ctx)
			if err != nil {
				return err
			}
			return nil
		}
	}

	return fmt.Errorf("p1 workers are busy")
}
