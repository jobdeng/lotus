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
	"os"
	"time"
)

var log = logging.Logger("auto")

var AutoSectorsPledgeInterval = 5 * time.Second

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

	envVar := make(map[string]string)
	for _, envKey := range []string{"FIL_PROOFS_USE_MULTICORE_SDR", "FIL_PROOFS_MULTICORE_SDR_PRODUCERS"} {
		envValue, found := os.LookupEnv(envKey)
		if found {
			envVar[envKey] = envValue
		}
	}

	ctx := context.Background()
	jobs := asp.storageMgr.WorkerJobs()

	proofType, err := sealing.CurrentSealProof(ctx)
	if err != nil {
		return err
	}

	// 查看调度器有多少个C2排队
	type WorkerCountOfHost = struct {
		APWorkers uint64
		P1Workers uint64
		P2Workers uint64
		C2Workers uint64
	}

	workerOfHost := make(map[string]*WorkerCountOfHost)
	ap_workers := make(map[uuid.UUID]storiface.WorkerStats)
	p1_workers := make(map[uuid.UUID]storiface.WorkerStats)
	c2_workers := make(map[uuid.UUID]storiface.WorkerStats)

	doPledge := false

	wst := asp.storageMgr.WorkerStats()

	totalAPTasks := 0
	totalC2Tasks := 0
	totalC2Reqs := 0

	for wid, st := range wst {
		workerCount := workerOfHost[st.Info.Hostname]
		if workerCount == nil {
			workerCount = &WorkerCountOfHost{}
		}
		if _, ok := st.Info.AcceptTasks[sealtasks.TTAddPiece]; ok && st.Enabled {
			totalAPTasks += len(jobs[wid])
			ap_workers[wid] = st
			workerCount.APWorkers = workerCount.APWorkers + 1
		}
		if _, ok := st.Info.AcceptTasks[sealtasks.TTCommit2]; ok && st.Enabled {
			totalC2Tasks += len(jobs[wid])
			c2_workers[wid] = st
			workerCount.C2Workers = workerCount.C2Workers + 1
		}
		if _, ok := st.Info.AcceptTasks[sealtasks.TTPreCommit1]; ok && st.Enabled {
			p1_workers[wid] = st
			workerCount.P1Workers = workerCount.P1Workers + 1
		}
		workerOfHost[st.Info.Hostname] = workerCount
	}

	schedInfo := asp.storageMgr.GetSchedDiagInfo()
	for _, req := range schedInfo.Requests {
		//log.Infof("req: %+v", req)
		if req.TaskType == sealtasks.TTCommit2 {
			totalC2Reqs = totalC2Reqs + 1
		}
	}


	// 1. 检查是否有空闲的AP-worker。
	if totalAPTasks >= len(ap_workers) {
		return fmt.Errorf("ap workers are busy")
	}

	// 2. 检查C2-worker是否有空闲。少于3 * C2 worker的待处理任务数量，则可以做pledge。
	if len(c2_workers) == 0 {
		return fmt.Errorf("c2 workers are not running")
	}
	if totalC2Tasks >= len(c2_workers) * 3 && totalC2Reqs >= len(c2_workers) * 2 {
		log.Infof("totalC2Tasks: %d, totalC2Reqs: %d", totalC2Tasks, totalC2Reqs)
		return fmt.Errorf("c2 workers are busy")
	}


	// 3. 检查P1-worker是否有空闲资源可以压盘。
	//taskPerCores := uint64(1)
	////1个任务在 FIL_PROOFS_USE_MULTICORE_SDR=1, FIL_PROOFS_MULTICORE_SDR_PRODUCERS=7 下用8个核
	//if envVar["FIL_PROOFS_USE_MULTICORE_SDR"] == "1" {
	//	producers := strToUInt64(envVar["FIL_PROOFS_MULTICORE_SDR_PRODUCERS"], 3)
	//	taskPerCores = producers + 1
	//}

	needRes := sectorstorage.ResourceTable[sealtasks.TTPreCommit1][proofType]
	for wid, st := range p1_workers {
		//tasks := len(jobs[wid])
		//cores := st.Info.Resources.CPUs
		//if uint64(tasks) * taskPerCores > cores {
		//	continue
		//}
		workerCount := workerOfHost[st.Info.Hostname]
		if workerCount.APWorkers == 0 {
			//AP worker is not exist, can not pledge sector
			log.Infof("Host: %s, AP worker is not exist, can not pledge sector", st.Info.Hostname)
			continue
		}
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
