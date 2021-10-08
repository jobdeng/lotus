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

var AutoSectorsPledgeInterval = 5 * time.Minute

type TasksLimit struct {
	Assigned int
	Request  int
}

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
	log.Infof("Current Seal Proof: %d", proofType)
	tasksCountOfHost := make(map[string]*TasksCountOfHost)
	ap_workers := make(map[uuid.UUID]storiface.WorkerStats)
	p1_workers := make(map[uuid.UUID]storiface.WorkerStats)
	p2_workers := make(map[uuid.UUID]storiface.WorkerStats)
	c2_workers := make(map[uuid.UUID]storiface.WorkerStats)

	doPledge := false

	wst := asp.storageMgr.WorkerStats()

	totalC2Ass := 0
	totalC2Reqs := 0

	for wid, st := range wst {

		// 先清理丢worker不在线的任务
		if !st.Enabled {
			for _, job := range jobs[wid] {
				log.Infof("aborting job %s, task %s, sector %d, running on host %s", job.ID.String(), job.Task.Short(), job.Sector.Number, job.Hostname)
				abortErr := asp.storageMgr.Abort(ctx, job.ID)
				if abortErr != nil {
					log.Errorf("abort disabled worker's jobs failed, error: %v", abortErr)
				}
			}
		}

		workerCount := tasksCountOfHost[st.Info.Hostname]
		if workerCount == nil {
			workerCount = NewTasksCountOfHost()
		}

		//统计各个类型的worker数
		if _, ok := st.Info.AcceptTasks[sealtasks.TTAddPiece]; ok && st.Enabled {
			//totalAPTasks += len(jobs[wid])
			ap_workers[wid] = st
			workerCount.APWorkers = append(workerCount.APWorkers, wid)

		}
		if _, ok := st.Info.AcceptTasks[sealtasks.TTPreCommit1]; ok && st.Enabled {
			p1_workers[wid] = st
			workerCount.P1Workers = append(workerCount.P1Workers, wid)
		}
		if _, ok := st.Info.AcceptTasks[sealtasks.TTPreCommit2]; ok && st.Enabled {
			p2_workers[wid] = st
			workerCount.P2Workers = append(workerCount.P2Workers, wid)
		}
		if _, ok := st.Info.AcceptTasks[sealtasks.TTCommit2]; ok && st.Enabled {
			//totalC2Tasks += len(jobs[wid])
			c2_workers[wid] = st
			workerCount.C2Workers = append(workerCount.C2Workers, wid)
		}

		//统计已分配任务数
		for _, wj := range jobs[wid] {
			switch wj.Task {
			case sealtasks.TTAddPiece:
				workerCount.APTasksAss = workerCount.APTasksAss + 1
			case sealtasks.TTPreCommit1:
				workerCount.P1TasksAss = workerCount.P1TasksAss + 1
			case sealtasks.TTPreCommit2:
				workerCount.P2TasksAss = workerCount.P2TasksAss + 1
			case sealtasks.TTCommit2:
				totalC2Ass = totalC2Ass + 1
				//workerCount.C2TasksAss = workerCount.C2TasksAss + 1

			}

		}

		tasksCountOfHost[st.Info.Hostname] = workerCount
	}

	schedInfo := asp.storageMgr.GetSchedDiagInfo()
	for _, req := range schedInfo.Requests {

		sectorInfo, terr := sealing.GetSectorInfo(req.Sector.Number)
		if terr != nil {
			log.Errorf("auto pledge GetSectorInfo failed, err: %v", terr)
			continue
		}
		if len(sectorInfo.PledgeHostname) == 0 {
			log.Warnf("auto pledge sector info is empty")
			continue
		}
		workerCount := tasksCountOfHost[sectorInfo.PledgeHostname]
		if workerCount == nil {
			workerCount = &TasksCountOfHost{}
		}

		switch req.TaskType {
		case sealtasks.TTAddPiece:
			workerCount.APTasksReq = workerCount.APTasksReq + 1
		case sealtasks.TTPreCommit1:
			workerCount.P1TasksReq = workerCount.P1TasksReq + 1
		case sealtasks.TTPreCommit2:
			workerCount.P2TasksReq = workerCount.P2TasksReq + 1
		case sealtasks.TTCommit2:
			//C2在证明机，被多个seal worker同享使用，需要单独计算总数
			totalC2Reqs = totalC2Reqs + 1
			//workerCount.C2TasksReq = workerCount.C2TasksReq + 1
		}
		tasksCountOfHost[sectorInfo.PledgeHostname] = workerCount
	}

	// 1.a 检查C2-worker是否有存在。
	if len(c2_workers) == 0 {
		return fmt.Errorf("c2 workers are not running")
	}
	// 1.b 检查C2-worker是否有过多请求
	c2TasksLimit := TasksLimitTable[sealtasks.TTCommit2][proofType]
	if totalC2Reqs+totalC2Ass >= len(c2_workers)*(c2TasksLimit.Assigned+c2TasksLimit.Request) {
		return fmt.Errorf("c2 workers are busy, total assigned tasks and requests: %d, limit: %+v", totalC2Reqs+totalC2Ass, c2TasksLimit)
	}

	needRes := sectorstorage.ResourceTable[sealtasks.TTPreCommit1][proofType]
loopHost:
	for hostname, taskCount := range tasksCountOfHost {
		// 2. 检查是否有空闲的AP-worker。
		if taskCount.APTasksAss >= len(taskCount.APWorkers) {
			log.Infof("%s: ap workers are busy", hostname)
			continue
		}

		// 3. 检查P1任务是否有过多请求
		if taskCount.P1TasksReq >= len(taskCount.P1Workers) {
			log.Infof("%s: p1 workers are busy", hostname)
			continue
		}

		// 4. 检查P2任务是否有过多请求
		p2TasksLimit := TasksLimitTable[sealtasks.TTPreCommit2][proofType]
		if taskCount.P2TasksReq >= len(taskCount.P2Workers)*p2TasksLimit.Request {
			log.Infof("%s: p2 workers are busy", hostname)
			continue
		}

		for _, wid := range taskCount.P1Workers {
			st := p1_workers[wid]
			doPledge = sectorstorage.WorkerCanHandleRequest(needRes, sectorstorage.WorkerID(wid), "executeSectorsPledge", st)
			if doPledge {
				sec, err := sealing.PledgeSector(ctx, st.Info.Hostname)
				if err != nil {
					return err
				}
				log.Infof("wokrer: %s starting pledge sector: %d", st.Info.Hostname, sec.ID.Number)
				time.Sleep(10 * time.Second)
				continue loopHost
			}
		}
	}

	return nil
}

// host主机的任务统计数
type TasksCountOfHost = struct {
	APWorkers []uuid.UUID
	P1Workers []uuid.UUID
	P2Workers []uuid.UUID
	C2Workers []uuid.UUID

	APTasksAss int
	P1TasksAss int
	P2TasksAss int
	C2TasksAss int

	APTasksReq int
	P1TasksReq int
	P2TasksReq int
	C2TasksReq int
}

func NewTasksCountOfHost() *TasksCountOfHost {
	t := TasksCountOfHost{
		APWorkers: make([]uuid.UUID, 0),
		P1Workers: make([]uuid.UUID, 0),
		P2Workers: make([]uuid.UUID, 0),
		C2Workers: make([]uuid.UUID, 0),
	}
	return &t
}
