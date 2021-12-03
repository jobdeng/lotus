package sectorstorage

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/filecoin-project/lotus/extern/sector-storage/storiface"
	"github.com/filecoin-project/specs-storage/storage"
	"github.com/google/uuid"
	"golang.org/x/xerrors"
)

//
// @job@
// A scheduler to centrally and precisely arrange computing resources for each work executed in the target worker.
//

type WorkMeta struct { //workMeta
	CallID      storiface.CallID
	SectorRef   storage.SectorRef
	ReturnType  ReturnType
	Params      []interface{}
	Retries     uint
	Tried       uint
	SchedStatus SchedStatus
	WorkLogs    []*WorkLog
	schedWork   schedWork
}
type SchedStatus uint //schedStatus

const (
	Received SchedStatus = iota
	Running
	Succeeded
	Failed
	Paused
	Cancelled
)

func (sts SchedStatus) String() string {
	switch sts {
	case Received:
		return "received"
	case Running:
		return "running"
	case Succeeded:
		return "succeeded"
	case Failed:
		return "failed"
	case Paused:
		return "paused"
	case Cancelled:
		return "cancelled"
	default:
		return "unknown"
	}
}

// TODO just make use of worker calltracker datastore?
type WorkLog struct { //workLog
	//Start time.Time
	//End     time.Time
	Time    time.Time
	Status  SchedStatus
	Message string
}

func saveContent(path string, content []byte) error {
	return os.WriteFile(path, content, os.ModePerm)
}
func loadContent(path string) ([]byte, error) {
	return os.ReadFile(path)
}

func (w *WorkMeta) save() error {
	if w == nil {
		return xerrors.Errorf("work meta is nil")
	}
	ser, err := json.Marshal(w)
	if err != nil {
		return xerrors.Errorf("failed to marshal work: %v, error: %v", w.CallID.ID, err)
	}
	path := workStore() + "/" + w.CallID.ID.String()
	if err := saveContent(path, ser); err != nil {
		return xerrors.Errorf("failed to save work: %s, path: %s, error: %v", ser, path, err)
	}
	return nil
}
func (w *WorkMeta) load() error {
	callid := w.CallID.ID.String()
	if len(callid) == 0 {
		return xerrors.Errorf("no call id")
	}
	path := workStore() + "/" + w.CallID.ID.String()
	des, err := loadContent(path)
	if err != nil {
		return xerrors.Errorf("failed to load the work from path: %s, error: %v", path, err)
	}
	return json.Unmarshal(des, w)
}

func (w *WorkMeta) canRetry() bool {
	return w.SchedStatus == Failed && w.Tried > 0 && w.Retries > w.Tried
}

type workReg struct {
	wlist []*WorkMeta
	wbook map[uuid.UUID]*WorkMeta
	wcats map[ReturnType][]*WorkMeta
	mtx   *sync.Mutex
}

func (r *workReg) save() error {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	ids := make([]uuid.UUID, 0)
	for id, _ := range r.wbook {
		ids = append(ids, id)
	}
	jsn, err := json.Marshal(struct {
		Works []uuid.UUID
	}{
		Works: ids,
	})
	if err != nil {
		return xerrors.Errorf("failed to marshal the work registry: %v", err)
	}
	if err := saveContent(workStore()+"/work-registry", jsn); err != nil {
		return xerrors.Errorf("failed to save the work registry: %v", err)
	}
	for idx, work := range r.wlist {
		if err := work.save(); err != nil {
			log.Errorf("failed to save work[%d]: %v, error: %v", idx, work.CallID.ID, err)
		}
	}
	return nil
}
func (wreg *workReg) load() error {
	wreg.mtx.Lock()
	defer wreg.mtx.Unlock()
	path := workStore() + "/work-registry"
	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		log.Warnf("work registry (%s) does not exist", path)
		return nil
	}
	if err != nil {
		return xerrors.Errorf("invalid work registry: %v", err)
	}
	jsn, err := loadContent(path)
	if err != nil {
		return xerrors.Errorf("failed to load work registry: %s, error: %v", path, err)
	}
	works := &struct {
		Works []uuid.UUID
	}{
		Works: make([]uuid.UUID, 0),
	}
	if err := json.Unmarshal(jsn, works); err != nil {
		return xerrors.Errorf("failed to unmarshal work registry: %s, content: %s, error: %v", path, string(jsn), err)
	}
	for idx, id := range works.Works {
		meta := &WorkMeta{
			CallID: storiface.CallID{
				ID: id,
			},
		}
		if err := meta.load(); err != nil {
			log.Errorf("failed to load work[%d]: %v, error: %v", idx, id, err)
			continue
		}
		if err := wreg.appendWork(meta); err != nil {
			log.Errorf("failed to append work[%d]: %v, error: %v", idx, id, err)
		}
	}
	return nil
}

func (wreg *workReg) registerWork(meta *WorkMeta) error {
	wreg.mtx.Lock()
	defer wreg.mtx.Unlock()
	if err := wreg.appendWork(meta); err != nil {
		return err
	}
	if err := meta.save(); err != nil {
		return err
	}
	if err := wreg.save(); err != nil {
		return err
	}
	return nil
}
func (wreg *workReg) appendWork(meta *WorkMeta) error {
	if meta == nil {
		return xerrors.Errorf("no work specified")
	}
	wreg.mtx.Lock()
	defer wreg.mtx.Unlock()
	if _, ok := wreg.wbook[meta.CallID.ID]; ok {
		return xerrors.Errorf("work (%s) has ready added", meta.CallID.ID)
	}
	wreg.wlist = append(wreg.wlist, meta)
	wreg.wbook[meta.CallID.ID] = meta
	works := wreg.wcats[meta.ReturnType]
	if len(works) == 0 {
		works = make([]*WorkMeta, 0)
	}
	works = append(works, meta)
	log.Infof("the work (%v) is appended", meta.CallID.ID)
	return nil
}
func (wreg *workReg) removeWork(id uuid.UUID) *WorkMeta {
	wreg.mtx.Lock()
	defer wreg.mtx.Unlock()
	meta, ok := wreg.wbook[id]
	if !ok {
		log.Warnf("the work (%v) does not exist", id)
		return nil
	}
	delete(wreg.wbook, id)
	for i := 0; i < len(wreg.wlist); i++ {
		if wreg.wlist[i] == meta {
			wreg.wlist = append(wreg.wlist[:i], wreg.wlist[i+1:]...)
			break
		}
	}
	works := wreg.wcats[meta.ReturnType]
	for i := 0; i < len(works); i++ {
		if works[i] == meta {
			works = append(works[:i], works[i+1:]...)
			wreg.wcats[meta.ReturnType] = works
			break
		}
	}
	log.Infof("the work (%v) is removed", id)
	if err := wreg.save(); err != nil {
		log.Errorf("removed the work (%v), but failed to save the work registry: %v", id, err)
	}
	return meta
}
func (wreg *workReg) pauseWork(id uuid.UUID) error {
	wreg.mtx.Lock()
	defer wreg.mtx.Unlock()
	work, ok := wreg.wbook[id]
	if !ok {
		return xerrors.Errorf("failed to pause a non-exist work: %v", id)
	}
	if work.SchedStatus != Received {
		return xerrors.Errorf("cannot pause the work: %v, status: %s", id, work.SchedStatus)
	}
	work.SchedStatus = Paused
	work.WorkLogs = append(work.WorkLogs, &WorkLog{
		Time:   time.Now(),
		Status: Paused,
	})
	log.Infof("the work (%v) is paused", id)
	return work.save()
}
func (wreg *workReg) resumeWork(id uuid.UUID) error {
	wreg.mtx.Lock()
	wreg.mtx.Unlock()
	work, ok := wreg.wbook[id]
	if !ok {
		return xerrors.Errorf("failed to resume a non-exist work: %v", id)
	}
	if work.SchedStatus != Paused {
		return xerrors.Errorf("cannot resume the work: %v, status: %s", id, work.SchedStatus)
	}
	work.SchedStatus = Received
	work.WorkLogs = append(work.WorkLogs, &WorkLog{
		Time:   time.Now(),
		Status: Received,
	})
	log.Infof("the work (%v) is resumed", id)
	return work.save()
}
func (wreg *workReg) cancelWork(id uuid.UUID) error {
	wreg.mtx.Lock()
	wreg.mtx.Unlock()
	work, ok := wreg.wbook[id]
	if !ok {
		return xerrors.Errorf("failed to cancel a non-exist work: %v", id)
	}
	if work.SchedStatus != Received {
		return xerrors.Errorf("cannot resume the work: %v, status: %s", id, work.SchedStatus)
	}
	work.SchedStatus = Cancelled
	work.WorkLogs = append(work.WorkLogs, &WorkLog{
		Time:   time.Now(),
		Status: Cancelled,
	})
	log.Infof("the work (%v) is cancelled", id)
	return work.save()
}
func (wreg *workReg) getWorks(rtype ReturnType, sts SchedStatus) []*WorkMeta {
	wreg.mtx.Lock()
	defer wreg.mtx.Unlock()
	works := make([]*WorkMeta, 0)
	for _, meta := range wreg.wcats[rtype] {
		if meta.SchedStatus == sts {
			works = append(works, meta)
		}
	}
	return works
}

type schedWork = func(ctx context.Context, meta *WorkMeta) (interface{}, error)

type workSched struct {
	wstore     string
	wreg       *workReg
	running    bool
	stopping   chan struct{}
	scheduling chan *WorkMeta
	mtx        *sync.Mutex
	worker     *LocalWorker
}

var _wsched *workSched

func newWorkSched(worker *LocalWorker) *workSched {
	if _wsched != nil {
		return _wsched
	}
	wstore := os.Getenv("WORK_STORE")
	if _, err := os.Stat(wstore); err != nil {
		log.Errorf("the WORK_STORE(%s) is an invalid path: %v", wstore, err)
		return nil
	}
	wreg := &workReg{
		wlist: make([]*WorkMeta, 0),
		wbook: make(map[uuid.UUID]*WorkMeta),
		wcats: make(map[ReturnType][]*WorkMeta),
		mtx:   &sync.Mutex{},
	}
	_wsched = &workSched{
		wstore:     wstore,
		wreg:       wreg,
		running:    false,
		stopping:   make(chan struct{}),
		scheduling: make(chan *WorkMeta, 16), //AP: 1, P1: 14, P2: 1, C2: 4
		mtx:        &sync.Mutex{},
		worker:     worker,
	}
	return _wsched
}

func workStore() string {
	if _wsched == nil {
		return ""
	}
	return _wsched.workStore()
}
func (wreg *workSched) workStore() string {
	return wreg.wstore
}
func (wreg *workSched) setWorkStore(path string) error {
	if len(wreg.wstore) > 0 {
		return xerrors.Errorf("work store has already been set as: %s", wreg.wstore)
	}
	if len(path) == 0 {
		return xerrors.Errorf("the work store path is empty")
	}
	if _, err := os.Stat(path); err != nil {
		return xerrors.Errorf("the new work store is invalid: %v", err)
	}
	wreg.wstore = path
	return nil
}

func submitWork(sector storage.SectorRef, rtype ReturnType, work schedWork, params ...interface{}) (storiface.CallID, error) {
	if _wsched == nil {
		return storiface.UndefCall, xerrors.Errorf("no work scheduler")
	}
	return _wsched.submitWork(sector, rtype, work, params...)
}
func (wsched *workSched) submitWork(sector storage.SectorRef, rtype ReturnType, work schedWork, params ...interface{}) (storiface.CallID, error) {
	callid := storiface.CallID{
		Sector: sector.ID,
		ID:     uuid.New(),
	}
	meta := &WorkMeta{
		CallID:      callid,
		SectorRef:   sector,
		ReturnType:  rtype,
		Params:      params,
		Retries:     1,
		Tried:       0,
		SchedStatus: Received,
		WorkLogs:    make([]*WorkLog, 0),
		schedWork:   work,
	}
	meta.WorkLogs = append(meta.WorkLogs, &WorkLog{
		Time:   time.Now(),
		Status: Received,
	})
	if err := wsched.wreg.registerWork(meta); err != nil {
		return storiface.UndefCall, err
	}
	return callid, nil
}

func (wsched *workSched) startWorks() error {
	wsched.mtx.Lock()
	defer wsched.mtx.Unlock()
	if wsched.running {
		return xerrors.Errorf("work scheduler is alreay running")
	}
	log.Infof("start scheduling works")
	go wsched.doSched()
	go wsched.doExec()
	wsched.running = true
	return nil
}

func (wsched *workSched) stopWorks() error {
	wsched.mtx.Lock()
	defer wsched.mtx.Unlock()
	if !wsched.running {
		return xerrors.Errorf("work scheduler is not running")
	}
	log.Warnf("stop scheduling works")
	wsched.stopping <- struct{}{}
	return nil
}

func (wsched *workSched) doSched() {
	for {
		select {
		case <-wsched.stopping:
			wsched.mtx.Lock()
			wsched.running = false
			wsched.mtx.Unlock()
			break
		case <-time.After(5 * time.Second): //TODO to be flexible
			wsched.wreg.mtx.Lock()
			for _, meta := range wsched.wreg.wlist {
				if wsched.canSched(meta) {
					wsched.scheduling <- meta
				}
			}
			wsched.wreg.mtx.Unlock()
		}
	}
}
func (wsched *workSched) doExec() {
	for meta := range wsched.scheduling {
		log.Infof("scheduling work: %v, type: %s, sector: %d", meta.CallID.ID, meta.ReturnType, meta.SectorRef.ID.Number)
		go func(meta *WorkMeta) {
			meta.SchedStatus = Running
			meta.Tried++
			wlog := &WorkLog{
				Time:   time.Now(),
				Status: Running,
			}
			meta.WorkLogs = append(meta.WorkLogs, wlog)
			if err := meta.save(); err != nil {
				log.Errorf("%v", err)
			}
			ctx := context.Background()
			res, err := wsched.doWork(ctx, meta)
			if err == nil {
				meta.SchedStatus = Succeeded
			} else {
				meta.SchedStatus = Failed
			}
			if meta.SchedStatus == Succeeded || !meta.canRetry() {
				tracker := wsched.worker.ct
				if err != nil {
					byts, err := json.Marshal(res)
					if err == nil {
						tracker.onDone(meta.CallID, byts)
					}
				}
				if doReturn(ctx, meta.ReturnType, meta.CallID, wsched.worker.ret, res, toCallError(err)) {
					tracker.onReturned(meta.CallID)
				}
			}
			wlog = &WorkLog{
				Time: time.Now(),
			}
			meta.WorkLogs = append(meta.WorkLogs, wlog)
			torm := true
			if meta.SchedStatus == Succeeded {
				wlog.Status = Succeeded
			} else {
				wlog.Status = Failed
				wlog.Message = fmt.Sprintf("%v", err) //TODO truncate message to a propoer size
				torm = meta.canRetry()
			}
			if err := meta.save(); err != nil {
				log.Errorf("%v", err)
			}
			if torm {
				wsched.wreg.removeWork(meta.CallID.ID)
			}
		}(meta)
	}
}

func (wsched *workSched) doWork(ctx context.Context, work *WorkMeta) (interface{}, error) {
	worker := wsched.worker
	tracker := worker.ct
	if work.Tried == 1 {
		tracker.onStart(work.CallID, work.ReturnType)
	}

	worker.running.Add(1)
	defer worker.running.Done()

	nctx := &wctx{
		vals:    ctx,
		closing: worker.closing,
	}

	return work.schedWork(nctx, work)
}

func (wsched *workSched) canSched(work *WorkMeta) bool {
	return work.SchedStatus == Received || (work.SchedStatus == Failed && work.canRetry()) //TODO more precise justifications on resource usage ...
}
