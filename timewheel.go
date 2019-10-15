package timewheel

import (
	"container/list"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

type (
	bucket = *list.List
	taskAc = int
	tasKFn = func()
)

const (
	addTaskAction = iota + 1
	delTaskAction
	reLoadTask
	updateTaskAction
)

const (
	ready = iota + 1
	run
	stop
)

const (
	maxBuckets            uint32 = 1024 * 1024
	defaultTaskFnChanSize        = 16
)

var (
	IllegalBucketNumError = errors.New("illegal bucket num")
	IllegalTaskDelayError = errors.New("illegal delay time ")
	TaskKeyExistError     = errors.New("task key already exists")
	NotRunError           = errors.New("timeWheel not run")
)

type TimeWheel struct {
	ticker             *time.Ticker
	tickDuration       time.Duration
	bucketsNum         uint32
	buckets            []bucket
	curPos             uint32
	keyPosMap          map[string]uint32
	taskChannel        chan taskAction
	taskExecuteChannel chan tasKFn
	status             int32
	begin              sync.Once
	end                sync.Once
}

type taskAction struct {
	task   *task
	action taskAc
}

type task struct {
	key      string
	delay    time.Duration
	pos      uint32
	circle   uint32
	fn       tasKFn
	schedule bool
	e        *list.Element
}

func New(tickDuration time.Duration, bucketsNum uint32) (*TimeWheel, error) {
	if bucketsNum <= 0 {
		return nil, IllegalBucketNumError
	}

	num := normalizeTicksPerWheel(bucketsNum)

	tw := &TimeWheel{
		tickDuration:       tickDuration,
		bucketsNum:         num,
		buckets:            make([]bucket, num),
		keyPosMap:          make(map[string]uint32),
		curPos:             0,
		taskChannel:        make(chan taskAction),
		taskExecuteChannel: make(chan tasKFn, defaultTaskFnChanSize),
		status:             ready,
	}

	for i := 0; i < int(num); i++ {
		tw.buckets[i] = list.New()
	}
	return tw, nil
}

// add once task
func (tw *TimeWheel) AddTask(key string, delay time.Duration, fn func()) error {
	err := tw.addTask(key, delay, false, fn)
	if err != nil {
		return err
	}
	return nil
}

// remove the task by key
func (tw *TimeWheel) RemoveTask(taskKey string) {
	task := &task{key: taskKey}
	tw.taskChannel <- taskAction{task, delTaskAction}
}

// add schedule task
func (tw *TimeWheel) AddScheduleTask(key string, delay time.Duration, fn func()) error {
	err := tw.addTask(key, delay, true, fn)
	if err != nil {
		return err
	}
	return nil
}

// add task
func (tw *TimeWheel) addTask(key string, delay time.Duration, schedule bool, fn func()) error {
	if _, exist := tw.keyPosMap[key]; exist {
		return TaskKeyExistError
	}
	if delay <= 0 {
		return IllegalTaskDelayError
	}
	if atomic.LoadInt32(&tw.status) != run {
		return NotRunError
	}
	if delay < tw.tickDuration {
		delay = tw.tickDuration
	}

	task := &task{
		delay:    delay,
		key:      key,
		fn:       fn,
		schedule: schedule,
	}
	tw.taskChannel <- taskAction{task, addTaskAction}
	return nil
}

// get task pos and circle
func (tw *TimeWheel) getPositionAndCircle(delay time.Duration) (pos uint32, circle uint32) {
	dd := uint32(delay / tw.tickDuration)
	circle = dd / tw.bucketsNum
	pos = (tw.curPos + dd) & (tw.bucketsNum - 1)
	if circle > 0 && pos == tw.curPos {
		circle--
	}
	return
}

// start the timeWheel
func (tw *TimeWheel) Start() {
	tw.begin.Do(func() {
		tw.ticker = time.NewTicker(tw.tickDuration)
		tw.status = run
		go tw.startTickerHandle()
		go tw.startTaskActionHandle()
		go tw.starTaskFnExecuteHandle()
	})
}

// stop the timeWheel
func (tw *TimeWheel) Stop() {
	tw.end.Do(func() {
		tw.status = stop
	})
}

// start ticker handler
func (tw *TimeWheel) startTickerHandle() {
	for {
		select {
		case <-tw.ticker.C:
			tw.handleTicker()
		default:
			if tw.status == stop {
				tw.ticker.Stop()
				return
			}
		}
	}
}

// start task action handler
func (tw *TimeWheel) startTaskActionHandle() {
	for {
		select {
		case taskAction := <-tw.taskChannel:
			tw.handleTask(taskAction)
		default:
			if tw.status == stop {
				return
			}
		}
	}
}

// start task func execute handler
func (tw *TimeWheel) starTaskFnExecuteHandle() {
	for {
		select {
		case tasKFn := <-tw.taskExecuteChannel:
			tasKFn()
		default:
			if tw.status == stop {
				return
			}
		}
	}
}

// the ticker handler
func (tw *TimeWheel) handleTicker() {
	tw.curPos = (tw.curPos + 1) & (tw.bucketsNum - 1)
	l := tw.buckets[tw.curPos]

	for e := l.Front(); e != nil; e = e.Next() {
		task := e.Value.(*task)
		task.e = e

		if task.circle > 0 {
			task.circle--
			continue
		}

		tw.taskExecuteChannel <- task.fn
		tw.taskChannel <- taskAction{task, reLoadTask}
	}
}

// the task handler
func (tw *TimeWheel) handleTask(action taskAction) {
	tk := action.task
	ac := action.action
	switch ac {
	case addTaskAction:
		tw.addTaskHandle(tk)
	case reLoadTask:
		l := tw.buckets[tk.pos]
		l.Remove(tk.e)
		if !tk.schedule {
			delete(tw.keyPosMap, tk.key)
		}
		tw.addTaskHandle(tk)
	case delTaskAction:
		tw.removeTaskHandle(tk.key)
	}
}

// execute add the task
func (tw *TimeWheel) addTaskHandle(task *task) {
	pos, circle := tw.getPositionAndCircle(task.delay)
	task.pos = pos
	task.circle = circle
	task.e = nil
	tw.buckets[pos].PushBack(task)
	tw.keyPosMap[task.key] = pos
}

// execute remove the task by key
func (tw *TimeWheel) removeTaskHandle(key string) {
	pos, ok := tw.keyPosMap[key]
	if !ok {
		return
	}
	l := tw.buckets[pos]
	for e := l.Front(); e != nil; e = e.Next() {
		task := e.Value.(*task)
		if key == task.key {
			l.Remove(e)
			delete(tw.keyPosMap, key)
			return
		}
	}
}

// get the bucket capacity
func normalizeTicksPerWheel(ticksPerWheel uint32) uint32 {
	u := ticksPerWheel - 1
	u |= u >> 1
	u |= u >> 2
	u |= u >> 4
	u |= u >> 8
	u |= u >> 16
	if u+1 > maxBuckets {
		return maxBuckets
	}
	return u + 1
}
