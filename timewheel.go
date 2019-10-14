package timewheel

import (
	"container/list"
	"errors"
	"fmt"
	"time"
)

type
(
	bucket = *list.List
	taskAc = int
)

const (
	addTaskAction = iota + 1
	delTaskAction
	updateTaskAction
)

var notValidError = errors.New("not allow < 0")

const maxBuckets uint32 = 1024 * 1024

type TimeWheel struct {
	ticker       *time.Ticker
	tickDuration time.Duration
	bucketsNum   uint32
	buckets      []bucket
	keyPosMap    map[string]uint32
	curPos       uint32
	taskChannel  chan taskAction
}

type taskAction struct {
	task   *task
	action taskAc
}

type task struct {
	key    string
	delay  time.Duration
	pos    uint32
	circle uint32
	fn     func()
	e      *list.Element
}

func New(tickDuration time.Duration, bucketsNum uint32) (*TimeWheel, error) {
	num, err := normalizeTicksPerWheel(bucketsNum)
	if err != nil {
		return nil, err
	}
	tw := &TimeWheel{
		tickDuration: tickDuration,
		bucketsNum:   num,
		buckets:      make([]bucket, num),
		keyPosMap:    make(map[string]uint32),
		curPos:       0,
		taskChannel:  make(chan taskAction),
	}

	for i := 0; i < int(num); i++ {
		tw.buckets[i] = list.New()
	}
	return tw, nil
}

func (tw *TimeWheel) AddTask(key string, delay time.Duration, fn func()) {
	if delay < tw.tickDuration {
		delay = tw.tickDuration
	}
	task := &task{delay: delay, key: key, fn: fn}
	tw.taskChannel <- taskAction{task, addTaskAction}
}

func (tw *TimeWheel) getPositionAndCircle(delay time.Duration) (pos uint32, circle uint32) {
	circle = uint32(delay.Microseconds() / tw.tickDuration.Microseconds() / (int64(tw.bucketsNum))+1)
	pos = (tw.curPos + uint32(delay.Microseconds()/tw.tickDuration.Microseconds())) & (tw.bucketsNum - 1)

	return
}

func (tw *TimeWheel) Run() {
	tw.ticker = time.NewTicker(tw.tickDuration)
	go tw.run()
}

func (tw *TimeWheel) run() {

	for {
		select {
		case <-tw.ticker.C:
			tw.handle()
		case taskAction := <-tw.taskChannel:
			tw.handleTask(taskAction)
		}
	}
}

func (tw *TimeWheel) handle() {
	tw.curPos = (tw.curPos + 1) & (tw.bucketsNum - 1)

	l := tw.buckets[tw.curPos]

	for e := l.Front(); e != nil; e = e.Next() {
		task := e.Value.(*task)

		if task.circle > 0 {
			task.circle--
			continue
		}

		go task.fn()
		task.e = e
		go tw.removeTask(task)
	}
}

func (tw *TimeWheel) removeTask(task *task) {
	tw.taskChannel <- taskAction{task, delTaskAction}
}

func (tw *TimeWheel) handleTask(action taskAction) {
	task := action.task
	ac := action.action
	switch ac {
	case addTaskAction:
		pos, circle := tw.getPositionAndCircle(task.delay)
		fmt.Println("key = ", task.key, ",pos = ", pos, ", circle = ", circle)
		task.pos = pos
		task.circle = circle
		tw.buckets[pos].PushBack(task)
		tw.keyPosMap[task.key] = pos
	case delTaskAction:
		tw.buckets[task.pos].Remove(task.e)
		delete(tw.keyPosMap, task.key)
	}
}

func normalizeTicksPerWheel(ticksPerWheel uint32) (uint32, error) {
	if ticksPerWheel < 0 {
		return ticksPerWheel, notValidError
	}
	u := ticksPerWheel - 1
	u |= u >> 1
	u |= u >> 2
	u |= u >> 4
	u |= u >> 8
	u |= u >> 16
	if u+1 > maxBuckets {
		return maxBuckets, nil
	}
	return u + 1, nil

}
