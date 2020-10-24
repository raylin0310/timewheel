package timewheel

import (
	"errors"
	"sync"
	"time"
)

type taskFn func()

type status int8

const (
	ready status = iota + 1
	run
	stop
)

const (
	maxBuckets int = 1024 * 1024
)

var (
	IllegalBucketNumError = errors.New("illegal bucket num")
	IllegalTaskDelayError = errors.New("illegal delay time ")
	TaskKeyExistError     = errors.New("task key already exists")
	NotRunError           = errors.New("timeWheel not running")
)

type TimeWheel struct {
	ticker       *time.Ticker
	tickDuration time.Duration
	bucketsNum   int
	buckets      []*bucket
	curPos       int
	keyPosMap    sync.Map
	status       status
	begin        sync.Once
	end          sync.Once
	stopChan     chan struct{}
}

type bucket struct {
	list []*task
	mu   sync.Mutex
}

type task struct {
	key      string
	delay    time.Duration
	pos      int
	circle   int
	fn       taskFn
	schedule bool
}

func (b *bucket) push(t *task) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.list = append(b.list, t)
}

func New(tickDuration time.Duration, bucketsNum int32) (*TimeWheel, error) {
	if bucketsNum <= 0 {
		return nil, IllegalBucketNumError
	}

	num := normalizeTicksPerWheel(bucketsNum)

	tw := &TimeWheel{
		tickDuration: tickDuration,
		bucketsNum:   num,
		buckets:      make([]*bucket, num),
		curPos:       0,
		status:       ready,
		stopChan:     make(chan struct{}),
	}

	for i := 0; i < num; i++ {
		tw.buckets[i] = &bucket{list: make([]*task, 0)}
	}
	return tw, nil
}

func (tw *TimeWheel) AddTask(key string, delay time.Duration, fn func()) error {
	return tw.addTask(key, delay, false, fn)
}

func (tw *TimeWheel) AddScheduleTask(key string, delay time.Duration, fn func()) error {
	return tw.addTask(key, delay, true, fn)
}

func (tw *TimeWheel) addTask(key string, delay time.Duration, schedule bool, fn func()) error {

	if tw.status != run {
		return NotRunError
	}
	if _, exist := tw.keyPosMap.Load(key); exist {
		return TaskKeyExistError
	}
	if delay <= 0 {
		return IllegalTaskDelayError
	}

	if delay < tw.tickDuration {
		delay = tw.tickDuration
	}
	pos, circle := tw.getPositionAndCircle(delay)
	task := &task{
		delay:    delay,
		key:      key,
		pos:      pos,
		circle:   circle,
		fn:       fn,
		schedule: schedule,
	}
	tw.buckets[pos].push(task)
	tw.keyPosMap.Store(key, pos)
	return nil
}

func (tw *TimeWheel) RemoveTask(key string) {

	pos, ok := tw.keyPosMap.Load(key)
	if !ok {
		return
	}
	bucket := tw.buckets[pos.(int)]
	bucket.mu.Lock()
	defer bucket.mu.Unlock()

	for i := 0; i < len(bucket.list); i++ {
		t := bucket.list[i]
		if key == t.key {
			bucket.list = append(bucket.list[:i], bucket.list[i+1:]...)
			tw.keyPosMap.Delete(key)
			return
		}
	}
}

func (tw *TimeWheel) getPositionAndCircle(delay time.Duration) (pos int, circle int) {
	dd := int(delay / tw.tickDuration)
	circle = dd / tw.bucketsNum
	// 此处的tw.curPos表示该position中的任务都已经处理完毕
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
	})
}

func (tw *TimeWheel) Stop() {
	tw.end.Do(func() {
		tw.stopChan <- struct{}{}
		tw.status = stop
	})
}

func (tw *TimeWheel) startTickerHandle() {
	for {
		select {
		case <-tw.ticker.C:
			tw.handleTicker()
		case <-tw.stopChan:
			tw.ticker.Stop()
			return
		}
	}
}

func (tw *TimeWheel) handleTicker() {
	curPos := (tw.curPos + 1) & (tw.bucketsNum - 1) //equals (tw.curPos + 1) % tw.bucketsNum
	bucket := tw.buckets[curPos]
	bucket.mu.Lock()
	defer bucket.mu.Unlock()
	tw.curPos = curPos
	k := 0
	for i := 0; i < len(bucket.list); i++ {
		task := bucket.list[i]
		if task.circle > 0 {
			task.circle--
			bucket.list[k] = task
			k++
			continue
		}
		go task.fn()
		if task.schedule {
			_, ok := tw.keyPosMap.Load(task.key)
			if !ok {
				continue
			}
			//reload
			pos, circle := tw.getPositionAndCircle(task.delay)
			task.pos = pos
			task.circle = circle
			if pos == curPos {
				bucket.list[k] = task
				k++
			} else {
				tw.buckets[pos].push(task)
				tw.keyPosMap.Store(task.key, pos)
			}
		} else {
			tw.keyPosMap.Delete(task.key)
		}
	}
	bucket.list = bucket.list[:k]
}

// get the bucket capacity
func normalizeTicksPerWheel(ticksPerWheel int32) int {
	u := ticksPerWheel - 1
	u |= u >> 1
	u |= u >> 2
	u |= u >> 4
	u |= u >> 8
	u |= u >> 16
	if int(u+1) > maxBuckets {
		return maxBuckets
	}
	return int(u + 1)
}
