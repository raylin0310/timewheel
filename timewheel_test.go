package timewheel

import (
	"fmt"
	"log"
	"strconv"
	"testing"
	"time"
)

func TestTimeWheel_AddScheduleTask(t *testing.T) {
	tw, err := New(100*time.Millisecond, 1024)
	if err != nil {
		t.Fail()
		return
	}
	tw.Start()
	begin := time.Now()

	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("task == %d ", i)
		_ = tw.AddScheduleTask(key, 200*time.Millisecond, func() {
			cost := time.Now().Sub(begin).Seconds()
			t.Logf(" %s cost =======  %.1f", key, cost)
		})

	}
	time.Sleep(5 * time.Second)

}

func TestTimeWheel_AddScheduleTask2(t *testing.T) {
	tw, err := New(1000*time.Millisecond, 1024)
	if err != nil {
		t.Fail()
		return
	}
	tw.Start()
	begin := time.Now()

	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("task == %s ", strconv.Itoa(i))
		_ = tw.AddScheduleTask(key, 2000*time.Millisecond, func() {
			cost := time.Now().Sub(begin).Seconds()
			t.Logf(" %s cost =======  %.1f", key, cost)
			tw.RemoveTask(key)
		})

	}
	time.Sleep(5 * time.Second)

}

func TestTimeWheel_RemoveTask(t *testing.T) {
	tw, err := New(100*time.Millisecond, 1024)
	if err != nil {
		t.Fail()
		return
	}
	tw.Start()
	begin := time.Now()
	_ = tw.AddScheduleTask("task_1", 500*time.Millisecond, func() {
		cost := time.Now().Sub(begin).Seconds()
		t.Logf("task_1 cost ======= %.1f", cost)
	})
	//两秒之后删除
	time.Sleep(2 * time.Second)
	tw.RemoveTask("task_1")
	time.Sleep(4 * time.Second)
	log.Print("end")
}

func TestTimeWheel_AddTask(t *testing.T) {
	tw, err := New(100*time.Millisecond, 1024)
	if err != nil {
		t.Fail()
		return
	}
	tw.Start()
	begin := time.Now()
	_ = tw.AddTask("task_2", 500*time.Millisecond, func() {
		cost := time.Now().Sub(begin).Seconds()
		t.Logf("task_2 cost ======= %.1f", cost)
	})

	time.Sleep(2 * time.Second)
	log.Print("end")
}

func TestConcurrentAddScheduleTask(t *testing.T) {
	tw, err := New(100*time.Millisecond, 1024)
	if err != nil {
		t.Fail()
		return
	}
	tw.Start()
	begin := time.Now()
	for i := 0; i < 10000; i++ {
		key := "task" + strconv.Itoa(i)
		go tw.AddScheduleTask(key, 200*time.Millisecond, func() {
			cost := time.Now().Sub(begin).Seconds()
			t.Logf(" %s cost =======  %.1f", key, cost)
		})
	}

	time.Sleep(20 * time.Second)
	log.Print("end")
}

func TestTimeWheel_Stop(t *testing.T) {
	tw, err := New(100*time.Millisecond, 1024)
	if err != nil {
		t.Fail()
		return
	}
	tw.Start()
	begin := time.Now()
	_ = tw.AddScheduleTask("task_3", 500*time.Millisecond, func() {
		cost := time.Now().Sub(begin).Seconds()
		t.Logf("task_3 cost =======  %.1f", cost)
	})
	time.Sleep(2 * time.Second)

	tw.Stop()
	err = tw.AddTask("task_4", 500*time.Millisecond, func() {
		cost := time.Now().Sub(begin).Seconds()
		t.Logf("task_4 cost ======= %.1f", cost)
	})
	if err == NotRunError {
		log.Print("already stop")
	} else {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Second)
}
