package timewheel

import (
	"fmt"
	"log"
	"strconv"
	"testing"
	"time"
)

func TestTimeWheel_AddScheduleTask(t *testing.T) {
	tw, err := New(1000*time.Millisecond, 1024)
	if err != nil {
		t.Fail()
		return
	}
	tw.Start()
	begin := time.Now()

	for i := 0; i < 10; i++ {
		for j := 0; j < 200; j++ {
			key := "task == " + strconv.Itoa(i) + "-" + strconv.Itoa(j)
			_ = tw.AddScheduleTask(key, 2000*time.Millisecond, func() {
				cost := time.Now().Sub(begin).Seconds()
				log.Print(key+" cost =======  ", fmt.Sprintf("%.1f", cost))
			})
		}

	}
	time.Sleep(10 * time.Second)

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
		log.Print("task_1 cost =======  ", fmt.Sprintf("%.1f", cost))
	})
	time.Sleep(2 * time.Second)
	tw.RemoveTask("task_1")
	time.Sleep(2 * time.Second)
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
		log.Print("task_1 cost =======  ", fmt.Sprintf("%.1f", cost))
	})

	time.Sleep(2 * time.Second)
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
	_ = tw.AddTask("task_3", 500*time.Millisecond, func() {
		cost := time.Now().Sub(begin).Seconds()
		log.Print("task_3 cost =======  ", fmt.Sprintf("%.1f", cost))
	})
	time.Sleep(2 * time.Second)

	tw.Stop()

	err = tw.AddTask("task_4", 500*time.Millisecond, func() {
		cost := time.Now().Sub(begin).Seconds()
		log.Print("task_4 cost =======  ", fmt.Sprintf("%.1f", cost))
	})
	if err == NotRunError {
		log.Print("already stop")
	} else {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Second)
}
