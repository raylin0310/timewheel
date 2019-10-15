package timewheel

import (
	"fmt"
	"log"
	"testing"
	"time"
)

func TestTimeWheel_AddTask(t *testing.T) {
	tw, err := New(100*time.Millisecond, 8)
	if err != nil {
		t.Fail()
		return
	}
	tw.Start()
	tw.Start()
	tw.Start()
	begin := time.Now()
	delay := 300 * time.Millisecond
	err = tw.AddScheduleTask("key", delay, func() {
		cost := time.Now().Sub(begin).Seconds()
		fmt.Println("cost =======  ", fmt.Sprintf("%.1f", cost))

	})
	if err != nil {
		log.Fatal(err)
	}
	time.Sleep(3 * time.Second)
	tw.RemoveTask("key")
	time.Sleep(5 * time.Second)
	tw.Stop()
	tw.Stop()
	tw.Stop()
	err = tw.AddTask("key1", delay, func() {
		fmt.Println("stop and add task")
	})
	if err == NotRunError {
		fmt.Println("tw closed")
	} else {
		log.Fatal(err)
	}

	time.Sleep(10 * time.Second)
}
