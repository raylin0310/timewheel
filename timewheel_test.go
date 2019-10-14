package timewheel

import (
	"fmt"
	"testing"
	"time"
)

func TestSizeOf(t *testing.T) {
	u, _ := normalizeTicksPerWheel(33)
	fmt.Println(u)
}

func TestTimeWheel_AddTask(t *testing.T) {
	tw, e := New(100*time.Millisecond, 4)
	if e != nil {
		t.Fail()
		return
	}
	tw.Run()
	begin := time.Now()
	tw.AddTask("task1", 100*time.Millisecond, func() {
		fmt.Println("taks1 cost ", time.Now().Sub(begin).Seconds())
	})
	//go tw.AddTask("task2", 2000*time.Millisecond, func() {
	//	fmt.Println("taks2 cost ", time.Now().Sub(begin).Seconds())
	//})
	//time.Sleep(2*time.Second)
	//tw.AddTask("task2", 3000*time.Millisecond, func() {
	//	fmt.Println("taks2 cost ", time.Now().Sub(begin).Seconds())
	//})

	time.Sleep(100*time.Second)
}

func TestTimewheel_getPositionAndCircle(t *testing.T) {
	tw, e := New(100*time.Millisecond, 4)
	if e != nil {
		t.Fail()
		return
	}
	tw.curPos = 2
	pos, circle := tw.getPositionAndCircle(2400 * time.Millisecond)
	fmt.Println(pos)
	fmt.Println(circle)
}

func TestTicker(t *testing.T) {
	ticker := time.NewTicker(1 * time.Second)
	begin := time.Now()
	for  {
		select {
		case  <-ticker.C:
			fmt.Println("cost ",time.Now().Sub(begin).Seconds())
		}
	}
}

func TestGetYushu(t *testing.T) {
	num := 4
	for i := 0; i < 20; i++ {
		fmt.Println((i+1)%num)
	}
}