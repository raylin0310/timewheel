普通延时任务：
```go
func main() {

	tw, _ := timewheel.New(100*time.Millisecond, 1024)

	_ = tw.AddTask("task_1", 100*time.Millisecond, func() {
		fmt.Println("time : ", time.Now())
	})
}
```

定时任务：
```go
func main() {

	tw, _ := timewheel.New(100*time.Millisecond, 1024)

	_ = tw.AddScheduleTask("task_1", 100*time.Millisecond, func() {
		fmt.Println("time : ", time.Now())
	})
}
```
删除任务：
```go
func main() {

	tw, _ := timewheel.New(100*time.Millisecond, 1024)

	_ = tw.AddScheduleTask("task_1", 100*time.Millisecond, func() {
		fmt.Println("time : ", time.Now())
	})
	tw.RemoveTask("task_1")
}
```