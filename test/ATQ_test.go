package test

import (
	"sync"
	"testing"
	"time"

	"github.com/murInJ/Asynchronous-Temporal-Queue/core"
)

func TestAsynchronousTemporalQueue(t *testing.T) {
	queue := core.NewAsynchronousTemporalQueue()

	// Test CreateChannel
	queue.CreateChannel("channel1")

	// Test Push and Pop
	wg := sync.WaitGroup{}
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			queue.Push("channel1", index, time.Now().UnixNano())
		}(i)
	}
	wg.Wait()

	prevNTP := int64(0)
	for !queue.Empty() {
		values, ntp, ok := queue.Pop()
		if !ok {
			t.Error("Pop operation failed.")
		}
		if len(values) != 1 {
			t.Error("Incorrect data retrieved from Pop.")
		}
		if prevNTP > ntp {
			t.Error("Pop operation returned out of order data.")
		}
		prevNTP = ntp
	}

	// Test Head
	queue.Push("channel1", "data2", time.Now().UnixNano())
	values, _, ok := queue.Head()
	if !ok {
		t.Error("Head operation failed.")
	}
	if len(values) != 1 || values["channel1"] != "data2" {
		t.Error("Incorrect data retrieved from Head.")
	}

	// Test CloseChannel
	queue.CloseChannel("channel1")

	// Test StartSample and CloseSample
	queue.StartSample(60, sync.Map{})
	queue.CloseSample()
}

func TestAsynchronousTemporalQueueWithSampling(t *testing.T) {
	queue := core.NewAsynchronousTemporalQueue()

	// Test CreateChannel
	queue.CreateChannel("channel1")

	sampleWeights := sync.Map{}
	sampleWeights.Store("channel1", 1.0)
	queue.StartSample(60, sampleWeights)

	// Test Push and Pop
	wg := sync.WaitGroup{}
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			queue.Push("channel1", index, time.Now().UnixNano())
		}(i)
	}
	wg.Wait()

	prevNTP := int64(0)
	for !queue.Empty() {
		values, ntp, ok := queue.Pop()
		if !ok {
			t.Error("Pop operation failed.")
		}
		if len(values) != 1 {
			t.Error("Incorrect data retrieved from Pop.")
		}
		if prevNTP > ntp {
			t.Error("Pop operation returned out of order data.")
		}
		prevNTP = ntp
	}
	// Test CloseSample
	queue.CloseSample()
}

// BenchmarkCreateChannel 测试创建通道的性能
func BenchmarkCreateChannel(b *testing.B) {
	// 并发数量，可根据需要调整
	concurrency := 100

	b.ResetTimer()

	// 启动并发创建通道任务
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < b.N; j++ {
				key := "channel_" + time.Now().String()
				queue := core.NewAsynchronousTemporalQueue()
				queue.CreateChannel(key)
			}
		}()
	}
	wg.Wait()
}

// BenchmarkCloseChannel 测试关闭通道的性能
func BenchmarkCloseChannel(b *testing.B) {
	// 创建异步时间队列实例
	queue := core.NewAsynchronousTemporalQueue()

	// 并发数量，可根据需要调整
	concurrency := 100

	// 创建通道
	for i := 0; i < concurrency; i++ {
		key := "channel_" + time.Now().String()
		queue.CreateChannel(key)
	}

	b.ResetTimer()

	// 启动并发关闭通道任务
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < b.N; j++ {
				key := "channel_" + time.Now().String()
				queue.CloseChannel(key)
			}
		}()
	}
	wg.Wait()
}

// BenchmarkPush 测试推送任务的性能
func BenchmarkPush(b *testing.B) {
	// 创建异步时间队列实例
	queue := core.NewAsynchronousTemporalQueue()

	// 并发数量，可根据需要调整
	concurrency := 100

	b.ResetTimer()

	// 启动并发推送任务
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < b.N; j++ {
				key := "channel_" + time.Now().String()
				queue.Push(key, "test_value", time.Now().UnixNano())
			}
		}()
	}
	wg.Wait()
}

// BenchmarkPop 测试弹出任务的性能
func BenchmarkPop(b *testing.B) {
	// 创建异步时间队列实例
	queue := core.NewAsynchronousTemporalQueue()

	// 并发数量，可根据需要调整
	concurrency := 100

	b.ResetTimer()

	// 启动并发推送任务
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < b.N; j++ {
				queue.Pop()
			}
		}()
	}
	wg.Wait()
}

// BenchmarkHead 测试获取队首任务的性能
func BenchmarkHead(b *testing.B) {
	// 创建异步时间队列实例
	queue := core.NewAsynchronousTemporalQueue()

	// 并发数量，可根据需要调整
	concurrency := 100

	b.ResetTimer()

	// 启动并发推送任务
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < b.N; j++ {
				queue.Head()
			}
		}()
	}
	wg.Wait()
}
