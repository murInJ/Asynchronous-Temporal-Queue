package core

import (
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestAsynchronousTemporalQueue(t *testing.T) {
	t.Run("CreateChannel", func(t *testing.T) {
		queue := NewAsynchronousTemporalQueue()
		key := "test_channel"

		queue.CreateChannel(key)

		_, ok := queue.channelMap.Load(key)
		if !ok {
			t.Errorf("Failed to create channel with key %s", key)
		}
	})

	t.Run("CloseChannel", func(t *testing.T) {
		queue := NewAsynchronousTemporalQueue()
		key := "test_channel"
		queue.CreateChannel(key)

		queue.CloseChannel(key)
		runtime.Gosched()
		_, ok := queue.channelMap.Load(key)
		if ok {
			t.Errorf("Failed to close channel with key %s", key)
		}
	})

	t.Run("Push", func(t *testing.T) {
		queue := NewAsynchronousTemporalQueue()
		key := "test_channel"
		queue.CreateChannel(key)

		value := "test_value"
		NTP := time.Now().UnixNano()

		queue.Push(key, value, NTP)

		item, _ := queue.channelMap.Load(key)
		queueItem := item.(*asynchronousTemporalQueueItem)

		if queueItem.queue.Empty() {
			t.Error("Failed to push value to the queue")
		}
	})

	t.Run("Pop", func(t *testing.T) {
		queue := NewAsynchronousTemporalQueue()
		key := "test_channel"
		queue.CreateChannel(key)

		value := "test_value"
		NTP := time.Now().UnixNano()

		queue.Push(key, value, NTP)

		values, NTP, ok := queue.Pop()

		if !ok || len(values) != 1 || values[key] != value {
			t.Errorf("Failed to pop value from the queue. Got: %v, Expected: {%s: %s}", values, key, value)
		}

		if _, ok := queue.channelMap.Load(key); !ok {
			t.Errorf("Channel should not be closed after popping a value")
		}
	})

	t.Run("Head", func(t *testing.T) {
		queue := NewAsynchronousTemporalQueue()
		key := "test_channel"
		queue.CreateChannel(key)

		value := "test_value"
		NTP := time.Now().UnixNano()

		queue.Push(key, value, NTP)

		values, NTP, ok := queue.Head(key)

		if !ok || len(values) != 1 || values[key] != value {
			t.Errorf("Failed to get head value from the queue. Got: %v, Expected: {%s: %s}", values, key, value)
		}

		if _, ok := queue.channelMap.Load(key); !ok {
			t.Errorf("Channel should not be closed after getting head value")
		}
	})
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
				queue := NewAsynchronousTemporalQueue()
				queue.CreateChannel(key)
			}
		}()
	}
	wg.Wait()
}

// BenchmarkCloseChannel 测试关闭通道的性能
func BenchmarkCloseChannel(b *testing.B) {
	// 创建异步时间队列实例
	queue := NewAsynchronousTemporalQueue()

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
	queue := NewAsynchronousTemporalQueue()

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
	queue := NewAsynchronousTemporalQueue()

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
	queue := NewAsynchronousTemporalQueue()

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
				queue.Head("test_key")
			}
		}()
	}
	wg.Wait()
}
