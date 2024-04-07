package core

import (
	"sync"
	"time"
)

type AsynchronousTemporalQueue struct {
	channelMap sync.Map
}

// NewAsynchronousTemporalQueue 创建一个新的异步时间队列实例。
//
// 返回值 *AsynchronousTemporalQueue: 返回一个初始化好的异步时间队列指针。
//
// 这个函数不接受任何参数。
func NewAsynchronousTemporalQueue() *AsynchronousTemporalQueue {
	// 初始化异步时间队列，其中channelMap使用sync.Map来保证并发安全。
	return &AsynchronousTemporalQueue{
		channelMap: sync.Map{},
	}
}

// (q *AsynchronousTemporalQueue) CreateChannel 根据给定的键（key）在异步时间队列（q）中创建一个新的通道。

// 参数 key string: 用于唯一标识新通道的字符串键。

// 函数首先检查队列中是否已存在与给定键关联的通道。如果不存在（即ok为false），则创建一个新的AsynchronousTemporalQueueItem，并将其存储到队列的channelMap中，以键key作为索引。
func (q *AsynchronousTemporalQueue) CreateChannel(key string) {
	if _, ok := q.channelMap.Load(key); !ok {
		q.channelMap.Store(key, NewAsynchronousTemporalQueueItem())
	}
}

// (q *AsynchronousTemporalQueue) CloseChannel 关闭异步时间队列（q）中与给定键（key）关联的通道。

// 参数 key string: 要关闭的通道的字符串键。

// 函数首先从队列的channelMap中加载与键key对应的值（通道项）。若该键存在且加载成功（ok为true），执行以下操作：
//  1. 将通道项的_close标志设置为true，表示该通道应被关闭。
//  2. 启动一个新的goroutine，用于等待当前正在处理的所有任务完成，并最终删除已关闭的通道。此goroutine执行如下逻辑：
//     a. 无限循环，直到满足退出条件。
//     b. 使用_item._wg等待所有正在执行的任务完成。
//     c. 检查通道项的queue是否为空。若为空，表示所有任务已完成，此时从队列的channelMap中删除键key，并退出goroutine。
func (q *AsynchronousTemporalQueue) CloseChannel(key string) {
	if v, ok := q.channelMap.Load(key); ok {
		item := v.(*asynchronousTemporalQueueItem)
		item._close = true
		go func() {
			for {
				item._wg.Wait()
				if item.queue.Empty() {
					q.channelMap.Delete(key)
					return
				}
			}
		}()

	}
}

// (q *AsynchronousTemporalQueue) Push 向异步时间队列（q）中与给定键（key）关联的通道添加一个带有NTP时间戳的新任务（value）。

// 参数：
//   key string: 目标通道的字符串键。
//   value any: 要添加到通道的任务数据。
//   NTP int64: 任务关联的NTP时间戳（单位：纳秒）。

// 函数首先从队列的channelMap中加载与键key对应的值（通道项）。若该键存在且加载成功（ok为true），执行以下操作：
// 1. 检查通道项的_close标志，确保通道未被关闭。若通道未关闭，继续执行。
// 2. 增加通道项的_wg计数器，表示开始一个新任务。
// 3. 将任务数据（value）及其NTP时间戳（NTP）推入通道项的queue中。
// 4. 减少通道项的_wg计数器，表示新任务添加完毕。

// 注意：若给定键对应的通道已关闭，此函数将不会向其添加任务。
func (q *AsynchronousTemporalQueue) Push(key string, value any, NTP int64) {
	if v, ok := q.channelMap.Load(key); ok {
		item := v.(*asynchronousTemporalQueueItem)
		if !item._close {
			item._wg.Add(1)
			item.queue.Push(value, NTP)
			item._wg.Done()
		}
	}
}

// (q *AsynchronousTemporalQueue) Pop 从异步时间队列（q）中弹出最早到期的任务（按NTP时间戳排序），并返回一个包含所有弹出任务的数据及其所属通道键的映射，以及当前系统时间对应的NTP时间戳。

// 返回值：
//   values map[string]any: 包含弹出任务数据及其所属通道键的映射。键为通道键（string类型），值为任务数据（any类型）。
//   NTP int64: 当前系统时间对应的NTP时间戳（单位：纳秒）。
//   ok bool: 若成功弹出至少一个任务，则返回true；否则返回false。

// 函数执行流程如下：
//  1. 初始化结果映射（results）、待处理通道键列表（keys）及当前系统时间对应的NTP时间戳（curNTP）。
//  2. 遍历队列（q）中的所有通道项（channelMap），查找最早到期的任务（按NTP时间戳排序）：
//     a. 若通道项未关闭且非空，则获取其队列头任务的NTP时间戳。
//     b. 根据当前系统时间与队列头任务NTP时间戳的关系，更新keys列表和curNTP。
//  3. 对于keys列表中的每个通道键，再次检查其对应通道项是否符合条件（未关闭且非空），并尝试弹出任务：
//     a. 增加通道项的_wg计数器，表示开始处理任务。
//     b. 弹出任务数据并减少通道项的_wg计数器。
//     c. 若弹出成功，将任务数据添加到结果映射（results）。
//  4. 检查结果映射（results）是否为空。若为空，返回nil、0和false；否则返回结果映射、当前NTP时间戳和true。
func (q *AsynchronousTemporalQueue) Pop() (values map[string]any, NTP int64, ok bool) {
	results := make(map[string]any)
	keys := make([]string, 0)
	curNTP := time.Now().UnixNano()

	q.channelMap.Range(func(key, value any) bool {
		item := value.(*asynchronousTemporalQueueItem)
		if !item._close && !item.queue.Empty() {
			_, NTP, ok := item.queue.Head()
			if ok {
				if curNTP == NTP {
					keys = append(keys, key.(string))
				}
				if curNTP > NTP {
					clear(keys)
					keys = append(keys, key.(string))
					curNTP = NTP
				}
			}
		}
		return true
	})

	for _, key := range keys {
		if v, ok := q.channelMap.Load(key); ok {
			item := v.(*asynchronousTemporalQueueItem)
			if !item._close && !item.queue.Empty() {
				item._wg.Add(1)
				value, _, ok := item.queue.Pop()
				item._wg.Done()
				if ok {
					results[key] = value
				}
			}
		}
	}

	if len(results) == 0 {
		return nil, 0, false
	} else {
		return results, curNTP, true
	}
}

// (q *AsynchronousTemporalQueue) Head 获取异步时间队列（q）中与给定键（key）关联的通道的队首任务数据（按NTP时间戳排序），并返回一个包含所有队首任务数据及其所属通道键的映射，以及当前系统时间对应的NTP时间戳。

// 参数：
//   key string: 目标通道的字符串键。

// 返回值：
//   values map[string]any: 包含队首任务数据及其所属通道键的映射。键为通道键（string类型），值为队首任务数据（any类型）。
//   NTP int64: 当前系统时间对应的NTP时间戳（单位：纳秒）。
//   ok bool: 若成功获取至少一个队首任务，则返回true；否则返回false。

// 函数执行流程如下：
//  1. 初始化结果映射（results）、待处理通道键列表（keys）及当前系统时间对应的NTP时间戳（curNTP）。
//  2. 遍历队列（q）中的所有通道项（channelMap），查找最早到期的任务（按NTP时间戳排序）：
//     a. 若通道项未关闭且非空，则获取其队列头任务的NTP时间戳。
//     b. 根据当前系统时间与队列头任务NTP时间戳的关系，更新keys列表和curNTP。
//  3. 对于keys列表中的每个通道键，再次检查其对应通道项是否符合条件（未关闭且非空），并尝试获取队首任务数据：
//     a. 获取队首任务数据。
//     b. 若获取成功，将任务数据添加到结果映射（results）。
//  4. 检查结果映射（results）是否为空。若为空，返回nil、0和false；否则返回结果映射、当前NTP时间戳和true。
func (q *AsynchronousTemporalQueue) Head(key string) (values map[string]any, NTP int64, ok bool) {
	results := make(map[string]any)
	keys := make([]string, 0)
	curNTP := time.Now().UnixNano()

	q.channelMap.Range(func(key, value any) bool {
		item := value.(*asynchronousTemporalQueueItem)
		if !item._close && !item.queue.Empty() {
			_, NTP, ok := item.queue.Head()
			if ok {
				if curNTP == NTP {
					keys = append(keys, key.(string))
				}
				if curNTP > NTP {
					clear(keys)
					keys = append(keys, key.(string))
					curNTP = NTP
				}
			}
		}
		return true
	})

	for _, key := range keys {
		if v, ok := q.channelMap.Load(key); ok {
			item := v.(*asynchronousTemporalQueueItem)
			if !item._close && !item.queue.Empty() {
				value, _, ok := item.queue.Head()
				if ok {
					results[key] = value
				}
			}
		}
	}

	if len(results) == 0 {
		return nil, 0, false
	} else {
		return results, curNTP, true
	}
}

type asynchronousTemporalQueueItem struct {
	queue  *PriorityQueue[any, int64]
	_close bool
	_wg    *sync.WaitGroup
}

func NewAsynchronousTemporalQueueItem() *asynchronousTemporalQueueItem {
	return &asynchronousTemporalQueueItem{
		queue:  NewMinPriorityQueue[any, int64](),
		_close: false,
		_wg:    &sync.WaitGroup{},
	}
}
