package core

import (
	"runtime"
	"sync"
	"time"
)

type AsynchronousTemporalQueue struct {
	channelMap     sync.Map
	durationWindow int64
	curNTP         int64
	sampleMode     bool
	sampleWeights  sync.Map
	item_buffer    []map[string]any
	out            *asynchronousTemporalQueueItem
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

// taskSample 方法用于对队列中的数据进行采样。
func (q *AsynchronousTemporalQueue) taskSample() {
	for q.sampleMode { // 当 sampleMode 为真时，执行采样循环。
		clear(q.item_buffer) // 清空 item_buffer，这是队列的内部缓冲区。

		max_index := 0                      // 定义 max_index 用于跟踪最大权重的索引。
		max_val := 0.0                      // 定义 max_val 用于跟踪最大权重值。
		approxy_res := make(map[string]any) // 创建一个映射，用于存储近似结果。

		for { // 开始一个无限循环，用于处理队列中的数据。
			// 从队列中弹出一个元素，包括其值、NTP时间戳和成功标志。
			values, ntp, ok := q.pop()

			if ok { // 如果弹出成功（ok 为真）。
				if ntp-q.curNTP < q.durationWindow { // 如果当前 NTP 时间戳与 curNTP 的差值小于 durationWindow。
					sumWeight := 0.0                 // 初始化权重和。
					for key, value := range values { // 遍历 values 中的每个键值对。
						if weight, ok := q.sampleWeights.Load(key); ok { // 如果键对应的权重存在。
							sumWeight += weight.(float64) // 累加权重。
						}
						approxy_res[key] = value // 将值添加到近似结果映射中。
					}
					q.item_buffer = append(q.item_buffer, values) // 将当前的 values 添加到 item_buffer 中。

					// 如果当前累加的权重大于或等于之前的最大权重，更新最大权重和索引。
					if sumWeight >= max_val {
						max_val = sumWeight
						max_index = len(q.item_buffer) - 1
					}
				} else { // 如果 NTP 时间戳与 curNTP 的差值不小于 durationWindow。
					q.curNTP = ntp // 更新 curNTP 为当前的 NTP 时间戳。
					// 将 item_buffer 中最大权重对应的元素复制到近似结果映射中。
					for key, value := range q.item_buffer[max_index] {
						approxy_res[key] = value
					}

					// 如果 item_buffer 不为空，将近似结果和当前的 NTP 时间戳推送到输出队列。
					if len(q.item_buffer) != 0 {
						q.out.queue.Push(approxy_res, q.curNTP)
					}
					break // 退出循环，因为我们已经处理了所有需要的数据。
				}
			} else { // 如果弹出失败（ok 为假）。
				runtime.Gosched() // 让出当前 goroutine，以便其他 goroutine 可以运行。
			}
		}
	}
}

func (q *AsynchronousTemporalQueue) StartSample(sampleRate int, sampleWeights sync.Map) {
	sampleWeights.Range(func(key, value any) bool {
		if _, ok := q.sampleWeights.Load(key); !ok {
			if _, ok = q.channelMap.Load(key); ok {
				q.sampleWeights.Store(key, value)
			}
		}
		return true
	})
	intervalInSeconds := 1.0 / float64(sampleRate)
	q.durationWindow = int64(intervalInSeconds * 1000000000)
	if q.sampleMode {
		return
	}
	q.item_buffer = make([]map[string]any, 0)
	q.out = NewAsynchronousTemporalQueueItem()
	q.curNTP = time.Now().UnixNano()
	q.sampleMode = true
	go q.taskSample()
}

func (q *AsynchronousTemporalQueue) CloseSample() {
	q.sampleMode = false
}

// (q *AsynchronousTemporalQueue) CreateChannel 根据给定的键（key）在异步时间队列（q）中创建一个新的通道。
//
// 参数 key string: 用于唯一标识新通道的字符串键。
//
// 函数首先检查队列中是否已存在与给定键关联的通道。如果不存在（即ok为false），则创建一个新的AsynchronousTemporalQueueItem，并将其存储到队列的channelMap中，以键key作为索引。
func (q *AsynchronousTemporalQueue) CreateChannel(key string) {
	if _, ok := q.channelMap.Load(key); !ok {
		q.channelMap.Store(key, NewAsynchronousTemporalQueueItem())
	}
}

// (q *AsynchronousTemporalQueue) CloseChannel 关闭异步时间队列（q）中与给定键（key）关联的通道。
//
// 参数 key string: 要关闭的通道的字符串键。
//
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
//
// 参数：
//
//	key string: 目标通道的字符串键。
//	value any: 要添加到通道的任务数据。
//	NTP int64: 任务关联的NTP时间戳（单位：纳秒）。
//
// 函数首先从队列的channelMap中加载与键key对应的值（通道项）。若该键存在且加载成功（ok为true），执行以下操作：
// 1. 检查通道项的_close标志，确保通道未被关闭。若通道未关闭，继续执行。
// 2. 增加通道项的_wg计数器，表示开始一个新任务。
// 3. 将任务数据（value）及其NTP时间戳（NTP）推入通道项的queue中。
// 4. 减少通道项的_wg计数器，表示新任务添加完毕。
//
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
//
//	values map[string]any: 包含弹出任务数据及其所属通道键的映射。键为通道键（string类型），值为任务数据（any类型）。
//	NTP int64: 当前系统时间对应的NTP时间戳（单位：纳秒）。
//	ok bool: 若成功弹出至少一个任务，则返回true；否则返回false。
//
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
func (q *AsynchronousTemporalQueue) pop() (values map[string]any, NTP int64, ok bool) {
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

func (q *AsynchronousTemporalQueue) Pop() (values map[string]any, NTP int64, ok bool) {
	if q.sampleMode {
		v, ntp, ok := q.out.queue.Pop()
		if ok {
			return v.(map[string]any), ntp, true
		} else {
			return q.pop()
		}
	}
	return q.pop()
}

// (q *AsynchronousTemporalQueue) Head 获取异步时间队列（q）中与给定键（key）关联的通道的队首任务数据（按NTP时间戳排序），并返回一个包含所有队首任务数据及其所属通道键的映射，以及当前系统时间对应的NTP时间戳。
// 参数：
//
//	key string: 目标通道的字符串键。
//
// 返回值：
//
//	values map[string]any: 包含队首任务数据及其所属通道键的映射。键为通道键（string类型），值为队首任务数据（any类型）。
//	NTP int64: 当前系统时间对应的NTP时间戳（单位：纳秒）。
//	ok bool: 若成功获取至少一个队首任务，则返回true；否则返回false。
//
// 函数执行流程如下：
//  1. 初始化结果映射（results）、待处理通道键列表（keys）及当前系统时间对应的NTP时间戳（curNTP）。
//  2. 遍历队列（q）中的所有通道项（channelMap），查找最早到期的任务（按NTP时间戳排序）：
//     a. 若通道项未关闭且非空，则获取其队列头任务的NTP时间戳。
//     b. 根据当前系统时间与队列头任务NTP时间戳的关系，更新keys列表和curNTP。
//  3. 对于keys列表中的每个通道键，再次检查其对应通道项是否符合条件（未关闭且非空），并尝试获取队首任务数据：
//     a. 获取队首任务数据。
//     b. 若获取成功，将任务数据添加到结果映射（results）。
//  4. 检查结果映射（results）是否为空。若为空，返回nil、0和false；否则返回结果映射、当前NTP时间戳和true。
func (q *AsynchronousTemporalQueue) head() (values map[string]any, NTP int64, ok bool) {
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

func (q *AsynchronousTemporalQueue) Head() (values map[string]any, NTP int64, ok bool) {
	if q.sampleMode {
		v, ntp, ok := q.out.queue.Head()
		if ok {
			return v.(map[string]any), ntp, true
		} else {
			return q.head()
		}
	}
	return q.head()
}

func (q *AsynchronousTemporalQueue) Empty() bool {
	if q.sampleMode {
		return q.out.queue.Empty()
	} else {
		flag := true
		q.channelMap.Range(func(key, value any) bool {
			item := value.(*asynchronousTemporalQueueItem)
			if !item._close && !item.queue.Empty() {
				flag = false
				return true
			}
			return true
		})
		return flag
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
