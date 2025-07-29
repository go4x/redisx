package redisx

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	maxRetry = 6
)

// ==============================
// normal queue
// ==============================

func NewQueue(rc Client, key string) Queue {
	return &queueImpl{rc: rc, key: key}
}

type queueImpl struct {
	rc  Client // redis client
	key string // list key
}

func (q *queueImpl) Push(v ...any) error {
	return q.rc.LPush(context.Background(), q.key, v...).Err()
}

func (q *queueImpl) Pop() (string, error) {
	cmd := q.rc.RPop(context.Background(), q.key)
	if cmd.Err() != nil {
		return "", cmd.Err()
	}
	return cmd.Val(), nil
}

func (q *queueImpl) PopN(n int) ([]string, error) {
	cmd := q.rc.RPopCount(context.Background(), q.key, n)
	if cmd.Err() != nil {
		return []string{}, cmd.Err()
	}
	return cmd.Val(), nil
}

func (q *queueImpl) BPop() (string, error) {
	cmd := q.rc.BRPop(context.Background(), 0, q.key) // 没有数据时阻塞
	if cmd.Err() != nil {
		return "", cmd.Err()
	}
	if len(cmd.Val()) > 0 { // []string 为两个元素，第一个为key，第二个为value
		return cmd.Val()[1], nil
	}
	return "", nil
}

func (q *queueImpl) Len() uint64 {
	return uint64(q.rc.LLen(context.Background(), q.key).Val())
}

// ==============================
// unique queue
// ==============================

func NewUniqueQueue(rc Client, key string) Queue {
	// To avoid the "CROSSSLOT Keys" error in a Redis Cluster setup, we can use the
	// hash tag feature to map keys to the same slot on the cluster node. This is
	// important because certain operations, like transactions involving keys from
	// different slots, can trigger the error. By adding braces "{}" around the key
	// , only the content within the braces is used for hash calculation. This ensures
	// that keys related to operations like queues and sets are mapped to the same
	// slot, preventing errors. See: https://redis.io/docs/reference/cluster-spec/
	key = "{" + key + "}"
	setkey := key + "_set"
	return &uniqueQueue{
		queueImpl: NewQueue(rc, key).(*queueImpl),
		setkey:    setkey,
		set:       NewSet(rc, setkey),
	}
}

func createSetWhenExists(rc Client, key string) (string, Set) {
	var setExists bool
	var setkey string
	setkeyPrefix := key + "_"
	if cmd := rc.Keys(context.Background(), setkeyPrefix+"*"); cmd.Err() != nil {
		panic(cmd.Err())
	} else {
		setExists = len(cmd.Val()) > 0
		if setExists {
			setkey = cmd.Val()[len(cmd.Val())-1]
		} else {
			setkey = fmt.Sprintf("%s%d", setkeyPrefix, time.Now().Unix())
		}
	}
	return setkey, NewSet(rc, setkey)
}

type uniqueQueue struct {
	mu sync.Mutex
	*queueImpl

	set    Set
	setkey string
}

func (uq *uniqueQueue) retryPush(vs ...any) error {
	ctx := context.TODO()
	go func() {
		defer func() {
			if err := recover(); err != nil {
				fmt.Printf("error recovered: %v\n", err)
			}
		}()
		times := 0
		for {
			cmd := uq.rc.LPush(ctx, uq.key, vs...)
			if cmd.Err() == nil {
				break
			}
			times++
			if times > maxRetry {
				fmt.Println(fmt.Errorf("ERROR: push failed after retry the max %d times", maxRetry))
				break
			}
			fmt.Printf("ERROR: push failed, will retry at the %d time after %d second\n", times, 1<<times)
			time.Sleep(time.Second * (1 << times))
		}
	}()
	return nil
}

func (uq *uniqueQueue) Push(vs ...any) error {
	uq.mu.Lock()
	defer uq.mu.Unlock()

	// 去重
	bs := uq.set.IsMembers(vs...)
	var dest []any
	for i, v := range vs {
		if !bs[i] {
			dest = append(dest, v)
		}
	}
	if len(dest) == 0 {
		return nil
	}

	// use Pipliner to start transaction
	ctx := context.TODO()
	_, err := uq.queueImpl.rc.TxPipelined(ctx, func(pip redis.Pipeliner) error {
		cmd := pip.LPush(ctx, uq.key, dest...)
		if cmd.Err() != nil {
			return cmd.Err()
		}
		return pip.SAdd(context.Background(), uq.setkey, dest...).Err()
	})
	return err
}

func (uq *uniqueQueue) Pop() (string, error) {
	if s, err := uq.queueImpl.Pop(); err != nil {
		return "", err
	} else {
		// if an error occured, retry to push s into the queue
		if err := uq.set.Rem(s); err != nil {
			fmt.Printf("rem [%v] from set error, will retry %d times to push to queue: %v\n", s, maxRetry, err)
			return s, uq.retryPush(s)
			// return s, err
		}
		return s, nil
	}
}

func (uq *uniqueQueue) PopN(n int) ([]string, error) {
	//var vs []string
	//var ctx = context.TODO()
	//_, err := uq.queueImpl.rc.TxPipelined(ctx, func(pip redis.Pipeliner) error {
	//	sscmd := pip.RPopCount(ctx, uq.key, n)
	//	if sscmd.Err() != nil {
	//		return sscmd.Err()
	//	}
	//	vs = sscmd.Val()
	//	if len(vs) == 0 {
	//		return nil
	//	}
	//	var ss = make([]any, len(vs))
	//	for i, v := range vs {
	//		ss[i] = v
	//	}
	//	return pip.SRem(ctx, uq.setkey, ss...).Err()
	//})
	//return vs, err
	if vs, err := uq.queueImpl.PopN(n); err != nil {
		return vs, err
	} else {
		if len(vs) == 0 {
			return []string{}, nil
		}
		ss := make([]any, len(vs))
		for i, v := range vs {
			ss[i] = v
		}
		if err := uq.set.Rem(ss...); err != nil {
			// fmt.Printf("rem [%v] from set error, will retry %d times to push to queue: %v\n", ss, maxRetry, err)
			return vs, uq.retryPush(ss...)
		}
		return vs, nil
	}
}

func (uq *uniqueQueue) BPop() (string, error) {
	if s, err := uq.queueImpl.BPop(); err != nil { // BPop will block if the queue is empty until popped a string
		return "", err
	} else {
		// Failed to remove an element from the set, reinsert it into the queue
		if err := uq.set.Rem(s); err != nil {
			fmt.Printf("rem [%s] from set error, will retry %d times to push to queue: %v\n", s, maxRetry, err)
			return s, uq.retryPush(s)
			// return s, err
		}
		return s, err
	}
}

// ==============================
// bounded queue
// ==============================

func NewBoundedQueue(rc Client, key string, cap uint64) BoundedQueue {
	return &boundedQueue{
		queueImpl: NewQueue(rc, key).(*queueImpl),
		cap:       cap,
	}
}

type boundedQueue struct {
	*queueImpl

	cap uint64
}

func (bq *boundedQueue) Cap() uint64 {
	return bq.cap
}

func (bq *boundedQueue) Push(vs ...any) error {
	if bq.Len() >= bq.cap {
		return fmt.Errorf("quene reachs max capacity, can not push now")
	}
	return bq.queueImpl.Push(vs...)
}

// ==============================
// bounded unique queue
// ==============================

func NewBoundedUniqueQueue(rc Client, key string, cap uint64) BoundedQueue {
	return &boundedUniqueQueue{
		uniqueQueue: NewUniqueQueue(rc, key).(*uniqueQueue),
		cap:         cap,
	}
}

type boundedUniqueQueue struct {
	*uniqueQueue

	cap uint64
}

func (bq *boundedUniqueQueue) Cap() uint64 {
	return bq.cap
}

func (bq *boundedUniqueQueue) Push(vs ...any) error {
	if bq.Len() >= bq.cap {
		return fmt.Errorf("quene reachs max capacity, can not push now")
	}
	return bq.uniqueQueue.Push(vs...)
}
