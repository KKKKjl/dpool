package dpool

import (
	"context"
	"errors"
	"log"
	"runtime/debug"
	"sync/atomic"
)

const (
	_defaultConcurrentNum = 100
)

var (
	ErrPoolClosed = errors.New("pool has already been closed")
)

type (
	Task struct {
		ctx   context.Context
		fn    TaskFn
		errCh chan error
	}

	Dpool struct {
		taskQueue     chan *Task
		concurrentNum int
		pendingNum    int64
		cancel        context.CancelFunc
		closed        atomic.Bool
	}

	Options func(*Dpool)

	TaskFn func() error
)

func WithConcurrent(concurrentNum int) Options {
	return func(d *Dpool) {
		d.concurrentNum = concurrentNum
	}
}

func New(opts ...Options) *Dpool {
	var (
		ctx    context.Context
		cancel context.CancelFunc
		pool   *Dpool
	)

	ctx, cancel = context.WithCancel(context.Background())

	pool = &Dpool{
		cancel:        cancel,
		concurrentNum: _defaultConcurrentNum,
	}

	for _, opt := range opts {
		opt(pool)
	}

	pool.taskQueue = make(chan *Task, pool.concurrentNum*2)

	pool.dispatch(ctx)

	return pool
}

// block mode, block until task completed
func (d *Dpool) Do(ctx context.Context, fn TaskFn) error {
	task := &Task{
		fn:    fn,
		errCh: make(chan error),
		ctx:   ctx,
	}

	if _, err := d.attach(task); err != nil {
		return err
	}

	return <-task.errCh
}

// async mode, tasks are executed asynchronously and would not wait for them to complete
func (d *Dpool) DoAsync(ctx context.Context, fn TaskFn) (*Task, error) {
	task := &Task{
		ctx:   ctx,
		fn:    fn,
		errCh: make(chan error, 1),
	}

	return d.attach(task)
}

func (d *Dpool) attach(task *Task) (*Task, error) {
	if d.IsClosed() {
		return task, ErrPoolClosed
	}

	atomic.AddInt64(&d.pendingNum, 1)
	defer atomic.AddInt64(&d.pendingNum, -1)

	d.taskQueue <- task
	return task, nil
}

func (d *Dpool) dispatch(ctx context.Context) {
	for i := 0; i < d.concurrentNum; i++ {
		go d.consume(ctx)
	}
}

func (d *Dpool) consume(ctx context.Context) {
	defer func() {
		if err := recover(); err != nil {
			log.Printf("Recover, err %v with stack: %s", err, string(debug.Stack()))
		}
	}()

	for {
		select {
		case task := <-d.taskQueue:
			closed := make(chan struct{}, 1)
			go func() {
				select {
				case <-task.ctx.Done():
					task.errCh <- task.ctx.Err()
					return
				case <-closed:
					return
				}
			}()

			select {
			case task.errCh <- task.fn():
			default:
			}

			close(closed)
		case <-ctx.Done():
			return
		}
	}
}

func (p *Dpool) PendingNum() int64 {
	return atomic.LoadInt64(&p.pendingNum)
}

// check wheither pool is closed or not
func (p *Dpool) IsClosed() bool {
	return p.closed.Load()
}

// cancel all pending task
func (p *Dpool) Cancel() {
	if p.closed.CompareAndSwap(false, true) {
		p.cancel()
	}
}
