package dpool

import (
	"context"
	"log"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestContext(t *testing.T) {
	assert := assert.New(t)

	pool := New()
	defer pool.Cancel()

	t.Run("test context timeout", func(t *testing.T) {
		ctx, _ := context.WithTimeout(context.Background(), time.Second)

		err := pool.Do(ctx, func() error {
			<-time.After(time.Second * 2)
			return nil
		})

		assert.EqualError(err, context.DeadlineExceeded.Error())
	})

	t.Run("test conext cancel", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())

		go func() {
			<-time.After(time.Second)
			cancel()
		}()

		err := pool.Do(ctx, func() error {
			<-time.After(time.Second * 2)
			return nil
		})

		assert.EqualError(err, context.Canceled.Error())
	})
}

func TestAsync(t *testing.T) {
	assert := assert.New(t)

	pool := New()
	defer pool.Cancel()

	err := pool.Do(context.Background(), func() error {
		log.Printf("execute")
		return nil
	})

	assert.NoError(err)
}

func TestDoAsync(t *testing.T) {
	var (
		num int32
	)

	assert := assert.New(t)
	wg := &sync.WaitGroup{}

	pool := New()
	defer pool.Cancel()

	for i := 0; i < 1000; i++ {
		wg.Add(1)
		pool.DoAsync(context.Background(), func() error {
			defer wg.Done()
			atomic.AddInt32(&num, 1)
			return nil
		})
	}

	wg.Wait()
	assert.Equal(int32(1000), num)
}
