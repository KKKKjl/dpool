package main

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kkkkjl/dpool"
)

func main() {
	var (
		num      int32
		expected int = 100
		wg       sync.WaitGroup
		pool     *dpool.Dpool
	)

	pool = dpool.New()
	defer pool.Cancel()

	for i := 0; i < expected; i++ {
		wg.Add(1)
		pool.DoAsync(context.Background(), func() error {
			defer wg.Done()
			<-time.After(time.Millisecond * 100)
			atomic.AddInt32(&num, 1)
			return nil
		})
	}

	wg.Wait()
	fmt.Printf("expected %d, got: %d", expected, num)
}
