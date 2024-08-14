package quit

import (
	"github.com/sirupsen/logrus"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

func WaitForQuitSignal() {
	// Wait for interrupt signal to gracefully shutdown the server with a timeout of 5 seconds.
	quit := make(chan os.Signal, 1)
	// kill (no param) default send syscall.SIGTERM
	// kill -2 is syscall.SIGINT
	// kill -9 is syscall.SIGKILL but can't be caught, so don't need to add it
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
}

type waitForQuitGoroutine struct {
	name string
	c    chan struct{}
}

var waitForQuitGoroutines = make([]waitForQuitGoroutine, 0)
var mutex = sync.Mutex{}

type Goroutine interface {
	Done()
}

type done struct {
	*waitForQuitGoroutine
	once sync.Once
}

func (d *done) Done() {
	d.once.Do(func() {
		d.c <- struct{}{}
	})
}

func ReportGoroutine(name string) Goroutine {
	mutex.Lock()
	defer mutex.Unlock()
	c := make(chan struct{}, 1)
	wg := waitForQuitGoroutine{
		name: name,
		c:    c,
	}

	waitForQuitGoroutines = append(waitForQuitGoroutines, wg)
	return &done{
		waitForQuitGoroutine: &wg,
	}
}

const WaitForQuitGoroutinesTimeout = time.Second * 10

func WaitForAllGoroutineEnd() {
	//	iterate all chan in reverse order
	for i := len(waitForQuitGoroutines) - 1; i >= 0; i-- {
		select {
		case <-waitForQuitGoroutines[i].c:
			logrus.Infof("Goroutine \"%s\" ended", waitForQuitGoroutines[i].name)
		case <-time.After(WaitForQuitGoroutinesTimeout):
			logrus.Errorf("Goroutine \"%s\" didn't end in time (%s)", waitForQuitGoroutines[i].name, WaitForQuitGoroutinesTimeout.String())
		}
	}
}
