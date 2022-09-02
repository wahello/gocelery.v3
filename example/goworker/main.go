// Copyright (c) 2019 Sick Yoon
// This file is part of gocelery which is released under MIT license.
// See file LICENSE for full license details.

package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/wahello/gocelery.v3"
)

func add(a, b int) int {
	return a + b
}

// exampleAddTask is integer addition task
// with named arguments
type exampleAddTask struct {
	a int
	b int
}

func (a *exampleAddTask) ParseKwargs(kwargs map[string]interface{}) error {

	kwargA, ok := kwargs["a"]
	if !ok {
		return fmt.Errorf("undefined kwarg a")
	}
	kwargAFloat, ok := kwargA.(float64)
	if !ok {
		return fmt.Errorf("malformed kwarg a")
	}
	a.a = int(kwargAFloat)
	kwargB, ok := kwargs["b"]
	if !ok {
		return fmt.Errorf("undefined kwarg b")
	}
	kwargBFloat, ok := kwargB.(float64)
	if !ok {
		return fmt.Errorf("malformed kwarg b")
	}
	a.b = int(kwargBFloat)
	return nil
}

func (a *exampleAddTask) RunTask() (interface{}, error) {
	result := a.a + a.b
	return result, nil
}

func main() {

	// create redis connection pool
	ctx := context.Background()

	redisClient := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs: []string{"localhost:6379"},
		DB:    3,
	})

	qname := "inpman"
	// initialize celery client
	cli, _ := gocelery.NewCeleryClient(
		gocelery.NewRedisBroker(&ctx, qname, redisClient),
		gocelery.NewRedisBackend(&ctx, redisClient),
		5, // number of workers
	)

	// register task

	// start workers (non-blocking call)
	cli.StartWorker()

	cli.Register("worker.add", add)
	cli.Register("worker.add_reflect", &exampleAddTask{})

	// wait for client request
	time.Sleep(30 * time.Second)

	// stop workers gracefully (blocking call)
	cleanFunc := func() { cli.StopWorker() }
	exitGracefully(cleanFunc)
}

func exitGracefully(cleanFunc func()) {
	c := make(chan os.Signal, 1)
	signal.Reset(syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)
	signal.Notify(c, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)

	select {
	case s := <-c:
		log.Println("receive a signal", s.String())

		cleanFunc()

		os.Exit(0)
	}
}
