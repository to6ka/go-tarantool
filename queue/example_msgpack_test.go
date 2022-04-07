// Setup queue module and start Tarantool instance before execution:
// Terminal 1:
// $ make deps
// $ TT_LISTEN=3013 tarantool queue/config.lua
//
// Terminal 2:
// $ cd queue
// $ go test -v example_test.go
package queue_test

import (
	"fmt"
	"time"

	"github.com/tarantool/go-tarantool"
	"github.com/tarantool/go-tarantool/queue"
	"gopkg.in/vmihailenco/msgpack.v2"
	"log"
)

type dummyData struct {
	Dummy bool
}

func (c *dummyData) DecodeMsgpack(d *msgpack.Decoder) error {
	var err error
	if c.Dummy, err = d.DecodeBool(); err != nil {
		return err
	}
	return nil
}

func (c *dummyData) EncodeMsgpack(e *msgpack.Encoder) error {
	return e.EncodeBool(c.Dummy)
}

// Example demonstrates an operations like Put and Take with queue and custom
// MsgPack structure.
func Example_simpleQueueCustomMsgPack() {
	opts := tarantool.Opts{
		Reconnect:     time.Second,
		Timeout:       2500 * time.Millisecond,
		MaxReconnects: 5,
		User:          "test",
		Pass:          "test",
	}
	conn, err := tarantool.Connect("127.0.0.1:3013", opts)
	if err != nil {
		log.Fatalf("connection: %s", err)
		return
	}
	defer conn.Close()

	cfg := queue.Cfg{
		Temporary:   true,
		IfNotExists: true,
		Kind:        queue.FIFO,
		Opts: queue.Opts{
			Ttl:   10 * time.Second,
			Ttr:   5 * time.Second,
			Delay: 3 * time.Second,
			Pri:   1,
		},
	}

	que := queue.New(conn, "test_queue")
	if err = que.Create(cfg); err != nil {
		log.Fatalf("queue create: %s", err)
		return
	}

	// Put data
	task, err := que.Put("test_data")
	if err != nil {
		log.Fatalf("put task: %s", err)
	}
	fmt.Println("Task id is", task.Id())

	// Take data
	task, err = que.Take() // Blocking operation
	if err != nil {
		log.Fatalf("take task: %s", err)
	}
	fmt.Println("Data is", task.Data())
	task.Ack()

	// Take typed example
	putData := dummyData{}
	// Put data
	task, err = que.Put(&putData)
	if err != nil {
		log.Fatalf("put typed task: %s", err)
	}
	fmt.Println("Task id is ", task.Id())

	takeData := dummyData{}
	// Take data
	task, err = que.TakeTyped(&takeData) // Blocking operation
	if err != nil {
		log.Fatalf("take take typed: %s", err)
	}
	fmt.Println("Data is ", takeData)
	// Same data
	fmt.Println("Data is ", task.Data())

	task, err = que.Put([]int{1, 2, 3})
	task.Bury()

	task, err = que.TakeTimeout(2 * time.Second)
	if task == nil {
		fmt.Println("Task is nil")
	}

	que.Drop()

	// Unordered output:
	// Task id is 0
	// Data is test_data
	// Task id is  0
	// Data is  {false}
	// Data is  &{false}
	// Task is nil
}
