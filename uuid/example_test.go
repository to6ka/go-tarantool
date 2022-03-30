// To enable support of UUID in msgpack with
// [google/uuid](https://github.com/google/uuid), import tarantool/uuid
// submodule.
//
// Run Tarantool instance before example execution:
// $ cd uuid
// $ TT_LISTEN=3013 TT_WORK_DIR=$(mktemp -d -t 'tarantool.XXX') tarantool config.lua
// $ go test -v example_test.go
package uuid_test

import (
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/tarantool/go-tarantool"
	_ "github.com/tarantool/go-tarantool/uuid"
)

// Example demonstrate how to use tuples with UUID.
func Example() {
	server := "127.0.0.1:3013"
	opts := tarantool.Opts{
		Timeout:       500 * time.Millisecond,
		Reconnect:     1 * time.Second,
		MaxReconnects: 3,
		User:          "test",
		Pass:          "test",
	}
	client, err := tarantool.Connect(server, opts)
	if err != nil {
		log.Fatalf("Failed to connect: %s", err.Error())
	}

	spaceNo := uint32(524)

	id, uuidErr := uuid.Parse("c8f0fa1f-da29-438c-a040-393f1126ad39")
	if uuidErr != nil {
		log.Fatalf("Failed to prepare uuid: %s", uuidErr)
	}

	resp, err := client.Replace(spaceNo, []interface{}{id})

	fmt.Println("UUID tuple replace")
	fmt.Println("Error", err)
	fmt.Println("Code", resp.Code)
	fmt.Println("Data", resp.Data)

	// Output:
	// UUID tuple replace
	// Error <nil>
	// Code 0
	// Data [[c8f0fa1f-da29-438c-a040-393f1126ad39]]
}
