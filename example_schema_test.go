// Start Tarantool instance before example execution:
// Terminal 1:
// $ TT_LISTEN=3013 TT_WORK_DIR=$(mktemp -d -t 'tarantool.XXX') tarantool config.lua
//
// Terminal 2:
// $ go test -v example_schema_test.go
package tarantool_test

import (
	"fmt"
	"log"
	"time"

	"github.com/tarantool/go-tarantool"
)

// Example demonstrates how to retrieve information about space schema.
// Learn more about data model in Tarantool's documentation -
// https://www.tarantool.io/en/doc/latest/book/box/data_model/.
func Example_spaceSchema() {
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

	// Save Schema to local variable to avoid races
	schema := client.Schema
	if schema.SpacesById == nil {
		log.Fatalf("schema.SpacesById is nil")
	}
	if schema.Spaces == nil {
		log.Fatalf("schema.Spaces is nil")
	}

	// Access Space objects by name or id
	space1 := schema.Spaces["test"]
	space2 := schema.SpacesById[514] // it's a map
	fmt.Printf("Space 1 ID %d %s %s\n", space1.Id, space1.Name, space1.Engine)
	fmt.Printf("Space 1 ID %d %t\n", space1.FieldsCount, space1.Temporary)

	// Access index information by name or id
	index1 := space1.Indexes["primary"]
	index2 := space2.IndexesById[3] // it's a map
	fmt.Printf("Index %d %s\n", index1.Id, index1.Name)

	// Access index fields information by index
	indexField1 := index1.Fields[0] // It's a slice
	indexField2 := index2.Fields[1] // It's a slice
	fmt.Println(indexField1, indexField2)

	// Access space fields information by name or id (index)
	spaceField1 := space2.Fields["name0"]
	spaceField2 := space2.FieldsById[3]
	fmt.Printf("SpaceField 1 %s %s\n", spaceField1.Name, spaceField1.Type)
	fmt.Printf("SpaceField 2 %s %s\n", spaceField2.Name, spaceField2.Type)

	// Output:
	// Space 1 ID 512 test memtx
	// Space 1 ID 0 false
	// Index 0 primary
	// &{0 unsigned} &{2 string}
	// SpaceField 1 name0 unsigned
	// SpaceField 2 name3 unsigned

}
