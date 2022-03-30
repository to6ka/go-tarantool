// Run Tarantool instance before example execution:
// Terminal 1:
// $ TT_LISTEN=3013 TT_WORK_DIR=$(mktemp -d -t 'tarantool.XXX') tarantool config.lua
//
// Terminal 2:
// $ go test -v example_test.go
package tarantool_test

// The line "`github.com/tarantool/go-tarantool`" in the
// `import(...)` section brings in all Tarantool-related functions and
// structures.
import (
	"fmt"
	"github.com/tarantool/go-tarantool"
)

// Example walks a new user through the steps required to insert a new tuple
// into Tarantool's space.
func Example_walkingThrough() {
	// The line beginning with "Opts :=" sets up the options for
	// Connect(). In this example, there is only one thing in the structure, a user
	// name. The structure can also contain:
	//  - Pass (password),
	//  - Timeout (maximum number of milliseconds to wait before giving up),
	//  - Reconnect (number of seconds to wait before retrying if a
	//  connection fails),
	//  - MaxReconnect (maximum number of times to retry).
	opts := tarantool.Opts{User: "guest"}
	// The line containing "tarantool.Connect" is essential for
	// beginning any session. There are two parameters:
	//  - a string with host:port format or path to a UNIX socket, and
	//  - the optional structure that was set up earlier.
	// The err structure will be nil if there is no error,
	// otherwise it will have a description which can be retrieved with err.Error().
	conn, err := tarantool.Connect("127.0.0.1:3301", opts)
	if err != nil {
		fmt.Println("Connection refused:", err)
	}
	// The Insert request, like almost all requests, is preceded by
	// "conn." which is the name of the object that was returned by Connect().
	// There are two parameters:
	//  - a space number (it could just as easily have been a space name), and
	//  - a tuple.
	resp, err := conn.Insert(999, []interface{}{99999, "BB"})
	if err != nil {
		fmt.Println("Error", err)
		fmt.Println("Code", resp.Code)
	}
}
