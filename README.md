# TCP based JSON RPC 1.0 Client\Server

The package combines TCP connections management from (valyala/fasthttp)[https://github.com/valyala/fasthttp] and
Go's standard [jsonrpc codec](https://pkg.go.dev/net/rpc/jsonrpc).

Here is a simple example. A server wishes to export an object of type Arith:

```go
package server

import "errors"

type Args struct {
	A, B int
}

type Quotient struct {
	Quo, Rem int
}

type Arith int

func (t *Arith) Multiply(args *Args, reply *int) error {
	*reply = args.A * args.B
	return nil
}

func (t *Arith) Divide(args *Args, quo *Quotient) error {
	if args.B == 0 {
		return errors.New("divide by zero")
	}
	quo.Quo = args.A / args.B
	quo.Rem = args.A % args.B
	return nil
}
```

The server calls:
```go
package main

import (
	"context"
	"log"
	"net"
	
	"github.com/makasim/jsonrpc"

	"server"
)

func main() {
	s := &jsonrpc.Server{
		Receivers: []interface{}{
			&server.Arith{},
		},
	}
	
	if err := s.ListenAndServe(":1234"); err != nil {
		log.Fatalf("serve: %s", err)
	}
}


```


At this point, clients can see a service "Arith" with methods "Arith.Multiply" and "Arith.Divide". 
To invoke one, a client does a remote call:

Client:
```go
package main

import (
	"context"
	"log"
	
	"github.com/makasim/jsonrpc"
	
	"server"
)

func main() {
	ctx := context.Background()
	args := &server.Args{7,8}
	var reply int
	
	c := &jsonrpc.Client{}
	
	err := c.Call(ctx, "127.0.0.1:9999", "FooService.Func", args, &reply)
	if err != nil {
		log.Fatal("call error:", err)
	}
	
	log.Println(reply)
}
```
