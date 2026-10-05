package client_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/Shrutij516/paxos-txn-store/api"
	"github.com/Shrutij516/paxos-txn-store/client"
)

// This example moves 30 from one account to another in a transaction that
// may span two shards. It imports only public packages, as a program
// outside this module would. It needs a running cluster (docs/running.md),
// so it is compiled but not run by go test.
func ExampleClient_Run() {
	c, err := client.New([]string{"127.0.0.1:7001", "127.0.0.1:7002", "127.0.0.1:7003"}, client.Options{})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var id api.TxnID
	err = c.Run(ctx, func(ctx context.Context, t *client.Txn) error {
		id = t.ID()
		a, err := t.Read(ctx, "alice")
		if err != nil {
			return err
		}
		d, err := t.Read(ctx, "dave")
		if err != nil {
			return err
		}
		na, _ := strconv.Atoi(a)
		nd, _ := strconv.Atoi(d)
		if na < 30 {
			return errors.New("insufficient funds") // aborts, not retried
		}
		if err := t.Write(ctx, "alice", strconv.Itoa(na-30)); err != nil {
			return err
		}
		return t.Write(ctx, "dave", strconv.Itoa(nd+30))
	})
	switch {
	case errors.Is(err, api.ErrUnknown):
		fmt.Printf("txn %d may or may not have committed; read the balances to find out\n", id)
	case err != nil:
		fmt.Println("transfer failed:", err)
	default:
		fmt.Printf("txn %d committed\n", id)
	}
}
