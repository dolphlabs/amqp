// A Go amqp091-go peer for tests/live: publishes messages slang must read,
// and reads what slang published.
//
//	peer publish URL QUEUE N
//	peer consume URL QUEUE N
package main

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strconv"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func must(err error, what string) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "peer: "+what+": "+err.Error())
		os.Exit(1)
	}
}

func main() {
	mode, url, queue := os.Args[1], os.Args[2], os.Args[3]
	n, _ := strconv.Atoi(os.Args[4])
	conn, err := amqp.Dial(url)
	must(err, "dial")
	defer conn.Close()
	ch, err := conn.Channel()
	must(err, "channel")
	_, err = ch.QueueDeclare(queue, false, false, false, false, nil)
	must(err, "declare")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if mode == "publish" {
		must(ch.Confirm(false), "confirm")
		for i := 0; i < n; i++ {
			dc, err := ch.PublishWithDeferredConfirmWithContext(ctx, "", queue, false, false, amqp.Publishing{
				ContentType:   "application/json",
				CorrelationId: "go-" + strconv.Itoa(i),
				DeliveryMode:  amqp.Persistent,
				Priority:      4,
				Headers: amqp.Table{"from": "go", "n": int32(i), "big": int64(1) << 40, "ok": true,
					"pi": 3.25, "list": []interface{}{"a", int32(2)}},
				Body: []byte(fmt.Sprintf(`{"go":%d}`, i)),
			})
			must(err, "publish")
			if !dc.Wait() {
				must(fmt.Errorf("nacked"), "confirm")
			}
		}
		return
	}
	msgs, err := ch.Consume(queue, "", false, false, false, false, nil)
	must(err, "consume")
	for i := 0; i < n; i++ {
		select {
		case d := <-msgs:
			keys := make([]string, 0, len(d.Headers))
			for k := range d.Headers {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			hs := ""
			for _, k := range keys {
				hs += fmt.Sprintf("%s=%v(%T);", k, d.Headers[k], d.Headers[k])
			}
			fmt.Printf("go got %s type=%s corr=%s mode=%d headers=%s\n", d.Body, d.ContentType, d.CorrelationId,
				d.DeliveryMode, hs)
			must(d.Ack(false), "ack")
		case <-ctx.Done():
			must(ctx.Err(), "consume")
		}
	}
}
