// The Go amqp091-go side of bench/run.sh, scenario for scenario the same
// as bench/slang/main.sl.
//
//	bench publish|confirm|consume|latency URL N SIZE
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
		fmt.Fprintln(os.Stderr, what+": "+err.Error())
		os.Exit(1)
	}
}

func main() {
	mode, url := os.Args[1], os.Args[2]
	n, _ := strconv.Atoi(os.Args[3])
	size, _ := strconv.Atoi(os.Args[4])
	conn, err := amqp.Dial(url)
	must(err, "dial")
	defer conn.Close()
	ch, err := conn.Channel()
	must(err, "channel")
	body := make([]byte, size)
	ctx := context.Background()
	switch mode {
	case "publish", "confirm":
		_, err = ch.QueueDeclare("bench.q", false, false, false, false, nil)
		must(err, "declare")
		_, err = ch.QueuePurge("bench.q", false)
		must(err, "purge")
		var confirms chan amqp.Confirmation
		if mode == "confirm" {
			must(ch.Confirm(false), "confirm")
			confirms = ch.NotifyPublish(make(chan amqp.Confirmation, 1000))
		}
		t0 := time.Now()
		pending := 0
		for i := 0; i < n; i++ {
			must(ch.PublishWithContext(ctx, "", "bench.q", false, false, amqp.Publishing{Body: body}), "publish")
			if mode == "confirm" {
				pending++
				if pending == 1000 {
					for ; pending > 0; pending-- {
						c := <-confirms
						if !c.Ack {
							must(fmt.Errorf("nacked"), "confirm")
						}
					}
				}
			}
		}
		for ; pending > 0; pending-- {
			<-confirms
		}
		_, err = ch.QueueDeclarePassive("bench.q", false, false, false, false, nil)
		must(err, "barrier")
		fmt.Printf("rate=%d\n", int(float64(n)/time.Since(t0).Seconds()))
	case "consume":
		must(ch.Qos(500, 0, false), "qos")
		msgs, err := ch.Consume("bench.q", "", false, false, false, false, nil)
		must(err, "consume")
		t0 := time.Now()
		for i := 0; i < n; i++ {
			d := <-msgs
			must(d.Ack(false), "ack")
		}
		fmt.Printf("rate=%d\n", int(float64(n)/time.Since(t0).Seconds()))
	case "latency":
		q, err := ch.QueueDeclare("", false, true, true, false, nil)
		must(err, "declare")
		pconn, err := amqp.Dial(url)
		must(err, "dial")
		defer pconn.Close()
		pch, err := pconn.Channel()
		must(err, "channel")
		msgs, err := ch.Consume(q.Name, "", true, false, false, false, nil)
		must(err, "consume")
		lat := make([]int, 0, n)
		for i := 0; i < n; i++ {
			t0 := time.Now()
			must(pch.PublishWithContext(ctx, "", q.Name, false, false, amqp.Publishing{Body: body}), "publish")
			<-msgs
			lat = append(lat, int(time.Since(t0).Microseconds()))
		}
		sort.Ints(lat)
		fmt.Printf("p50_us=%d p99_us=%d\n", lat[n/2], lat[n*99/100])
	}
}
