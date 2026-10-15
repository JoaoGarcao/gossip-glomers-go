package main

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var msgValues = make(map[uint64]bool)

var lock sync.RWMutex

var neighbors []string

func main() {
	node := maelstrom.NewNode()

	node.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		value := uint64(body["message"].(float64))

		lock.Lock()
		if !msgValues[value] {
			for _, neighbor := range neighbors {
				go func(dest string, val uint64) {
					neighborMsg := map[string]any{
						"type":    "neighbor",
						"message": val,
					}

					success := make(chan bool, 1)

					for {
						node.RPC(dest, neighborMsg, func(reply maelstrom.Message) error {
							select {
							case success <- true:

							default:
							}

							return nil
						})

						select {
						case <-success:
							return

						case <-time.After(1 * time.Second):
						}
					}
				}(neighbor, value)

			}

			msgValues[value] = true
		}
		lock.Unlock()

		replyBody := map[string]any{
			"type": "broadcast_ok",
		}

		return node.Reply(msg, replyBody)
	})

	node.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		body["type"] = "read_ok"

		lock.RLock()

		localMsgValues := make([]uint64, 0, len(msgValues))
		for value := range msgValues {
			localMsgValues = append(localMsgValues, value)
		}

		lock.RUnlock()

		body["messages"] = localMsgValues

		return node.Reply(msg, body)
	})

	node.Handle("topology", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		lock.Lock()
		neighbors = make([]string, 0)

		topologyMap := body["topology"].(map[string]any)
		for _, neighbor := range topologyMap[node.ID()].([]any) {
			neighbors = append(neighbors, neighbor.(string))
		}
		lock.Unlock()

		replyBody := map[string]any{
			"type": "topology_ok",
		}

		return node.Reply(msg, replyBody)
	})

	node.Handle("neighbor", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		value := uint64(body["message"].(float64))

		replyBody := map[string]any{
			"type": "neighbor_ok",
		}

		lock.Lock()
		if !msgValues[value] {
			for _, neighbor := range neighbors {
				go func(dest string, val uint64) {

					neighborMsg := map[string]any{
						"type":    "neighbor",
						"message": val,
					}
					success := make(chan bool, 1)

					for {
						node.RPC(dest, neighborMsg, func(reply maelstrom.Message) error {
							select {
							case success <- true:

							default:
							}

							return nil
						})

						select {
						case <-success:
							return

						case <-time.After(1 * time.Second):
						}
					}

				}(neighbor, value)
			}

			msgValues[value] = true
		}
		lock.Unlock()

		return node.Reply(msg, replyBody)
	})

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
}
