package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var msgValues []uint64

var lock sync.RWMutex

func main() {
	node := maelstrom.NewNode()

	node.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		value := uint64(body["message"].(float64))

		lock.Lock()
		msgValues = append(msgValues, value)
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

		localMsgValues := make([]uint64, len(msgValues))
		copy(localMsgValues, msgValues)

		lock.RUnlock()

		body["messages"] = localMsgValues

		return node.Reply(msg, body)
	})

	node.Handle("topology", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		replyBody := map[string]any{
			"type": "topology_ok",
		}

		return node.Reply(msg, replyBody)
	})

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
}
