package main

import (
	"Dekvium/common"
	"Dekvium/config"
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"
)

func sendRequestToAllPeers(peers []config.Peer, req common.ClientRequest) []common.Message {
	payload, err := json.Marshal(req)
	if err != nil {
		fmt.Printf("Failed to marshal request: %v\n", err)
		return nil
	}
	responses := make([]common.Message, 0)
	for _, peer := range peers {
		url := fmt.Sprintf("http://%s/client", peer.Addr)
		resp, err := http.Post(url, "application/json", bytes.NewReader(payload))
		if err != nil {
			fmt.Printf("Failed to send request to %s: %v\n", peer.Addr, err)
			continue
		}
		defer resp.Body.Close()
		var pbftMsg common.Message
		if err := json.NewDecoder(resp.Body).Decode(&pbftMsg); err != nil {
			fmt.Printf("Failed to decode response from %s: %v\n", peer.Addr, err)
			continue
		}
		responses = append(responses, pbftMsg)
	}
	return responses
}

// getMajorityResult determines the consensus result from PBFT replies
func getMajorityResult(responses []common.Message) (bool, string) {
	type result struct {
		success bool
		value   string
	}
	counts := make(map[string]int)
	values := make(map[string]string)
	for _, msg := range responses {
		// Only consider reply messages
		if msg.Type != common.Reply {
			continue
		}
		var resp string
		// _ = json.Unmarshal(msg.Payload, &resp)
		resp = string(msg.Payload)
		key := fmt.Sprintf("%v", resp)
		counts[key]++
		values[key] = resp
	}
	// PBFT: need n >= 3f+1, consensus is 2f+1
	majority := 0
	var majorityKey string
	for k, v := range counts {
		if v > majority {
			majority = v
			majorityKey = k
		}
	}
	if majority >= (len(responses)-1)/3*2+1 {
		parts := strings.SplitN(majorityKey, "|", 2)
		success := parts[0] == "true"
		value := ""
		if len(parts) > 1 {
			value = parts[1]
		}
		return success, value
	}
	return false, ""
}

func main() {
	configPath := flag.String("c", "", "Path to config file")
	flag.Parse()

	if *configPath == "" {
		fmt.Println("Usage: client -c <config.yaml>")
		os.Exit(1)
	}

	cfg := config.LoadConfig(*configPath)
	fmt.Printf("Loaded config for client. %d PBFT peers.\n", len(cfg.Peers))

	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print("Dekvium> ")
		line, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("Error reading input:", err)
			continue
		}
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		parts := strings.Fields(line)
		if len(parts) < 2 {
			fmt.Println("Usage: set <key> <value> | get <key>")
			continue
		}
		cmd := strings.ToLower(parts[0])
		var req common.ClientRequest
		switch cmd {
		case "set":
			if len(parts) != 3 {
				fmt.Println("Usage: set <key> <value>")
				continue
			}
			req = common.ClientRequest{
				Operation: "set",
				Key:       parts[1],
				Value:     parts[2],
			}
		case "get":
			if len(parts) != 2 {
				fmt.Println("Usage: get <key>")
				continue
			}
			req = common.ClientRequest{
				Operation: "get",
				Key:       parts[1],
			}
		default:
			fmt.Println("Unknown command. Use 'set' or 'get'.")
			continue
		}

		fmt.Printf("Sending request to all peers...\n")
		responses := sendRequestToAllPeers(cfg.Peers, req)
		if len(responses) == 0 {
			fmt.Println("No responses received.")
			continue
		}
		success, value := getMajorityResult(responses)
		if success {
			if req.Operation == "get" {
				fmt.Printf("GET %s = %s (PBFT consensus)\n", req.Key, value)
			} else {
				fmt.Printf("SET %s OK (PBFT consensus)\n", req.Key)
			}
		} else {
			fmt.Println("Consensus not reached or operation failed.")
		}
		time.Sleep(200 * time.Millisecond) // avoid flooding
	}
}
