package dekvium

import (
	"flag"
	"fmt"
	"os"

	"Dekvium/config"
	"Dekvium/pbft"
)

func main() {
	configPath := flag.String("c", "", "Path to config file")
	flag.Parse()

	if *configPath == "" {
		fmt.Println("Usage: daemon -c <config.yaml>")
		os.Exit(1)
	}

	cfg := config.LoadConfig(*configPath)
	node := pbft.NewNode(*cfg)

	fmt.Printf("Node %d started with %d peers\n", node.Config.ID, len(node.Config.Peers))
	select {} // Block forever
}
