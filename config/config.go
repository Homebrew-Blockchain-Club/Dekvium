package config

import (
	"crypto/ecdsa"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

//	type Peer struct {
//		Addr   string
//		Pubkey ecdsa.PublicKey
//	}
//
// Peer holds address and public key PEM for a peer.
type Peer struct {
	Addr         string `yaml:"addr"`
	PubKey       string `yaml:"pubkey"` // PEM encoded
	ParsedPubKey *ecdsa.PublicKey
}

// Config holds all PBFT node configuration.
type Config struct {
	ID               int    `yaml:"id"`
	Peers            []Peer `yaml:"peers"`
	PrivateKey       string `yaml:"private_key"` // PEM encoded
	ParsedPrivateKey *ecdsa.PrivateKey
	ConsensusTimeout time.Duration `yaml:"consensus_timeout"`
}

// LoadConfig reads and parses the YAML config file. Panics on error.
func LoadConfig(path string) *Config {
	data, err := os.ReadFile(path)
	if err != nil {
		panic(err)
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		panic(err)
	}
	return &cfg
}
