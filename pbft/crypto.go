package pbft

import (
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"math/big"

	c "Dekvium/common"
)

// Digest computes and sets the digest (SHA256 hash) of the message payload.
func Digest(msg *c.Message) {
	data, _ := json.Marshal(msg.Payload)
	hash := sha256.Sum256(data)
	msg.Digest = fmt.Sprintf("%x", hash[:])
}

// Sign signs the hash of (msg.Type || msg.Digest) and stores the signature in msg.Signature.
func Sign(msg *c.Message, priv *ecdsa.PrivateKey) {
	typeAndDigest := append([]byte{byte(msg.Type)}, []byte(msg.Digest)...)
	hash := sha256.Sum256(typeAndDigest)
	r, s, _ := ecdsa.Sign(rand.Reader, priv, hash[:])
	sig := append(r.Bytes(), s.Bytes()...)
	msg.Signature = fmt.Sprintf("%x", sig)
}

// Verify verifies the signature in msg.Signature using pubkey and (msg.Type || msg.Digest).
func Verify(msg *c.Message, pub *ecdsa.PublicKey) bool {
	typeAndDigest := append([]byte{byte(msg.Type)}, []byte(msg.Digest)...)
	hash := sha256.Sum256(typeAndDigest)
	sigBytes, err := hex.DecodeString(msg.Signature)
	if err != nil || len(sigBytes)%2 != 0 {
		return false
	}
	r := new(big.Int).SetBytes(sigBytes[:len(sigBytes)/2])
	s := new(big.Int).SetBytes(sigBytes[len(sigBytes)/2:])
	return ecdsa.Verify(pub, hash[:], r, s)
}

// ParsePrivateKey parses a PEM encoded ECDSA private key. Panics on error.
func ParsePrivateKey(pemStr string) *ecdsa.PrivateKey {
	block, _ := pem.Decode([]byte(pemStr))
	if block == nil {
		panic("invalid PEM for private key")
	}
	key, err := x509.ParseECPrivateKey(block.Bytes)
	if err != nil {
		panic(err)
	}
	return key
}

// ParsePublicKey parses a PEM encoded ECDSA public key. Panics on error.
func ParsePublicKey(pemStr string) *ecdsa.PublicKey {
	block, _ := pem.Decode([]byte(pemStr))
	if block == nil {
		panic("invalid PEM for public key")
	}
	pub, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		panic(err)
	}
	if pk, ok := pub.(*ecdsa.PublicKey); ok {
		return pk
	}
	panic("not ECDSA public key")
}
