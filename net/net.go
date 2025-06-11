package net

import (
	c "Dekvium/common"
	"Dekvium/pbft"
	"bytes"
	"encoding/json"
	"io"
	"net/http"

	"github.com/sirupsen/logrus"
)

// SendPBFTMessage sends a PBFT message to a peer using HTTP POST.
func SendMessage(addr string, msg *c.Message) {
	data, _ := json.Marshal(msg)
	resp, err := http.Post("http://"+addr+"/pbft", "application/json", bytes.NewReader(data))
	if err != nil {
		logrus.Warnf("%s: Failed to send message to %s: %v", c.CurFuncName(), addr, err)
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
}

func BroadcastMessage(peers []string, msg *c.Message) {
	for _, addr := range peers {
		go func(addr string) {
			SendMessage(addr, msg)
		}(addr)
	}
}

// StartPBFTServer starts an HTTP server to receive PBFT messages.
func StartListening(addr string, n *pbft.Node) {
	http.HandleFunc("/pbft", func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var msg c.Message
		if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		n.SendMessage(msg)
	})
	// http.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
	// 	defer r.Body.Close()
	// 	key := r.URL.Query().Get("key")
	// 	w.Write([]byte(n.Storage.Get(key)))
	// })
	logrus.Infof("%s: PBFT HTTP server listening on %s", c.CurFuncName(), addr)
	http.ListenAndServe(addr, nil)
}
