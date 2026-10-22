// Command mq is a minimal in-memory Pub/Sub emulator for local runs
// and the integration test. It serves the REST calls internal/pubsub makes: topic and
// subscription creation, publish, pull, acknowledge and modifyAckDeadline.
//
//	go run ./cmd/mq -addr localhost:8085
//
// Unacked messages are redelivered when their ack deadline passes or when
// they are nacked (deadline 0), with an increasing deliveryAttempt.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

type message struct {
	ID       string
	Data     []byte
	Attrs    map[string]string
	Attempt  int
	Deadline time.Time // zero while ready
}

type subscription struct {
	topic    string
	ackSecs  int
	ready    []*message
	inflight map[string]*message // ack ID -> message
}

type broker struct {
	mu     sync.Mutex
	topics map[string]bool
	subs   map[string]*subscription
	seq    int
}

func main() {
	addr := flag.String("addr", "localhost:8085", "listen address")
	flag.Parse()
	b := &broker{topics: map[string]bool{}, subs: map[string]*subscription{}}
	slog.Info("mq listening", "addr", *addr)
	if err := http.ListenAndServe(*addr, b); err != nil {
		slog.Error("stopped", "err", err)
		os.Exit(1)
	}
}

func (b *broker) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// /v1/projects/{project}/{topics|subscriptions}/{name}[:verb]
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/v1/projects/"), "/")
	if len(parts) != 3 {
		http.NotFound(w, r)
		return
	}
	kind, name, verb := parts[1], parts[2], ""
	if i := strings.IndexByte(name, ':'); i >= 0 {
		name, verb = name[:i], name[i+1:]
	}
	var in struct {
		Topic       string   `json:"topic"`
		AckDeadline int      `json:"ackDeadlineSeconds"`
		MaxMessages int      `json:"maxMessages"`
		AckIDs      []string `json:"ackIds"`
		Messages    []struct {
			Data       []byte            `json:"data"`
			Attributes map[string]string `json:"attributes"`
		} `json:"messages"`
	}
	_ = json.NewDecoder(r.Body).Decode(&in)

	b.mu.Lock()
	defer b.mu.Unlock()
	out := map[string]any{}
	switch {
	case kind == "topics" && verb == "" && r.Method == http.MethodPut:
		if b.topics[name] {
			http.Error(w, "topic exists", http.StatusConflict)
			return
		}
		b.topics[name] = true
	case kind == "subscriptions" && verb == "" && r.Method == http.MethodPut:
		if b.subs[name] != nil {
			http.Error(w, "subscription exists", http.StatusConflict)
			return
		}
		topic := in.Topic[strings.LastIndex(in.Topic, "/")+1:]
		if !b.topics[topic] {
			http.Error(w, "unknown topic", http.StatusNotFound)
			return
		}
		b.subs[name] = &subscription{topic: topic, ackSecs: max(in.AckDeadline, 10), inflight: map[string]*message{}}
	case kind == "topics" && verb == "publish":
		if !b.topics[name] {
			http.Error(w, "unknown topic", http.StatusNotFound)
			return
		}
		ids := []string{}
		for _, m := range in.Messages {
			b.seq++
			id := fmt.Sprint(b.seq)
			ids = append(ids, id)
			for _, s := range b.subs {
				if s.topic == name {
					s.ready = append(s.ready, &message{ID: id, Data: m.Data, Attrs: m.Attributes})
				}
			}
		}
		out["messageIds"] = ids
	case kind == "subscriptions" && b.subs[name] != nil:
		s := b.subs[name]
		switch verb {
		case "pull":
			now := time.Now()
			for ackID, m := range s.inflight {
				if now.After(m.Deadline) {
					delete(s.inflight, ackID)
					s.ready = append(s.ready, m)
				}
			}
			n := min(max(in.MaxMessages, 1), len(s.ready))
			received := []map[string]any{}
			for _, m := range s.ready[:n] {
				b.seq++
				ackID := fmt.Sprintf("ack-%d", b.seq)
				m.Attempt++
				m.Deadline = now.Add(time.Duration(s.ackSecs) * time.Second)
				s.inflight[ackID] = m
				received = append(received, map[string]any{
					"ackId":           ackID,
					"deliveryAttempt": m.Attempt,
					"message":         map[string]any{"data": m.Data, "attributes": m.Attrs, "messageId": m.ID},
				})
			}
			s.ready = s.ready[n:]
			out["receivedMessages"] = received
		case "acknowledge":
			for _, id := range in.AckIDs {
				delete(s.inflight, id)
			}
		case "modifyAckDeadline":
			for _, id := range in.AckIDs {
				m := s.inflight[id]
				if m == nil {
					continue
				}
				if in.AckDeadline == 0 {
					delete(s.inflight, id)
					s.ready = append(s.ready, m)
				} else {
					m.Deadline = time.Now().Add(time.Duration(in.AckDeadline) * time.Second)
				}
			}
		default:
			http.NotFound(w, r)
			return
		}
	default:
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}
