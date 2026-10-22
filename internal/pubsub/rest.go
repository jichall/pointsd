package pubsub

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

// Client speaks the Pub/Sub REST API using only the standard library. It sends
// no credentials, so it targets the Pub/Sub emulator (PUBSUB_EMULATOR_HOST).
type Client struct {
	base    string
	project string
	http    *http.Client
}

// NewEmulatorClient returns a Client for the emulator at host ("pubsub:8085").
func NewEmulatorClient(host, project string) *Client {
	return &Client{base: "http://" + host + "/v1/projects/" + project, project: project, http: &http.Client{Timeout: 30 * time.Second}}
}

type statusError struct {
	code int
	body string
}

func (e *statusError) Error() string { return fmt.Sprintf("pubsub: http %d: %s", e.code, e.body) }

func (c *Client) do(ctx context.Context, method, path string, in, out any) error {
	var body io.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	if resp.StatusCode/100 != 2 {
		return &statusError{resp.StatusCode, string(data)}
	}
	if out != nil {
		return json.Unmarshal(data, out)
	}
	return nil
}

func ignoreConflict(err error) error {
	if se, ok := err.(*statusError); ok && se.code == http.StatusConflict {
		return nil
	}
	return err
}

// CreateTopic creates the topic if it does not exist.
func (c *Client) CreateTopic(ctx context.Context, topic string) error {
	return ignoreConflict(c.do(ctx, http.MethodPut, "/topics/"+topic, struct{}{}, nil))
}

// CreateSubscription creates the subscription if it does not exist.
func (c *Client) CreateSubscription(ctx context.Context, subscription, topic string) error {
	in := map[string]any{"topic": "projects/" + c.project + "/topics/" + topic, "ackDeadlineSeconds": 10}
	return ignoreConflict(c.do(ctx, http.MethodPut, "/subscriptions/"+subscription, in, nil))
}

type wireMessage struct {
	Data       []byte            `json:"data"`
	Attributes map[string]string `json:"attributes,omitempty"`
	MessageID  string            `json:"messageId,omitempty"`
}

// PublishBatch publishes payloads to topic in one request.
func (c *Client) PublishBatch(ctx context.Context, topic string, payloads [][]byte) error {
	msgs := make([]wireMessage, len(payloads))
	for i, p := range payloads {
		msgs[i] = wireMessage{Data: p}
	}
	return c.do(ctx, http.MethodPost, "/topics/"+topic+":publish", map[string]any{"messages": msgs}, nil)
}

type topicPublisher struct {
	c     *Client
	topic string
}

// NewPublisher returns a Publisher for topic.
func (c *Client) NewPublisher(topic string) Publisher { return topicPublisher{c, topic} }

func (t topicPublisher) Publish(ctx context.Context, data []byte, attrs map[string]string) error {
	msgs := []wireMessage{{Data: data, Attributes: attrs}}
	return t.c.do(ctx, http.MethodPost, "/topics/"+t.topic+":publish", map[string]any{"messages": msgs}, nil)
}

type restMessage struct {
	id      string
	data    []byte
	attempt int
	once    sync.Once
	settle  func(ack bool)
}

func (m *restMessage) ID() string           { return m.id }
func (m *restMessage) Data() []byte         { return m.data }
func (m *restMessage) DeliveryAttempt() int { return m.attempt }
func (m *restMessage) Ack()                 { m.once.Do(func() { m.settle(true) }) }
func (m *restMessage) Nack()                { m.once.Do(func() { m.settle(false) }) }

// Receive pulls from subscription until ctx is cancelled, running handle for up
// to maxOutstanding unsettled messages at once. It returns ctx's error.
func (c *Client) Receive(ctx context.Context, subscription string, maxOutstanding int, handle func(context.Context, Message)) error {
	slots := make(chan struct{}, maxOutstanding)
	var wg sync.WaitGroup
	defer wg.Wait()

	for {
		select {
		case slots <- struct{}{}:
		case <-ctx.Done():
			return ctx.Err()
		}
		free := cap(slots) - len(slots) + 1
		var resp struct {
			ReceivedMessages []struct {
				AckID           string      `json:"ackId"`
				Message         wireMessage `json:"message"`
				DeliveryAttempt int         `json:"deliveryAttempt"`
			} `json:"receivedMessages"`
		}
		in := map[string]any{"maxMessages": free, "returnImmediately": true}
		if err := c.do(ctx, http.MethodPost, "/subscriptions/"+subscription+":pull", in, &resp); err != nil || len(resp.ReceivedMessages) == 0 {
			<-slots
			select {
			case <-time.After(100 * time.Millisecond):
			case <-ctx.Done():
				return ctx.Err()
			}
			continue
		}
		for i, rm := range resp.ReceivedMessages {
			if i > 0 {
				slots <- struct{}{}
			}
			attempt := max(rm.DeliveryAttempt, 1)
			ackID := rm.AckID
			m := &restMessage{id: rm.Message.MessageID, data: rm.Message.Data, attempt: attempt}
			m.settle = func(ack bool) {
				defer func() { <-slots }()
				// Settling must outlive a cancelled ctx so shutdown does not strand messages.
				sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if ack {
					_ = c.do(sctx, http.MethodPost, "/subscriptions/"+subscription+":acknowledge", map[string]any{"ackIds": []string{ackID}}, nil)
					return
				}
				_ = c.do(sctx, http.MethodPost, "/subscriptions/"+subscription+":modifyAckDeadline", map[string]any{"ackIds": []string{ackID}, "ackDeadlineSeconds": 0}, nil)
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				handle(ctx, m)
			}()
		}
	}
}
