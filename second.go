package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/pubsub"
	"google.golang.org/api/iterator"
)

const doc = `Pub/Sub Demo Service
--------------------
GET    /topics                      # list topics
PUT    /topics                      # create topic;        payload: '{"name":"<topic-name>"}'
POST   /topics/<topic-name>         # publish messages;    payload: '["<message-1-text>", "<message-2-text>", ...]'
DELETE /topics/<topic-name>         # delete topic

GET    /subscriptions               # list subscriptions
PUT    /subscriptions               # create subscription: payload: '{"name":"<subscr-name">, "topic":"<topic-name>"}'
POST   /subscriptions/<subscr-name> # receive messages:    payload: (none)
DELETE /subscriptions/<subscr-name> # delete subscription
`

func main() {
	http.HandleFunc("/", indexHandler)

	http.HandleFunc("/topics", topicsHandler)               // GET, PUT
	http.HandleFunc("/topics/", topicHandler)               // GET, POST, DELETE

	http.HandleFunc("/subscriptions", subscriptionsHandler) // GET, PUT
	http.HandleFunc("/subscriptions/", subscriptionHandler) // GET, POST, DELETE

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
		log.Printf("Defaulting to port %s", port)
	}

	log.Printf("Listening on port %s", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatal(err)
	}
}

// indexHandler returns the doc page
func indexHandler(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	fmt.Fprint(w, doc)
}

// topicsHandler handles GET and PUT to /topics
func topicsHandler(w http.ResponseWriter, r *http.Request) {
	projectID := os.Getenv("GOOGLE_CLOUD_PROJECT")
	if projectID == "" {
		http.Error(w, "failed to get project ID", http.StatusServiceUnavailable)
		return
	}

	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	switch r.Method {
	case http.MethodGet:
		it := client.Topics(ctx)
		fmt.Fprintln(w, "Topics\n------")
		i := 0
		for ; ; i++ {
			t, err := it.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			fmt.Fprintln(w, t)
		}
		if i == 0 {
			fmt.Fprintln(w, "(none)")
		}

	case http.MethodPut:
		// get topic name from body: '{"name":"my-topic"}', maybe other options someday
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		var props map[string]interface{}
		if err := json.Unmarshal(body, &props); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		name, ok := props["name"].(string)
		if !ok {
			http.Error(w, "name property not provided or wrong type", http.StatusBadRequest)
			return
		}
		topic, err := client.CreateTopic(ctx, name)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		fmt.Fprintf(w, "created topic %s\n", topic.String())

	default:
		http.Error(w, fmt.Sprintf("method %s not allowed", r.Method), http.StatusMethodNotAllowed)
	}
}

// topicHandler handles GET, POST and DELETE to /topic/<topic-name>
func topicHandler(w http.ResponseWriter, r *http.Request) {
	projectID := os.Getenv("GOOGLE_CLOUD_PROJECT")
	if projectID == "" {
		http.Error(w, "failed to get project ID", http.StatusServiceUnavailable)
		return
	}

	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// get topic name from url (must be only path element after "/topics/")
	if r.URL == nil {
		http.Error(w, "request URL is nil", http.StatusInternalServerError)
		return
	}
	topicName := strings.TrimPrefix(r.URL.Path, "/topics/")
	topic := client.Topic(topicName)
	exists, err := topic.Exists(ctx)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if !exists {
		http.Error(w, fmt.Sprintf("topic %s not found", topicName), http.StatusNotFound)
		return
	}
	topicResourceName := topic.String()

	switch r.Method {
	case http.MethodGet:
		// maybe later show additional details of topic
		fmt.Fprintln(w, topicResourceName)

	case http.MethodPost:
		// get messages to publish from body:
		// '["this is message 1", "second message", ...]', maybe other options someday
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		var msgs []string
		if err := json.Unmarshal(body, &msgs); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		defer topic.Stop()
		var results []*pubsub.PublishResult
		for _, msg := range msgs {
			r := topic.Publish(ctx, &pubsub.Message{
				Data: []byte(msg),
			})
			results = append(results, r)
		}
		for i, r := range results {
			id, err := r.Get(ctx)
			if err != nil {
				fmt.Fprintf(w, "[%d] %s\n", i, err.Error())
				continue
			}
			fmt.Fprintf(w, "[%d] published message ID %s\n", i, id)
		}

	case http.MethodDelete:
		err := topic.Delete(ctx)
		if err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		fmt.Fprintf(w, "deleted topic %s\n", topicResourceName)

	default:
		http.Error(w, fmt.Sprintf("method %s not allowed", r.Method), http.StatusMethodNotAllowed)
	}
}

// subscriptionsHandler handles GET and PUT to /subscriptions
func subscriptionsHandler(w http.ResponseWriter, r *http.Request) {
	projectID := os.Getenv("GOOGLE_CLOUD_PROJECT")
	if projectID == "" {
		http.Error(w, "failed to get project ID", http.StatusServiceUnavailable)
		return
	}

	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	switch r.Method {
	case http.MethodGet:
		it := client.Subscriptions(ctx)
		fmt.Fprintln(w, "Subscriptions\n-------------")
		i := 0
		for ; ; i++ {
			t, err := it.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			fmt.Fprintln(w, t)
		}
		if i == 0 {
			fmt.Fprintln(w, "(none)")
		}

	case http.MethodPut:
		// get subscription details from body:
		// '{"name":"my-subscription", "topic": "my-topic"}', maybe other options someday
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		var props map[string]interface{}
		if err := json.Unmarshal(body, &props); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		subscrName, ok := props["name"].(string)
		if !ok {
			http.Error(w, "name property not provided or wrong type", http.StatusBadRequest)
			return
		}
		topicName, ok := props["topic"].(string)
		if !ok {
			http.Error(w, "topic property not provided or wrong type", http.StatusBadRequest)
			return
		}
		topic := client.Topic(topicName)
		if topic == nil {
			http.Error(w, fmt.Sprintf("topic %s not found", topicName), http.StatusBadRequest)
			return
		}
		subscr, err := client.CreateSubscription(ctx, subscrName, pubsub.SubscriptionConfig{
			Topic:            topic,
			AckDeadline:      60 * time.Second,
			ExpirationPolicy: 25 * time.Hour,
		})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		fmt.Fprintf(w, "created subscription %s\n", subscr.String())

	default:
		http.Error(w, fmt.Sprintf("method %s not allowed", r.Method), http.StatusMethodNotAllowed)
	}
}

// subscriptionHandler handles GET, POST and DELETE to /subscriptions/<subscription-name>
func subscriptionHandler(w http.ResponseWriter, r *http.Request) {
	projectID := os.Getenv("GOOGLE_CLOUD_PROJECT")
	if projectID == "" {
		http.Error(w, "failed to get project ID", http.StatusServiceUnavailable)
		return
	}

	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// get subscription name from url (must be only path element after "/subscriptions/")
	if r.URL == nil {
		http.Error(w, "request URL is nil", http.StatusInternalServerError)
		return
	}
	subscrName := strings.TrimPrefix(r.URL.Path, "/subscriptions/")
	subscr := client.Subscription(subscrName)
	exists, err := subscr.Exists(ctx)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if !exists {
		http.Error(w, fmt.Sprintf("subscription %s not found", subscrName), http.StatusNotFound)
		return
	}
	subscrResourceName := subscr.String()

	switch r.Method {
	case http.MethodGet:
		// maybe later show additional details of subscription
		fmt.Fprintln(w, subscrResourceName)

	case http.MethodPost:
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		var (
			msgsMu sync.Mutex
			msgs   []*pubsub.Message
		)
		
		// Receive blocks until the context is cancelled or an error occurs.
		err = subscr.Receive(ctx, func(_ context.Context, msg *pubsub.Message) {
			msgsMu.Lock()
			defer msgsMu.Unlock()
			msgs = append(msgs, msg)
			msg.Ack()
		})
		if err != nil {
			fmt.Fprintf(w, "sub.Receive: %v", err)
		}
		for i, msg := range msgs {
			fmt.Fprintf(w, "[%d] Data: \"%s\"\n", i, string(msg.Data))
			if len(msg.Attributes) == 0 {
				continue
			}
			fmt.Fprintf(w, "[%d] Attributes:\n", i)
			for key, value := range msg.Attributes {
				fmt.Fprintf(w, "    %s = %s\n", key, value)
			}
		}
		
	case http.MethodDelete:
		err := subscr.Delete(ctx)
		if err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		fmt.Fprintf(w, "deleted subscription %s\n", subscrResourceName)

	default:
		http.Error(w, fmt.Sprintf("method %s not allowed", r.Method), http.StatusMethodNotAllowed)
	}
}
