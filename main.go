package main

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Define our custom types.
type EventType struct {
	Key  string
	Data []byte
}

type Publisher interface {
	Publish(ctx context.Context, event EventType)
}

// Any type that implements the KafkaClient interface can be used.
// This includes the kgo.Client type.
type KafkaClient interface {
	Produce(ctx context.Context, r *kgo.Record, promise func(*kgo.Record, error))
}

// This is a compile time check that ensures kgo.Client implements the KafkaClient interface.
var _ KafkaClient = &kgo.Client{}

func NewKafkaPublisher(client KafkaClient) *KafkaPublisher {
	return &KafkaPublisher{
		client: client,
	}
}

type KafkaPublisher struct {
	// Don't use the kgo.Client directly in the KafkaPublisher struct.
	// Use our own KafkaClient interface to abstract the kafka client,
	// so we can mock it in our tests.
	client KafkaClient
}

// This is a compile time check that ensures the KafkaPublisher
// struct implements the Publisher interface.
var _ Publisher = &KafkaPublisher{}

func (kp *KafkaPublisher) Publish(ctx context.Context, event EventType) {
	r := &kgo.Record{
		Key:   []byte(event.Key),
		Value: event.Data,
	}
	// I have no idea what the callback is for.
	callback := func(r *kgo.Record, err error) {}
	kp.client.Produce(ctx, r, callback)
	return
}

func main() {
	// Carry out dependency injection.
	client, err := kgo.NewClient()
	if err != nil {
		panic(err)
	}
	// Run the app.
	// Note that we passed a kgo.Client to the NewKafkaPublisher function, and that
	// kgo.Client implicitly implements the KafkaClient interface.
	p := NewKafkaPublisher(client)
	app := NewApp(p)
	app.Run(context.Background())
}

// Create an app struct that has a Publisher interface.
func NewApp(p Publisher) *App {
	return &App{
		Publisher: p,
	}
}

type App struct {
	Publisher Publisher
}

// Handling logic goes here.
func (a *App) Run(ctx context.Context) (err error) {
	a.Publisher.Publish(ctx, EventType{Key: "test", Data: []byte("test")})
	return nil
}
