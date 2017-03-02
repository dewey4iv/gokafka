package gokafka

import (
	"fmt"
	"net"

	"github.com/dewey4iv/gokafka/offset-handlers/csv"

	"github.com/Shopify/sarama"
)

// Option applies a config to the provided Kafka
type Option interface {
	Apply(*Kafka) error
}

// WithDefaults sets some sensible defaults
func WithDefaults() Option {
	return &withDefaults{}
}

type withDefaults struct {
}

func (opt *withDefaults) Apply(k *Kafka) error {
	if err := WithCreds("172.20.16.233", "9092").Apply(k); err != nil {
		return err
	}

	offsetWriter, err := csv.New("./offsets.csv")
	if err != nil {
		return err
	}

	k.offsetWriter = offsetWriter

	return nil
}

// WithFuncMap takes a map[string]func(*Message) error to be used by the Kafka instance
func WithFuncMap(funcs map[string]func(*sarama.ConsumerMessage) error) Option {
	return &withFuncMap{funcs}
}

type withFuncMap struct {
	funcs map[string]func(*sarama.ConsumerMessage) error
}

func (opt *withFuncMap) Apply(k *Kafka) error {
	k.funcs = opt.funcs

	return nil
}

// WithMap takes a map and converts the fields to options
func WithMap(opts map[string]string) Option {
	return &withMap{opts}
}

type withMap struct {
	opts map[string]string
}

var reqiredFields = []string{"host", "port"}

// ErrMissingOption returns a customer error with the missing field
func ErrMissingOption(field string) error {
	return fmt.Errorf("ErrMissionOption: field - %s", field)
}

func (opt *withMap) Apply(k *Kafka) error {
	for _, field := range reqiredFields {
		if _, exists := opt.opts[field]; !exists {
			return ErrMissingOption(field)
		}
	}

	if err := WithCreds(opt.opts["host"], opt.opts["port"]).Apply(k); err != nil {
		return err
	}

	return nil
}

// WithCreds takes the host and port
func WithCreds(host string, port string) Option {
	return &withCreds{host, port}
}

type withCreds struct {
	host string
	port string
}

func (opt *withCreds) Apply(k *Kafka) error {
	consumer, err := sarama.NewConsumer([]string{net.JoinHostPort(opt.host, opt.port)}, nil)
	if err != nil {
		return err
	}

	if err := WithConsumer(consumer).Apply(k); err != nil {
		return err
	}

	return nil
}

// WithConsumer takes a consumer and sets it directly
func WithConsumer(consumer sarama.Consumer) Option {
	return &withConsumer{consumer}
}

type withConsumer struct {
	consumer sarama.Consumer
}

func (opt *withConsumer) Apply(k *Kafka) error {
	k.consumer = opt.consumer

	return nil
}
