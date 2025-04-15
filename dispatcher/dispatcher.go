package dispatcher

import (
	"bufio"
	"fmt"
	"log"
	"os"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type Runnable interface {
	Run()
}

type Dispatcher struct {
	Producer     *kafka.Producer
	Topic        string
	FilePath     string
	DeliveryChan chan kafka.Event
}

func NewDispatcher(producer *kafka.Producer, topic string, filePath string, deliveryChan chan kafka.Event) *Dispatcher {
	return &Dispatcher{
		Producer:     producer,
		Topic:        topic,
		FilePath:     filePath,
		DeliveryChan: deliveryChan,
	}
}
func (d *Dispatcher) Run() {
	fmt.Println("start processing file: ", d.FilePath)
	file, err := os.OpenFile(d.FilePath, os.O_RDONLY, 0664)
	if err != nil {
		fmt.Println("could not open file: ", d.FilePath)
	}
	scanner := bufio.NewScanner(file)
	counter := 0
	for scanner.Scan() {
		counter++
		d.Producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &d.Topic, Partition: kafka.PartitionAny},
			Value:          []byte(scanner.Text()),
			Key:            []byte{},
		}, d.DeliveryChan)
	}
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("finished sending message ", counter, "from file: ", d.FilePath)
}
