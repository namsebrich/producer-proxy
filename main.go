package main

import (
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"log"
	"net/http"
	"os"
	"time"
)

type Event struct {
	Topic string
	Data  json.RawMessage
}

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s bootstrap-servers>\n", os.Args[0])
		os.Exit(1)
	}

	bootstrapServers := os.Args[1]

	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": bootstrapServers})

	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		os.Exit(1)
	}

	fmt.Printf("Created Producer %v\n", p)

	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				m := ev
				if m.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
				} else {
					fmt.Printf("Delivered message to topic %s [%d] at offset %v\n", *m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
				}
			case kafka.Error:
				fmt.Printf("Error: %v\n", ev)
			}
		}
	}()

	http.HandleFunc("/produce", func(w http.ResponseWriter, r *http.Request) {
		var ev Event
		if err := json.NewDecoder(r.Body).Decode(&ev); err != nil {
			fmt.Printf("요청 형식이 잘못되었습니다: %v\n", err)
			http.Error(w, "요청 형식이 잘못되었습니다.", http.StatusBadRequest)
			return
		}

		defer r.Body.Close()

		msg := &kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &ev.Topic, Partition: kafka.PartitionAny},
			Value:          ev.Data,
		}

		err = p.Produce(msg, nil)

		if err != nil {
			if err.(kafka.Error).Code() == kafka.ErrQueueFull {
				fmt.Printf("Kafka queue is full, %v\n", err)
				time.Sleep(time.Second)
				err = p.Produce(msg, nil)
				// 얼마나 재시도할지, 언제 실패로 보고 넘어갈지 확인
			} else {
				fmt.Printf("전송이 실패했습니다: %v\n", err)
				http.Error(w, "전송이 실패했습니다.", http.StatusInternalServerError)
				return
			}
		}

		for p.Flush(10000) > 0 {
			fmt.Print("Still waiting to flush outstanding messages\n")
		}
	})
	log.Fatal(http.ListenAndServe(":8080", nil))
}
