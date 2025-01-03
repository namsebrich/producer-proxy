package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"testing"
)

// 해당 테스트 진행 전 해야될 작업들
// $ docker compose up -d
// $ go run . localhost:9092
// $ go test (새로운 터미널에서)
func TestProduce(t *testing.T) {
	ev := Event{
		Topic: "test",
		Data:  []byte(`{"action": "product-event", "productId": "1234"}`),
	}

	msg, err := json.Marshal(&ev)
	if err != nil {
		t.Error(err)
	}

	resp, err := http.Post("http://localhost:8080/produce", "application/json", bytes.NewBuffer(msg))
	if err != nil {
		t.Error(err)
	}

	defer resp.Body.Close()
}
