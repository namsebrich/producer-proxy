package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"testing"
)

// 실제로 전송 테스트하기 위한 코드. 아래 명령대로 실행 후 새로운 터미널에서 $ go test
// $ docker compose up -d
// $ go run . localhost:9092
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
