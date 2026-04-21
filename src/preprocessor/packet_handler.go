package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"joselucas/f1-telemetry/src/utils"
	"strconv"

	"github.com/segmentio/kafka-go"
)

const (
	kafkaHost  = "kafka"
	kafkaPort  = 9092
	kafkaTopic = "f1-telemetry"
)

type KafkaProducer struct {
	writer *kafka.Writer
}

func NewKafkaProducer() *KafkaProducer {
	return &KafkaProducer{
		writer: &kafka.Writer{
			Addr:     kafka.TCP(kafkaHost + ":" + strconv.Itoa(kafkaPort)),
			Topic:    kafkaTopic,
			Balancer: &kafka.LeastBytes{},
		},
	}
}

func (kp *KafkaProducer) PublishTelemetry(data interface{}) error {
	payload, err := json.Marshal(data)
	if err != nil {
		return err
	}
	return kp.writer.WriteMessages(context.Background(), kafka.Message{
		Value: payload,
	})
}

func (kp *KafkaProducer) Close() {
	kp.writer.Close()
}

var byteOrder = binary.LittleEndian

func ParseAndPublishPacket(data []byte, producer *KafkaProducer) {
	reader := bytes.NewReader(data)
	var header PacketHeader
	if err := binary.Read(reader, byteOrder, &header); err != nil {
		utils.Logger.Printf("Erro ao decodificar cabeçalho: %v", err)
		return
	}

	if header.PacketID == 6 {
		telemetryPacket, err := DecodeTelemetryPacket(reader)
		if err != nil {
			utils.Logger.Printf("Erro ao decodificar pacote de telemetria: %v", err)
			return
		}
		payload := BuildTelemetryPayload(telemetryPacket, header.PlayerCarIndex)
		if err := producer.PublishTelemetry(payload); err != nil {
			utils.Logger.Printf("Erro ao publicar no Kafka: %v", err)
		} else {
			utils.Logger.Printf("Payload publicado no Kafka para o carro %d.", header.PlayerCarIndex)
		}
	}
}
