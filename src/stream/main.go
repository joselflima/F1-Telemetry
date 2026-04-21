package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
	kafka "github.com/segmentio/kafka-go"
)

var logger = log.New(os.Stdout, "[Telemetry] ", log.LstdFlags)

// TelemetryPayload espelha o JSON produzido pelo preprocessor.
type TelemetryPayload struct {
	CarIndex                uint8      `json:"car_index"`
	Speed                   uint16     `json:"speed"`
	Throttle                float32    `json:"throttle"`
	Steer                   float32    `json:"steer"`
	Brake                   float32    `json:"brake"`
	Gear                    int8       `json:"gear"`
	EngineRPM               uint16     `json:"engine_rpm"`
	DRS                     uint8      `json:"drs"`
	TyresPressure           [4]float32 `json:"tyres_pressure"`
	TyresSurfaceTemperature [4]uint8   `json:"tyres_surface_temperature"`
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// influxHandler encapsula o cliente InfluxDB e sua write API não-bloqueante.
type influxHandler struct {
	client   influxdb2.Client
	writeAPI api.WriteAPI
}

func newInfluxHandler(url, token, org, bucket string) *influxHandler {
	client := influxdb2.NewClientWithOptions(url, token,
		influxdb2.DefaultOptions().SetHTTPRequestTimeout(5))
	writeAPI := client.WriteAPI(org, bucket)

	// Loga erros assíncronos de escrita em background.
	go func() {
		for err := range writeAPI.Errors() {
			logger.Printf("Erro ao escrever no InfluxDB: %v", err)
		}
	}()

	return &influxHandler{client: client, writeAPI: writeAPI}
}

func (h *influxHandler) writePoint(p TelemetryPayload) {
	point := influxdb2.NewPointWithMeasurement("car_telemetry").
		AddTag("car_index", fmt.Sprintf("%d", p.CarIndex)).
		AddField("speed", int(p.Speed)).
		AddField("throttle", float64(p.Throttle)).
		AddField("steer", float64(p.Steer)).
		AddField("brake", float64(p.Brake)).
		AddField("gear", int(p.Gear)).
		AddField("engine_rpm", int(p.EngineRPM)).
		AddField("drs", int(p.DRS)).
		AddField("tyres_pressure_RL", float64(p.TyresPressure[0])).
		AddField("tyres_pressure_RR", float64(p.TyresPressure[1])).
		AddField("tyres_pressure_FL", float64(p.TyresPressure[2])).
		AddField("tyres_pressure_FR", float64(p.TyresPressure[3])).
		AddField("tyres_surface_temp_RL", int(p.TyresSurfaceTemperature[0])).
		AddField("tyres_surface_temp_RR", int(p.TyresSurfaceTemperature[1])).
		AddField("tyres_surface_temp_FL", int(p.TyresSurfaceTemperature[2])).
		AddField("tyres_surface_temp_FR", int(p.TyresSurfaceTemperature[3])).
		SetTime(time.Now().UTC())

	h.writeAPI.WritePoint(point)
}

func (h *influxHandler) close() {
	h.writeAPI.Flush()
	h.client.Close()
}

func main() {
	logger.Println("Iniciando servico stream (Kafka -> InfluxDB)...")

	kafkaBroker := getEnv("KAFKA_BROKER", "kafka:9092")
	kafkaTopic := getEnv("KAFKA_TOPIC", "f1-telemetry")
	kafkaGroupID := getEnv("KAFKA_GROUP_ID", "influxdb-writer-group")

	influxURL := getEnv("INFLUXDB_URL", "http://influxdb:8086")
	influxToken := getEnv("INFLUXDB_TOKEN", "meu-token-secreto")
	influxOrg := getEnv("INFLUXDB_ORG", "f1-org")
	influxBucket := getEnv("INFLUXDB_BUCKET", "f1-bucket")

	influx := newInfluxHandler(influxURL, influxToken, influxOrg, influxBucket)
	defer influx.close()

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{kafkaBroker},
		Topic:       kafkaTopic,
		GroupID:     kafkaGroupID,
		MinBytes:    1,
		MaxBytes:    10_000_000,
		StartOffset: kafka.LastOffset,
	})
	defer reader.Close()

	logger.Printf("Consumidor Kafka inicializado no topico '%s'", kafkaTopic)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				logger.Println("Sinal recebido. Encerrando stream...")
				break
			}
			logger.Printf("Erro ao ler mensagem do Kafka: %v", err)
			continue
		}

		var payload TelemetryPayload
		if err := json.Unmarshal(msg.Value, &payload); err != nil {
			logger.Printf("JSON invalido no offset %d: %v", msg.Offset, err)
			if commitErr := reader.CommitMessages(ctx, msg); commitErr != nil {
				logger.Printf("Falha ao commitar mensagem invalida no offset %d: %v", msg.Offset, commitErr)
			}
			continue
		}

		influx.writePoint(payload)

		if err := reader.CommitMessages(ctx, msg); err != nil {
			logger.Printf("Erro ao commitar offset %d: %v", msg.Offset, err)
			continue
		}

		logger.Printf("Offset %d processado e salvo no InfluxDB.", msg.Offset)
	}

	logger.Println("Servico de stream encerrado.")
}
