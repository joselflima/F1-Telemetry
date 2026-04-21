package main

import (
	"context"
	"joselucas/f1-telemetry/src/utils"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

func main() {
	utils.Logger.Println("Iniciando serviço de telemetria F1...")

	kafkaProducer := NewKafkaProducer()
	defer kafkaProducer.Close()

	// --- Contexto para Gerenciamento de Desligamento (Graceful Shutdown) ---
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup // WaitGroup para esperar as goroutines terminarem

	conn, err := ListenUDP(20777)
	if err != nil {
		log.Fatalf("Falha fatal ao iniciar listener UDP: %v", err)
	}
	defer conn.Close()

	// Inicia o listener UDP em outra goroutine para não bloquear o shutdown
	wg.Add(1)
	go func() {
		defer wg.Done()
		buffer := make([]byte, 2048)
		for {
			select {
			case <-ctx.Done(): // Verifica se o contexto foi cancelado
				utils.Logger.Println("Encerrando listener UDP...")
				return
			default:
				n, _, err := conn.ReadFromUDP(buffer)
				if err != nil {
					// Ignora o erro se o contexto foi cancelado
					if ctx.Err() == nil {
						utils.Logger.Printf("Erro ao ler pacote UDP: %v", err)
					}
					continue
				}
				ParseAndPublishPacket(buffer[:n], kafkaProducer)
			}
		}
	}()

	// --- Espera por um sinal de interrupção (Ctrl+C) ---
	utils.Logger.Println("Serviço em execução. Pressione Ctrl+C para sair.")
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	<-sigChan // Bloqueia até receber o sinal

	utils.Logger.Println("Recebido sinal de desligamento. Encerrando serviços...")
	cancel() // Cancela o contexto, sinalizando para as goroutines pararem

	// Espera as goroutines finalizarem de forma limpa
	wg.Wait()
	utils.Logger.Println("Todos os serviços foram encerrados.")
}
