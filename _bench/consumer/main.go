package main

import (
	"consumer/socket"
	"log"
)

func main() {
	brokerClient := socket.NewBrokerClient()

	if brokerClient == nil {
		log.Fatal("failed to create broker client")
	}

	if err := brokerClient.Connect(); err != nil {
		log.Fatal(err)
	}

	select {}
}
