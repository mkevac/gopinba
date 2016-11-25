package main

import (
	"log"
	"net"
)

func main() {
	// We listen on a random port
	udpAddr, err := net.ResolveUDPAddr("udp", "0.0.0.0:6666")
	if err != nil {
		log.Fatalf("net.ResolveUDPAddr() failed: %v", err)
	}

	udpListener, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		log.Fatalf("net.ListenUDP() failed: %v", err)
	}

	var udpBuf = make([]byte, 4096)

	for {
		_, _, _ = udpListener.ReadFromUDP(udpBuf)
	}
}
