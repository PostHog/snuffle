package main

import (
	"log"

	"github.com/PostHog/snuffle/internal/snuffle"
)

func main() {
	cfg := snuffle.ConfigFromEnv()
	if err := snuffle.Run(cfg); err != nil {
		log.Fatal(err)
	}
}
