package main

import (
	"log"

	"github.com/rorylshanks/snuffle/internal/snuffle"
)

func main() {
	cfg := snuffle.ConfigFromEnv()
	if err := snuffle.Run(cfg); err != nil {
		log.Fatal(err)
	}
}
