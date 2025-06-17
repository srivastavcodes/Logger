package main

import (
	"log"
	"logger/internal/server"
	"os"

	"github.com/rs/zerolog"
)

func main() {
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()
	zerolog.SetGlobalLevel(zerolog.DebugLevel)

	srv := server.NewHttpServer(":8080")
	logger.Info().Msg("starting a server at port :8080")

	log.Fatal("could not start server", srv.ListenAndServe())
}
