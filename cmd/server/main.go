package main

import (
	"fmt"
	"log"

	"github.com/nickstrad/dkv_store/internal/server"
)

func main() {
	addr := ":8080"
	srv := server.NewHTTPServer(addr)
	fmt.Println("Starting server on " + addr)
	log.Fatal(srv.ListenAndServe())
}
