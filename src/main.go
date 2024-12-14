package main

import (
	"fmt"
	"log"
	"net/http"

	"github.com/cockroachdb/pebble"
	"github.com/julienschmidt/httprouter"
)

type Server struct {
	port string
	db   *pebble.DB
}

const (
	HOST = "localhost"
)

func (s *Server) addDocument(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	panic("unimplemented")
}

func (s *Server) searchDocument(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	panic("unimplemented")
}

func (s *Server) getDocument(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	panic("unimplemented")
}

func newServer(databaseFile string, port string) (*Server, error) {
	db, err := pebble.Open(databaseFile, &pebble.Options{})

	return &Server{
		port: port,
		db:   db,
	}, err
}

func main() {
	s, err := newServer("doc_db.data", "8080")
	if err != nil {
		log.Fatal(err)
	}

	defer s.db.Close()

	router := httprouter.New()
	router.POST("/docs", s.addDocument)
	router.GET("/docs", s.searchDocument)
	router.GET("/docs/:id", s.getDocument)

	log.Println("Listening on port: " + s.port)

	listenAddr := fmt.Sprintf("%v:%v", HOST, s.port)

	err = http.ListenAndServe(listenAddr, router)
	if err != nil {
		log.Fatal(err)
	}
}
