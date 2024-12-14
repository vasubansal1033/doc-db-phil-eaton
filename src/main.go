package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/cockroachdb/pebble"
	"github.com/google/uuid"
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
	decoder := json.NewDecoder(r.Body)

	var document map[string]any
	err := decoder.Decode(&document)
	if err != nil {
		jsonResponse(w, nil, err)
		return
	}

	id := uuid.New().String()

	documentBytes, err := json.Marshal(document)
	if err != nil {
		jsonResponse(w, nil, err)
		return
	}

	err = s.db.Set([]byte(id), documentBytes, &pebble.WriteOptions{})
	if err != nil {
		jsonResponse(w, nil, err)
		return
	}

	jsonResponse(w, map[string]any{
		"id": id,
	}, nil)
}

func (s *Server) searchDocument(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
}

func (s *Server) getDocument(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	id := params.ByName("id")

	document, err := s.getDocumentById([]byte(id))
	if err != nil {
		jsonResponse(w, nil, err)
		return
	}

	jsonResponse(w, map[string]any{
		"document": document,
	}, nil)
}

func (s *Server) getDocumentById(id []byte) (map[string]any, error) {
	valueBytes, closer, err := s.db.Get([]byte(id))
	if err != nil {
		return nil, err
	}

	defer closer.Close()

	var document map[string]any
	err = json.Unmarshal(valueBytes, &document)

	return document, err
}

func jsonResponse(w http.ResponseWriter, body map[string]any, err error) {
	data := map[string]any{
		"body":   body,
		"status": "ok",
	}

	if err != nil {
		data["status"] = "error"
		data["error"] = err.Error()
		w.WriteHeader(http.StatusBadRequest)
	}

	w.Header().Set("Content-Type", "application/json")

	encoder := json.NewEncoder(w)
	err = encoder.Encode(data)
	if err != nil {
		panic(err)
	}
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
