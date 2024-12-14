package main

import (
	"net/http"

	"github.com/julienschmidt/httprouter"
)

type Server struct {
	port string
}

func (s *Server) addDocument(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	panic("unimplemented")
}

func (s *Server) searchDocument(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	panic("unimplemented")
}

func (s *Server) getDocument(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	panic("unimplemented")
}

func main() {
	s := Server{
		port: "80",
	}

	router := httprouter.New()
	router.POST("/docs", s.addDocument)
	router.GET("/docs", s.searchDocument)
	router.GET("/docs/:id", s.getDocument)
}
