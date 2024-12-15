package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"unicode"

	"github.com/cockroachdb/pebble"
	"github.com/google/uuid"
	"github.com/julienschmidt/httprouter"
)

type Server struct {
	port    string
	db      *pebble.DB
	indexDb *pebble.DB
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

	// index call
	s.index(id, document)

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
	requestQuery := r.URL.Query().Get("q")
	q, err := parseQuery(requestQuery)
	if err != nil {
		jsonResponse(w, nil, err)
		return
	}

	isRangeScan := false
	idsArgumentCount := map[string]int{}
	nonRangeArguments := 0
	for _, argument := range q.ands {
		if argument.op == "=" {
			nonRangeArguments++

			ids, err := s.lookup(fmt.Sprintf("%s=%v", strings.Join(argument.key, "."), argument.value))
			if err != nil {
				jsonResponse(w, nil, err)
				return
			}

			for _, id := range ids {
				_, ok := idsArgumentCount[id]
				if !ok {
					idsArgumentCount[id] = 0
				}

				idsArgumentCount[id]++
			}

		} else {
			isRangeScan = true
		}
	}

	// idsInAll contains ids which appear satisfy all equality statments
	var idsInAll []string
	for id, count := range idsArgumentCount {
		if count == nonRangeArguments {
			idsInAll = append(idsInAll, id)
		}
	}

	var documents []any
	if r.URL.Query().Get("skipIndex") == "true" {
		idsInAll = nil
	}

	/*
	 The results matching the overall filter must be
	 the set intersection of ids that match each individual
	 equality filter.
	 Greater than and less than filters will be filtered out
	 after fetching all possible ids that match equality filters.
	*/
	if len(idsInAll) > 0 {
		for _, id := range idsInAll {
			document, err := s.getDocumentById([]byte(id))
			if err != nil {
				jsonResponse(w, nil, err)
				return
			}

			if !isRangeScan || q.match(document) {
				documents = append(documents, map[string]any{
					"id":   id,
					"body": document,
				})
			}
		}
	} else {
		var documents []map[string]any

		it, err := s.db.NewIter(&pebble.IterOptions{})
		if err != nil {
			jsonResponse(w, nil, err)
			return
		}
		defer it.Close()

		for it.First(); it.Valid(); it.Next() {
			var document map[string]any
			err = json.Unmarshal(it.Value(), &document)
			if err != nil {
				jsonResponse(w, nil, err)
				return
			}

			if q.match(document) {
				documents = append(documents, map[string]any{
					"id":   string(it.Key()),
					"body": document,
				})
			}
		}
	}
	jsonResponse(w, map[string]any{
		"documents": documents,
		"count":     len(documents),
	}, nil)
}

func (s *Server) lookup(pathValue string) ([]string, error) {
	idsString, closer, err := s.indexDb.Get([]byte(pathValue))
	if err != nil && err != pebble.ErrNotFound {
		return nil, fmt.Errorf("Could not look up pathValue [%#v]: %s", pathValue, err)
	}

	if closer != nil {
		defer closer.Close()
	}

	if len(idsString) == 0 {
		return nil, nil
	}

	return strings.Split(string(idsString), ","), nil
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

func (s *Server) index(id string, document map[string]any) {
	pathValues := getPathValues(document, "")

	for _, pathValue := range pathValues {
		idsString, closer, err := s.indexDb.Get([]byte(pathValue))
		if err != nil && err != pebble.ErrNotFound {
			log.Printf("Could not look up path value [%#v]: %s", document, err)
		}

		if len(idsString) == 0 {
			idsString = []byte(id)
		} else {
			ids := strings.Split(string(idsString), ",")
			found := false
			for _, existingId := range ids {
				if id == existingId {
					found = true
					break
				}
			}

			if !found {
				idsString = append(idsString, []byte(","+id)...)
			}
		}

		if closer != nil {
			err = closer.Close()
			if err != nil {
				log.Printf("Could not close: %s", err)
			}
		}

		err = s.indexDb.Set([]byte(pathValue), idsString, pebble.Sync)
		if err != nil {
			log.Printf("Could not update index: %s", err)
		}
	}
}

func getPathValues(obj map[string]any, prefix string) []string {
	var pathValues []string
	for key, value := range obj {
		switch t := value.(type) {
		case map[string]any:
			pathValues = append(pathValues, getPathValues(t, key)...)
			continue
		case []interface{}:
			// can't handle arrays
			continue
		}

		if prefix != "" {
			key = prefix + "." + key
		}
		pathValues = append(pathValues, fmt.Sprintf("%s=%v", key, value))
	}

	return pathValues
}

// to handle documents that were ingested and not indexed
func (s *Server) reIndex() {
	it, err := s.db.NewIter(nil)
	if err != nil {
		log.Printf("Unable to creater iterator for db: %s", err)
	}
	defer it.Close()

	for it.First(); it.Valid(); it.Next() {
		var document map[string]any
		err := json.Unmarshal(it.Value(), &document)
		if err != nil {
			log.Printf("Unable to parse bad document, %s: %s", string(it.Key()), err)
		}

		s.index(string(it.Key()), document)
	}
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
	if err != nil {
		return nil, err
	}

	indexDb, err := pebble.Open(databaseFile+"index", &pebble.Options{})
	return &Server{
		port:    port,
		db:      db,
		indexDb: indexDb,
	}, err
}

func main() {
	s, err := newServer("doc_db.data", "8080")
	if err != nil {
		log.Fatal(err)
	}

	defer s.db.Close()

	s.reIndex()

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

type QueryComparison struct {
	key   []string
	value string
	op    string
}

type Query struct {
	ands []QueryComparison
}

func (q *Query) match(doc map[string]any) bool {
	for _, argument := range q.ands {
		value, ok := getPath(doc, argument.key)
		if !ok {
			return false
		}

		// handle equality
		if argument.op == "=" {
			match := fmt.Sprint("%v", value) == argument.value
			if !match {
				return false
			}

			continue
		}

		// handle <, >
		right, err := strconv.ParseFloat(argument.value, 64)
		var left float64

		switch t := value.(type) {
		case float64:
			left = t
		case float32:
			left = float64(t)
		case uint:
			left = float64(t)
		case uint8:
			left = float64(t)
		case uint16:
			left = float64(t)
		case uint32:
			left = float64(t)
		case uint64:
			left = float64(t)
		case int:
			left = float64(t)
		case int8:
			left = float64(t)
		case int16:
			left = float64(t)
		case int32:
			left = float64(t)
		case int64:
			left = float64(t)
		case string:
			left, err = strconv.ParseFloat(t, 64)
			if err != nil {
				return false
			}
		default:
			return false
		}

		if argument.op == ">" {
			if left <= right {
				return false
			}
			continue
		}

		if left >= right {
			return false
		}
	}

	return true
}

func getPath(doc map[string]any, parts []string) (any, bool) {
	var docSegment any = doc
	for _, part := range parts {
		m, ok := docSegment.(map[string]any)
		if !ok {
			return nil, false
		}

		if docSegment, ok = m[part]; !ok {
			return nil, false
		}
	}

	return docSegment, true
}

// e.g. q=a.b:12
func parseQuery(q string) (*Query, error) {
	if q == "" {
		return &Query{}, nil
	}

	i := 0
	var parsedQuery Query
	var qRune = []rune(q)
	for i < len(qRune) {
		// eat whitespace
		for unicode.IsSpace(qRune[i]) {
			i++
		}

		key, nextIdx, err := lexString(qRune, i)
		if err != nil {
			return nil, fmt.Errorf("expected valid key, got [%s]: `%d", err, q[nextIdx])
		}

		// expect operator
		if q[nextIdx] != ':' {
			return nil, fmt.Errorf("expected colon at %d, got: %d", nextIdx, q[nextIdx])
		}

		i = nextIdx + 1
		op := "="
		if q[i] == '>' || q[i] == '<' {
			op = string(q[i])
			i++
		}

		value, nextIdx, err := lexString(qRune, i)
		if err != nil {
			return nil, fmt.Errorf("expected valid value, got [%s]: `%d", err, q[nextIdx])
		}

		i = nextIdx

		argument := QueryComparison{
			key:   strings.Split(key, "."),
			value: value,
			op:    op,
		}

		parsedQuery.ands = append(parsedQuery.ands, argument)
	}

	return &parsedQuery, nil
}

// handles either quoted strings or unquoted strings of only contiguous digits and letters
func lexString(input []rune, idx int) (string, int, error) {
	if idx >= len(input) {
		return "", idx, nil
	}

	if input[idx] == '"' {
		idx++

		foundEnd := false
		var s []rune
		// TODO: handle nested quotes
		for idx < len(input) {
			if input[idx] == '"' {
				foundEnd = true
				break
			}

			s = append(s, input[idx])
			idx++
		}

		if !foundEnd {
			return "", idx, fmt.Errorf("expected end of quoted string")
		}

		return string(s), idx + 1, nil
	}

	// if unquoted, read as much contiguous digits/letters as there are
	var s []rune
	var c rune
	for idx < len(input) {
		c = input[idx]
		if !(unicode.IsLetter((c)) || unicode.IsDigit(c) || c == '.') {
			break
		}
		s = append(s, c)
		idx++
	}

	if len(s) == 0 {
		return "", idx, fmt.Errorf("no string found")
	}

	return string(s), idx, nil
}
