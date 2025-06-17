package server

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/gorilla/mux"
)

func NewHttpServer(addr string) *http.Server {
	httpsrv := newHTTPServer()
	rtr := mux.NewRouter()

	rtr.HandleFunc("/", httpsrv.handleProduce).Methods("POST")
	rtr.HandleFunc("/", httpsrv.handleConsume).Methods("GET")
	return &http.Server{
		Addr:    addr,
		Handler: rtr,
	}
}

type httpServer struct {
	Log *Log
}

func newHTTPServer() *httpServer {
	return &httpServer{Log: NewLog()}
}

type ProduceRequest struct {
	Record Record `json:"record"`
}

type ProduceResponse struct {
	Offset uint64 `json:"offset"`
}

type ConsumeRequest struct {
	Offset uint64 `json:"offset"`
}

type ConsumeResponse struct {
	Record Record `json:"record"`
}

func (srv *httpServer) handleProduce(w http.ResponseWriter, r *http.Request) {
	var req ProduceRequest

	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	off, err := srv.Log.Append(req.Record)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	res := ProduceResponse{Offset: off}

	err = json.NewEncoder(w).Encode(res)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (srv *httpServer) handleConsume(w http.ResponseWriter, r *http.Request) {
	var req ConsumeRequest

	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	record, err := srv.Log.Read(req.Offset)
	if errors.Is(err, ErrOffsetNotFound) {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	res := ConsumeResponse{Record: record}

	err = json.NewEncoder(w).Encode(res)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
