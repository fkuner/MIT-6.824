package kvraft

import (
	"net/http"
	"net/http/pprof"
)

const (
	pprofAddr string = ":7890"
)

func StartHTTPDebugger() {
	pprofHandler := http.NewServeMux()
	pprofHandler.Handle("/debug/pprof/", http.HandlerFunc(pprof.Index))
	server := &http.Server{Addr: pprofAddr, Handler: pprofHandler}
	go server.ListenAndServe()
}