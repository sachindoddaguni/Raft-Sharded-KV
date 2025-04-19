// logserver.go
package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
)

func logHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "unable to read request body", http.StatusInternalServerError)
			return
		}
		// Print log message to standard output.
		fmt.Printf("LOG: \033[31m%s\033[31m\n", body)
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "logged")
	} else {
		http.Error(w, "only POST allowed", http.StatusMethodNotAllowed)
	}
}

func main() {
	http.HandleFunc("/log", logHandler)
	log.Println("Log server listening on :8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatalf("ListenAndServe failed: %v", err)
	}
}
