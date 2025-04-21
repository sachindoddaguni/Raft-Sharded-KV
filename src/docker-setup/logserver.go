package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
)

func logHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "only POST allowed", http.StatusMethodNotAllowed)
		return
	}

	// Read the log message body
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "unable to read request body", http.StatusInternalServerError)
		return
	}

	// Parse the body as form data (color=green&message=...)
	formData, err := url.ParseQuery(string(body))
	if err != nil {
		http.Error(w, "unable to parse form data", http.StatusInternalServerError)
		return
	}

	// Extract the color and message from the form data
	color := formData.Get("color")     // "green"
	message := formData.Get("message") // "I am elected as the leader for the term 1"

	// Print the log message, including the color, using ANSI escape codes
	// (The server handles the color mapping)
	colorCode := getColorCode(color)

	// Log the message with color
	fmt.Printf("LOG: \033[%sm%s\033[0m\n", colorCode, message)

	// Respond to client
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, "logged")
}

// Get the ANSI color code based on the color name
func getColorCode(color string) string {
	// Map color names to ANSI color codes
	colorCodes := map[string]string{
		"green":  "32", // Green color
		"red":    "31", // Red color (default)
		"blue":   "34", // Blue color
		"yellow": "33", // Yellow color
		// Add more colors as needed
	}

	// Default to red if color is not recognized
	colorCode, exists := colorCodes[color]
	if !exists {
		colorCode = "31" // Default to red
	}
	return colorCode
}

func main() {
	http.HandleFunc("/log", logHandler)
	log.Println("Log server listening on :8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatalf("ListenAndServe failed: %v", err)
	}
}
