package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"regexp"
	"strings"
)

func main() {
	fmt.Println("Starting worker...")
	http.HandleFunc("/", handler)
	log.Fatal(http.ListenAndServe(":8080", nil))

}

func queueWorker() {
	return
}

/*
Provides a method to call with parameters
*/
func parseCommand(raw string) {
	var pattern = regexp.MustCompile(`^\[(\d+)\] ([A-Z_]+),([^ ]+) ?$`)

	var match = pattern.FindAllString(raw, -1)

	if match == nil {
		fmt.Println("No matching command found.")
		return
	}
	sp := strings.Split(match[0], " ")
	transactionNum := sp[0]
	spd := strings.Split(sp[1], ",")
	command := spd[0]
	arguments := spd[1:]

	//Quote Command
	if command == "QUOTE" {
		fmt.Println("got in quote:", arguments)
		userID := arguments[0]
		stockSymbol := arguments[1]
		quote(transactionNum, userID, stockSymbol)
	} else if command == "ADD" {
		// handle ADD command
	}

}

func handler(w http.ResponseWriter, r *http.Request) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Printf("Error reading body: %v", err)
		http.Error(w, "can't read body", http.StatusBadRequest)
		return
	}
	parseCommand(string(body))
}
