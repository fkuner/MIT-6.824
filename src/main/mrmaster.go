package main

//
// start the master process, which is implemented
// in ../mr/master.go
//
// go run mrmaster.go pg*.txt
//
// Please do not change this file.
//

import (
	"../mr"
	"log"
)
import "time"
import "os"
import "fmt"

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrmaster inputfiles...\n")
		os.Exit(1)
	}

	m := mr.MakeMaster(os.Args[1:], 10)

	fileName := "master-log"
	_, err := os.Stat(fileName)
	if err != nil {
		if os.IsNotExist(err) {
			_, err := os.Create(fileName)
			if err != nil {
				log.Fatalf("cannot create %v\n", fileName)
			}
		}
	}
	logFile, err := os.OpenFile(fileName, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
	if err != nil {
		log.Fatalln("open file error !")
	}
	log.SetOutput(logFile)

	for m.Done() == false {
		time.Sleep(time.Second)
	}

	time.Sleep(time.Second)
	logFile.Close()
}
