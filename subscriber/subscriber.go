package main

import (
    "fmt"
     zmq "github.com/zeromq/goczmq"
)


func main() {

   fmt.Println("Started Event Daemon !!")
   inputQueue := make(chan []byte)
   db := initDB("eventDUMP.db")
   defer db.Close()
   createTables(db)
   go func(){
       for{
          data := <-inputQueue
          fmt.Println("Received serial data:", data)
          writeToDb(data, db)
       }
   }()

   go listener(inputQueue)
   select{}
}


// Listener thread

func listener(inputQ chan []byte) {
    fmt.Println("Started listener thread")
    //'@' at the beginning of source endpoint specifies that this socket has to bind(instead of connect).
    src := "@tcp://127.0.0.1:2000"
    subSock, err := zmq.NewSub(src, "")
    if err != nil {
        fmt.Println("error creating socket:", err)
    }
    defer subSock.Destroy()

    fmt.Println("listening on socket ", src)
    for {
        fmt.Println("receieve frame")
        data, flag, err := subSock.RecvFrame()
        if err != nil {
            fmt.Println("error receiving frame:", err)
        }
        fmt.Println("data, flag", data, flag)
        inputQ <- data
    }
}


