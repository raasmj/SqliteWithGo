package main

import (
    "database/sql"
    _ "github.com/mattn/go-sqlite3"
    "fmt"
    events "SqliteWithGo/events"
//    "github.com/golang/protobuf/ptypes"
    "reflect"
    "github.com/golang/protobuf/proto"
)

func initDB(filePath string) *sql.DB {

    fmt.Println("db init")
    db, err := sql.Open("sqlite3", filePath)
    if err != nil { panic(err) }
    if db == nil { panic("db nil") }
    return db
}

func createTables(db *sql.DB) {

    fmt.Println("creating tables")
    queryStmt, err := db.Prepare("CREATE TABLE IF NOT EXISTS properties (id INTEGER PRIMARY KEY, property VARCHAR(60), UNIQUE(property)) ")
    _, err = queryStmt.Exec()
    if err != nil {
        panic(err)
    }

    queryStmt, err = db.Prepare("CREATE TABLE IF NOT EXISTS propertyValues (id INTEGER PRIMARY KEY, value VARCHAR(255), UNIQUE(value))")
    _, err = queryStmt.Exec()
    if err != nil {
        panic(err)
    }

    /*queryStmt, err = db.Prepare("CREATE TABLE IF NOT EXISTS timestamps (id INTEGER PRIMARY KEY, timestamp TEXT)")
    _, err = queryStmt.Exec()
    if err != nil {
        panic(err)
    }*/

    queryStmt, err = db.Prepare("CREATE TABLE IF NOT EXISTS mappings (event_id INTERGER, prop_id INTEGER, val_id INTEGER )")
    _, err = queryStmt.Exec()
    if err != nil {
        panic(err)
    }

    defer queryStmt.Close()

}

func writeToDb(binData []byte, db *sql.DB) {

    event := new(events.EventOne)
    err := proto.Unmarshal(binData, event)
     if err != nil {
        fmt.Println("Error in unmarshaling data:", err.Error())
        return
    }
    eventRef := reflect.ValueOf(event).Elem()
    eventType := eventRef.Type()


     tx, err := db.Begin()
     if err != nil { panic(err) }

     errFlag := false
     //var timestamp string
     var mappingId int

     err = db.QueryRow("select event_id from mappings order by event_id desc limit 1").Scan(&mappingId)
     if err != nil && err != sql.ErrNoRows { panic(err) }
     mappingId += 1
     fmt.Println("mapping id ", mappingId)
     for i:=0; i < eventRef.NumField(); i++ {

         var id [2]int64
         prop := eventType.Field(i).Name
         val := eventRef.Field(i).Interface()
         fmt.Printf("prop=%s, val=%s\n", prop, val)
         value := fmt.Sprintf("%v", val)
       /* 
        if prop == "Timestamp" {
            timestamp = value
            continue
        }*/

         err1 := db.QueryRow("select id from properties where property=?", prop).Scan(&id[0])
         err2 := db.QueryRow("select id from propertyValues where value=?", value).Scan(&id[1])

         if err1 != nil {
             if err1 != sql.ErrNoRows{
                 fmt.Println("1")
                 panic(err1)
                 errFlag = true
             }

         }else if err2 != nil {
              if err2 != sql.ErrNoRows{
                  fmt.Println("2")
                  panic(err2)
                  errFlag = true
              }

         }

         if errFlag {
             fmt.Println("3")
             return

         }
         fmt.Println("values after..iter ", i)
         fmt.Println("id ", id[0])
         fmt.Println("id ", id[1])

         if id[0] == 0 {
             //Property not present in DB..Insert
             stmt, err := tx.Prepare("INSERT INTO properties (property) VALUES (?)")
             if err != nil { panic(err)}
             defer stmt.Close()
             res, err := stmt.Exec(prop)
             if err != nil { panic(err)}
             id[0], err = res.LastInsertId()
             if err != nil { 
                 panic(err)
             }
             fmt.Println("last inserted id in properties table=", id[0])
         }

         if id[1] == 0 && val != ""{
             //Property value not present in DB..insert
             stmt, err := tx.Prepare("INSERT INTO propertyValues (value) VALUES (?)")
             if err != nil { panic(err)}
             defer stmt.Close()
             res, err := stmt.Exec(value)
             if err != nil { panic(err)}
             id[1], err = res.LastInsertId()
             if err != nil { 
                 panic(err)
             }
             fmt.Println("last inserted id in propValues table =", id[1])
         }

         stmt, err := tx.Prepare("INSERT INTO mappings (event_id, prop_id, val_id) VALUES (?, ?, ?)")
         if err != nil { panic(err)}
         defer stmt.Close()
         _, err = stmt.Exec(mappingId, id[0], id[1])
         if err != nil { panic(err)}

     }

    tx.Commit()

}


