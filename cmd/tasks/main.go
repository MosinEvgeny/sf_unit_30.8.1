package main

import (
	"fmt"
	"log"
	"os"

	postgres "github.com/MosinEvgeny/pkg/storage"
	"github.com/joho/godotenv"
)

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	dbuser := os.Getenv("DBUSER")
	dbpass := os.Getenv("DPPASS")
	dbname := os.Getenv("DBNAME")
	dbhost := os.Getenv("DBHOST")
	dbport := os.Getenv("DBPORT")

	if dbuser == "" || dbpass == "" || dbname == "" || dbhost == "" || dbport == "" {
		log.Fatal("Не все переменные окружения установлены")
	}

	connstr := fmt.Sprintf("postgres://%s:%s@%s:%s/%s", dbuser, dbpass, dbhost, dbport, dbname)

	db, err := postgres.New(connstr)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
}
