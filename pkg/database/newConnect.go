package database

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"

	_ "github.com/lib/pq"
	elastic "gopkg.in/olivere/elastic.v5"
)

func NewPGConn() (*sql.DB, error) {

	var db *sql.DB
	var err error
	var pgOnce sync.Once

	pgHost := os.Getenv("POSTGRES_HOST")
	pgUser := os.Getenv("POSTGRES_USER")
	pgPwd := os.Getenv("POSTGRES_PASSWORD")
	if pgHost == "" || pgUser == "" || pgPwd == "" {
		log.Printf("POSTGRES_HOST: %s, POSTGRES_USER: %s, POSTGRES_PASSWORD: %s\n", pgHost, pgUser, pgPwd)
		return nil, errors.New("environment variable not set.")
	}

	dataSource := fmt.Sprintf("postgres://%s:%s@%s:5432/meepshop?sslmode=disable", pgUser, pgPwd, pgHost)
	pgOnce.Do(func() {
		db, err = sql.Open("postgres", dataSource)
	})
	if err != nil {
		log.Println(err)
		return nil, err
	}

	return db, nil
}

func NewESConn() (*elastic.Client, error) {

	var es *elastic.Client
	var err error
	var esOnce sync.Once

	esUrls := os.Getenv("ELASTIC_URLS")
	esSniff := os.Getenv("ELASTIC_SNIFF")
	esIndex := os.Getenv("ELASTIC_DB")
	if esUrls == "" || esSniff == "" || esIndex == "" {
		log.Printf("ELASTIC_URLS: %s, ELASTIC_SNIFF: %s, ELASTIC_DB: %s\n", esUrls, esSniff, esIndex)
		return nil, errors.New("environment variable not set.")
	}

	elasticURLs := strings.Split(esUrls, ",")
	elasticSniff := esSniff == "true"
	esOnce.Do(func() {
		es, err = elastic.NewClient(elastic.SetURL(elasticURLs...), elastic.SetSniff(elasticSniff))
	})
	if err != nil {
		log.Println(err)
		return nil, err
	}

	return es, nil
}
