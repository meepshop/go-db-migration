package main

import (
	"log"
	"os"
	"strings"

	"github.com/meepshop/go-db-migration/pkg/dbMigration"
	"github.com/meepshop/go-db-migration/pkg/recover"
)

// go run main.go --query="select * from users" | ./testPlugin.js | go run main.go --consumer
// go run main.go --recover=20171222085205

func main() {

	if len(os.Args) < 2 {
		log.Println("no illegal action")
	}

	params := strings.Split(os.Args[1], "=")
	switch params[0] {
	case "--query":

		// 確認是SELECT開頭之Query
		if index := strings.Index(os.Args[1][8:], "SELECT"); index != 0 {
			log.Println("Query format error")
		} else {
			dbMigration.QueryDataAndOutput(os.Args[1][8:])
		}

	case "--recover":

		rc, err := recover.NewRecover(params[1])
		if err == nil {
			rc.ProcRecover()
		}
		rc.Close()

	case "--consumer":

		mgt, err := dbMigration.NewMigration()
		if err == nil {
			mgt.ProcDbBigration()
		}
		mgt.Close()

	default:
		log.Println("no illegal action")
	}
}
