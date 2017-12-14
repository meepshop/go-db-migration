package main

import (
	"fmt"
	"time"

	"github.com/meepshop/go-db-migration/pkg/dbMigration"
	"github.com/meepshop/go-db-migration/pkg/recover"
)

func main() {

	var ans string

	for {
		if ans = qIndex(); ans == "A" || ans == "B" {
			break
		}
	}

	switch ans {
	case "A":
		qDbMigration()
	case "B":
		qRecover()
	}
}

func qIndex() string {
	var input string
	fmt.Print("A. DB-Migration\nB. Recover\n[A/B]:")
	fmt.Scanln(&input)
	return input
}

func qDbMigration() string {
	var input string
	fmt.Print("Plugin Path:")
	fmt.Scanln(&input)

	localLocation, _ := time.LoadLocation("UTC")
	execTime := time.Now().In(localLocation).Format("20060102150405")
	fmt.Print("本次備份時間:" + execTime + "\n")

	mgt := dbMigration.NewMigration(input, execTime)
	mgt.ProcDbBigration()

	return "qDbMigration"
}

func qRecover() string {
	var input string
	fmt.Print("請輸入備份時間:")
	fmt.Scanln(&input)

	rc := recover.NewRecover(input)
	err := rc.ProcRecover()
	if err != nil {
		fmt.Printf("%+v", err)
	}

	return input
}
