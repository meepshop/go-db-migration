package dbMigration

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	_ "github.com/lib/pq"
	"github.com/meepshop/go-db-migration/pkg/database"
	"github.com/meepshop/go-db-migration/pkg/utils"
	elastic "gopkg.in/olivere/elastic.v5"
)

func QueryDataAndOutput(query string) error {

	pg, err := database.NewPGConn()
	if err != nil {
		return err
	}
	defer pg.Close()

	rows, err := pg.Query(query)
	if err != nil {
		log.Println(err)
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var id, data string
		err := rows.Scan(&id, &data)
		if err != nil {
			log.Printf("Db Scan error ID: %s. %q\n", id, err)
			return err
		}

		fmt.Println(data)
	}

	return nil
}

type Migration struct {
	db       *sql.DB
	es       *elastic.Client
	oFile    *os.File
	uFile    *os.File
	execTime int64
}

type MigrationData struct {
	Table  string
	Action string
	Id     string
	Data   string
	Parent string
}

type OriginData struct {
	Id     string
	Data   string
	Parent string
}

func NewMigration() (Migration, error) {

	m := Migration{}

	localLocation, _ := time.LoadLocation("UTC")
	execTime := time.Now().In(localLocation)
	m.execTime = execTime.UnixNano()

	pg, err := database.NewPGConn()
	if err != nil {
		return m, err
	}
	m.db = pg

	es, err := database.NewESConn()
	if err != nil {
		return m, err
	}
	m.es = es

	timeString := execTime.Format("20060102150405")
	log.Println(timeString)
	originFile, err := os.Create("backup/" + timeString + "_originData")
	if err != nil {
		log.Printf("%+v", err)
		return m, err
	}
	m.oFile = originFile

	upsertFile, err := os.Create("backup/" + timeString + "_upsertID")
	if err != nil {
		log.Printf("%+v", err)
		return m, err
	}
	m.uFile = upsertFile

	return m, nil
}

func (m *Migration) ProcDbBigration() error {

	scanner := bufio.NewScanner(os.Stdin)
	if err := scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "reading standard input:", err)
	}

	fmt.Println(scanner.Text())
	return nil

	count := 0
	batchBuffer := map[string][]MigrationData{}
	for scanner.Scan() {

		var mDatas []MigrationData
		err := json.Unmarshal(scanner.Bytes(), &mDatas)
		if err != nil {
			log.Printf("Exec migration unmarshal error. Data: %s.\n", scanner.Text())
			return err
		}

		for _, mData := range mDatas {
			count += 1
			batchBuffer[mData.Table] = append(batchBuffer[mData.Table], mData)
		}

		// 累積達到一定數量 批次進行資料更新
		if count >= 100 {
			err = m.dataUpdateAndBackup(batchBuffer)
			if err != nil {
				return err
			}

			count = 0
			batchBuffer = map[string][]MigrationData{}
		}
	}

	if len(batchBuffer) > 0 {
		if err := m.dataUpdateAndBackup(batchBuffer); err != nil {
			return err
		}
	}

	return nil
}

func (m *Migration) dataUpdateAndBackup(batchBuffer map[string][]MigrationData) error {

	ctx := context.Background()

	for table, mDatas := range batchBuffer {

		esTable := utils.PgEsTableMapping[table]
		if esTable == "" {
			esTable = table
		}

		var values []string
		bulk := m.es.Bulk().Index(os.Getenv("ELASTIC_DB")).Type(esTable)

		oDatas := []OriginData{}
		changeIds := []string{}

		for _, mData := range mDatas {

			changeIds = append(changeIds, mData.Id)

			if mData.Action == "DELETE" {
				bulk.Add(elastic.NewBulkDeleteRequest().Id(mData.Id))
			} else if mData.Action == "UPSERT" {
				values = append(values, fmt.Sprintf("('%s', '%s')", mData.Id, mData.Data))
				bulk.Add(elastic.NewBulkIndexRequest().Id(mData.Id).VersionType("external").Version(m.execTime).Parent(mData.Parent).Doc(mData.Data))
			}
		}

		// 備份所有有變動的資料
		oQuery := `SELECT id, CASE WHEN data->>'__parent' IS NOT NULL THEN data->>'__parent' ELSE '' END, data FROM %s WHERE id IN ('%s')`
		rows, err := m.db.Query(fmt.Sprintf(oQuery, table, strings.Join(changeIds, "','")))
		if err != nil {
			log.Printf("PG error: %+v", err)
			return err
		}

		for rows.Next() {
			var id, parent, data string
			err = rows.Scan(&id, &parent, &data)
			if err != nil {
				log.Printf("Db Scan error Table: %s ID: %s DATA: %s\n", table, id, data)
				return err
			}

			oDatas = append(oDatas, OriginData{Id: id, Parent: parent, Data: data})
		}

		// 將原有資料寫入備份檔案
		m.writeToBackupFile(table, oDatas, changeIds)

		// PG DELETE
		delSql := fmt.Sprintf("DELETE FROM %s WHERE id IN ('%s')", table, strings.Join(changeIds, "','"))
		if _, err := m.db.Exec(delSql); err != nil {
			log.Printf("PG exec error: %+v", err)
			return err
		}

		// PG UPSERT
		if len(values) > 0 {
			// upsSql := fmt.Sprintf("INSERT INTO %s (id, data) VALUES %s ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data", table, strings.Join(values, ","))
			upsSql := fmt.Sprintf("INSERT INTO %s (id, data) VALUES %s", table, strings.Join(values, ","))
			if _, err := m.db.Exec(upsSql); err != nil {
				log.Printf("PG exec error: %+v", err)
				return err
			}
		}

		// ES Bulk Do
		res, err := bulk.Do(ctx)
		if err != nil {
			log.Printf("ES bulk.Do error: %+v", err)
			return errors.New("Bulk error")
		}
		if res.Errors {
			for _, item := range res.Failed() {
				log.Printf("type: %s, Id: %s", item.Type, item.Id)
				log.Printf("reason type: %s, reason: %s", item.Error.Type, item.Error.Reason)
				return errors.New("Bulk error")
			}
		}
	}

	return nil
}

func (m *Migration) writeToBackupFile(table string, oDatas []OriginData, changeIds []string) error {

	oWriter := bufio.NewWriter(m.oFile)
	for _, oData := range oDatas {
		oWriter.WriteString(table + "!@#" + oData.Id + "!@#" + oData.Parent + "!@#" + oData.Data + "\n")
	}
	err := oWriter.Flush()
	if err != nil {
		log.Println(err)
		return err
	}

	uWriter := bufio.NewWriter(m.uFile)
	uWriter.WriteString(table + "\n" + strings.Join(changeIds, ",") + "\n")
	err = uWriter.Flush()
	if err != nil {
		log.Println(err)
		return err
	}

	return nil
}

func (m *Migration) Close() {

	if m.db != nil {
		m.db.Close()
	}

	if m.es != nil {
		m.es.Stop()
	}

	if m.oFile != nil {
		m.oFile.Close()
	}

	if m.uFile != nil {
		m.uFile.Close()
	}
}
