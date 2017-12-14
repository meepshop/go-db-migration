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
	"os/exec"
	"strings"
	"time"

	_ "github.com/lib/pq"
	"github.com/meepshop/go-db-migration/pkg/database"
	"github.com/meepshop/go-db-migration/pkg/utils"
	elastic "gopkg.in/olivere/elastic.v5"
)

type Migration struct {
	plugin string
	db     *sql.DB
	es     *elastic.Client
	oFile  *os.File
	uFile  *os.File
}

type MigrationData struct {
	Table     string
	Action    string
	Id        string
	Data      string
	Parent    string
	UpdatedAt string
}

type OriginData struct {
	Id     string
	Data   string
	Parent string
}

func NewMigration(plugin string, execTime string) Migration {

	pg, err := database.NewPGConn()
	if err != nil {
		os.Exit(1)
	}

	es, err := database.NewESConn()
	if err != nil {
		os.Exit(1)
	}

	originFile, err := os.Create("backup/" + execTime + "_originData")
	if err != nil {
		log.Fatal(err)
	}

	upsertFile, err := os.Create("backup/" + execTime + "_upsertID")
	if err != nil {
		log.Fatal(err)
	}

	return Migration{plugin, pg, es, originFile, upsertFile}
}

func (m *Migration) ProcDbBigration() {

	defer m.oFile.Close()
	defer m.uFile.Close()

	// 從Plugin取得要撈取的query
	query := m.getQuery()
	rows, err := m.db.Query(query)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	count := 0
	batchBuffer := map[string][]MigrationData{}
	for rows.Next() {
		var id, data string
		err := rows.Scan(&id, &data)
		if err != nil {
			log.Printf("Db Scan error ID: %s. %q\n", id, err)
			return
		}

		// 從Plugin取得轉換後的資料
		mDatas, err := m.getMigrationResult(id, data)
		if err != nil {
			return
		}

		for _, mData := range mDatas {
			count += 1
			batchBuffer[mData.Table] = append(batchBuffer[mData.Table], mData)
		}

		// 累積達到一定數量 批次進行資料更新
		if count >= 100 {
			err = m.dataUpdateAndBackup(batchBuffer)
			if err != nil {
				return
			}

			count = 0
			batchBuffer = map[string][]MigrationData{}
		}
	}

	if len(batchBuffer) > 0 {
		err = m.dataUpdateAndBackup(batchBuffer)
		if err != nil {
			return
		}
	}
}

func (m *Migration) getQuery() string {

	stdout, err := exec.Command(m.plugin, "-query").Output()
	if err != nil {
		log.Fatal(err)
	}

	query := string(stdout)

	// 確認是SELECT開頭之Query
	if index := strings.Index(query, "SELECT"); index != 0 {
		log.Fatal("plugin's stdout not query")
	}

	return query
}

func (m *Migration) getMigrationResult(id string, originData string) ([]MigrationData, error) {

	stdout, err := exec.Command(m.plugin, "-migration", strings.Replace(originData, `"`, `\"`, -1)).Output()
	if err != nil {
		log.Printf("Exec migration error ID: %s. %q\n", id, err)
		return []MigrationData{}, err
	}

	result := string(stdout)
	if result == "error" {
		log.Printf("Exec migration error ID: %s.\n", id)
		return []MigrationData{}, err
	}

	var mDatas []MigrationData
	err = json.Unmarshal([]byte(result), &mDatas)
	if err != nil {
		log.Printf("Exec migration unmarshal error ID: %s.\n", id)
		return []MigrationData{}, err
	}

	return mDatas, nil
}

func (m *Migration) dataUpdateAndBackup(batchBuffer map[string][]MigrationData) error {

	ctx := context.Background()

	tx, err := m.db.Begin()
	if err != nil {
		log.Fatalf("Pg tx Begin err: %+v", err)
	}

	defer tx.Rollback()

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
				updateAt, err := time.Parse(time.RFC3339, mData.UpdatedAt)
				if err != nil {
					log.Printf("convert time err: %+v", err)
					return err
				}

				values = append(values, fmt.Sprintf("('%s', '%s')", mData.Id, mData.Data))
				bulk.Add(elastic.NewBulkIndexRequest().Id(mData.Id).VersionType("external").Version(updateAt.UnixNano()).Parent(mData.Parent).Doc(mData.Data))
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
		stmt, err := tx.Prepare(delSql)
		_, err = stmt.Exec()
		if err != nil {
			log.Printf("PG exec error: %+v", err)
			return err
		}

		// PG UPSERT
		if len(values) > 0 {
			// upsSql := fmt.Sprintf("INSERT INTO %s (id, data) VALUES %s ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data", table, strings.Join(values, ","))
			upsSql := fmt.Sprintf("INSERT INTO %s (id, data) VALUES %s", table, strings.Join(values, ","))
			stmt, err := tx.Prepare(upsSql)
			if err != nil {
				log.Printf("PG tx.Prepare error: %+v", err)
				return err
			}

			_, err = stmt.Exec()
			if err != nil {
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
				if item.Error.Type == "version_conflict_engine_exception" {
					// continue
				}
				log.Printf("type: %s, Id: %s", item.Type, item.Id)
				log.Printf("reason type: %s, reason: %s", item.Error.Type, item.Error.Reason)
				return errors.New("Bulk error")
			}
		}
	}

	err = tx.Commit()
	if err != nil {
		log.Printf("PG commit error: %+v", err)
		return err
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
