package recover

import (
	"bufio"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	_ "github.com/lib/pq"
	"github.com/meepshop/go-db-migration/pkg/database"
	"github.com/meepshop/go-db-migration/pkg/utils"
	elastic "gopkg.in/olivere/elastic.v5"
)

type Recover struct {
	db          *sql.DB
	es          *elastic.Client
	oFile       *os.File
	uFile       *os.File
	curTimeNano int64
}

type DeleteIDs struct {
	Table string
	IDs   string
}

type BackupOrigin struct {
	Table  string
	Id     string
	Parent string
	Data   string
}

func NewRecover(backup string) (Recover, error) {

	r := Recover{}
	r.curTimeNano = time.Now().UnixNano()

	pg, err := database.NewPGConn()
	if err != nil {
		return Recover{}, err
	}
	r.db = pg

	es, err := database.NewESConn()
	if err != nil {
		return r, err
	}
	r.es = es

	oFile, err := os.Open("backup/" + backup + "_originData")
	if err != nil {
		log.Printf("%+v", err)
		return r, err
	}
	r.oFile = oFile

	uFile, err := os.Open("backup/" + backup + "_upsertID")
	if err != nil {
		log.Printf("%+v", err)
		return r, err
	}
	r.uFile = uFile

	return r, nil
}

func (r *Recover) ProcRecover() error {

	// 先讀取upsertID 把所有變更過的資料刪除
	uScanner := bufio.NewScanner(r.uFile)
	uScanner.Split(bufio.ScanLines)

	curTable := ""
	curTableRow := true
	allDeleteIDs := [][]string{}
	for uScanner.Scan() {

		if curTableRow {
			curTable = uScanner.Text()
			curTableRow = false
		} else {
			delIDs := []string{curTable, uScanner.Text()}
			allDeleteIDs = append(allDeleteIDs, delIDs)
			curTableRow = true
		}
	}

	err := r.doDelete(allDeleteIDs)
	if err != nil {
		return err
	}

	// 再讀取originData 將原資料寫回
	originDatas := map[string][]BackupOrigin{}
	oReader := bufio.NewReader(r.oFile)
	for {
		line, err := oReader.ReadString('\n')
		if err == io.EOF {
			break
		} else if err != nil {
			log.Print(err)
			return err
		}

		o := strings.Split(line, "!@#")
		originDatas[o[0]] = append(originDatas[o[0]], BackupOrigin{
			Table:  o[0],
			Id:     o[1],
			Parent: o[2],
			Data:   o[3],
		})

		if len(originDatas) == 100 {
			if err := r.doInsert(originDatas); err != nil {
				return err
			}

			originDatas = map[string][]BackupOrigin{}
		}

	}

	if err := r.doInsert(originDatas); err != nil {
		return err
	}

	/*
		// 用NewScanner有個天殺的坑 會讀不完全部 原因不明
		oScanner := bufio.NewScanner(r.oFile)
		oScanner.Split(bufio.ScanLines)

		originDatas := map[string][]BackupOrigin{}
		count := 0
		for oScanner.Scan() {
			count += 1

			o := strings.Split(oScanner.Text(), "!@#")
			originDatas[o[0]] = append(originDatas[o[0]], BackupOrigin{
				Table:  o[0],
				Id:     o[1],
				Parent: o[2],
				Data:   o[3],
			})

			if count == 100 {
				err = r.doInsert(originDatas)
				if err != nil {
					return err
				}

				originDatas = map[string][]BackupOrigin{}
			}
		}

		err = r.doInsert(originDatas)
		if err != nil {
			return err
		}
	*/
	return nil
}

func (r *Recover) doInsert(originDatas map[string][]BackupOrigin) error {

	ctx := context.Background()

	for table, oDatas := range originDatas {

		esTable := utils.PgEsTableMapping[table]
		if esTable == "" {
			esTable = table
		}

		var values []string
		bulk := r.es.Bulk().Index(os.Getenv("ELASTIC_DB")).Type(esTable)

		for _, oData := range oDatas {
			// escape '
			ecoData := strings.Replace(oData.Data, "'", "''", -1)
			values = append(values, fmt.Sprintf("('%s', '%s')", oData.Id, ecoData))
			bulk.Add(elastic.NewBulkIndexRequest().Id(oData.Id).VersionType("external").Version(r.curTimeNano).Parent(oData.Parent).Doc(oData.Data))
		}

		// PG Insert
		upsSql := fmt.Sprintf("INSERT INTO %s (id, data) VALUES %s", table, strings.Join(values, ","))
		_, err := r.db.Exec(upsSql)
		if err != nil {
			log.Printf("PG Insert error: %+v", err)
			return err
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

	return nil
}

func (r *Recover) doDelete(allDeleteIDs [][]string) error {

	ctx := context.Background()

	for _, delIDs := range allDeleteIDs {

		esTable := utils.PgEsTableMapping[delIDs[0]]
		if esTable == "" {
			esTable = delIDs[0]
		}

		bulk := r.es.Bulk().Index(os.Getenv("ELASTIC_DB")).Type(esTable)

		idArr := strings.Split(delIDs[1], ",")
		delSql := fmt.Sprintf("DELETE FROM %s WHERE id IN ('%s')", delIDs[0], strings.Join(idArr, "','"))

		_, err := r.db.Exec(delSql)
		if err != nil {
			log.Printf("doRecover pg exec error: %+v", err)
			return err
		}

		for _, id := range idArr {
			bulk.Add(elastic.NewBulkDeleteRequest().Id(id))
		}

		// ES 批次執行
		res, err := bulk.Do(ctx)
		if err != nil {
			log.Printf("ES bulk.Do error: %+v", err)
			return err
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

func (r *Recover) Close() {

	if r.db != nil {
		r.db.Close()
	}

	if r.es != nil {
		r.es.Stop()
	}

	if r.oFile != nil {
		r.oFile.Close()
	}

	if r.uFile != nil {
		r.uFile.Close()
	}
}
