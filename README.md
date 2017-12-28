# go-db-migration

## Migration

main.go --query 撈取資料後，pipe 給 Plugin，
plugin再將回轉換後的結果pipe 回 main.go --consumer，
go 累積到一定數量後 批次進行PG ES的更新．

```
    go run main.go --query="select * from users" | ./pluginExample | go run main.go --consumer
```

**一但執行過程有任何一筆錯誤 程式將會中斷 此時PG、ES可能會不同步，請進行Recover**

**由於ES限制 不論新增修改還原 會將version設為執行當下的UnixNano**

## Recover
每次執行migration時
會依照執行時間在/backup 已執行時間為檔名 產生備份檔案
還原時要傳入欲還原的備份時間
```
    go run main.go --recover=20060102150405
```

## Plugin
每個plugin需接收sidin，內容為query出的data，格式為json line，需判斷是否有多筆；
並透過stdout一筆一筆傳出轉換後的結果(json string)
轉換後的結果請遵照以下格式：
```
[
    {
        "table": "store", // POSTGRES Table name
        "action": "UPSERT", // UPSERT or DELETE
        "id": "000e5620-9a0d-44d1-b155-0e9ed6f589a2", // 資料ID
        "parent": "", // Parent ID 若無請給空字串
        "data": "{\"id\": \"000e5620-9a0d-44d1-b155-0e9ed6f589a2\", \"storeStatus\": 1}"
    },
    {
        "table": "product",
        "action": "DELETE",
        "id": "000e5620-9a0d-44d1-b155-0e9ed6f589a2",
        "parent": "",
        "data": ""
    },
]
```
**若不需進行任何處理，請回傳空array**
