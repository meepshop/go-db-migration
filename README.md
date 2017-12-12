# go-db-migration

## Migration
依照plugin -query 的內容撈取資料
再一筆一筆透過 Plugin -migration 取回轉換後的結果
累積到一定數量 批次進行PG ES的更新
一但執行過程有任何一筆錯誤 程式將會中斷 此時PG、ES可能會不同步
可選擇排除問題重新執行 或進行Recover

## Recover
每次執行migration時
會依照執行時間在/backup 產生備份檔案
要還原時要輸入之前備份的時間
ex: 20060102150405

## Plugin
每個plugin需提供以下兩個功能
* -query:
    * input: null
    * output: 印出查詢query 條件不拘 但必須要查出id、data
    ex: SELECT id, data FROM ... WHERE ...
* -migration
    * input: 吃兩個參數 依序為 id、data
    * output:
```
[
    {
        "table": "store", // POSTGRES Table name
        "action": "UPSERT", // UPSERT or DELETE
        "id": "000E5620-9A0D-44D1-B155-0E9ED6F589A2", // 資料ID 大寫
        "parent": "", // Parent ID 小寫，若無請給空字串
        "updatedAt": "1986-01-01T00:00:00.002Z",
        "data": "{\"id\": \"000e5620-9a0d-44d1-b155-0e9ed6f589a2\", \"storeStatus\": 1}"
    },
    {
        "table": "product",
        "action": "DELETE",
        "id": "000E5620-9A0D-44D1-B155-0E9ED6F589A2",
        "parent": "",
        "updatedAt": "",
        "data": ""
    },
]
```
