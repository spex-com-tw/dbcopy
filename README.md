# DbCopy

`DbCopy` 是一個 ASP.NET Core 工具，用來比較並複製兩個資料庫之間的結構與資料，支援：
- SQL Server
- PostgreSQL

專案同時提供 Web UI（Razor Pages）與 API（`/api/db/*`）。

## 功能重點

- 測試來源/目標資料庫連線
- 讀取來源資料庫物件清單
- 比對來源與目標差異
- 依階段複製資料庫物件
- 記錄 SQL 與操作日誌（Serilog，輸出至 console 與 `Logs/`）

## 技術棧

- .NET `net10.0`
- ASP.NET Core Razor Pages + Controllers
- Dapper
- Microsoft.Data.SqlClient
- Npgsql
- Serilog

## 開發環境需求

- .NET SDK 10.x
- 可連線的 SQL Server / PostgreSQL

## 快速開始

1. 還原與建置

```bash
dotnet restore
dotnet build
```

2. 啟動

```bash
dotnet run
```

預設開發網址（`Properties/launchSettings.json`）：
- `http://localhost:5281`
- `https://localhost:7256`

程式啟動後會自動開啟瀏覽器。

## API 概覽

Base path: `/api/db`

### 1. 測試連線

- `POST /api/db/test`

Request body：

```json
{
  "id": "src-1",
  "name": "Source DB",
  "type": 0,
  "connectionString": "Server=...;Database=...;User Id=...;Password=...;"
}
```

`type`：
- `0` = `SqlServer`
- `1` = `PostgreSql`

### 2. 取得物件清單

- `POST /api/db/objects`

Request body 與 `/test` 相同。

### 3. 比較差異

- `POST /api/db/compare`

```json
{
  "source": {
    "id": "src-1",
    "name": "Source",
    "type": 0,
    "connectionString": "..."
  },
  "target": {
    "id": "dst-1",
    "name": "Target",
    "type": 1,
    "connectionString": "..."
  }
}
```

回傳每個來源物件在目標端是否存在、依賴關係、資料列數與索引對應狀態。

### 4. 執行複製

- `POST /api/db/copy`

```json
{
  "source": {
    "id": "src-1",
    "name": "Source",
    "type": 0,
    "connectionString": "..."
  },
  "target": {
    "id": "dst-1",
    "name": "Target",
    "type": 1,
    "connectionString": "..."
  },
  "object": {
    "schema": "dbo",
    "name": "Orders",
    "type": 3
  },
  "phase": 0,
  "batchSize": 1000
}
```

`phase`：
- `0`：執行完整流程
- `1`：建立結構
- `2`：複製資料
- `3`：建立索引
- `4`：建立外鍵

## 發佈與版本

### 多平台發佈

```bash
./publish.sh
```

輸出至 `publish/<rid>/`，目前包含：
- `win-x64`
- `linux-x64`
- `linux-arm64`
- `osx-x64`
- `osx-arm64`

### 建立 release tag

```bash
./release.sh
```

腳本會：
- 檢查工作樹是否乾淨
- 根據本機與遠端 tag 推算下一個版本
- 建立並推送 tag 到 `origin`

## 專案結構

```text
Controllers/   API 控制器
Models/        請求/回應與領域模型
Services/      SQL Server / PostgreSQL 實作
Pages/         Razor Pages UI
wwwroot/       靜態資源（以 EmbeddedResource 打包）
Program.cs     啟動與 DI 設定
```

## 注意事項

- 請先確認來源與目標資料庫帳號權限足夠（讀取來源、建立/寫入目標）。
- 大量資料搬移建議先在測試環境驗證 `batchSize` 與執行時間。
- 日誌預設寫入 `Logs/log-*.txt`，可用於排錯與稽核。
