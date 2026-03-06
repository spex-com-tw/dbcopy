using Dapper;
using DbCopy.Models;
using Microsoft.Data.SqlClient;

namespace DbCopy.Services;

/// <summary>
/// <see cref="IDbService"/> 的 SQL Server 實作。
/// 所有 Dapper 呼叫均透過 <see cref="DapperExtensions"/> 的 WithLog wrapper，
/// 確保每一條 SQL 恰好被記錄一次。
/// 直接使用 <see cref="SqlCommand"/>（繞過 Dapper）的路徑則呼叫
/// <see cref="LogExecutingSqlSql"/> 明確記錄。
/// </summary>
public partial class SqlServerService(ILogger<SqlServerService> logger) : IDbService
{
    /// <summary>
    /// 在 <c>sys.*</c> 目錄檢視中，<c>max_length</c> 以 <em>位元組</em> 儲存。
    /// nvarchar(50) → max_length = 100；varchar(50) → max_length = 50。
    /// 此陣列列出所有受此規則影響的類型，<see cref="FormatSqlServerTypeLength"/>
    /// 會據此換算為 T-SQL DDL 所需的字元長度。
    /// </summary>
    private static readonly string[] TypesWithByteLength =
        ["varchar", "nvarchar", "char", "nchar", "varbinary", "binary"];

    /// <summary>
    /// 開啟連線以驗證連線字串有效且伺服器可連線。
    /// </summary>
    public async Task<bool> TestConnectionAsync(string connectionString)
    {
        await using var conn = new SqlConnection(connectionString);
        await conn.OpenAsync();
        return true;
    }

    /// <summary>
    /// 傳回資料庫中所有使用者定義物件：資料表、檢視、預存程序、
    /// 純量/資料表/行內函數、使用者定義型別、使用者定義資料表型別、序列。
    /// 系統物件（<c>is_ms_shipped = 1</c>）不包含在內。
    /// </summary>
    public async Task<List<DbObject>> GetDbObjectsAsync(string connectionString)
    {
        await using var conn = new SqlConnection(connectionString);
        const string sql = """
                           SELECT SCHEMA_NAME(schema_id) AS [Schema], name AS Name, 'Table' AS Type FROM sys.tables WHERE is_ms_shipped = 0
                           UNION ALL
                           SELECT SCHEMA_NAME(schema_id) AS [Schema], name AS Name, 'View' AS Type FROM sys.views WHERE is_ms_shipped = 0
                           UNION ALL
                           SELECT SCHEMA_NAME(schema_id) AS [Schema], name AS Name, 'Procedure' AS Type FROM sys.procedures WHERE is_ms_shipped = 0
                           UNION ALL
                           SELECT SCHEMA_NAME(schema_id) AS [Schema], name AS Name, 'Function' AS Type FROM sys.objects WHERE type IN ('FN', 'IF', 'TF') AND is_ms_shipped = 0
                           UNION ALL
                           SELECT SCHEMA_NAME(schema_id) AS [Schema], name AS Name, 'UserDefinedType' AS Type FROM sys.types WHERE is_user_defined = 1 AND is_table_type = 0
                           UNION ALL
                           SELECT SCHEMA_NAME(schema_id) AS [Schema], name AS Name, 'UserDefinedTableType' AS Type FROM sys.table_types WHERE is_user_defined = 1
                           UNION ALL
                           SELECT SCHEMA_NAME(schema_id) AS [Schema], name AS Name, 'Sequence' AS Type FROM sys.sequences
                           """;

        var results = await conn.QueryWithLogAsync(logger, sql);
        return results.Select(r => new DbObject
        {
            Schema = r.Schema,
            Name = r.Name,
            Type = Enum.Parse<DbObjectType>(r.Type)
        }).ToList();
    }

    /// <summary>
    /// 傳回物件的 DDL 定義，供目標資料庫重建之用。
    /// <list type="bullet">
    ///   <item>資料表與 UDT：從目錄中繼資料重組。</item>
    ///   <item>檢視、程序、函數：使用 <c>OBJECT_DEFINITION()</c>。</item>
    ///   <item>序列：從 <c>sys.sequences</c> 重組（不支援 OBJECT_DEFINITION）。</item>
    /// </list>
    /// </summary>
    public async Task<string> GetObjectDefinitionAsync(string connectionString, DbObject obj)
    {
        await using var conn = new SqlConnection(connectionString);
        if (obj.Type == DbObjectType.Table)
            return await GetTableDefinitionAsync(conn, obj);

        if (obj.Type == DbObjectType.UserDefinedType)
            return await GetUserDefinedTypeDefinitionAsync(conn, obj);

        if (obj.Type == DbObjectType.UserDefinedTableType)
            return await GetUserDefinedTableTypeDefinitionAsync(conn, obj);

        // 序列沒有儲存在 sys.sql_modules 的文字定義，需從 sys.sequences 重組。
        if (obj.Type == DbObjectType.Sequence)
            return await GetSequenceDefinitionAsync(conn, obj);

        // 檢視、程序、函數的原始碼儲存於 sys.sql_modules，OBJECT_DEFINITION() 可直接取得。
        const string sql = "SELECT OBJECT_DEFINITION(OBJECT_ID(QUOTENAME(@Schema) + '.' + QUOTENAME(@Name)))";
        return await conn.ExecuteScalarWithLogAsync<string>(logger, sql, new { obj.Schema, obj.Name }) ?? "";
    }

    /// <summary>
    /// 當物件已存在於目標資料庫時傳回 <c>true</c>。
    /// 每種物件類型各自查詢對應的目錄檢視。
    /// </summary>
    public async Task<bool> CheckObjectExistsAsync(string connectionString, DbObject obj)
    {
        await using var conn = new SqlConnection(connectionString);
        var sql = obj.Type switch
        {
            DbObjectType.Table => "SELECT 1 FROM sys.tables WHERE SCHEMA_NAME(schema_id) = @Schema AND name = @Name",
            DbObjectType.View => "SELECT 1 FROM sys.views WHERE SCHEMA_NAME(schema_id) = @Schema AND name = @Name",
            DbObjectType.Procedure =>
                "SELECT 1 FROM sys.procedures WHERE SCHEMA_NAME(schema_id) = @Schema AND name = @Name",
            DbObjectType.Function =>
                "SELECT 1 FROM sys.objects WHERE type IN ('FN', 'IF', 'TF') AND SCHEMA_NAME(schema_id) = @Schema AND name = @Name",
            DbObjectType.UserDefinedType =>
                "SELECT 1 FROM sys.types WHERE is_user_defined = 1 AND is_table_type = 0 AND SCHEMA_NAME(schema_id) = @Schema AND name = @Name",
            DbObjectType.UserDefinedTableType =>
                "SELECT 1 FROM sys.table_types WHERE is_user_defined = 1 AND SCHEMA_NAME(schema_id) = @Schema AND name = @Name",
            DbObjectType.Sequence =>
                "SELECT 1 FROM sys.sequences WHERE SCHEMA_NAME(schema_id) = @Schema AND name = @Name",
            _ => throw new ArgumentOutOfRangeException()
        };
        return await conn.ExecuteScalarWithLogAsync<int?>(logger, sql, new { obj.Schema, obj.Name }) != null;
    }

    /// <summary>
    /// 若 Schema 不存在則在目標伺服器建立之。
    /// <c>dbo</c> 在 SQL Server 中永遠存在，故略過。
    /// Schema 名稱中的 <c>]</c> 字元會被雙重跳脫以防止 identifier injection。
    /// </summary>
    public async Task EnsureSchemaExistsAsync(string connectionString, string schema)
    {
        if (string.IsNullOrEmpty(schema) || schema.Equals("dbo", StringComparison.OrdinalIgnoreCase)) return;

        await using var conn = new SqlConnection(connectionString);
        var exists =
            await conn.ExecuteScalarWithLogAsync<int?>(logger,
                "SELECT 1 FROM sys.schemas WHERE name = @schema", new { schema }) != null;
        if (!exists)
        {
            // 在方括號識別符內部，] 須雙重為 ]] 以避免注入。
            await conn.ExecuteWithLogAsync(logger, $"CREATE SCHEMA [{schema.Replace("]", "]]")}]");
        }
    }

    /// <summary>
    /// 傳回 <paramref name="obj"/> 所依賴的物件名稱清單，供呼叫端決定複製順序。
    /// <list type="bullet">
    ///   <item>所有類型：走訪 <c>sys.sql_expression_dependencies</c>。</item>
    ///   <item>資料表/檢視/程序/函數：額外查詢欄位所使用的 UDT 類型。</item>
    ///   <item>程序/函數：額外查詢參數所使用的 UDT/UDTT 類型。</item>
    /// </list>
    /// </summary>
    public async Task<List<string>> GetDependenciesAsync(string connectionString, DbObject obj)
    {
        await using var conn = new SqlConnection(connectionString);
        const string sql = """
                           SELECT DISTINCT
                               COALESCE(referenced_schema_name, SCHEMA_NAME(o.schema_id), 'dbo') + '.' + referenced_entity_name
                           FROM sys.sql_expression_dependencies sed
                           LEFT JOIN sys.objects o ON sed.referenced_id = o.object_id
                           WHERE referencing_id = OBJECT_ID(QUOTENAME(@Schema) + '.' + QUOTENAME(@Name))
                             AND referenced_entity_name IS NOT NULL
                           """;

        var results = (await conn.QueryWithLogAsync<string>(logger, sql, new { obj.Schema, obj.Name })).ToList();

        // 只有資料表、檢視、程序、函數才可能透過欄位或參數參考 UDT。
        if (obj.Type is not (DbObjectType.Table or DbObjectType.View or DbObjectType.Procedure
            or DbObjectType.Function))
            return results.Distinct().ToList();

        // 查詢欄位所參考的使用者定義型別（資料表與檢視）。
        const string udtSql = """
                              SELECT DISTINCT SCHEMA_NAME(t.schema_id) + '.' + t.name
                              FROM sys.columns c
                              JOIN sys.types t ON c.user_type_id = t.user_type_id
                              WHERE c.object_id = OBJECT_ID(QUOTENAME(@Schema) + '.' + QUOTENAME(@Name)) AND t.is_user_defined = 1
                              """;
        var udtDeps = await conn.QueryWithLogAsync<string>(logger, udtSql, new { obj.Schema, obj.Name });
        results.AddRange(udtDeps);

        // 查詢參數所使用的 UDT/UDTT（僅程序與函數；檢視無參數）。
        if (obj.Type is DbObjectType.Procedure or DbObjectType.Function)
        {
            const string paramSql = """
                                    SELECT DISTINCT SCHEMA_NAME(t.schema_id) + '.' + t.name
                                    FROM sys.parameters p
                                    JOIN sys.types t ON p.user_type_id = t.user_type_id
                                    WHERE p.object_id = OBJECT_ID(QUOTENAME(@Schema) + '.' + QUOTENAME(@Name)) AND t.is_user_defined = 1
                                    """;
            var paramDeps = await conn.QueryWithLogAsync<string>(logger, paramSql, new { obj.Schema, obj.Name });
            results.AddRange(paramDeps);
        }

        return results.Distinct().ToList();
    }

    /// <summary>
    /// 使用 <c>COUNT_BIG(*)</c> 傳回資料表的精確列數，
    /// 可正確處理超過 20 億列的大型資料表。
    /// 非資料表物件傳回 0。
    /// </summary>
    public async Task<long> GetRowCountAsync(string connectionString, DbObject obj)
    {
        if (obj.Type != DbObjectType.Table) return 0;
        await using var conn = new SqlConnection(connectionString);
        return await conn.ExecuteScalarWithLogAsync<long>(logger,
            $"SELECT COUNT_BIG(*) FROM [{obj.Schema.Replace("]", "]]")}].[{obj.Name.Replace("]", "]]")}]");
    }

    /// <summary>
    /// 傳回資料表的索引中繼資料：名稱、唯一性、索引鍵欄（依鍵序排列）
    /// 及 INCLUDE 欄（非鍵欄）。<c>type &gt; 0</c> 過濾器排除 Heap 虛擬索引。
    /// </summary>
    public async Task<List<DbIndex>> GetTableIndexesAsync(string connectionString, DbObject obj)
    {
        await using var conn = new SqlConnection(connectionString);
        const string sql = """
                           SELECT
                               i.name AS Name,
                               i.is_unique AS IsUnique,
                               STRING_AGG(CASE WHEN ic.is_included_column = 0 THEN c.name END, ', ') WITHIN GROUP (ORDER BY ic.key_ordinal, ic.index_column_id) AS Columns,
                               STRING_AGG(CASE WHEN ic.is_included_column = 1 THEN c.name END, ', ') WITHIN GROUP (ORDER BY ic.key_ordinal, ic.index_column_id) AS IncludedColumns
                           FROM sys.indexes i
                           JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
                           JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                           WHERE i.object_id = OBJECT_ID(QUOTENAME(@Schema) + '.' + QUOTENAME(@Name)) AND i.type > 0 -- skip heap
                           GROUP BY i.name, i.is_unique
                           """;

        var results = await conn.QueryWithLogAsync(logger, sql, new { obj.Schema, obj.Name });
        return results.Select(r =>
        {
            var dict = (IDictionary<string, object>)r;
            var columns = dict["Columns"].ToString();
            var includedColumns = dict["IncludedColumns"].ToString();
            var displayColumns = string.IsNullOrEmpty(includedColumns)
                ? columns
                : $"{columns} (Include: {includedColumns})";
            return new DbIndex
            {
                Name = r.Name,
                IsUnique = r.IsUnique,
                Columns = displayColumns ?? ""
            };
        }).ToList();
    }

    /// <summary>
    /// 將物件從來源複製到目標資料庫，分為最多四個階段：
    /// <list type="number">
    ///   <item>Phase 1 — 結構（CREATE TABLE / CREATE PROCEDURE / …）</item>
    ///   <item>Phase 2 — 資料（大量複製資料列；僅資料表）</item>
    ///   <item>Phase 3 — 索引（非主鍵；僅資料表）</item>
    ///   <item>Phase 4 — 外部索引鍵（僅資料表）</item>
    /// </list>
    /// 傳入 <c>phase = 0</c> 可依序執行全部四個階段。
    /// 索引與外鍵建立失敗時記錄警告，不中斷整體複製流程。
    /// </summary>
    public async Task CopyObjectAsync(string sourceConnectionString, string targetConnectionString, DbObject obj,
        int phase = 0, int batchSize = 1000)
    {
        var definition = await GetObjectDefinitionAsync(sourceConnectionString, obj);
        if (string.IsNullOrEmpty(definition)) throw new Exception("Could not get object definition.");

        await using var conn = new SqlConnection(targetConnectionString);

        if (obj.Type == DbObjectType.Table)
        {
            if (phase is 0 or 1) // Phase 1: 建立資料表結構（欄位、主鍵、條件約束）。
            {
                await conn.ExecuteWithLogAsync(logger, definition);
            }

            if (phase is 0 or 2) // Phase 2: 大量複製來源資料列至目標。
            {
                await CopyTableDataAsync(sourceConnectionString, targetConnectionString, obj, batchSize);
            }

            if (phase is 0 or 3) // Phase 3: 建立非主鍵索引；失敗時記錄警告後繼續。
            {
                var indexSqls = await GetTableIndexDefinitionsAsync(sourceConnectionString, obj);
                foreach (var idx in indexSqls)
                {
                    try
                    {
                        await conn.ExecuteWithLogAsync(logger, idx);
                    }
                    catch (Exception ex)
                    {
                        logger.LogWarning(ex, "Failed to create index for {Schema}.{Name}", obj.Schema, obj.Name);
                    }
                }
            }

            if (phase is 0 or 4) // Phase 4: 新增外部索引鍵；若參考資料表不存在則跳過。
            {
                var fkSqls = await GetTableForeignKeyDefinitionsAsync(sourceConnectionString, obj);
                foreach (var fk in fkSqls)
                {
                    try
                    {
                        await conn.ExecuteWithLogAsync(logger, fk);
                    }
                    catch (SqlException ex) when (ex.Number == 4902) // 參考物件不存在（Error 4902）
                    {
                        logger.LogInformation(
                            "Skipped foreign key creation for {Schema}.{Name} - referenced object does not exist in target (Error 4902)",
                            obj.Schema, obj.Name);
                    }
                    catch (Exception ex)
                    {
                        logger.LogWarning(ex, "Failed to create foreign key for {Schema}.{Name}", obj.Schema, obj.Name);
                    }
                }
            }
        }
        else
        {
            // 非資料表物件（檢視、程序、函數、UDT、序列）只有結構階段。
            if (phase is 0 or 1)
            {
                await conn.ExecuteWithLogAsync(logger, definition);
            }
        }
    }

    /// <summary>
    /// 使用 <see cref="SqlBulkCopy"/> 搭配 KEEP_IDENTITY 與 KEEP_NULLS 選項，
    /// 將來源資料表的所有資料列大量複製至目標資料表。
    /// 複製完成後重設 IDENTITY 欄位的種子值，避免後續 INSERT 發生主鍵衝突。
    /// </summary>
    private async Task CopyTableDataAsync(string sourceConnectionString, string targetConnectionString, DbObject obj,
        int batchSize)
    {
        await using var sourceConn = new SqlConnection(sourceConnectionString);
        await sourceConn.OpenAsync();

        var sql = $"SELECT * FROM [{obj.Schema.Replace("]", "]]")}].[{obj.Name.Replace("]", "]]")}]";
        await using var cmd = new SqlCommand(sql, sourceConn);
        // 此處使用 SqlCommand 而非 Dapper，需明確呼叫 LogExecutingSqlSql 記錄 SQL。
        LogExecutingSqlSql(logger, sql);
        await using var reader = await cmd.ExecuteReaderAsync();

        await using var targetConn = new SqlConnection(targetConnectionString);
        await targetConn.OpenAsync();

        using var bulkCopy = new SqlBulkCopy(targetConn, SqlBulkCopyOptions.KeepIdentity | SqlBulkCopyOptions.KeepNulls,
            null);
        bulkCopy.DestinationTableName = $"[{obj.Schema.Replace("]", "]]")}].[{obj.Name.Replace("]", "]]")}]";
        bulkCopy.BulkCopyTimeout = 600; // 10 分鐘
        bulkCopy.BatchSize = batchSize;

        await bulkCopy.WriteToServerAsync(reader);

        // 若有 IDENTITY 欄位，將種子重設為目前最大值，防止後續 INSERT 發生 PK 衝突。
        var hasIdentity = await targetConn.ExecuteScalarWithLogAsync<int?>(logger,
            "SELECT 1 FROM sys.identity_columns WHERE object_id = OBJECT_ID(QUOTENAME(@Schema) + '.' + QUOTENAME(@Name))",
            new { obj.Schema, obj.Name }) != null;

        if (hasIdentity)
        {
            try
            {
                var reseedSql =
                    $"DBCC CHECKIDENT ('[{obj.Schema.Replace("]", "]]")}].[{obj.Name.Replace("]", "]]")}]', RESEED)";
                // DBCC CHECKIDENT 透過 SqlCommand 執行，需明確記錄 SQL。
                LogExecutingSqlSql(logger, reseedSql);
                await using var reseedCmd = new SqlCommand(reseedSql, targetConn);
                await reseedCmd.ExecuteNonQueryAsync();
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "Failed to reseed identity for {Schema}.{Name}", obj.Schema, obj.Name);
            }
        }
    }

    /// <summary>
    /// 將 <c>sys.*</c> 目錄檢視中以 <em>位元組</em> 儲存的長度換算為
    /// T-SQL DDL 所需的字元長度字串。
    /// <list type="bullet">
    ///   <item>Unicode 類型（<c>nvarchar</c>、<c>nchar</c>）：除以 2。</item>
    ///   <item>非 Unicode 類型（<c>varchar</c>、<c>char</c>、<c>varbinary</c>、<c>binary</c>）：直接使用。</item>
    ///   <item><c>-1</c> 代表 SQL Server 的 MAX。</item>
    ///   <item>不在 <see cref="TypesWithByteLength"/> 中的類型傳回空字串。</item>
    /// </list>
    /// </summary>
    private static string FormatSqlServerTypeLength(string typeName, int maxLengthBytes)
    {
        if (!TypesWithByteLength.Contains(typeName.ToLower())) return "";
        var charLength = typeName.StartsWith("n", StringComparison.OrdinalIgnoreCase)
            ? maxLengthBytes / 2
            : maxLengthBytes;
        return charLength == -1 ? "(MAX)" : $"({charLength})";
    }

    /// <summary>
    /// 從 <c>sys.sequences</c> 重組 <c>CREATE SEQUENCE</c> 陳述式。
    /// 內建的 <c>OBJECT_DEFINITION()</c> 不支援序列，因此需直接查詢目錄。
    /// </summary>
    private async Task<string> GetSequenceDefinitionAsync(SqlConnection conn, DbObject obj)
    {
        var seqInfo = await conn.QueryFirstOrDefaultWithLogAsync(logger, """
                                                                         SELECT
                                                                             t.name  AS DataType,
                                                                             s.start_value   AS StartValue,
                                                                             s.increment     AS Increment,
                                                                             s.minimum_value AS MinimumValue,
                                                                             s.maximum_value AS MaximumValue,
                                                                             s.is_cycling    AS IsCycling
                                                                         FROM sys.sequences s
                                                                         JOIN sys.types t ON s.user_type_id = t.user_type_id
                                                                         WHERE SCHEMA_NAME(s.schema_id) = @Schema AND s.name = @Name
                                                                         """, new { obj.Schema, obj.Name });

        if (seqInfo == null) return "";

        return $"CREATE SEQUENCE [{obj.Schema}].[{obj.Name}] " +
               $"AS [{seqInfo.DataType}] " +
               $"START WITH {seqInfo.StartValue} " +
               $"INCREMENT BY {seqInfo.Increment} " +
               $"MINVALUE {seqInfo.MinimumValue} " +
               $"MAXVALUE {seqInfo.MaximumValue} " +
               (seqInfo.IsCycling ? "CYCLE" : "NO CYCLE") + ";";
    }

    /// <summary>
    /// 從 <c>INFORMATION_SCHEMA.COLUMNS</c> 重組 <c>CREATE TABLE</c> 陳述式，
    /// 並從 <c>INFORMATION_SCHEMA.TABLE_CONSTRAINTS</c> 取得主鍵定義。
    /// <para>
    /// 注意：<c>INFORMATION_SCHEMA.COLUMNS.CHARACTER_MAXIMUM_LENGTH</c>
    /// 已是字元數（非位元組），不需再除以 2；
    /// 這與 <see cref="FormatSqlServerTypeLength"/> 處理 <c>sys.*</c> 位元組長度的方式不同。
    /// </para>
    /// </summary>
    private async Task<string> GetTableDefinitionAsync(SqlConnection conn, DbObject obj)
    {
        // INFORMATION_SCHEMA.COLUMNS.CHARACTER_MAXIMUM_LENGTH 已是字元數，直接使用。
        var columns = await conn.QueryWithLogAsync(logger, """
                                                           SELECT
                                                               c.COLUMN_NAME,
                                                               c.DATA_TYPE,
                                                               c.CHARACTER_MAXIMUM_LENGTH,
                                                               c.IS_NULLABLE,
                                                               COLUMNPROPERTY(OBJECT_ID(QUOTENAME(c.TABLE_SCHEMA) + '.' + QUOTENAME(c.TABLE_NAME)), c.COLUMN_NAME, 'IsIdentity') AS IsIdentity
                                                           FROM INFORMATION_SCHEMA.COLUMNS c
                                                           WHERE c.TABLE_SCHEMA = @Schema AND c.TABLE_NAME = @Name
                                                           ORDER BY c.ORDINAL_POSITION
                                                           """, new { obj.Schema, obj.Name });

        var columnDefs = columns.Select(c =>
        {
            string type = c.DATA_TYPE;
            var length = "";
            // CHARACTER_MAXIMUM_LENGTH 已是字元數；-1 代表 MAX。
            if (TypesWithByteLength.Contains(type.ToLower()) && c.CHARACTER_MAXIMUM_LENGTH != null)
                length = $"({(c.CHARACTER_MAXIMUM_LENGTH == -1 ? "MAX" : c.CHARACTER_MAXIMUM_LENGTH)})";

            return $"[{c.COLUMN_NAME}] [{type}]" +
                   length +
                   (c.IsIdentity == 1 ? " IDENTITY(1,1)" : "") +
                   (c.IS_NULLABLE == "YES" ? " NULL" : " NOT NULL");
        });

        // 取得主鍵條件約束名稱，以便在目標保留原始名稱。
        var pkInfo = await conn.QueryFirstOrDefaultWithLogAsync(logger, """
                                                                        SELECT tc.CONSTRAINT_NAME
                                                                        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                                                                        WHERE tc.TABLE_SCHEMA = @Schema AND tc.TABLE_NAME = @Name AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
                                                                        """, new { obj.Schema, obj.Name });

        // 依 ORDINAL_POSITION 取得主鍵欄位清單。
        var pkColumns = await conn.QueryWithLogAsync<string>(logger, """
                                                                     SELECT k.COLUMN_NAME
                                                                     FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE k
                                                                     JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                                                                       ON k.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
                                                                      AND k.TABLE_SCHEMA = tc.TABLE_SCHEMA
                                                                     WHERE k.TABLE_SCHEMA = @Schema
                                                                       AND k.TABLE_NAME = @Name
                                                                       AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
                                                                     ORDER BY k.ORDINAL_POSITION
                                                                     """, new { obj.Schema, obj.Name });

        var pkSql = "";
        // INFORMATION_SCHEMA 中的主鍵欄位不會重複，無需 .Distinct()。
        var pkColumnList = pkColumns.ToList();
        if (pkColumnList.Count != 0)
        {
            var pkName = pkInfo?.CONSTRAINT_NAME ?? $"PK_{obj.Name}";
            pkSql = $",\n  CONSTRAINT [{pkName.Replace("]", "]]")}] PRIMARY KEY " +
                    $"({string.Join(", ", pkColumnList.Select(c => $"[{c.Replace("]", "]]")}]"))})";
        }

        return $"CREATE TABLE [{obj.Schema}].[{obj.Name}] (\n  {string.Join(",\n  ", columnDefs)}{pkSql}\n)";
    }

    /// <summary>
    /// 為資料表的所有外部索引鍵產生 <c>ALTER TABLE … ADD CONSTRAINT … FOREIGN KEY</c> 陳述式。
    /// 多欄位外鍵以條件約束名稱分組，並依 <c>constraint_column_id</c> 排序以確保欄位順序正確。
    /// </summary>
    private async Task<List<string>> GetTableForeignKeyDefinitionsAsync(string connectionString, DbObject obj)
    {
        await using var conn = new SqlConnection(connectionString);
        var fks = await conn.QueryWithLogAsync(logger, """
                                                       SELECT
                                                           f.name AS ForeignKeyName,
                                                           SCHEMA_NAME(f.schema_id) AS SchemaName,
                                                           OBJECT_NAME(f.parent_object_id) AS TableName,
                                                           COL_NAME(fc.parent_object_id, fc.parent_column_id) AS ColumnName,
                                                           SCHEMA_NAME(referenced_object.schema_id) AS ReferencedSchemaName,
                                                           referenced_object.name AS ReferencedTableName,
                                                           COL_NAME(fc.referenced_object_id, fc.referenced_column_id) AS ReferencedColumnName,
                                                           fc.constraint_column_id
                                                       FROM sys.foreign_keys AS f
                                                       INNER JOIN sys.foreign_key_columns AS fc ON f.object_id = fc.constraint_object_id
                                                       INNER JOIN sys.objects AS referenced_object ON f.referenced_object_id = referenced_object.object_id
                                                       WHERE f.parent_object_id = OBJECT_ID(QUOTENAME(@Schema) + '.' + QUOTENAME(@Name))
                                                       ORDER BY f.name, fc.constraint_column_id
                                                       """, new { obj.Schema, obj.Name });

        var grouped = fks.GroupBy(f => (string)f.ForeignKeyName);
        var sqls = new List<string>();

        foreach (var g in grouped)
        {
            var first = g.First();
            var cols = string.Join(", ", g.Select(r => $"[{((string)r.ColumnName).Replace("]", "]]")}]"));
            var refCols = string.Join(", ", g.Select(r => $"[{((string)r.ReferencedColumnName).Replace("]", "]]")}]"));

            sqls.Add(
                $"ALTER TABLE [{((string)first.SchemaName).Replace("]", "]]")}].[{((string)first.TableName).Replace("]", "]]")}] " +
                $"ADD CONSTRAINT [{g.Key.Replace("]", "]]")}] FOREIGN KEY ({cols}) " +
                $"REFERENCES [{((string)first.ReferencedSchemaName).Replace("]", "]]")}].[{((string)first.ReferencedTableName).Replace("]", "]]")}] ({refCols})");
        }

        return sqls;
    }

    /// <summary>
    /// 為資料表的所有非主鍵索引產生 <c>CREATE [UNIQUE] INDEX</c> 陳述式。
    /// 索引鍵欄與 INCLUDE 欄由 <c>is_included_column</c> 區分。
    /// 無索引鍵欄的索引（邊界情形）會被略過。
    /// </summary>
    private async Task<List<string>> GetTableIndexDefinitionsAsync(string connectionString, DbObject obj)
    {
        await using var conn = new SqlConnection(connectionString);
        var rows = await conn.QueryWithLogAsync(logger, """
                                                        SELECT
                                                            i.name AS IndexName,
                                                            i.is_unique AS IsUnique,
                                                            c.name AS ColumnName,
                                                            ic.is_included_column AS IsIncluded,
                                                            ic.key_ordinal AS KeyOrdinal,
                                                            ic.index_column_id AS ColumnId
                                                        FROM sys.indexes i
                                                        JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
                                                        JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                                                        WHERE i.object_id = OBJECT_ID(QUOTENAME(@Schema) + '.' + QUOTENAME(@Name))
                                                          AND i.type > 0
                                                          AND i.is_primary_key = 0
                                                        ORDER BY i.name, ic.is_included_column, ic.key_ordinal, ic.index_column_id
                                                        """, new { obj.Schema, obj.Name });

        var grouped = rows.GroupBy(r => new { Name = (string)r.IndexName, IsUnique = (bool)r.IsUnique });
        var sqls = new List<string>();

        foreach (var g in grouped)
        {
            var keyCols = g.Where(r => !(bool)r.IsIncluded)
                .Select(r => $"[{((string)r.ColumnName).Replace("]", "]]")}]")
                .Distinct()
                .ToList();

            var incCols = g.Where(r => (bool)r.IsIncluded)
                .Select(r => $"[{((string)r.ColumnName).Replace("]", "]]")}]")
                .Distinct()
                .ToList();

            if (!keyCols.Any()) continue;

            var includeSql = incCols.Any() ? $" INCLUDE ({string.Join(", ", incCols)})" : "";
            sqls.Add($"CREATE {(g.Key.IsUnique ? "UNIQUE " : "")}INDEX [{g.Key.Name.Replace("]", "]]")}] " +
                     $"ON [{obj.Schema.Replace("]", "]]")}].[{obj.Name.Replace("]", "]]")}] " +
                     $"({string.Join(", ", keyCols)}){includeSql}");
        }

        return sqls;
    }

    /// <summary>
    /// 從 <c>sys.columns</c> / <c>sys.table_types</c> 重組
    /// <c>CREATE TYPE … AS TABLE</c> 陳述式。
    /// 欄位長度來自 <c>sys.columns.max_length</c>（位元組），
    /// 需透過 <see cref="FormatSqlServerTypeLength"/> 換算為字元長度。
    /// </summary>
    private async Task<string> GetUserDefinedTableTypeDefinitionAsync(SqlConnection conn, DbObject obj)
    {
        var columns = await conn.QueryWithLogAsync(logger, """
                                                           SELECT
                                                               c.name      AS COLUMN_NAME,
                                                               t.name      AS DATA_TYPE,
                                                               c.max_length AS CHARACTER_MAXIMUM_LENGTH,
                                                               c.is_nullable AS IS_NULLABLE,
                                                               c.is_identity AS IsIdentity
                                                           FROM sys.columns c
                                                           JOIN sys.table_types tt ON c.object_id = tt.type_table_object_id
                                                           JOIN sys.types t ON c.user_type_id = t.user_type_id
                                                           WHERE tt.schema_id = SCHEMA_ID(@Schema) AND tt.name = @Name
                                                           ORDER BY c.column_id
                                                           """, new { obj.Schema, obj.Name });

        var columnDefs = columns.Select(c =>
        {
            string type = c.DATA_TYPE;
            // sys.columns.max_length 為位元組，需換算為字元長度。
            var length = FormatSqlServerTypeLength(type, (int)c.CHARACTER_MAXIMUM_LENGTH);
            return $"[{c.COLUMN_NAME}] [{type}]" +
                   length +
                   (c.IsIdentity == 1 ? " IDENTITY(1,1)" : "") +
                   (c.IS_NULLABLE ? " NULL" : " NOT NULL");
        });

        return $"CREATE TYPE [{obj.Schema}].[{obj.Name}] AS TABLE (\n  {string.Join(",\n  ", columnDefs)}\n)";
    }

    /// <summary>
    /// 從 <c>sys.types</c> 重組別名型別（Alias Type）的
    /// <c>CREATE TYPE … FROM</c> 陳述式。
    /// 自連結 <c>sys.types</c> 以取得底層系統基礎型別及其精確度/小數位數/長度。
    /// 長度來自 <c>sys.types.max_length</c>（位元組），需透過 <see cref="FormatSqlServerTypeLength"/> 換算。
    /// </summary>
    private async Task<string> GetUserDefinedTypeDefinitionAsync(SqlConnection conn, DbObject obj)
    {
        var typeInfo = await conn.QueryFirstOrDefaultWithLogAsync(logger, """
                                                                          SELECT
                                                                              SCHEMA_NAME(t.schema_id) AS [Schema],
                                                                              t.name      AS Name,
                                                                              st.name     AS BaseType,
                                                                              t.max_length,
                                                                              t.precision,
                                                                              t.scale,
                                                                              t.is_nullable
                                                                          FROM sys.types t
                                                                          JOIN sys.types st ON t.system_type_id = st.system_type_id AND st.is_user_defined = 0
                                                                          WHERE t.schema_id = SCHEMA_ID(@Schema) AND t.name = @Name
                                                                          """, new { obj.Schema, obj.Name });

        if (typeInfo == null) return "";

        // sys.types.max_length 為位元組，需換算為字元長度。
        var length = FormatSqlServerTypeLength((string)typeInfo.BaseType, (int)typeInfo.max_length);
        // decimal / numeric 使用精確度與小數位數，而非位元組長度。
        if (length == "" && (typeInfo.BaseType == "decimal" || typeInfo.BaseType == "numeric"))
            length = $"({typeInfo.precision}, {typeInfo.scale})";

        return $"CREATE TYPE [{obj.Schema}].[{obj.Name}] FROM [{typeInfo.BaseType}]{length} " +
               $"{(typeInfo.is_nullable ? "NULL" : "NOT NULL")}";
    }

    /// <summary>
    /// 由 <c>[LoggerMessage]</c> 原始碼產生器產生的高效能結構化日誌方法，
    /// 專用於直接使用 <see cref="SqlCommand"/> 而繞過 Dapper 的程式碼路徑
    /// （大量複製的 SELECT 及 DBCC CHECKIDENT）。
    /// 與 <see cref="DapperExtensions"/> 中的版本使用相同訊息範本，
    /// 確保日誌格式一致。
    /// </summary>
    [LoggerMessage(LogLevel.Information, "Executing SQL: {Sql}")]
    static partial void LogExecutingSqlSql(ILogger<SqlServerService> logger, string Sql);
}