using Dapper;
using DbCopy.Models;
using Npgsql;

namespace DbCopy.Services;

/// <summary>
/// <see cref="IDbService"/> 的 PostgreSQL 實作。
/// 所有 Dapper 呼叫均透過 <see cref="DapperExtensions"/> 的 WithLog wrapper，
/// 確保每一條 SQL 恰好被記錄一次。
/// </summary>
public class PostgreSqlService(ILogger<PostgreSqlService> logger) : IDbService
{
    /// <summary>
    /// 將字串中的單引號 <c>'</c> 雙重跳脫為 <c>''</c>，
    /// 使其可安全嵌入 PL/pgSQL 字串常值。
    /// 識別符（schema/table 名稱）請改用 <c>format('%I', …)</c>。
    /// </summary>
    private static string EscapePgLiteral(string s) => s.Replace("'", "''");

    /// <summary>
    /// 開啟連線以驗證連線字串有效且伺服器可連線。
    /// </summary>
    public async Task<bool> TestConnectionAsync(string connectionString)
    {
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();
        return true;
    }

    /// <summary>
    /// 傳回資料庫中所有使用者定義物件：資料表（含分區表）、序列、
    /// 檢視（含物化檢視）、程序、函數、使用者定義型別（Composite/Enum/Domain）。
    /// 使用 <c>pg_catalog</c> 並排除屬於任何 Extension 的成員物件（<c>pg_depend.deptype = 'e'</c>）。
    /// </summary>
    public async Task<List<DbObject>> GetDbObjectsAsync(string connectionString)
    {
        await using var conn = new NpgsqlConnection(connectionString);
        // 使用 pg_catalog 並排除屬於任何 EXTENSION 的成員物件（pg_depend.deptype = 'e'）
        const string sql = """
                           -- Tables (relkind: r=heap, p=partitioned)
                           SELECT n.nspname AS Schema, c.relname AS Name, 'Table' AS Type
                           FROM pg_class c
                           JOIN pg_namespace n ON n.oid = c.relnamespace
                           WHERE c.relkind IN ('r','p')
                             AND n.nspname NOT IN ('information_schema', 'pg_catalog')
                             AND NOT EXISTS (
                                 SELECT 1
                                 FROM pg_depend d
                                 JOIN pg_extension e ON e.oid = d.refobjid
                                 WHERE d.objid = c.oid AND d.deptype = 'e'
                             )

                           UNION ALL

                           -- Sequences
                           SELECT n.nspname AS Schema, c.relname AS Name, 'Sequence' AS Type
                           FROM pg_class c
                           JOIN pg_namespace n ON n.oid = c.relnamespace
                           WHERE c.relkind = 'S'
                             AND n.nspname NOT IN ('information_schema', 'pg_catalog')
                             AND NOT EXISTS (
                                 SELECT 1
                                 FROM pg_depend d
                                 JOIN pg_extension e ON e.oid = d.refobjid
                                 WHERE d.objid = c.oid AND d.deptype = 'e'
                             )

                           UNION ALL

                           -- Views (including materialized views)
                           SELECT n.nspname AS Schema, c.relname AS Name, 'View' AS Type
                           FROM pg_class c
                           JOIN pg_namespace n ON n.oid = c.relnamespace
                           WHERE c.relkind IN ('v','m')
                             AND n.nspname NOT IN ('information_schema', 'pg_catalog')
                             AND NOT EXISTS (
                                 SELECT 1
                                 FROM pg_depend d
                                 JOIN pg_extension e ON e.oid = d.refobjid
                                 WHERE d.objid = c.oid AND d.deptype = 'e'
                             )

                           UNION ALL

                           -- Routines (functions/procedures)
                           SELECT pn.nspname AS Schema, p.proname AS Name,
                                  CASE WHEN p.prokind = 'p' THEN 'Procedure' ELSE 'Function' END AS Type
                           FROM pg_proc p
                           JOIN pg_namespace pn ON pn.oid = p.pronamespace
                           WHERE pn.nspname NOT IN ('information_schema', 'pg_catalog')
                             AND p.prokind IN ('f','p')
                             AND NOT EXISTS (
                                 SELECT 1
                                 FROM pg_depend d
                                 JOIN pg_extension e ON e.oid = d.refobjid
                                 WHERE d.objid = p.oid AND d.deptype = 'e'
                             )

                           UNION ALL

                           -- User defined types (exclude base types; only composite/enum/domain) and not extension-owned
                           SELECT n.nspname AS Schema, t.typname AS Name, 'UserDefinedType' AS Type
                           FROM pg_type t
                           JOIN pg_namespace n ON n.oid = t.typnamespace
                           LEFT JOIN pg_class c ON c.oid = t.typrelid
                           WHERE ((t.typtype = 'c' AND c.relkind = 'c')
                                  OR (t.typtype = 'e')
                                  OR (t.typtype = 'd'))
                             AND n.nspname NOT IN ('information_schema', 'pg_catalog')
                             AND NOT EXISTS (
                                 SELECT 1
                                 FROM pg_depend d
                                 JOIN pg_extension e ON e.oid = d.refobjid
                                 WHERE d.objid = t.oid AND d.deptype = 'e'
                             )
                           """;

        var results = await conn.QueryWithLogAsync(logger, sql);
        return results.Select(r => new DbObject
        {
            Schema = r.schema,
            Name = r.name,
            Type = Enum.Parse<DbObjectType>(r.type)
        }).ToList();
    }

    /// <summary>
    /// 傳回物件的 DDL 定義，供目標資料庫重建之用。
    /// <list type="bullet">
    ///   <item>資料表：從 <c>pg_attribute</c> 重組 CREATE TABLE。</item>
    ///   <item>使用者定義型別：依子類型（Enum/Composite/Domain/Base）分別處理。</item>
    ///   <item>序列：從 <c>information_schema.sequences</c> 重組。</item>
    ///   <item>檢視：從 <c>information_schema.views</c> 取得原始定義。</item>
    ///   <item>程序/函數：使用 <c>pg_get_functiondef()</c>。</item>
    /// </list>
    /// </summary>
    public async Task<string> GetObjectDefinitionAsync(string connectionString, DbObject obj)
    {
        await using var conn = new NpgsqlConnection(connectionString);
        if (obj.Type == DbObjectType.Table)
            return await GetTableDefinitionAsync(conn, obj);

        if (obj.Type == DbObjectType.UserDefinedType)
            return await GetUserDefinedTypeDefinitionAsync(conn, obj);

        if (obj.Type == DbObjectType.Sequence)
            return await GetSequenceDefinitionAsync(conn, obj);

        if (obj.Type == DbObjectType.View)
        {
            // information_schema.views 儲存完整的 VIEW 定義文字。
            var definition = await conn.ExecuteScalarWithLogAsync<string>(logger,
                "SELECT view_definition FROM information_schema.views WHERE table_schema = @Schema AND table_name = @Name",
                new { obj.Schema, obj.Name });
            return $"CREATE VIEW \"{obj.Schema}\".\"{obj.Name}\" AS\n{definition}";
        }

        if (obj.Type is DbObjectType.Procedure or DbObjectType.Function)
        {
            // prokind: 'p' = 程序, 'f' = 函數；
            // 同名多載函數（overloads）各自取得定義並合併。
            var prokind = obj.Type == DbObjectType.Procedure ? "p" : "f";
            var defs = await conn.QueryWithLogAsync<string>(logger,
                """
                SELECT pg_get_functiondef(p.oid)
                FROM pg_proc p
                JOIN pg_namespace n ON p.pronamespace = n.oid
                WHERE n.nspname = @Schema AND p.proname = @Name AND p.prokind = @Prokind
                ORDER BY p.oid
                """,
                new { obj.Schema, obj.Name, Prokind = prokind });

            var list = defs.Where(d => !string.IsNullOrWhiteSpace(d)).ToList();
            return list.Count == 0 ? "" : string.Join("\n", list);
        }

        throw new ArgumentOutOfRangeException();
    }

    /// <summary>
    /// 依使用者定義型別的子類型產生對應的 DDL：
    /// <list type="bullet">
    ///   <item>Enum（<c>typtype = 'e'</c>）：<c>CREATE TYPE … AS ENUM (…)</c></item>
    ///   <item>Composite（<c>typtype = 'c'</c>）：<c>CREATE TYPE … AS (…)</c></item>
    ///   <item>Domain（<c>typtype = 'd'</c>）：<c>CREATE DOMAIN … AS …</c></item>
    ///   <item>Base（<c>typtype = 'b'</c>）：僅產生 Shell 定義（完整基礎型別需 C 函數）</item>
    /// </list>
    /// </summary>
    private async Task<string> GetUserDefinedTypeDefinitionAsync(NpgsqlConnection conn, DbObject obj)
    {
        // --- Enum ---
        // 依 enumsortorder 排序以確保 ENUM 值順序正確。
        var enumValues = await conn.QueryWithLogAsync<string>(logger, """
                                                                      SELECT e.enumlabel
                                                                      FROM pg_type t
                                                                      JOIN pg_enum e ON t.oid = e.enumtypid
                                                                      JOIN pg_namespace n ON n.oid = t.typnamespace
                                                                      WHERE t.typname = @Name AND n.nspname = @Schema
                                                                      ORDER BY e.enumsortorder
                                                                      """, new { obj.Name, obj.Schema });

        var enumList = enumValues.ToList();
        if (enumList.Count != 0)
            return $"CREATE TYPE \"{obj.Schema}\".\"{obj.Name}\" AS ENUM ('{string.Join("', '", enumList)}')";

        // --- Composite ---
        var compositeCols = await conn.QueryWithLogAsync(logger, """
                                                                 SELECT a.attname as Name, format_type(a.atttypid, a.atttypmod) as DataType
                                                                 FROM pg_type t
                                                                 JOIN pg_class c ON t.typrelid = c.oid
                                                                 JOIN pg_attribute a ON a.attrelid = c.oid
                                                                 JOIN pg_namespace n ON n.oid = t.typnamespace
                                                                 WHERE t.typname = @Name AND n.nspname = @Schema AND a.attnum > 0 AND NOT a.attisdropped
                                                                 ORDER BY a.attnum
                                                                 """, new { obj.Name, obj.Schema });

        var compositeList = compositeCols.ToList();
        if (compositeList.Count != 0)
        {
            var colDefs = compositeList.Select(c => $"\"{c.Name}\" {c.DataType}");
            return $"CREATE TYPE \"{obj.Schema}\".\"{obj.Name}\" AS (\n  {string.Join(",\n  ", colDefs)}\n)";
        }

        // --- Domain ---
        var domainInfo = await conn.QueryFirstOrDefaultWithLogAsync(logger, """
                                                                            SELECT format_type(typbasetype, typtypmod) as BaseType, typnotnull, typdefault
                                                                            FROM pg_type t
                                                                            JOIN pg_namespace n ON n.oid = t.typnamespace
                                                                            WHERE t.typname = @Name AND n.nspname = @Schema AND t.typtype = 'd'
                                                                            """, new { obj.Name, obj.Schema });

        if (domainInfo != null)
        {
            var def = $"CREATE DOMAIN \"{obj.Schema}\".\"{obj.Name}\" AS {domainInfo.BaseType}";
            if (domainInfo.typnotnull) def += " NOT NULL";
            if (domainInfo.typdefault != null) def += $" DEFAULT {domainInfo.typdefault}";
            return def;
        }

        // --- Base type（Shell 定義）---
        // 完整基礎型別需要 C 語言函數，此處僅產生 Shell 宣告。
        var baseInfo = await conn.QueryFirstOrDefaultWithLogAsync(logger, """
                                                                          SELECT typname
                                                                          FROM pg_type t
                                                                          JOIN pg_namespace n ON n.oid = t.typnamespace
                                                                          WHERE t.typname = @Name AND n.nspname = @Schema AND t.typtype = 'b'
                                                                          """, new { obj.Name, obj.Schema });

        if (baseInfo != null)
            return $"CREATE TYPE \"{obj.Schema}\".\"{obj.Name}\"";

        return "";
    }

    /// <summary>
    /// 從 <c>information_schema.sequences</c> 重組 <c>CREATE SEQUENCE</c> 陳述式。
    /// </summary>
    private async Task<string> GetSequenceDefinitionAsync(NpgsqlConnection conn, DbObject obj)
    {
        var seqInfo = await conn.QueryFirstOrDefaultWithLogAsync(logger, """
                                                                         SELECT data_type, start_value, minimum_value, maximum_value, increment, cycle_option
                                                                         FROM information_schema.sequences
                                                                         WHERE sequence_schema = @Schema AND sequence_name = @Name
                                                                         """, new { obj.Schema, obj.Name });

        if (seqInfo == null) return "";

        return $"CREATE SEQUENCE \"{obj.Schema}\".\"{obj.Name}\" " +
               $"AS {seqInfo.data_type} " +
               $"START WITH {seqInfo.start_value} " +
               $"INCREMENT BY {seqInfo.increment} " +
               $"MINVALUE {seqInfo.minimum_value} " +
               $"MAXVALUE {seqInfo.maximum_value} " +
               (seqInfo.cycle_option == "YES" ? "CYCLE" : "NO CYCLE") + ";";
    }

    /// <summary>
    /// 從 <c>pg_attribute</c> / <c>pg_attrdef</c> 重組 <c>CREATE TABLE</c> 陳述式，
    /// 並附上主鍵條件約束（取自 <c>information_schema</c>）。
    /// <c>format_type(atttypid, atttypmod)</c> 會自動產生完整型別字串（含長度/精確度）。
    /// </summary>
    private async Task<string> GetTableDefinitionAsync(NpgsqlConnection conn, DbObject obj)
    {
        var columns = await conn.QueryWithLogAsync(logger, """
                                                           SELECT
                                                               a.attname AS column_name,
                                                               format_type(a.atttypid, a.atttypmod) AS data_type,
                                                               a.attnotnull AS is_not_null,
                                                               pg_get_expr(d.adbin, d.adrelid) AS column_default
                                                           FROM pg_attribute a
                                                           LEFT JOIN pg_attrdef d ON a.attrelid = d.adrelid AND a.attnum = d.adnum
                                                           JOIN pg_class c ON a.attrelid = c.oid
                                                           JOIN pg_namespace n ON c.relnamespace = n.oid
                                                           WHERE c.relname = @Name AND n.nspname = @Schema
                                                             AND a.attnum > 0 AND NOT a.attisdropped
                                                           ORDER BY a.attnum
                                                           """, new { obj.Schema, obj.Name });

        var columnDefs = columns.Select(c =>
            $"\"{c.column_name}\" {c.data_type}" +
            (c.is_not_null ? " NOT NULL" : " NULL") +
            (c.column_default != null ? $" DEFAULT {c.column_default}" : ""));

        // 取得主鍵條件約束名稱，以便在目標保留原始名稱。
        var pkInfo = await conn.QueryFirstOrDefaultWithLogAsync(logger, """
                                                                        SELECT tc.constraint_name
                                                                        FROM information_schema.table_constraints tc
                                                                        WHERE tc.constraint_type = 'PRIMARY KEY' AND tc.table_schema = @Schema AND tc.table_name = @Name
                                                                        """, new { obj.Schema, obj.Name });

        // 依 ordinal_position 取得主鍵欄位清單。
        var pkColumns = await conn.QueryWithLogAsync<string>(logger, """
                                                                     SELECT kcu.column_name
                                                                     FROM information_schema.table_constraints tc
                                                                     JOIN information_schema.key_column_usage kcu
                                                                       ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
                                                                     WHERE tc.constraint_type = 'PRIMARY KEY' AND tc.table_schema = @Schema AND tc.table_name = @Name
                                                                     ORDER BY kcu.ordinal_position
                                                                     """, new { obj.Schema, obj.Name });

        var pkSql = "";
        var enumerable = pkColumns.ToList();
        if (enumerable.Count != 0)
        {
            var pkName = pkInfo?.constraint_name ?? $"{obj.Name}_pkey";
            pkSql =
                $",\n  CONSTRAINT \"{pkName}\" PRIMARY KEY ({string.Join(", ", enumerable.Select(c => $"\"{c}\""))})";
        }

        return $"CREATE TABLE \"{obj.Schema}\".\"{obj.Name}\" (\n  {string.Join(",\n  ", columnDefs)}{pkSql}\n)";
    }

    /// <summary>
    /// 當物件已存在於目標資料庫時傳回 <c>true</c>。
    /// 每種物件類型查詢對應的 <c>information_schema</c> 檢視或 <c>pg_type</c>。
    /// </summary>
    public async Task<bool> CheckObjectExistsAsync(string connectionString, DbObject obj)
    {
        await using var conn = new NpgsqlConnection(connectionString);
        var sql = obj.Type switch
        {
            DbObjectType.Table =>
                "SELECT 1 FROM information_schema.tables WHERE table_schema = @Schema AND table_name = @Name",
            DbObjectType.Sequence =>
                "SELECT 1 FROM information_schema.sequences WHERE sequence_schema = @Schema AND sequence_name = @Name",
            DbObjectType.View =>
                "SELECT 1 FROM information_schema.views WHERE table_schema = @Schema AND table_name = @Name",
            DbObjectType.Procedure or DbObjectType.Function =>
                "SELECT 1 FROM information_schema.routines WHERE routine_schema = @Schema AND routine_name = @Name",
            DbObjectType.UserDefinedType =>
                "SELECT 1 FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace WHERE n.nspname = @Schema AND t.typname = @Name",
            _ => throw new ArgumentOutOfRangeException()
        };
        return await conn.ExecuteScalarWithLogAsync<int?>(logger, sql, new { obj.Schema, obj.Name }) != null;
    }

    /// <summary>
    /// 若 Schema 不存在則在目標伺服器建立之。
    /// <c>public</c> Schema 在 PostgreSQL 中永遠存在，故略過。
    /// Schema 名稱以雙引號識別符傳遞；<c>"</c> 字元雙重跳脫以防止 injection。
    /// </summary>
    public async Task EnsureSchemaExistsAsync(string connectionString, string schema)
    {
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();

        if (!string.IsNullOrEmpty(schema) && !schema.Equals("public", StringComparison.OrdinalIgnoreCase))
        {
            var exists =
                await conn.ExecuteScalarWithLogAsync<int?>(logger,
                    "SELECT 1 FROM information_schema.schemata WHERE schema_name = @schema",
                    new { schema }) != null;
            if (!exists)
            {
                // 在雙引號識別符內部，" 須雙重為 "" 以避免注入。
                await conn.ExecuteWithLogAsync(logger, $"CREATE SCHEMA \"{schema.Replace("\"", "\"\"")}\"");
            }
        }
    }

    /// <summary>
    /// 自動偵測來源物件所需的 PostgreSQL Extension，並在目標資料庫安裝（若尚未安裝）。
    /// 目前偵測邏輯：
    /// <list type="bullet">
    ///   <item>欄位型別含 geometry/geography/raster → postgis</item>
    ///   <item>欄位型別為 citext → citext</item>
    ///   <item>欄位型別為 hstore → hstore</item>
    ///   <item>欄位預設值含 uuid_generate_* → uuid-ossp</item>
    ///   <item>索引使用 gin_trgm_ops/gist_trgm_ops → pg_trgm</item>
    /// </list>
    /// 安裝失敗時記錄警告，不中斷複製流程。
    /// </summary>
    private async Task EnsureExtensionsIfUsedAsync(NpgsqlConnection targetConn, string sourceConnectionString,
        DbObject obj)
    {
        await using var sourceConn = new NpgsqlConnection(sourceConnectionString);
        await sourceConn.OpenAsync();

        var extsToEnsure = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        // 1) 基於欄位型別偵測（table/udt）
        if (obj.Type == DbObjectType.Table)
        {
            var typeUsage = await sourceConn.QueryWithLogAsync<string>(logger,
                """
                SELECT DISTINCT t.typname
                FROM pg_attribute a
                JOIN pg_class c ON a.attrelid = c.oid
                JOIN pg_namespace n ON c.relnamespace = n.oid
                JOIN pg_type t ON a.atttypid = t.oid
                WHERE c.relname = @Name AND n.nspname = @Schema
                  AND a.attnum > 0 AND NOT a.attisdropped
                """,
                new { obj.Name, obj.Schema });

            foreach (var tn in typeUsage)
            {
                if (tn.Equals("geometry", StringComparison.OrdinalIgnoreCase) ||
                    tn.Equals("geography", StringComparison.OrdinalIgnoreCase) ||
                    tn.Equals("raster", StringComparison.OrdinalIgnoreCase))
                    extsToEnsure.Add("postgis");

                if (tn.Equals("citext", StringComparison.OrdinalIgnoreCase))
                    extsToEnsure.Add("citext");

                if (tn.Equals("hstore", StringComparison.OrdinalIgnoreCase))
                    extsToEnsure.Add("hstore");
            }

            // 2) 預設值若使用 uuid_generate_* 則需要 uuid-ossp
            var usesUuid = await sourceConn.ExecuteScalarWithLogAsync<bool?>(logger,
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM pg_attribute a
                    JOIN pg_class c ON a.attrelid = c.oid
                    JOIN pg_namespace n ON c.relnamespace = n.oid
                    JOIN pg_attrdef d ON a.attrelid = d.adrelid AND a.attnum = d.adnum
                    WHERE c.relname = @Name AND n.nspname = @Schema
                      AND a.attnum > 0 AND NOT a.attisdropped
                      AND pg_get_expr(d.adbin, d.adrelid) ILIKE '%uuid_generate_%'
                )
                """,
                new { obj.Name, obj.Schema }) ?? false;
            if (usesUuid) extsToEnsure.Add("uuid-ossp");

            // 3) 索引使用 trigram opclass 則需要 pg_trgm
            var usesTrgm = await sourceConn.ExecuteScalarWithLogAsync<bool?>(logger,
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM pg_index ix
                    JOIN pg_class t ON t.oid = ix.indrelid
                    JOIN pg_namespace ns ON ns.oid = t.relnamespace
                    JOIN pg_class i ON i.oid = ix.indexrelid
                    JOIN pg_opclass opc ON opc.oid = ANY(ix.indclass)
                    WHERE t.relname = @Name AND ns.nspname = @Schema
                      AND opc.opcname IN ('gin_trgm_ops','gist_trgm_ops')
                )
                """,
                new { obj.Name, obj.Schema }) ?? false;
            if (usesTrgm) extsToEnsure.Add("pg_trgm");
        }

        if (extsToEnsure.Count == 0) return;

        foreach (var ext in extsToEnsure)
        {
            try
            {
                // EXISTS(...) 在 PostgreSQL 永遠傳回非 null 的 boolean，?? false 僅為型別安全。
                var exists = await targetConn.ExecuteScalarWithLogAsync<bool?>(logger,
                    "SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = @ext)", new { ext }) ?? false;
                if (!exists)
                {
                    await targetConn.ExecuteWithLogAsync(logger, $"CREATE EXTENSION IF NOT EXISTS \"{ext}\"");
                    logger.LogInformation("Created extension {Extension} on target database.", ext);
                }
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "Failed to create extension {Extension}. Dependent objects may fail.", ext);
            }
        }
    }

    /// <summary>
    /// 傳回 <paramref name="obj"/> 所依賴的物件名稱清單，供呼叫端決定複製順序。
    /// 以四段 UNION 涵蓋所有依賴類型：
    /// <list type="number">
    ///   <item>pg_depend 中的物件依賴（資料表、檢視、分區表、序列等）</item>
    ///   <item>欄位型別依賴（Enum、Domain、Composite）</item>
    ///   <item>函數/程序的參數與回傳值型別依賴</item>
    ///   <item>欄位預設值的序列依賴（serial/IDENTITY）</item>
    /// </list>
    /// </summary>
    public async Task<List<string>> GetDependenciesAsync(string connectionString, DbObject obj)
    {
        await using var conn = new NpgsqlConnection(connectionString);
        const string sql = """
                           SELECT DISTINCT
                               referenced_ns.nspname || '.' || referenced_objs.relname as ReferencedName
                           FROM pg_depend d
                           JOIN pg_class referencing_objs ON referencing_objs.oid = d.objid
                           JOIN pg_namespace referencing_ns ON referencing_ns.oid = referencing_objs.relnamespace
                           JOIN pg_class referenced_objs ON referenced_objs.oid = d.refobjid
                           JOIN pg_namespace referenced_ns ON referenced_ns.oid = referenced_objs.relnamespace
                           WHERE referencing_objs.relname = @Name
                             AND referencing_ns.nspname = @Schema
                             AND referenced_objs.oid <> referencing_objs.oid
                             AND referenced_objs.relkind IN ('r', 'v', 'm', 'p', 'S', 'c')
                             AND referenced_ns.nspname NOT IN ('pg_catalog', 'information_schema')

                           UNION

                           -- Type dependencies for columns (including Enums, Domains)
                           SELECT DISTINCT n.nspname || '.' || t.typname
                           FROM pg_attribute a
                           JOIN pg_class c ON a.attrelid = c.oid
                           JOIN pg_namespace cn ON c.relnamespace = cn.oid
                           JOIN pg_type t ON a.atttypid = t.oid
                           JOIN pg_namespace n ON t.typnamespace = n.oid
                           WHERE c.relname = @Name AND cn.nspname = @Schema
                             AND a.attnum > 0 AND NOT a.attisdropped
                             AND n.nspname NOT IN ('pg_catalog', 'information_schema')
                             AND t.typtype IN ('c', 'e', 'd')

                           UNION

                           -- Type dependencies for Function/Procedure parameters/return
                           SELECT DISTINCT n.nspname || '.' || t.typname
                           FROM pg_proc p
                           JOIN pg_namespace pn ON p.pronamespace = pn.oid
                           JOIN pg_type t ON t.oid = ANY(array_append(p.proallargtypes, p.prorettype))
                           JOIN pg_namespace n ON t.typnamespace = n.oid
                           WHERE p.proname = @Name AND pn.nspname = @Schema
                             AND n.nspname NOT IN ('pg_catalog', 'information_schema')
                             AND t.typtype IN ('c', 'e', 'd')

                           UNION

                           -- Sequence dependencies for column defaults
                           SELECT DISTINCT n.nspname || '.' || s.relname
                           FROM pg_attribute a
                           JOIN pg_class c ON a.attrelid = c.oid
                           JOIN pg_namespace cn ON c.relnamespace = cn.oid
                           JOIN pg_attrdef d ON a.attrelid = d.adrelid AND a.attnum = d.adnum
                           JOIN pg_depend dep ON d.oid = dep.objid
                           JOIN pg_class s ON dep.refobjid = s.oid
                           JOIN pg_namespace n ON s.relnamespace = n.oid
                           WHERE c.relname = @Name AND cn.nspname = @Schema
                             AND s.relkind = 'S'
                             AND n.nspname NOT IN ('pg_catalog', 'information_schema')
                           """;

        var results = await conn.QueryWithLogAsync<string>(logger, sql, new { obj.Schema, obj.Name });
        return results.ToList();
    }

    /// <summary>
    /// 使用 <c>COUNT(*)::bigint</c> 傳回資料表的精確列數。
    /// 強制轉型為 bigint 可確保 Npgsql 在各版本中都能正確對應至 <see cref="long"/>。
    /// 非資料表物件傳回 0。
    /// </summary>
    public async Task<long> GetRowCountAsync(string connectionString, DbObject obj)
    {
        if (obj.Type != DbObjectType.Table) return 0;
        await using var conn = new NpgsqlConnection(connectionString);
        return await conn.ExecuteScalarWithLogAsync<long>(logger,
            $"SELECT COUNT(*)::bigint FROM \"{obj.Schema}\".\"{obj.Name}\"");
    }

    /// <summary>
    /// 傳回資料表的索引中繼資料：名稱、唯一性、欄位清單（依索引鍵順序排列）。
    /// 使用 <c>string_agg … ORDER BY array_position</c> 確保欄位順序與索引定義一致。
    /// </summary>
    public async Task<List<DbIndex>> GetTableIndexesAsync(string connectionString, DbObject obj)
    {
        await using var conn = new NpgsqlConnection(connectionString);
        const string sql = """
                           SELECT
                               i.relname as Name,
                               ix.indisunique as IsUnique,
                               string_agg(a.attname, ', ' order by array_position(ix.indkey, a.attnum)) as Columns
                           FROM pg_index ix
                           JOIN pg_class t ON t.oid = ix.indrelid
                           JOIN pg_class i ON i.oid = ix.indexrelid
                           JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
                           JOIN pg_namespace n ON n.oid = t.relnamespace
                           WHERE t.relname = @Name AND n.nspname = @Schema
                           GROUP BY i.relname, ix.indisunique
                           """;

        var results = await conn.QueryWithLogAsync(logger, sql, new { obj.Name, obj.Schema });
        return results.Select(r =>
        {
            var dict = (IDictionary<string, object>)r;

            // 使用 case-insensitive 鍵查找，相容 Npgsql 動態物件欄位名稱大小寫差異。
            object GetValue(string key) =>
                dict.FirstOrDefault(x => x.Key.Equals(key, StringComparison.OrdinalIgnoreCase)).Value;

            return new DbIndex
            {
                Name = GetValue("name").ToString() ?? "",
                IsUnique = GetValue("isunique") as bool? ?? false,
                Columns = GetValue("columns").ToString() ?? ""
            };
        }).ToList();
    }

    /// <summary>
    /// 將物件從來源複製到目標資料庫，分為最多四個階段：
    /// <list type="number">
    ///   <item>Phase 1 — 結構（CREATE TABLE / CREATE PROCEDURE / …），資料表額外安裝所需 Extension</item>
    ///   <item>Phase 2 — 資料（COPY；僅資料表）</item>
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

        await using var conn = new NpgsqlConnection(targetConnectionString);

        if (obj.Type == DbObjectType.Table)
        {
            if (phase is 0 or 1) // Phase 1: 建立資料表結構，並安裝所需 Extension。
            {
                await conn.OpenAsync();
                await EnsureExtensionsIfUsedAsync(conn, sourceConnectionString, obj);
                await conn.ExecuteWithLogAsync(logger, definition);
            }

            if (phase is 0 or 2) // Phase 2: 以 COPY 協定大量複製資料列。
            {
                await CopyTableDataAsync(sourceConnectionString, targetConnectionString, obj);
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
                    catch (PostgresException ex) when (ex.SqlState == "42P01") // undefined_table
                    {
                        logger.LogInformation(
                            "Skipped foreign key creation for {Schema}.{Name} - referenced table does not exist in target (42P01)",
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
    /// 將來源資料表的所有資料列複製至目標資料表。
    /// <list type="bullet">
    ///   <item>含 geometry/geography/raster 欄位：使用文字模式 COPY，以 <c>ST_AsEWKT()</c> 序列化空間資料。</item>
    ///   <item>一般資料表：使用二進位模式 COPY（效率更高）。</item>
    /// </list>
    /// 複製完成後重設序列（serial/IDENTITY）的目前值，防止後續 INSERT 發生主鍵衝突。
    /// </summary>
    private async Task CopyTableDataAsync(string sourceConnectionString, string targetConnectionString, DbObject obj)
    {
        await using var sourceConn = new NpgsqlConnection(sourceConnectionString);
        await sourceConn.OpenAsync();

        // 取得欄位清單與型別，以決定序列化策略及 COPY 目標欄位順序。
        var columns = (await sourceConn.QueryWithLogAsync(logger, """
                                                                  SELECT
                                                                      a.attname AS column_name,
                                                                      format_type(a.atttypid, a.atttypmod) AS data_type
                                                                  FROM pg_attribute a
                                                                  JOIN pg_class c ON a.attrelid = c.oid
                                                                  JOIN pg_namespace n ON c.relnamespace = n.oid
                                                                  WHERE c.relname = @Name AND n.nspname = @Schema
                                                                    AND a.attnum > 0 AND NOT a.attisdropped
                                                                  ORDER BY a.attnum
                                                                  """, new { obj.Schema, obj.Name })).ToList();

        if (columns.Count == 0) return;

        var colList = string.Join(", ", columns.Select(c => $"\"{c.column_name}\""));

        // 空間欄位須以 ST_AsEWKT() 轉為文字；其餘欄位直接引用。
        var selectListForRead = string.Join(
            ", ",
            columns.Select(c =>
            {
                string dt = (string)c.data_type;
                bool isSpatial = dt.Contains("geometry", StringComparison.OrdinalIgnoreCase)
                                 || dt.Contains("geography", StringComparison.OrdinalIgnoreCase)
                                 || dt.Contains("raster", StringComparison.OrdinalIgnoreCase)
                                 || dt.Contains("public.geometry", StringComparison.OrdinalIgnoreCase)
                                 || dt.Contains("public.geography", StringComparison.OrdinalIgnoreCase);
                return isSpatial
                    ? $"ST_AsEWKT(\"{c.column_name}\") AS \"{c.column_name}\""
                    : $"\"{c.column_name}\"";
            }));

        await using var targetConn = new NpgsqlConnection(targetConnectionString);
        await targetConn.OpenAsync();

        var hasGeometry = columns.Any(c =>
            c.data_type.Contains("geometry", StringComparison.OrdinalIgnoreCase) ||
            c.data_type.Contains("geography", StringComparison.OrdinalIgnoreCase) ||
            c.data_type.Contains("raster", StringComparison.OrdinalIgnoreCase) ||
            c.data_type.Contains("USER-DEFINED", StringComparison.OrdinalIgnoreCase));

        await using var cmd = new NpgsqlCommand(
            $"SELECT {(hasGeometry ? selectListForRead : colList)} FROM \"{obj.Schema}\".\"{obj.Name}\"",
            sourceConn);
        cmd.CommandTimeout = 0;
        await using var reader = await cmd.ExecuteReaderAsync();

        if (hasGeometry)
        {
            // 文字模式 COPY：以 Tab 分隔，NULL 以 \N 表示。
            await using var writer = await targetConn.BeginTextImportAsync(
                $"COPY \"{obj.Schema}\".\"{obj.Name}\" ({colList}) FROM STDIN");
            while (await reader.ReadAsync())
            {
                var values = new string[reader.FieldCount];
                for (var i = 0; i < reader.FieldCount; i++)
                {
                    var val = reader.GetValue(i);
                    if (val == DBNull.Value)
                        values[i] = "\\N";
                    else if (val is byte[] bytes)
                        values[i] = "\\\\x" + BitConverter.ToString(bytes).Replace("-", "");
                    else if (val is bool b)
                        values[i] = b ? "t" : "f";
                    else
                    {
                        // 跳脫 Tab 分隔符號與換行符號以符合 COPY 文字格式規範。
                        var s = val.ToString() ?? "";
                        values[i] = s.Replace("\\", "\\\\").Replace("\t", "\\t").Replace("\n", "\\n")
                            .Replace("\r", "\\r");
                    }
                }

                await writer.WriteLineAsync(string.Join("\t", values));
            }
        }
        else
        {
            // 二進位模式 COPY：效率最高，由 Npgsql 處理所有型別對應。
            await using var importer = await targetConn.BeginBinaryImportAsync(
                $"COPY \"{obj.Schema}\".\"{obj.Name}\" ({colList}) FROM STDIN (FORMAT BINARY)");
            while (await reader.ReadAsync())
            {
                await importer.StartRowAsync();
                for (var i = 0; i < reader.FieldCount; i++)
                    await importer.WriteAsync(reader.GetValue(i));
            }

            await importer.CompleteAsync();
        }

        // 更新序列（serial / IDENTITY 欄位）的目前值，避免後續 INSERT 發生 PK 衝突。
        // 查詢目標庫中與此資料表欄位關聯的所有序列。
        const string seqSql = """
                              SELECT
                                  s.relname AS SequenceName,
                                  n.nspname AS SequenceSchema,
                                  a.attname AS ColumnName
                              FROM pg_class s
                              JOIN pg_depend d ON d.objid = s.oid
                              JOIN pg_class t ON d.refobjid = t.oid
                              JOIN pg_namespace n ON n.oid = s.relnamespace
                              JOIN pg_namespace tn ON tn.oid = t.relnamespace
                              JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = d.refobjsubid
                              WHERE s.relkind = 'S'
                                AND t.relname = @Name
                                AND tn.nspname = @Schema
                              """;

        var sequences = await targetConn.QueryWithLogAsync<dynamic>(logger, seqSql, new { obj.Schema, obj.Name });

        foreach (var seq in sequences)
        {
            try
            {
                var dict = (IDictionary<string, object>)seq;
                var seqName = dict["sequencename"].ToString() ?? dict["SequenceName"].ToString();
                var seqSchema = dict["sequenceschema"].ToString() ?? dict["SequenceSchema"].ToString();
                var colName = dict["columnname"].ToString() ?? dict["ColumnName"].ToString();

                if (string.IsNullOrEmpty(seqName) || string.IsNullOrEmpty(colName) || string.IsNullOrEmpty(seqSchema))
                    continue;

                // 以 EscapePgLiteral 跳脫識別符後嵌入 format('%I',…) 字串，防止 PL/pgSQL 注入。
                var escapedCol = EscapePgLiteral(colName);
                var escapedSchema = EscapePgLiteral(obj.Schema);
                var escapedName = EscapePgLiteral(obj.Name);
                var escapedSeqSchema = EscapePgLiteral(seqSchema);
                var escapedSeqName = EscapePgLiteral(seqName);

                var setValSql = $"""
                                 DO $$
                                 DECLARE max_val bigint;
                                 BEGIN
                                     EXECUTE format('SELECT MAX(%I) FROM %I.%I', '{escapedCol}', '{escapedSchema}', '{escapedName}') INTO max_val;
                                     IF max_val IS NULL THEN
                                         PERFORM setval(format('%I.%I', '{escapedSeqSchema}', '{escapedSeqName}'), 1, false);
                                     ELSE
                                         PERFORM setval(format('%I.%I', '{escapedSeqSchema}', '{escapedSeqName}'), max_val, true);
                                     END IF;
                                 END $$;
                                 """;
                await targetConn.ExecuteWithLogAsync(logger, setValSql);
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "Failed to reseed sequence for {Schema}.{Name}", obj.Schema, obj.Name);
            }
        }
    }

    /// <summary>
    /// 為資料表的所有非主鍵索引產生 <c>CREATE [UNIQUE] INDEX</c> 陳述式。
    /// 使用 <c>indnkeyatts</c> 區分索引鍵欄與 INCLUDE 欄（PostgreSQL 11+）。
    /// </summary>
    private async Task<List<string>> GetTableIndexDefinitionsAsync(string connectionString, DbObject obj)
    {
        await using var conn = new NpgsqlConnection(connectionString);
        const string sql = """
                           SELECT
                               i.relname as Name,
                               ix.indisunique as IsUnique,
                               string_agg(CASE WHEN array_position(ix.indkey, a.attnum) <= ix.indnkeyatts THEN '"' || a.attname || '"' END, ', ' order by array_position(ix.indkey, a.attnum)) as Columns,
                               string_agg(CASE WHEN array_position(ix.indkey, a.attnum) > ix.indnkeyatts THEN '"' || a.attname || '"' END, ', ' order by array_position(ix.indkey, a.attnum)) as IncludedColumns
                           FROM pg_index ix
                           JOIN pg_class t ON t.oid = ix.indrelid
                           JOIN pg_class i ON i.oid = ix.indexrelid
                           JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
                           JOIN pg_namespace n ON n.oid = t.relnamespace
                           WHERE t.relname = @Name AND n.nspname = @Schema
                             AND ix.indisprimary = false -- skip PK
                           GROUP BY i.relname, ix.indisunique, ix.indnkeyatts
                           """;

        var results = await conn.QueryWithLogAsync(logger, sql, new { obj.Name, obj.Schema });
        return results.Select(r =>
            {
                var dict = (IDictionary<string, object>)r;

                // 使用 case-insensitive 鍵查找，相容 Npgsql 動態物件欄位名稱大小寫差異。
                object GetValue(string key) =>
                    dict.FirstOrDefault(x => x.Key.Equals(key, StringComparison.OrdinalIgnoreCase)).Value;

                var isUnique = GetValue("IsUnique") as bool? ?? false;
                var name = GetValue("Name").ToString() ?? "";
                var columns = GetValue("Columns").ToString() ?? "";
                var includedColumns = GetValue("IncludedColumns").ToString();
                var includeSql = string.IsNullOrEmpty(includedColumns) ? "" : $" INCLUDE ({includedColumns})";

                return
                    $"CREATE {(isUnique ? "UNIQUE " : "")}INDEX \"{name}\" ON \"{obj.Schema}\".\"{obj.Name}\" ({columns}){includeSql}";
            })
            .ToList();
    }

    /// <summary>
    /// 為資料表的所有外部索引鍵產生 <c>ALTER TABLE … ADD CONSTRAINT … FOREIGN KEY</c> 陳述式。
    /// 多欄位外鍵以條件約束名稱分組，並依 <c>ordinal_position</c> 排序以確保欄位順序正確。
    /// </summary>
    private async Task<List<string>> GetTableForeignKeyDefinitionsAsync(string connectionString, DbObject obj)
    {
        await using var conn = new NpgsqlConnection(connectionString);
        var fks = await conn.QueryWithLogAsync(logger, """
                                                       SELECT
                                                           tc.constraint_name AS ForeignKeyName,
                                                           tc.table_schema AS SchemaName,
                                                           tc.table_name AS TableName,
                                                           kcu.column_name AS ColumnName,
                                                           rc.unique_constraint_schema AS ReferencedSchemaName,
                                                           kcu_ref.table_name AS ReferencedTableName,
                                                           kcu_ref.column_name AS ReferencedColumnName
                                                       FROM information_schema.table_constraints AS tc
                                                       JOIN information_schema.key_column_usage AS kcu
                                                           ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
                                                       JOIN information_schema.referential_constraints AS rc
                                                           ON tc.constraint_name = rc.constraint_name AND tc.table_schema = rc.constraint_schema
                                                       JOIN information_schema.key_column_usage AS kcu_ref
                                                           ON rc.unique_constraint_name = kcu_ref.constraint_name
                                                          AND rc.unique_constraint_schema = kcu_ref.table_schema
                                                          AND kcu.position_in_unique_constraint = kcu_ref.ordinal_position
                                                       WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_schema = @Schema AND tc.table_name = @Name
                                                       ORDER BY tc.constraint_name, kcu.ordinal_position
                                                       """,
            new { obj.Schema, obj.Name });

        var grouped = fks.GroupBy(f => f.ForeignKeyName);

        return (from @group in grouped
            let first = @group.First()
            let cols = string.Join(", ", @group.Select(g => $"\"{g.ColumnName}\""))
            let refCols = string.Join(", ", @group.Select(g => $"\"{g.ReferencedColumnName}\""))
            select $"ALTER TABLE \"{first.SchemaName}\".\"{first.TableName}\" " +
                   $"ADD CONSTRAINT \"{@group.Key}\" FOREIGN KEY ({cols}) " +
                   $"REFERENCES \"{first.ReferencedSchemaName}\".\"{first.ReferencedTableName}\" ({refCols})").ToList();
    }
}