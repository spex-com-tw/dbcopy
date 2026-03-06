using System.Data;
using Dapper;

namespace DbCopy.Services;

/// <summary>
/// <see cref="IDbConnection"/> 的擴充方法，將每一次 Dapper 呼叫包裝成帶有結構化
/// 日誌的版本，在執行前以 <c>Information</c> 等級記錄 SQL 陳述式。
///
/// 設計原則：所有 Service 類別必須使用這些 wrapper，不得直接呼叫 Dapper，
/// 如此每一次 SQL 往返恰好在日誌中出現一次，不需在呼叫端重複 log。
/// </summary>
public static partial class DapperExtensions
{
    /// <summary>
    /// 執行 SELECT 查詢，以強型別 <typeparamref name="T"/> 傳回所有符合的資料列。
    /// 執行前記錄 SQL。
    /// </summary>
    public static Task<IEnumerable<T>> QueryWithLogAsync<T>(this IDbConnection conn, ILogger logger, string sql,
        object? param = null)
    {
        logger.LogExecutingSqlSql(sql);
        return conn.QueryAsync<T>(sql, param);
    }

    /// <summary>
    /// 執行 SELECT 查詢，以 <c>dynamic</c> 物件傳回所有符合的資料列。
    /// 當結果欄位在編譯期未知時使用此多載。
    /// 執行前記錄 SQL。
    /// </summary>
    public static Task<IEnumerable<dynamic>> QueryWithLogAsync(this IDbConnection conn, ILogger logger, string sql,
        object? param = null)
    {
        logger.LogExecutingSqlSql(sql);
        return conn.QueryAsync(sql, param);
    }

    /// <summary>
    /// 執行 SELECT 查詢，傳回第一筆 <c>dynamic</c> 資料列；
    /// 結果為空時傳回 <c>null</c>。
    /// 執行前記錄 SQL。
    /// </summary>
    public static Task<dynamic?> QueryFirstOrDefaultWithLogAsync(this IDbConnection conn, ILogger logger, string sql,
        object? param = null)
    {
        logger.LogExecutingSqlSql(sql);
        return conn.QueryFirstOrDefaultAsync(sql, param);
    }

    /// <summary>
    /// 執行傳回單一純量值的 SELECT 查詢（<typeparamref name="T"/>）；
    /// 結果集為空時傳回 <c>null</c>。
    /// 常見用途：存在性檢查（<c>SELECT 1 …</c>）或彙總值。
    /// 執行前記錄 SQL。
    /// </summary>
    public static Task<T?> ExecuteScalarWithLogAsync<T>(this IDbConnection conn, ILogger logger, string sql,
        object? param = null)
    {
        logger.LogExecutingSqlSql(sql);
        return conn.ExecuteScalarAsync<T>(sql, param);
    }

    /// <summary>
    /// 執行非查詢陳述式（INSERT / UPDATE / DELETE / DDL），傳回受影響的資料列數。
    /// 執行前記錄 SQL。
    /// </summary>
    public static Task<int> ExecuteWithLogAsync(this IDbConnection conn, ILogger logger, string sql,
        object? param = null)
    {
        logger.LogExecutingSqlSql(sql);
        return conn.ExecuteAsync(sql, param);
    }

    /// <summary>
    /// 由 <c>[LoggerMessage]</c> 原始碼產生器產生的高效能結構化日誌方法。
    /// 使用原始碼產生版本可避免 <c>Information</c> 等級未啟用時的 boxing 與字串格式化開銷。
    /// </summary>
    [LoggerMessage(LogLevel.Information, "Executing SQL: {Sql}")]
    static partial void LogExecutingSqlSql(this ILogger logger, string Sql);
}