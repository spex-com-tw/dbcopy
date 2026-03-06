using DbCopy.Models;

namespace DbCopy.Services;

public interface IDbService
{
    Task<bool> TestConnectionAsync(string connectionString);
    Task<List<DbObject>> GetDbObjectsAsync(string connectionString);
    Task<string> GetObjectDefinitionAsync(string connectionString, DbObject obj);

    Task CopyObjectAsync(string sourceConnectionString, string targetConnectionString, DbObject obj, int phase = 0,
        int batchSize = 1000);

    Task<bool> CheckObjectExistsAsync(string connectionString, DbObject obj);
    Task EnsureSchemaExistsAsync(string connectionString, string schema);
    Task<List<string>> GetDependenciesAsync(string connectionString, DbObject obj);
    Task<long> GetRowCountAsync(string connectionString, DbObject obj);
    Task<List<DbIndex>> GetTableIndexesAsync(string connectionString, DbObject obj);
}