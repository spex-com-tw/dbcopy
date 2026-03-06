using DbCopy.Models;
using DbCopy.Services;
using Microsoft.AspNetCore.Mvc;

namespace DbCopy.Controllers;

[ApiController]
[Route("api/[controller]")]
public class DbController(
    SqlServerService sqlServerService,
    PostgreSqlService postgreSqlService,
    ILogger<DbController> logger)
    : ControllerBase
{
    private IDbService GetService(DbType type) => type switch
    {
        DbType.SqlServer => sqlServerService,
        DbType.PostgreSql => postgreSqlService,
        _ => throw new ArgumentException("Unsupported database type")
    };

    [HttpPost("test")]
    public async Task<IActionResult> TestConnection([FromBody] DbConnectionInfo connection)
    {
        try
        {
            var service = GetService(connection.Type);
            var success = await service.TestConnectionAsync(connection.ConnectionString);
            return Ok(new { success });
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "TestConnection failed");
            return BadRequest(ex.Message);
        }
    }

    [HttpPost("objects")]
    public async Task<IActionResult> GetObjects([FromBody] DbConnectionInfo connection)
    {
        try
        {
            var service = GetService(connection.Type);
            var objects = await service.GetDbObjectsAsync(connection.ConnectionString);
            return Ok(objects);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "GetObjects failed");
            return BadRequest(ex.Message);
        }
    }

    [HttpPost("compare")]
    public async Task<IActionResult> Compare([FromBody] CompareRequest request)
    {
        try
        {
            var sourceService = GetService(request.Source.Type);
            var targetService = GetService(request.Target.Type);

            var sourceObjects = await sourceService.GetDbObjectsAsync(request.Source.ConnectionString);
            var syncStatuses = new List<SyncStatus>();

            // Fix #11: local helpers wrap failable calls so Task.WhenAll can run them in parallel
            async Task<long?> SafeGetRowCount(IDbService svc, string cs, DbObject o, string side)
            {
                try
                {
                    return await svc.GetRowCountAsync(cs, o);
                }
                catch (Exception ex)
                {
                    logger.LogWarning(ex, "Failed to get {Side} row count for {Schema}.{Name}", side, o.Schema, o.Name);
                    return null;
                }
            }

            async Task<List<DbIndex>> SafeGetIndexes(IDbService svc, string cs, DbObject o, string side)
            {
                try
                {
                    return await svc.GetTableIndexesAsync(cs, o);
                }
                catch (Exception ex)
                {
                    logger.LogWarning(ex, "Failed to get {Side} indexes for {Schema}.{Name}", side, o.Schema, o.Name);
                    return [];
                }
            }

            foreach (var obj in sourceObjects)
            {
                // CheckObjectExists (target) and GetDependencies (source) are independent — run in parallel
                var existsTask = targetService.CheckObjectExistsAsync(request.Target.ConnectionString, obj);
                var depsTask = sourceService.GetDependenciesAsync(request.Source.ConnectionString, obj);
                await Task.WhenAll(existsTask, depsTask);
                var exists = existsTask.Result;
                var deps = depsTask.Result;

                long? sourceRows = null;
                long? targetRows = null;

                if (obj.Type == DbObjectType.Table)
                {
                    // All four table-level queries are independent — run them in parallel
                    var sourceRowsTask = SafeGetRowCount(sourceService, request.Source.ConnectionString, obj, "source");
                    var sourceIndexesTask =
                        SafeGetIndexes(sourceService, request.Source.ConnectionString, obj, "source");
                    var targetRowsTask = exists
                        ? SafeGetRowCount(targetService, request.Target.ConnectionString, obj, "target")
                        : Task.FromResult<long?>(null);
                    var targetIndexesTask = exists
                        ? SafeGetIndexes(targetService, request.Target.ConnectionString, obj, "target")
                        : Task.FromResult(new List<DbIndex>());

                    await Task.WhenAll(sourceRowsTask, sourceIndexesTask, targetRowsTask, targetIndexesTask);

                    sourceRows = sourceRowsTask.Result;
                    targetRows = targetRowsTask.Result;
                    var sourceIndexes = sourceIndexesTask.Result;
                    var targetIndexes = targetIndexesTask.Result;

                    foreach (var sIdx in sourceIndexes)
                    {
                        sIdx.ExistsInDestination = targetIndexes.Any(t =>
                            t.Name.Equals(sIdx.Name, StringComparison.OrdinalIgnoreCase));
                    }

                    obj.Indexes = sourceIndexes;
                }

                syncStatuses.Add(new SyncStatus
                {
                    SourceObject = obj,
                    ExistsInDestination = exists,
                    Status = exists ? "Exists" : "Pending",
                    Dependencies = deps,
                    SourceRowCount = sourceRows,
                    TargetRowCount = targetRows
                });
            }

            return Ok(syncStatuses);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Compare failed");
            return BadRequest(ex.Message);
        }
    }

    [HttpPost("copy")]
    public async Task<IActionResult> Copy([FromBody] CopyRequest request)
    {
        try
        {
            var sourceService = GetService(request.Source.Type);
            var targetService = GetService(request.Target.Type);

            // 1. Ensure schema exists
            await targetService.EnsureSchemaExistsAsync(request.Target.ConnectionString, request.Object.Schema);

            // 2. Check if exists in destination (only for Phase 0 or 1)
            if (request.Phase is 0 or 1)
            {
                var exists =
                    await targetService.CheckObjectExistsAsync(request.Target.ConnectionString, request.Object);
                if (exists)
                {
                    return BadRequest("Object already exists in destination.");
                }
            }

            await sourceService.CopyObjectAsync(request.Source.ConnectionString, request.Target.ConnectionString,
                request.Object, request.Phase, request.BatchSize);
            return Ok(new { success = true });
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Copy failed for {Schema}.{Name} (Phase: {Phase})", request.Object.Schema,
                request.Object.Name, request.Phase);
            return BadRequest(ex.Message);
        }
    }
}