namespace DbCopy.Models;

public enum DbType
{
    SqlServer,
    PostgreSql
}

public class DbConnectionInfo
{
    public string Id { get; set; } = Guid.NewGuid().ToString();
    public string Name { get; set; } = string.Empty;
    public DbType Type { get; set; }
    public string ConnectionString { get; set; } = string.Empty;
}

public enum DbObjectType
{
    UserDefinedType,
    UserDefinedTableType, // Added
    Sequence, // Added
    Table,
    View,
    Procedure,
    Function
}

public class DbObject
{
    public string Schema { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
    public DbObjectType Type { get; set; }
    public string? Definition { get; set; }

    public string FullName => string.IsNullOrEmpty(Schema) ? Name : $"{Schema}.{Name}";
    public List<DbIndex> Indexes { get; set; } = [];
}

public class DbIndex
{
    public string Name { get; set; } = string.Empty;
    public string Columns { get; set; } = string.Empty;
    public bool IsUnique { get; set; }
    public bool ExistsInDestination { get; set; }
}

public class SyncStatus
{
    public DbObject SourceObject { get; set; } = null!;
    public bool ExistsInDestination { get; set; }
    public string Status { get; set; } = "Pending"; // Pending, Success, Error, Skipped
    public string? Message { get; set; }
    public List<string> Dependencies { get; set; } = []; // FullNames of objects this object depends on
    public long? SourceRowCount { get; set; }
    public long? TargetRowCount { get; set; }
}

// Fix #6: moved from DbController.cs into the Models namespace
public class CompareRequest
{
    public DbConnectionInfo Source { get; set; } = null!;
    public DbConnectionInfo Target { get; set; } = null!;
}

public class CopyRequest
{
    public DbConnectionInfo Source { get; set; } = null!;
    public DbConnectionInfo Target { get; set; } = null!;
    public DbObject Object { get; set; } = null!;
    public int Phase { get; set; } = 0;
    public int BatchSize { get; set; } = 1000;
}