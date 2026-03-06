using DbCopy.Services;
using System.Net;
using System.Net.Sockets;
using Microsoft.Extensions.FileProviders;
using Serilog;

namespace DbCopy
{
    public class Program
    {
        public static void Main(string[] args)
        {
            var builder = WebApplication.CreateBuilder(args);

            // Configure Serilog
            Log.Logger = new LoggerConfiguration()
                .WriteTo.Console()
                .WriteTo.File("Logs/log-.txt", rollingInterval: RollingInterval.Day)
                .CreateLogger();

            builder.Host.UseSerilog();

            var configuredUrls = builder.Configuration["urls"];
            if (!string.IsNullOrWhiteSpace(configuredUrls))
            {
                var adjustedUrls = AdjustUrlsForPortConflicts(configuredUrls);
                if (!string.Equals(adjustedUrls, configuredUrls, StringComparison.Ordinal))
                {
                    builder.WebHost.UseUrls(adjustedUrls);
                    Log.Warning("Detected occupied port(s). Switched URLs from {OriginalUrls} to {AdjustedUrls}",
                        configuredUrls, adjustedUrls);
                }
            }

            // Add services to the container.
            builder.Services.AddRazorPages();
            builder.Services.AddControllers();
            builder.Services.AddScoped<SqlServerService>();
            builder.Services.AddScoped<PostgreSqlService>();

            var app = builder.Build();

            // Configure the HTTP request pipeline.
            if (!app.Environment.IsDevelopment())
            {
                app.UseExceptionHandler("/Error");
                // The default HSTS value is 30 days. You may want to change this for production scenarios, see https://aka.ms/aspnetcore-hsts.
                app.UseHsts();
                // Fix #12: only redirect to HTTPS when HTTPS is actually configured (not in development)
                app.UseHttpsRedirection();
            }

            app.UseRouting();
            // Fix #13: UseAuthorization removed — no auth scheme or policies are configured

            // 從嵌入資源提供靜態檔案，讓單一執行檔不需要 wwwroot 資料夾
            var embeddedProvider = new ManifestEmbeddedFileProvider(
                typeof(Program).Assembly, "wwwroot");
            app.UseStaticFiles(new StaticFileOptions
            {
                FileProvider = embeddedProvider
            });
            app.MapRazorPages();
            app.MapControllers();

            try
            {
                Log.Information("Starting web host");

                // 應用程式啟動後自動開啟瀏覽器
                app.Lifetime.ApplicationStarted.Register(() =>
                {
                    var url = app.Urls.FirstOrDefault() ?? "http://localhost:5000";
                    Log.Information("Opening browser at {Url}", url);
                    System.Diagnostics.Process.Start(new System.Diagnostics.ProcessStartInfo
                    {
                        FileName = url,
                        UseShellExecute = true
                    });
                });

                app.Run();
            }
            catch (Exception ex)
            {
                Log.Fatal(ex, "Host terminated unexpectedly");
            }
            finally
            {
                Log.CloseAndFlush();
            }
        }

        private static string AdjustUrlsForPortConflicts(string urls)
        {
            var candidates = urls.Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
            if (candidates.Length == 0)
            {
                return urls;
            }

            var reservedPorts = new HashSet<int>();
            var adjusted = new string[candidates.Length];

            for (var i = 0; i < candidates.Length; i++)
            {
                var current = candidates[i];
                if (!Uri.TryCreate(current, UriKind.Absolute, out var uri) || uri.IsDefaultPort || uri.Port <= 0)
                {
                    adjusted[i] = current;
                    continue;
                }

                if (!IsLocalHost(uri.Host))
                {
                    adjusted[i] = current;
                    continue;
                }

                var port = uri.Port;
                if (reservedPorts.Contains(port) || !IsPortAvailable(port))
                {
                    var newPort = FindNextAvailablePort(port + 1, reservedPorts);
                    var builder = new UriBuilder(uri) { Port = newPort };
                    adjusted[i] = builder.Uri.ToString().TrimEnd('/');
                    reservedPorts.Add(newPort);
                    continue;
                }

                adjusted[i] = current;
                reservedPorts.Add(port);
            }

            return string.Join(';', adjusted);
        }

        private static bool IsLocalHost(string host)
        {
            return host.Equals("localhost", StringComparison.OrdinalIgnoreCase)
                || host.Equals("127.0.0.1", StringComparison.OrdinalIgnoreCase)
                || host.Equals("::1", StringComparison.OrdinalIgnoreCase);
        }

        private static int FindNextAvailablePort(int startPort, HashSet<int> reservedPorts)
        {
            for (var port = startPort; port <= 65535; port++)
            {
                if (reservedPorts.Contains(port))
                {
                    continue;
                }

                if (IsPortAvailable(port))
                {
                    return port;
                }
            }

            throw new InvalidOperationException("Unable to find an available TCP port.");
        }

        private static bool IsPortAvailable(int port)
        {
            return TryBind(IPAddress.Loopback, port) && TryBind(IPAddress.IPv6Loopback, port);
        }

        private static bool TryBind(IPAddress ipAddress, int port)
        {
            try
            {
                using var listener = new TcpListener(ipAddress, port);
                listener.Start();
                listener.Stop();
                return true;
            }
            catch (SocketException)
            {
                return false;
            }
        }
    }
}
