using DbCopy.Services;
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
    }
}