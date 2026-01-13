using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Data.SqlClient;
using System.Data;

namespace UploadItemsCosmos;

class Program
{
    static async Task Main(string[] args)
    {
        var host = Host.CreateDefaultBuilder(args)
            .ConfigureAppConfiguration((ctx, cfg) =>
            {
                cfg.SetBasePath(Directory.GetCurrentDirectory())
                   .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true);
            })
            .ConfigureServices((ctx, services) =>
            {
                services.AddSingleton<UploadRunner>();
                services.AddHostedService<Services.ScheduledUploadService>();
                services.AddHttpClient("orchestrator");
                services.AddHostedService<Services.ScheduledOrchestratorService>();
            })
            .ConfigureLogging((ctx, logging) =>
            {
                logging.AddConsole();
            })
            .Build();

        await host.RunAsync();
    }

    static async Task<List<Connection>> LoadConnectionsAsync(IConfiguration configuration)
    {
        var sqlConnectionString = configuration["SqlServer:ConnectionString"];
        // Query only the fields that should come from SQL
        var sqlQuery = configuration["SqlServer:Query"] ?? "SELECT clientId, servidor, [user], password, repository FROM Clients";

        // Load shared defaults from appsettings.json (optional). Expected structure:
        // "ConnectionsDefaults": { "clientName": "...", "puerto": "1433", "adapter": "SqlServerSP" }
        var defaultsSection = configuration.GetSection("ConnectionsDefaults");
        var globalClientName = defaultsSection["clientName"] ?? string.Empty;
        var globalPuerto = defaultsSection["puerto"] ?? string.Empty;
        var globalAdapter = defaultsSection["adapter"] ?? string.Empty;

        if (string.IsNullOrWhiteSpace(sqlConnectionString))
        {
            Console.WriteLine("Error: SqlServer:ConnectionString is not configured. No fallback to connections.json is allowed.");
            return new List<Connection>();
        }

        try
        {
            var sb = new SqlConnectionStringBuilder(sqlConnectionString);
            Console.WriteLine($"SQL config detected. DataSource: {sb.DataSource}, InitialCatalog: {sb.InitialCatalog}, UserID: {sb.UserID}");
        }
        catch
        {
            Console.WriteLine("SQL config detected but couldn't parse connection string (will attempt to use it as-is).");
        }

        Console.WriteLine("Attempting to query SQL Server for connections...");
        try
        {
            var results = new List<Connection>();
            await using var conn = new SqlConnection(sqlConnectionString);
            await conn.OpenAsync();
            Console.WriteLine("SQL connection opened successfully.");
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = sqlQuery;
            cmd.CommandType = CommandType.Text;

            await using var reader = await cmd.ExecuteReaderAsync();
                while (await reader.ReadAsync())
            {
                string GetStringSafe(string name)
                {
                    try
                    {
                        var ordinal = reader.GetOrdinal(name);
                        if (reader.IsDBNull(ordinal)) return string.Empty;
                        return reader.GetValue(ordinal)?.ToString() ?? string.Empty;
                    }
                    catch
                    {
                        return string.Empty;
                    }
                }
                    var clientId = GetStringSafe("clientId");
                    var servidor = GetStringSafe("servidor");
                    var user = GetStringSafe("user");
                    var password = GetStringSafe("password");
                    var repository = GetStringSafe("repository");


                    var connItem = new Connection
                    {
                        id = Guid.NewGuid().ToString(),
                        ClientId = clientId,
                        ClientName = globalClientName,
                        Servidor = servidor,
                        Puerto = globalPuerto,
                        User = user,
                        Password = password,
                        Repository = repository,
                        Adapter = globalAdapter
                    };

                    results.Add(connItem);
            }

            if (results.Count > 0)
            {
                var writeOptions = new JsonSerializerOptions { WriteIndented = true };
                await File.WriteAllTextAsync("connections.json", JsonSerializer.Serialize(results, writeOptions));
                Console.WriteLine($"Generated 'connections.json' from SQL query (rows: {results.Count}).");
            }

            return results;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error querying SQL Server: {ex.Message}");
            Console.WriteLine("No fallback to connections.json is configured; aborting.");
            return new List<Connection>();
        }
    }
}

public class Connection
{
    public string id { get; set; } = string.Empty;

    [JsonPropertyName("clientId")]
    [Newtonsoft.Json.JsonProperty("clientId")]
    public string ClientId { get; set; } = string.Empty;

    [JsonPropertyName("clientName")]
    [Newtonsoft.Json.JsonProperty("clientName")]
    public string ClientName { get; set; } = string.Empty;

    [JsonPropertyName("servidor")]
    [Newtonsoft.Json.JsonProperty("servidor")]
    public string Servidor { get; set; } = string.Empty;

    [JsonPropertyName("puerto")]
    [Newtonsoft.Json.JsonProperty("puerto")]
    public string Puerto { get; set; } = string.Empty;

    [JsonPropertyName("user")]
    [Newtonsoft.Json.JsonProperty("user")]
    public string User { get; set; } = string.Empty;

    [JsonPropertyName("password")]
    [Newtonsoft.Json.JsonProperty("password")]
    public string Password { get; set; } = string.Empty;

    [JsonPropertyName("repository")]
    [Newtonsoft.Json.JsonProperty("repository")]
    public string Repository { get; set; } = string.Empty;

    [JsonPropertyName("adapter")]
    [Newtonsoft.Json.JsonProperty("adapter")]
    public string Adapter { get; set; } = string.Empty;
}

public class ItemIdPk
{
    public string id { get; set; } = string.Empty;
    public string __pk { get; set; } = string.Empty;
}

public class DefaultConnection
{
    public string id { get; set; } = string.Empty;
    public string clientId { get; set; } = string.Empty;
    public string clientName { get; set; } = string.Empty;
    public string puerto { get; set; } = string.Empty;
    public string adapter { get; set; } = string.Empty;
}
