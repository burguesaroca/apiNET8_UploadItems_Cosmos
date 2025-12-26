using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Configuration;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Data.SqlClient;
using System.Data;

namespace UploadItemsCosmos;

class Program
{
    static async Task Main(string[] args)
    {
        Console.WriteLine("=== Upload Items to Cosmos DB ===\n");

        // Load configuration
        var configuration = new ConfigurationBuilder()
            .SetBasePath(Directory.GetCurrentDirectory())
            .AddJsonFile("appsettings.json", optional: false)
            .Build();

        var cosmosEndpoint = configuration["CosmosDb:Endpoint"];
        var cosmosKey = configuration["CosmosDb:Key"];
        var databaseName = configuration["CosmosDb:DatabaseName"];
        var containerName = configuration["CosmosDb:ContainerName"];

        if (string.IsNullOrEmpty(cosmosEndpoint) || string.IsNullOrEmpty(cosmosKey))
        {
            Console.WriteLine("Error: Please configure Cosmos DB settings in appsettings.json");
            return;
        }

        // Create Cosmos Client
        var cosmosClient = new CosmosClient(cosmosEndpoint, cosmosKey);

        // Get database and container
        var database = cosmosClient.GetDatabase(databaseName);
        var container = database.GetContainer(containerName);

        // Show container partition key path
        string containerPartitionKeyPath = string.Empty;
        try
        {
            var containerResponse = await container.ReadContainerAsync();
            containerPartitionKeyPath = containerResponse.Resource.PartitionKeyPath ?? string.Empty;
            Console.WriteLine($"Connected to Cosmos DB: {databaseName}/{containerName} (PartitionKeyPath: {containerPartitionKeyPath})\n");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Connected to Cosmos DB: {databaseName}/{containerName} (Couldn't read container metadata: {ex.Message})\n");
        }

        // Load connections: try SQL (if configured) otherwise read from JSON
        var connections = await LoadConnectionsAsync(configuration);
        if (connections == null || connections.Count == 0)
        {
            Console.WriteLine("No connections found to upload.");
            return;
        }

        Console.WriteLine($"Found {connections.Count} connections to upload.\n");

        // Upload each connection
        int successCount = 0;
        int errorCount = 0;

        foreach (var connection in connections)
        {
            try
            {
                Console.Write($"Uploading connection {connection.ClientId} ({connection.ClientName})... ");

                // Debug: show serialized JSON to verify partition key property name/value
                try
                {
                    var debugJson = Newtonsoft.Json.JsonConvert.SerializeObject(connection);
                    Console.WriteLine($"\nDEBUG JSON: {debugJson}");
                }
                catch { }

                // Determine which property to use as partition key based on container's partition key path
                string pkPath = containerPartitionKeyPath?.TrimStart('/') ?? string.Empty;
                string pkValue;
                if (pkPath.Equals("clientId", StringComparison.OrdinalIgnoreCase))
                {
                    pkValue = connection.ClientId;
                }
                else if (pkPath.Equals("id", StringComparison.OrdinalIgnoreCase))
                {
                    pkValue = connection.id;
                }
                else
                {
                    // Fallback to clientId if unknown
                    pkValue = connection.ClientId;
                }

                Console.WriteLine($"Using partition key path '/{pkPath}' with value '{pkValue}'");

                var response = await container.UpsertItemAsync(
                    connection,
                    new PartitionKey(pkValue)
                );

                Console.WriteLine($"✓ Success (RU: {response.RequestCharge:F2})");
                successCount++;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"✗ Error: {ex.Message}");
                errorCount++;
            }
        }

        Console.WriteLine($"\n=== Upload Complete ===");
        Console.WriteLine($"Success: {successCount}");
        Console.WriteLine($"Errors: {errorCount}");
        Console.WriteLine($"Total: {connections.Count}");
    }

    static async Task<List<Connection>> LoadConnectionsAsync(IConfiguration configuration)
    {
        var jsonFilePath = "connections.json";

        var sqlConnectionString = configuration["SqlServer:ConnectionString"];
        var sqlQuery = configuration["SqlServer:Query"] ?? "SELECT id, clientId, clientName, servidor, puerto, [user], password, repository, adapter FROM Clients";

        if (!string.IsNullOrWhiteSpace(sqlConnectionString))
        {
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

                    var connItem = new Connection
                    {
                        id = string.IsNullOrWhiteSpace(GetStringSafe("id")) ? Guid.NewGuid().ToString() : GetStringSafe("id"),
                        ClientId = GetStringSafe("clientId"),
                        ClientName = GetStringSafe("clientName"),
                        Servidor = GetStringSafe("servidor"),
                        Puerto = GetStringSafe("puerto"),
                        User = GetStringSafe("user"),
                        Password = GetStringSafe("password"),
                        Repository = GetStringSafe("repository"),
                        Adapter = GetStringSafe("adapter")
                    };

                    results.Add(connItem);
                }

                if (results.Count > 0)
                {
                    var writeOptions = new JsonSerializerOptions { WriteIndented = true };
                    await File.WriteAllTextAsync(jsonFilePath, JsonSerializer.Serialize(results, writeOptions));
                    Console.WriteLine($"Generated '{jsonFilePath}' from SQL query (rows: {results.Count}).");
                }

                return results;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error querying SQL Server: {ex.Message}");
                Console.WriteLine("Falling back to reading existing connections.json (if present).");
            }
        }

        // Fallback: read from connections.json file
        if (!File.Exists(jsonFilePath))
        {
            Console.WriteLine($"Error: File '{jsonFilePath}' not found and no SQL configuration provided.");
            return new List<Connection>();
        }

        var jsonContent = await File.ReadAllTextAsync(jsonFilePath);
        var jsonOptions = new JsonSerializerOptions
        {
            PropertyNameCaseInsensitive = true
        };
        var connections = JsonSerializer.Deserialize<List<Connection>>(jsonContent, jsonOptions) ?? new List<Connection>();
        return connections;
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
