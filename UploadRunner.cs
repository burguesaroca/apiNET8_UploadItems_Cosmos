using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Configuration;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Data.SqlClient;
using System.Data;

namespace UploadItemsCosmos;

public class UploadRunner
{
    private readonly IConfiguration _configuration;

    public UploadRunner(IConfiguration configuration)
    {
        _configuration = configuration;
    }

    public async Task RunOnceAsync(CancellationToken cancellationToken = default)
    {
        Console.WriteLine("=== Upload Items to Cosmos DB ===\n");

        // Load configuration
        var configuration = _configuration;

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
        using var cosmosClient = new CosmosClient(cosmosEndpoint, cosmosKey);

        // Get database and container
        var database = cosmosClient.GetDatabase(databaseName);
        var container = database.GetContainer(containerName);

        // Show container partition key path
        string containerPartitionKeyPath = string.Empty;
        try
        {
            var containerResponse = await container.ReadContainerAsync(cancellationToken: cancellationToken);
            containerPartitionKeyPath = containerResponse.Resource.PartitionKeyPath ?? string.Empty;
            Console.WriteLine($"Connected to Cosmos DB: {databaseName}/{containerName} (PartitionKeyPath: {containerPartitionKeyPath})\n");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Connected to Cosmos DB: {databaseName}/{containerName} (Couldn't read container metadata: {ex.Message})\n");
        }

        // Load connections: try SQL (if configured) otherwise read from JSON
        var connections = await LoadConnectionsAsync(configuration, cancellationToken);
        if (connections == null || connections.Count == 0)
        {
            Console.WriteLine("No connections found to upload.");
            return;
        }

        Console.WriteLine($"Found {connections.Count} connections to upload.\n");

        // Clear existing items in the container before uploading new ones
        Console.WriteLine("Clearing existing items in container before upload...");
        try
        {
            string pkPathQuery = containerPartitionKeyPath?.TrimStart('/') ?? string.Empty;
            if (string.IsNullOrEmpty(pkPathQuery))
            {
                Console.WriteLine("Container has no partition key path; skipping delete-all step.");
            }
            else
            {
                var pkAlias = "__pk";
                var query = new QueryDefinition($"SELECT c.id, c.{pkPathQuery} AS {pkAlias} FROM c");
                int deleted = 0;
                var toDelete = new List<(string id, string pk)>();

                using var feed = container.GetItemQueryIterator<ItemIdPk>(query);
                while (feed.HasMoreResults)
                {
                    var resp = await feed.ReadNextAsync(cancellationToken);
                    foreach (var el in resp)
                    {
                        try
                        {
                            var id = el?.id ?? string.Empty;
                            var pk = el?.__pk ?? string.Empty;

                            if (string.IsNullOrEmpty(id)) continue;
                            if (string.IsNullOrEmpty(pk))
                            {
                                Console.WriteLine($"Skipping delete for id='{id}' because partition key value is empty.");
                                continue;
                            }

                            toDelete.Add((id, pk));
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"Warning inspecting item (deserialized): {ex}");
                        }
                    }
                }

                foreach (var item in toDelete)
                {
                    try
                    {
                        Console.WriteLine($"Deleting item id='{item.id}', pk='{item.pk}'...");
                        await container.DeleteItemAsync<JsonElement>(item.id, new PartitionKey(item.pk), cancellationToken: cancellationToken);
                        deleted++;
                    }
                    catch (Exception exDel)
                    {
                        Console.WriteLine($"Warning deleting item id='{item.id}', pk='{item.pk}': {exDel.Message}");
                    }
                }

                Console.WriteLine($"Deleted {deleted} existing items from container.");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error clearing container: {ex.Message}");
        }

        // Upload each connection
        int successCount = 0;
        int errorCount = 0;

        foreach (var connection in connections)
        {
            if (cancellationToken.IsCancellationRequested) break;

            try
            {
                Console.Write($"Uploading connection {connection.ClientId} ({connection.ClientName})... ");

                try
                {
                    var debugJson = Newtonsoft.Json.JsonConvert.SerializeObject(connection);
                    Console.WriteLine($"\nDEBUG JSON: {debugJson}");
                }
                catch { }

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
                    pkValue = connection.ClientId;
                }

                Console.WriteLine($"Using partition key path '/{pkPath}' with value '{pkValue}'");

                var response = await container.UpsertItemAsync(
                    connection,
                    new PartitionKey(pkValue),
                    cancellationToken: cancellationToken
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

    // Copy of original LoadConnectionsAsync but accepts a cancellation token
    public static async Task<List<Connection>> LoadConnectionsAsync(IConfiguration configuration, CancellationToken cancellationToken = default)
    {
        var sqlConnectionString = configuration["SqlServer:ConnectionString"];
        var sqlQuery = configuration["SqlServer:Query"] ?? "SELECT clientId, servidor, [user], password, repository FROM Clients";

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
            await conn.OpenAsync(cancellationToken);
            Console.WriteLine("SQL connection opened successfully.");
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = sqlQuery;
            cmd.CommandType = CommandType.Text;

            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
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
                await File.WriteAllTextAsync("connections.json", JsonSerializer.Serialize(results, writeOptions), cancellationToken);
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
