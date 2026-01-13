using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Collections.Generic;

namespace UploadItemsCosmos.Services;

public class ScheduledOrchestratorService : BackgroundService
{
    private readonly ILogger<ScheduledOrchestratorService> _logger;
    private readonly IConfiguration _configuration;
    private readonly IHttpClientFactory _httpFactory;

    public ScheduledOrchestratorService(ILogger<ScheduledOrchestratorService> logger, IConfiguration configuration, IHttpClientFactory httpFactory)
    {
        _logger = logger;
        _configuration = configuration;
        _httpFactory = httpFactory;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("ScheduledOrchestratorService started.");

        // Read schedule from configuration (format: HH:mm). Default to 02:00 if missing.
        var dailyTime = _configuration["SchedulerOrchestrator:DailyTime"] ?? "02:00";

        if (!TimeSpan.TryParse(dailyTime, out var scheduledTime))
        {
            _logger.LogWarning("Invalid SchedulerOrchestrator:DailyTime value '{time}', using 02:00.", dailyTime);
            scheduledTime = TimeSpan.FromHours(2);
        }

        var tzId = _configuration["SchedulerOrchestrator:TimeZone"] ?? "America/Bogota";
        TimeZoneInfo tzInfo;
        try
        {
            if (string.Equals(tzId, "Local", StringComparison.OrdinalIgnoreCase)) tzInfo = TimeZoneInfo.Local;
            else tzInfo = TimeZoneInfo.FindSystemTimeZoneById(tzId);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Could not resolve timezone '{tzId}', falling back to Local.", tzId);
            tzInfo = TimeZoneInfo.Local;
        }

        // Read HTTP config
        var baseUrl = _configuration["SchedulerOrchestrator:BaseUrl"]?.TrimEnd('/') ?? string.Empty;
        var tokenEndpoint = _configuration["SchedulerOrchestrator:TokenEndpoint"] ?? string.Empty;
        var orchestratorEndpoint = _configuration["SchedulerOrchestrator:OrchestratorEndpoint"] ?? string.Empty;
        var tokenName = _configuration["SchedulerOrchestrator:TokenCredentials:Name"] ?? string.Empty;
        var tokenPassword = _configuration["SchedulerOrchestrator:TokenCredentials:Password"] ?? string.Empty;

        _logger.LogInformation("SchedulerOrchestrator — daily at {time} ({tz}). BaseUrl: {base}", dailyTime, tzInfo.Id, baseUrl);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                // Compute next occurrence in configured timezone
                var nowUtc = DateTime.UtcNow;
                var nowInTz = TimeZoneInfo.ConvertTimeFromUtc(nowUtc, tzInfo);

                var candidate = new DateTime(nowInTz.Year, nowInTz.Month, nowInTz.Day, scheduledTime.Hours, scheduledTime.Minutes, 0);
                if (candidate <= nowInTz) candidate = candidate.AddDays(1);

                var nextUtc = TimeZoneInfo.ConvertTimeToUtc(candidate, tzInfo);
                var delay = nextUtc - nowUtc;

                var nextLocalDisplay = TimeZoneInfo.ConvertTimeFromUtc(nextUtc, TimeZoneInfo.Local);
                _logger.LogInformation("Next orchestrator run scheduled at {nextLocal} (local) / {nextTz} ({tz}) — in {delay}.", nextLocalDisplay, candidate, tzInfo.Id, delay);

                await Task.Delay(delay, stoppingToken);

                if (stoppingToken.IsCancellationRequested) break;

                _logger.LogInformation("Starting orchestrator POST at {time}.", DateTime.Now);

                // Acquire token
                string? token = null;
                if (!string.IsNullOrEmpty(baseUrl) && !string.IsNullOrEmpty(tokenEndpoint))
                {
                    try
                    {
                        var client = _httpFactory.CreateClient("orchestrator");
                        client.BaseAddress = new Uri(baseUrl);

                        var tokenUri = tokenEndpoint.StartsWith("/") ? tokenEndpoint : "/" + tokenEndpoint;
                        var tokenReq = new HttpRequestMessage(HttpMethod.Post, tokenUri);
                        var cred = new { name = tokenName, password = tokenPassword };
                        tokenReq.Content = new StringContent(JsonSerializer.Serialize(cred), Encoding.UTF8, "application/json");

                        var resp = await client.SendAsync(tokenReq, stoppingToken);
                        resp.EnsureSuccessStatusCode();
                        var respText = await resp.Content.ReadAsStringAsync(stoppingToken);
                        try
                        {
                            using var doc = JsonDocument.Parse(respText);
                            if (doc.RootElement.TryGetProperty("access_token", out var at)) token = at.GetString();
                            else if (doc.RootElement.TryGetProperty("token", out var t2)) token = t2.GetString();
                            else if (doc.RootElement.TryGetProperty("accessToken", out var t3)) token = t3.GetString();
                            else
                            {
                                token = doc.RootElement.GetRawText();
                            }
                        }
                        catch
                        {
                            token = respText;
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Error acquiring token from {endpoint}.", tokenEndpoint);
                    }
                }

                // Prepare orchestrator body
                string bodyJson = "{}";
                var bodySection = _configuration.GetSection("SchedulerOrchestrator:OrchestratorBody");
                if (bodySection.Exists())
                {
                    var dict = new Dictionary<string, object?>();
                    foreach (var child in bodySection.GetChildren())
                    {
                        dict[child.Key] = child.Value;
                    }
                    bodyJson = JsonSerializer.Serialize(dict);
                }
                else
                {
                    var raw = _configuration["SchedulerOrchestrator:OrchestratorBody"];
                    if (!string.IsNullOrEmpty(raw)) bodyJson = raw;
                }

                // Call orchestrator endpoint
                try
                {
                    var client2 = _httpFactory.CreateClient("orchestrator");
                    client2.BaseAddress = new Uri(baseUrl);

                    var orchUri = orchestratorEndpoint.StartsWith("/") ? orchestratorEndpoint : "/" + orchestratorEndpoint;
                    var req = new HttpRequestMessage(HttpMethod.Post, orchUri)
                    {
                        Content = new StringContent(bodyJson, Encoding.UTF8, "application/json")
                    };
                    if (!string.IsNullOrEmpty(token)) req.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token.Trim('"'));

                    var resp2 = await client2.SendAsync(req, stoppingToken);
                    var respText2 = await resp2.Content.ReadAsStringAsync(stoppingToken);
                    if (resp2.IsSuccessStatusCode)
                    {
                        _logger.LogInformation("Orchestrator POST succeeded. Status: {status}. Response: {resp}", resp2.StatusCode, respText2);
                    }
                    else
                    {
                        _logger.LogWarning("Orchestrator POST returned {status}. Response: {resp}", resp2.StatusCode, respText2);
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error calling orchestrator endpoint.");
                }

                _logger.LogInformation("Orchestrator run finished at {time}.", DateTime.Now);
            }
            catch (TaskCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in orchestrator schedule loop.");
                await Task.Delay(TimeSpan.FromMinutes(1), stoppingToken);
            }
        }

        _logger.LogInformation("ScheduledOrchestratorService stopping.");
    }
}
