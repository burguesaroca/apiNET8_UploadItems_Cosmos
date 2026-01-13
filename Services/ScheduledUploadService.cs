using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace UploadItemsCosmos.Services;

public class ScheduledUploadService : BackgroundService
{
    private readonly ILogger<ScheduledUploadService> _logger;
    private readonly IConfiguration _configuration;
    private readonly UploadRunner _runner;

    public ScheduledUploadService(ILogger<ScheduledUploadService> logger, IConfiguration configuration, UploadRunner runner)
    {
        _logger = logger;
        _configuration = configuration;
        _runner = runner;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("ScheduledUploadService started.");

        // Read schedule from configuration (format: HH:mm). Default to 01:00 if missing.
        var dailyTime = _configuration["SchedulerUploadConnections:DailyTime"] ?? "01:00";

        if (!TimeSpan.TryParse(dailyTime, out var scheduledTime))
        {
            _logger.LogWarning("Invalid SchedulerUploadConnections:DailyTime value '{time}', using 01:00.", dailyTime);
            scheduledTime = TimeSpan.FromHours(1);
        }

        // Read timezone. Default to America/Bogota
        var tzId = _configuration["SchedulerUploadConnections:TimeZone"] ?? "America/Bogota";
        TimeZoneInfo tzInfo;
        try
        {
            if (string.Equals(tzId, "Local", StringComparison.OrdinalIgnoreCase))
            {
                tzInfo = TimeZoneInfo.Local;
            }
            else
            {
                tzInfo = TimeZoneInfo.FindSystemTimeZoneById(tzId);
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Could not resolve timezone '{tzId}', falling back to Local.", tzId);
            tzInfo = TimeZoneInfo.Local;
        }

        _logger.LogInformation("SchedulerUploadConnections — daily at {time} ({tz}).", dailyTime, tzInfo.Id);

        // The service will only run at the configured daily time in Scheduler:DailyTime.

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                // Compute next occurrence in the configured timezone
                var nowUtc = DateTime.UtcNow;
                var nowInTz = TimeZoneInfo.ConvertTimeFromUtc(nowUtc, tzInfo);

                var candidate = new DateTime(nowInTz.Year, nowInTz.Month, nowInTz.Day, scheduledTime.Hours, scheduledTime.Minutes, 0);
                if (candidate <= nowInTz) candidate = candidate.AddDays(1);

                var nextUtc = TimeZoneInfo.ConvertTimeToUtc(candidate, tzInfo);
                var delay = nextUtc - nowUtc;

                var nextLocalDisplay = TimeZoneInfo.ConvertTimeFromUtc(nextUtc, TimeZoneInfo.Local);
                _logger.LogInformation("Next upload scheduled at {nextLocal} (local) / {nextTz} ({tz}) — in {delay}.", nextLocalDisplay, candidate, tzInfo.Id, delay);

                await Task.Delay(delay, stoppingToken);

                if (stoppingToken.IsCancellationRequested) break;

                _logger.LogInformation("Starting scheduled upload at {time}.", DateTime.Now);
                await _runner.RunOnceAsync(stoppingToken);
                _logger.LogInformation("Scheduled upload finished at {time}.", DateTime.Now);
            }
            catch (TaskCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                // shut down
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error during scheduled upload run.");
                // Wait a short interval before retrying schedule loop to avoid tight failure loops
                await Task.Delay(TimeSpan.FromMinutes(1), stoppingToken);
            }
        }

        _logger.LogInformation("ScheduledUploadService stopping.");
    }
}
