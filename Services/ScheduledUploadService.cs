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

        // Read schedule from configuration (format: HH:mm). Default to 02:00 if missing.
        var dailyTime = _configuration["Scheduler:DailyTime"] ?? "02:00";

        if (!TimeSpan.TryParse(dailyTime, out var scheduledTime))
        {
            _logger.LogWarning("Invalid Scheduler:DailyTime value '{time}', using 02:00.", dailyTime);
            scheduledTime = TimeSpan.FromHours(2);
        }

        // The service will only run at the configured daily time in Scheduler:DailyTime.

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var now = DateTime.Now;
                var next = new DateTime(now.Year, now.Month, now.Day, scheduledTime.Hours, scheduledTime.Minutes, 0);
                if (next <= now) next = next.AddDays(1);

                var delay = next - now;
                _logger.LogInformation("Next upload scheduled at {next} (in {delay}).", next, delay);

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
