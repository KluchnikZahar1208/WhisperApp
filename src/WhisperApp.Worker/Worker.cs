using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Whisper.net;

namespace WhisperApp.Worker;

public class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;
    private readonly IConfiguration _configuration;
    private IConnection? _connection;
    private IChannel? _channel;

    public record AudioSegmentMessage(Guid SessionId, int SectionIndex, int SectionsTotal, string FilePath, string Language, double StartTime, double EndTime);     
    public record SegmentTranscribedEvent(Guid SessionId, int SectionIndex, int SectionsTotal, string Text);

    public Worker(ILogger<Worker> logger, IConfiguration configuration)
    {
        _logger = logger;
        _configuration = configuration;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            var factory = new ConnectionFactory
            {
                HostName = _configuration["RabbitMqSettings:Host"] ?? "localhost",
                AutomaticRecoveryEnabled = true
            };

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    _connection = await factory.CreateConnectionAsync(stoppingToken);
                    _channel = await _connection.CreateChannelAsync(cancellationToken: stoppingToken);
                    break;
                }
                catch (Exception)
                {
                    _logger.LogWarning("RabbitMQ еще не готов. Ожидание 5 секунд...");
                    await Task.Delay(5000, stoppingToken);
                }
            }

            var args = new Dictionary<string, object> { { "x-max-priority", 100 } };
            await _channel.QueueDeclareAsync("audio_segments", true, false, false, args);
            await _channel.QueueDeclareAsync("transcription_results", true, false, false);
            await _channel.BasicQosAsync(0, 1, false);

            var consumer = new AsyncEventingBasicConsumer(_channel);
            consumer.ReceivedAsync += async (model, ea) =>
            {
                try
                {
                    var body = ea.Body.ToArray();
                    var message = JsonSerializer.Deserialize<AudioSegmentMessage>(Encoding.UTF8.GetString(body));

                    if (message != null && File.Exists(message.FilePath))
                    {
                        _logger.LogInformation("[{SessionId}] Processing segment {Index}/{Total}", 
                            message.SessionId, message.SectionIndex + 1, message.SectionsTotal);

                        string text = await RunWhisperCliAsync(message);
                        
                        var result = new SegmentTranscribedEvent(message.SessionId, message.SectionIndex, message.SectionsTotal, text);
                    
                        await _channel.BasicPublishAsync(
                            exchange: string.Empty,
                            routingKey: "transcription_results",
                            body: Encoding.UTF8.GetBytes(JsonSerializer.Serialize(result)));

                        await _channel.BasicAckAsync(ea.DeliveryTag, false);

                        _logger.LogInformation("[{SessionId}] DONE Processing segment {Index}/{Total}", 
                            message.SessionId, message.SectionIndex + 1, message.SectionsTotal);
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error processing segment. Moving back to queue.");
                    await _channel.BasicNackAsync(ea.DeliveryTag, false, true, stoppingToken);
                }
            };

            await _channel.BasicConsumeAsync("audio_segments", false, consumer, stoppingToken);
            await Task.Delay(-1, stoppingToken);
        }
        catch (Exception ex)
        {
            _logger.LogCritical(ex, "Worker failed to start.");
        }
    }

    private async Task<string> RunWhisperCliAsync(AudioSegmentMessage message)
    {
        string filePath = message.FilePath;
        string modelPath = Path.Combine(_configuration["WHISPER_MODELS_PATH"] ?? "/models", "ggml-large-v3-turbo.bin");
        string language = message.Language;

        int threads = _configuration.GetValue<int>("WhisperSettings:Threads", 4);
        
        var args = $"-m \"{modelPath}\" -f \"{filePath}\" -l {language} -nt -t {threads} -bo 2 -bs 2";

        var startInfo = new ProcessStartInfo
        {
            FileName = "whisper",
            Arguments = args,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true
        };

        using var process = new Process { StartInfo = startInfo };
        var output = new StringBuilder();

        process.OutputDataReceived += (s, e) => { if (e.Data != null) output.AppendLine(e.Data); };
        process.Start();
        process.BeginOutputReadLine();
        
        await process.WaitForExitAsync();

        if (process.ExitCode != 0)
        {
            throw new Exception($"Whisper CLI error. Exit code: {process.ExitCode}");
        }

        string transcribedText = output.ToString().Trim();

        try 
        {
            var segmentFileInfo = new FileInfo(filePath);
            var sessionDir = segmentFileInfo.Directory?.Parent?.FullName;

            if (sessionDir != null)
            {
                string transcriptionsDir = Path.Combine(sessionDir, "transcriptions");
                if (!Directory.Exists(transcriptionsDir))
                {
                    Directory.CreateDirectory(transcriptionsDir);
                }

                var jsonData = new 
                {
                    SessionId = message.SessionId,
                    SectionIndex = message.SectionIndex + 1,
                    SectionsTotal = message.SectionsTotal,
                    Text = transcribedText,
                    StartTime = message.StartTime,
                    EndTime = message.EndTime
                };

                var options = new JsonSerializerOptions
                {
                    Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
                    WriteIndented = true
                };

                string jsonString = JsonSerializer.Serialize(jsonData, options);

                string jsonFileName = Path.GetFileNameWithoutExtension(filePath) + ".json";
                string jsonFilePath = Path.Combine(transcriptionsDir, jsonFileName);

                await File.WriteAllTextAsync(jsonFilePath, jsonString, Encoding.UTF8);
                _logger.LogInformation("Saved transcription to: {Path}", jsonFilePath);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to save transcription file for {Path}", filePath);
        }

        return transcribedText;
    }
}
