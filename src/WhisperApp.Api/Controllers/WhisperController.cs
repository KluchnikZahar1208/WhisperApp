using System.Text;
using System.Text.Json;
using FFMpegCore;
using Microsoft.AspNetCore.Mvc;
using RabbitMQ.Client;

namespace WhisperApp.Api.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class WhisperController : ControllerBase
    {
        /// <summary>
        /// Контроллер для управления процессом транскрибации аудио.
        /// </summary>
        private readonly ILogger<WhisperController> _logger;
        
        private readonly IConfiguration _configuration;
        private readonly IConnection _rabbitConnection;

        //Models
        public record AudioSegmentMessage(Guid SessionId, int SectionIndex, int SectionsTotal, string FilePath);
        public record TranscriptionSegment(Guid SessionId, int SectionIndex, int SectionsTotal, string Text);
        public record UploadResponse(Guid SessionId, string Status);
        public record StatusResponse(Guid SessionId, string Status, ProgressInfo Progress, DateTime UpdatedAt);
        public record ProgressInfo(int Ready, int Total, double Percentage);
        public record AssembleResponse(Guid SessionId, bool IsComplete, int Count, int Total, string FullText);
        ///



        public WhisperController(
            IConfiguration configuration,
            IConnection connection,
            ILogger<WhisperController> logger)
        {
            _configuration = configuration;
            _rabbitConnection = connection;
            _logger = logger;
        }

        /// <summary>
        /// Собирает все готовые текстовые сегменты в единую транскрипцию.
        /// </summary>
        /// <remarks>
        /// Метод можно вызывать до завершения обработки — он вернет текущий собранный текст.
        /// </remarks>
        /// <param name="id">GUID сессии.</param>
        /// <response code="200">Текст успешно собран.</response>
        /// <response code="404">Данные транскрипции не найдены.</response>
        [HttpGet("assemble/{id:guid}")]
        [ProducesResponseType(typeof(AssembleResponse), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        public async Task<IActionResult> AssembleTranscriptionById(Guid id)
        {
            string rootStorage = _configuration["AudioSettings:BaseDirectory"] ?? "/app/temp_audio";
            string sessionDir = Path.Combine(rootStorage, id.ToString());
            string transcriptionsDir = Path.Combine(sessionDir, "transcriptions");

            if (!Directory.Exists(transcriptionsDir))
            {
                return NotFound(new { SessionId = id, Message = "Транскрипции не найдены или еще не созданы." });
            }

            try
            {
                var files = Directory.GetFiles(transcriptionsDir, "*.json");
                var segments = new List<TranscriptionSegment>();
                var jsonOptions = new JsonSerializerOptions { PropertyNameCaseInsensitive = true };

                foreach (var file in files)
                {
                    var content = await System.IO.File.ReadAllTextAsync(file);
                    var segment = JsonSerializer.Deserialize<TranscriptionSegment>(content, jsonOptions);
                    if (segment != null) segments.Add(segment);
                }

                if (!segments.Any()) 
                    return Ok(new { SessionId = id, FullText = "", Status = "Empty" });

                int totalExpected = segments[0].SectionsTotal;
                bool isComplete = segments.Count == totalExpected;

                var orderedSegments = segments.OrderBy(s => s.SectionIndex).ToList();

                StringBuilder finalBuilder = new StringBuilder();
                finalBuilder.Append(orderedSegments[0].Text);

                for (int i = 1; i < orderedSegments.Count; i++)
                {
                    string currentText = orderedSegments[i].Text;
                    string previousText = finalBuilder.ToString();

                    int overlapLen = FindOverlapLength(previousText, currentText);
                    
                    string uniquePart = currentText.Substring(overlapLen).TrimStart();
                    if (!string.IsNullOrEmpty(uniquePart))
                    {
                        finalBuilder.Append(" " + uniquePart);
                    }
                }

                var resultText = finalBuilder.ToString().Trim();

                return Ok(new AssembleResponse(id, isComplete, segments.Count, totalExpected, resultText));
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Ошибка при сборке текста для сессии {Id}", id);
                return StatusCode(500, "Внутренняя ошибка при сборке текста.");
            }
        }

        

        // Вспомогательный метод для поиска наложения текста
        private int FindOverlapLength(string back, string front)
        {
            if (string.IsNullOrEmpty(back) || string.IsNullOrEmpty(front)) return 0;

            // Сравниваем хвост предыдущего текста с началом текущего (до 300 символов)
            int maxSearch = Math.Min(back.Length, Math.Min(front.Length, 300));
            string tail = back.Substring(back.Length - maxSearch);

            for (int len = maxSearch; len > 0; len--)
            {
                string sub = tail.Substring(maxSearch - len);
                if (front.StartsWith(sub, StringComparison.OrdinalIgnoreCase))
                {
                    return len;
                }
            }
            return 0;
        }


        /// <summary>
        /// Получает текущий статус обработки и прогресс по ID сессии.
        /// </summary>
        /// <param name="id">GUID сессии.</param>
        /// <response code="200">Статус успешно получен.</response>
        /// <response code="404">Сессия с таким ID не найдена.</response>
        [HttpGet("status/{id:guid}")]
        [ProducesResponseType(typeof(StatusResponse), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        public IActionResult GetStatusById(Guid id)
        {
            string rootStorage = _configuration["AudioSettings:BaseDirectory"] ?? "/app/temp_audio";
            string sessionDir = Path.Combine(rootStorage, id.ToString());

            if (!Directory.Exists(sessionDir))
            {
                return NotFound(new { SessionId = id, Status = "Not Found", Message = "Сессия не найдена на сервере." });
            }

            string segmentsDir = Path.Combine(sessionDir, "segments");
            string transcriptionsDir = Path.Combine(sessionDir, "transcriptions");

            int totalSegments = Directory.Exists(segmentsDir) 
                ? Directory.GetFiles(segmentsDir, "*.wav").Length 
                : 0;
                
            int readyTranscriptions = Directory.Exists(transcriptionsDir) 
                ? Directory.GetFiles(transcriptionsDir, "*.json").Length 
                : 0;

            string status = "В обработке";
            
            if (totalSegments > 0 && readyTranscriptions >= totalSegments)
            {
                status = "Обработано";
            }
            else if (totalSegments == 0)
            {
                status = "Создано";
            }

            double progressPercent = totalSegments > 0 
                ? Math.Round((double)readyTranscriptions / totalSegments * 100, 2) 
                : 0;

            return Ok(new
            {
                SessionId = id,
                Status = status,
                Progress = new
                {
                    Ready = readyTranscriptions,
                    Total = totalSegments,
                    Percentage = progressPercent
                },
                UpdatedAt = DateTime.UtcNow
            });
        }

        /// <summary>
        /// Загружает аудиофайл и инициирует процесс сегментации.
        /// </summary>
        /// <remarks>
        /// При загрузке создается уникальный SessionId. 
        /// Если в куках был старый ID, данные этой сессии будут удалены.
        /// </remarks>
        /// <param name="file">Аудиофайл для обработки.</param>
        /// <response code="200">Файл успешно загружен, сегментация начата.</response>
        /// <response code="400">Файл не передан или имеет нулевой размер.</response>
        /// <response code="500">Внутренняя ошибка сервера или сбой FFmpeg.</response>
        [HttpPost("upload")]
        [ProducesResponseType(typeof(UploadResponse), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public async Task<IActionResult> UploadAudio(IFormFile? file)
        {
            if (file == null || file.Length == 0)
                return BadRequest("Файл не передан или имеет нулкевой размер.");
            
            string rootStorage = _configuration["AudioSettings:BaseDirectory"] ?? "/app/temp_audio";
            Guid sessionId = Guid.NewGuid();
            
            string sessionDir = Path.Combine(rootStorage, sessionId.ToString());
            string segmentsDir = Path.Combine(sessionDir, "segments");
            string transcriptionsDir = Path.Combine(sessionDir, "transcriptions");
            string sourceFilePath = Path.Combine(sessionDir, $"original{Path.GetExtension(file.FileName)}");

            try 
            {
                Directory.CreateDirectory(segmentsDir);
                Directory.CreateDirectory(transcriptionsDir);

                using (var stream = new FileStream(sourceFilePath, FileMode.Create))
                {
                    await file.CopyToAsync(stream);
                }
                
                await ProcessAndPublishSegments(sourceFilePath, segmentsDir, sessionId);

                return Ok(new 
                { 
                    SessionId = sessionId, 
                    Status = "Audio uploaded and segmented",
                    SegmentsPath = segmentsDir
                });
            }
            catch (Exception ex)
            {
                return StatusCode(500, $"Internal server error: {ex.Message}");
            }
        }
        private async Task ProcessAndPublishSegments(string inputPath, string outputFolder, Guid sessionId)
        {
            int segmentDuration = _configuration.GetValue<int>("AudioSettings:SegmentDuration", 240);
            int overlap = _configuration.GetValue<int>("AudioSettings:SegmentOverlap", 2);
            int step = segmentDuration - overlap;

            var analysis = await FFProbe.AnalyseAsync(inputPath);
            double totalDuration = analysis.Duration.TotalSeconds;

            int sectionsTotal = 0;
            for (double s = 0; s < totalDuration; s += step)
            {
                sectionsTotal++;
                if (s + segmentDuration >= totalDuration) break;
            }

            using var channel = await _rabbitConnection.CreateChannelAsync();
            
            var args = new Dictionary<string, object>
            {
                { "x-max-priority", 100 }
            };

            await channel.QueueDeclareAsync(
                queue: "audio_segments", 
                durable: true, 
                exclusive: false, 
                autoDelete: false,
                arguments: args);
            
            int index = 0;
            for (double start = 0; start < totalDuration; start += step)
            {
                string segmentFileName = $"seg_{index:D3}.wav";
                string segmentPath = Path.Combine(outputFolder, segmentFileName);

                await FFMpegArguments
                    .FromFileInput(inputPath)
                    .OutputToFile(segmentPath, overwrite: true, options => options
                        .Seek(TimeSpan.FromSeconds(start))
                        .WithCustomArgument($"-t {segmentDuration}")
                        .WithCustomArgument("-acodec pcm_s16le -ar 16000 -ac 1"))
                    .ProcessAsynchronously();

                byte priority = (byte)Math.Max(1, 100 - index);

                var properties = new BasicProperties
                {
                    Persistent = true,
                    Priority = priority
                };

                var message = new AudioSegmentMessage(sessionId, index, sectionsTotal, segmentPath);
                var body = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(message));

                await channel.BasicPublishAsync(
                    exchange: string.Empty,
                    routingKey: "audio_segments",
                    mandatory: true,
                    basicProperties: properties,
                    body: body);

                index++;
                if (start + segmentDuration >= totalDuration) break;
            }
        }
    }
}