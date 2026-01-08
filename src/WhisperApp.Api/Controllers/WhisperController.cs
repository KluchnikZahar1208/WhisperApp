using System.Runtime.Serialization;
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
        public record AudioSegmentMessage(Guid SessionId, int SectionIndex, int SectionsTotal, string FilePath, string Language, double StartTime, double EndTime);
        public record TranscriptionSegment(
            Guid SessionId, 
            int SectionIndex, 
            int SectionsTotal, 
            string Text, // Оставляем для совместимости, если нужно
            List<TranscriptionItem>? Phrases = null // Добавляем список фраз с таймингами
        );
        public record UploadResponse(Guid SessionId, string Status);
        public record StatusResponse(Guid SessionId, string Status, ProgressInfo Progress, DateTime UpdatedAt);
        public record ProgressInfo(int Ready, int Total, double Percentage);
        public record AssembleResponse(Guid SessionId, bool IsComplete, int Count, int Total, string FullText);

        public record TranscriptionResultResponse(
            Guid SessionId,
            bool IsComplete,
            int Count,
            int Total,
            List<TranscriptionItem> Segments
        );

        public record TranscriptionItem(
            double Start,
            double End,
            string Text
        );
        public enum WhisperLanguage
        {
            [EnumMember(Value = "auto")]
            Auto,
            [EnumMember(Value = "ru")]
            Russian,
            [EnumMember(Value = "en")]
            English,
            [EnumMember(Value = "de")]
            German,
            [EnumMember(Value = "fr")]
            French,
            [EnumMember(Value = "es")]
            Spanish,
            [EnumMember(Value = "it")]
            Italian,
            [EnumMember(Value = "zh")]
            Chinese
        }
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
        
        [HttpGet("transcription/{sessionId:guid}")]
        public async Task<IActionResult> GetTranscriptionSegments(Guid sessionId)
        {
            string rootStorage = _configuration["AudioSettings:BaseDirectory"] ?? "/app/temp_audio";
            string sessionDir = Path.Combine(rootStorage, sessionId.ToString());
            string transcriptionsDir = Path.Combine(sessionDir, "transcriptions");

            if (!Directory.Exists(transcriptionsDir))
                return NotFound(new { Message = "Данные сессии не найдены." });

            // Считаем количество аудио-файлов для определения общего прогресса
            string segmentsDir = Path.Combine(sessionDir, "segments");
            int totalSegments = Directory.Exists(segmentsDir) 
                ? Directory.GetFiles(segmentsDir, "*.wav").Length 
                : 0;
            
            var jsonFiles = Directory.GetFiles(transcriptionsDir, "*.json");
            var allItems = new List<TranscriptionItem>();

            var jsonOptions = new JsonSerializerOptions { PropertyNameCaseInsensitive = true };

            foreach (var file in jsonFiles)
            {
                try
                {
                    // Используем System.IO.File, чтобы избежать конфликта с ControllerBase.File
                    var content = await System.IO.File.ReadAllTextAsync(file);
                    var segmentData = JsonSerializer.Deserialize<TranscriptionSegmentData>(content, jsonOptions);

                    if (segmentData != null)
                    {
                        // ЛОГИКА: если в JSON есть массив Phrases (детальные таймкоды) — берем их.
                        // Если нет — берем всё поле Text как один большой айтем.
                        if (segmentData.Phrases != null && segmentData.Phrases.Any())
                        {
                            allItems.AddRange(segmentData.Phrases);
                        }
                        else if (!string.IsNullOrWhiteSpace(segmentData.Text))
                        {
                            allItems.Add(new TranscriptionItem(
                                segmentData.StartTime, 
                                segmentData.EndTime, 
                                segmentData.Text
                            ));
                        }
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Ошибка при чтении сегмента из файла: {File}", file);
                }
            }

            // Удаляем дубликаты (актуально при использовании overlap) и сортируем по времени начала
            var resultSegments = allItems
                .GroupBy(p => new { p.Start, p.Text })
                .Select(g => g.First())
                .OrderBy(p => p.Start)
                .ToList();

            return Ok(new TranscriptionResultResponse(
                SessionId: sessionId,
                IsComplete: jsonFiles.Length >= totalSegments && totalSegments > 0,
                Count: jsonFiles.Length,
                Total: totalSegments,
                Segments: resultSegments
            ));
        }

        #region Вспомогательные модели данных

        // Эта модель должна в точности соответствовать вашему JSON на диске
        public record TranscriptionSegmentData(
            Guid SessionId,
            int SectionIndex,
            int SectionsTotal,
            string Text,
            double StartTime, // Начало из вашего JSON
            double EndTime,   // Конец из вашего JSON
            List<TranscriptionItem>? Phrases // На случай, если воркер начнет слать детали
        );

        #endregion
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
        /// <param name="language">Выберите язык транскрибации из списка.</param>
        /// <response code="200">Файл успешно загружен, сегментация начата.</response>
        /// <response code="400">Файл не передан или имеет нулевой размер.</response>
        /// <response code="500">Внутренняя ошибка сервера или сбой FFmpeg.</response>
        [HttpPost("upload")]
        [ProducesResponseType(typeof(UploadResponse), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public async Task<IActionResult> UploadAudio(
            IFormFile? file, 
            WhisperLanguage language = WhisperLanguage.Auto
        )
        {
            if (file == null || file.Length == 0)
                return BadRequest("Файл не передан или имеет нулкевой размер.");
            
            // Превращаем Enum в строку-код для RabbitMQ (например, "ru")
            string languageCode = language.ToString().ToLower(); 
            // Если нужно именно значение из EnumMember ("auto"), можно использовать метод расширения или switch
            languageCode = language switch {
                WhisperLanguage.Auto => "auto",
                WhisperLanguage.Russian => "ru",
                WhisperLanguage.English => "en",
                _ => language.ToString().ToLower().Substring(0, 2)
            };
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
                
                await ProcessAndPublishSegments(sourceFilePath, segmentsDir, sessionId, languageCode);

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
        private async Task ProcessAndPublishSegments(string inputPath, string outputFolder, Guid sessionId, string language)
        {
            int segmentDuration = _configuration.GetValue<int>("AudioSettings:SegmentDuration", 240);
            int overlap = _configuration.GetValue<int>("AudioSettings:SegmentOverlap", 2);
            int step = segmentDuration - overlap;

            var analysis = await FFProbe.AnalyseAsync(inputPath);
            double totalDuration = analysis.Duration.TotalSeconds;

            // Считаем общее количество секций заранее
            int sectionsTotal = (int)Math.Ceiling(totalDuration / step);

            using var channel = await _rabbitConnection.CreateChannelAsync();
            
            var args = new Dictionary<string, object> { { "x-max-priority", 100 } };
            await channel.QueueDeclareAsync("audio_segments", true, false, false, args);
            
            int index = 0;
            for (double start = 0; start < totalDuration; start += step)
            {
                // Вычисляем реальный конец сегмента (чтобы не выйти за пределы файла)
                double currentSegmentEnd = Math.Min(start + segmentDuration, totalDuration);
                double actualDuration = currentSegmentEnd - start;

                string segmentFileName = $"seg_{index:D3}.wav";
                string segmentPath = Path.Combine(outputFolder, segmentFileName);

                // Нарезка через FFmpeg
                await FFMpegArguments
                    .FromFileInput(inputPath)
                    .OutputToFile(segmentPath, overwrite: true, options => options
                        .Seek(TimeSpan.FromSeconds(start))
                        .WithCustomArgument($"-t {actualDuration}") // Используем вычисленную длительность
                        .WithCustomArgument("-acodec pcm_s16le -ar 16000 -ac 1"))
                    .ProcessAsynchronously();

                byte priority = (byte)Math.Max(1, 100 - index);
                var properties = new BasicProperties { Persistent = true, Priority = priority };

                // Включаем StartTime и EndTime в сообщение
                var message = new AudioSegmentMessage(
                    sessionId, 
                    index, 
                    sectionsTotal, 
                    segmentPath, 
                    language,
                    Math.Round(start, 2), 
                    Math.Round(currentSegmentEnd, 2)
                );

                var body = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(message));

                await channel.BasicPublishAsync(
                    exchange: string.Empty,
                    routingKey: "audio_segments",
                    mandatory: true,
                    basicProperties: properties,
                    body: body);

                _logger.LogInformation("Сегмент {Index} (с {Start} по {End} сек) отправлен в очередь", 
                    index, message.StartTime, message.EndTime);

                index++;
                if (start + segmentDuration >= totalDuration) break;
            }
        }
    }
}