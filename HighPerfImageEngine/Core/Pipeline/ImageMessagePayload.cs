using System.Text.Json.Serialization;

namespace HighPerfImageEngine.Core.Pipeline;

public record ImageMessagePayload(
    [property: JsonPropertyName("file_name")] string FileName,
    [property: JsonPropertyName("content_base64")] string ContentBase64,
    [property: JsonPropertyName("brightness_offset")] byte BrightnessOffset
);