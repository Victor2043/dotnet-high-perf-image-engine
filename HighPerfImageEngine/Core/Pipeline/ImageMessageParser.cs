using System.Buffers;
using System.Buffers.Text;
using System.Text.Json;

namespace HighPerfImageEngine.Core.Pipeline;

/// <summary>
/// Owns a pooled byte[] holding the decoded image bytes for one message.
/// MUST be disposed (return the buffer to the pool) once processing is done —
/// use it in a `using` block.
/// </summary>
public readonly struct ParsedImageMessage : IDisposable
{
    private readonly byte[] _rentedBuffer;

    public string FileName { get; }
    public byte BrightnessOffset { get; }
    public int Length { get; }

    public ParsedImageMessage(string fileName, byte brightnessOffset, byte[] rentedBuffer, int length)
    {
        FileName = fileName;
        BrightnessOffset = brightnessOffset;
        _rentedBuffer = rentedBuffer;
        Length = length;
    }

    public ReadOnlySpan<byte> ImageBytes => _rentedBuffer.AsSpan(0, Length);

    public void Dispose()
    {
        if (_rentedBuffer is { Length: > 0 })
        {
            ArrayPool<byte>.Shared.Return(_rentedBuffer);
        }
    }
}

/// <summary>
/// Parses the RabbitMQ message payload without going through
/// JsonSerializer.Deserialize&lt;ImageMessagePayload&gt; + Convert.FromBase64String.
///
/// That combo used to allocate, per message:
///   1) a managed UTF-16 System.String holding the (huge) Base64 text
///   2) a managed byte[] holding the decoded image bytes
///
/// Both routinely exceed the 85,000-byte LOH threshold for realistic image
/// sizes, which is why Gen2 collections were tracking Gen0 collections almost
/// 1:1 in the original benchmark (worst case for the GC: large, short-lived
/// objects promoted straight to the most expensive generation).
///
/// This version reads the raw UTF-8 bytes of "content_base64" directly off
/// the wire via Utf8JsonReader.ValueSpan and decodes them straight into a
/// pooled buffer with System.Buffers.Text.Base64 — no intermediate string,
/// no intermediate byte[].
/// </summary>
public static class ImageMessageParser
{
    public static bool TryParse(ReadOnlySpan<byte> jsonUtf8, out ParsedImageMessage message)
    {
        message = default;

        string? fileName = null;
        byte brightnessOffset = 0;
        byte[]? rented = null;
        int decodedLength = 0;

        var reader = new Utf8JsonReader(jsonUtf8);

        try
        {
            while (reader.Read())
            {
                if (reader.TokenType != JsonTokenType.PropertyName)
                {
                    continue;
                }

                if (reader.ValueTextEquals("file_name"))
                {
                    reader.Read();
                    fileName = reader.GetString();
                }
                else if (reader.ValueTextEquals("brightness_offset"))
                {
                    reader.Read();
                    brightnessOffset = reader.GetByte();
                }
                else if (reader.ValueTextEquals("content_base64"))
                {
                    reader.Read();

                    // Reader was built over a single contiguous span (not a
                    // ReadOnlySequence), so ValueSpan is always safe here.
                    ReadOnlySpan<byte> base64Utf8 = reader.ValueSpan;

                    int maxLength = Base64.GetMaxDecodedFromUtf8Length(base64Utf8.Length);
                    rented = ArrayPool<byte>.Shared.Rent(maxLength);

                    OperationStatus status = Base64.DecodeFromUtf8(
                        base64Utf8,
                        rented,
                        out _,
                        out decodedLength);

                    if (status != OperationStatus.Done)
                    {
                        ArrayPool<byte>.Shared.Return(rented);
                        rented = null;
                        return false;
                    }
                }
            }
        }
        catch (JsonException)
        {
            if (rented != null)
            {
                ArrayPool<byte>.Shared.Return(rented);
            }

            return false;
        }

        if (fileName == null || rented == null)
        {
            if (rented != null)
            {
                ArrayPool<byte>.Shared.Return(rented);
            }

            return false;
        }

        message = new ParsedImageMessage(fileName, brightnessOffset, rented, decodedLength);
        return true;
    }
}