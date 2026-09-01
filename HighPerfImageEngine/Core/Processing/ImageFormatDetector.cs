using HighPerfImageEngine.Domain.Enums;

namespace HighPerfImageEngine.Core.Processing
{
    public static class ImageFormatDetector
    {
        public static ImageFormat DetectImageFormat(ReadOnlySpan<byte> header)
        {
            if (header.Length < 12) return ImageFormat.Unknown;

            ReadOnlySpan<byte> jpeg = [0xFF, 0xD8, 0xFF];
            ReadOnlySpan<byte> png = [0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A];
            ReadOnlySpan<byte> riff = [0x52, 0x49, 0x46, 0x46];
            ReadOnlySpan<byte> webp = [0x57, 0x45, 0x42, 0x50];

            if (header.StartsWith(png)) return ImageFormat.Png;
            if (header.StartsWith(jpeg)) return ImageFormat.Jpeg;
            if (header.Slice(0, 4).SequenceEqual(riff) && header.Slice(8, 4).SequenceEqual(webp)) return ImageFormat.Webp;

            return ImageFormat.Unknown;
        }
    }
}
