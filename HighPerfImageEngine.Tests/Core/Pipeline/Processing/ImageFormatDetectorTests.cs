using FluentAssertions;
using HighPerfImageEngine.Core.Processing;
using HighPerfImageEngine.Domain.Enums;

namespace HighPerfImageEngine.Tests.Core.Processing;

public class ImageFormatDetectorTests
{
    [Fact]
    public void DetectImageFormat_WhenHeaderIsSmallerThan12Bytes_ShouldReturnUnknown()
    {
        // Arrange
        byte[] shortHeader = [0xFF, 0xD8, 0xFF, 0xE0];

        // Act
        var result = ImageFormatDetector.DetectImageFormat(shortHeader);

        // Assert
        result.Should().Be(ImageFormat.Unknown);
    }

    [Fact]
    public void DetectImageFormat_WhenHeaderIsPng_ShouldReturnPng()
    {
        // Arrange (PNG magic numbers followed by padding to reach a total of 12 bytes)
        byte[] pngHeader = [0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, 0x00, 0x00, 0x00, 0x00];

        // Act
        var result = ImageFormatDetector.DetectImageFormat(pngHeader);

        // Assert
        result.Should().Be(ImageFormat.Png);
    }

    [Fact]
    public void DetectImageFormat_WhenHeaderIsJpeg_ShouldReturnJpeg()
    {
        // Arrange
        byte[] jpegHeader = [0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x10, 0x4A, 0x46, 0x49, 0x46, 0x00, 0x01];

        // Act
        var result = ImageFormatDetector.DetectImageFormat(jpegHeader);

        // Assert
        result.Should().Be(ImageFormat.Jpeg);
    }

    [Fact]
    public void DetectImageFormat_WhenHeaderIsWebp_ShouldReturnWebp()
    {
        // Arrange ("RIFF" + 4-byte size + "WEBP")
        byte[] webpHeader = [
            0x52, 0x49, 0x46, 0x46, // RIFF
            0x24, 0x00, 0x00, 0x00, // Size placeholder
            0x57, 0x45, 0x42, 0x50  // WEBP
        ];

        // Act
        var result = ImageFormatDetector.DetectImageFormat(webpHeader);

        // Assert
        result.Should().Be(ImageFormat.Webp);
    }

    [Fact]
    public void DetectImageFormat_WhenHeaderIsInvalid_ShouldReturnUnknown()
    {
        // Arrange
        byte[] corruptHeader = [0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xAA, 0xBB];

        // Act
        var result = ImageFormatDetector.DetectImageFormat(corruptHeader);

        // Assert
        result.Should().Be(ImageFormat.Unknown);
    }
}