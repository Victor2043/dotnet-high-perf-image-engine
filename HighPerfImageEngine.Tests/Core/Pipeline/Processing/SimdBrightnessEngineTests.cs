using FluentAssertions;
using HighPerfImageEngine.Core.Processing;

namespace HighPerfImageEngine.Tests.Core.Processing;

public class SimdBrightnessEngineTests
{
    [Fact]
    public void ApplyBrightnessSimdRgbOnly_ShouldIncreaseRgb_AndPreserveAlphaChannel()
    {
        // Arrange: 1 pixel BGRA8888 (4 bytes)
        // B=50, G=100, R=150, A=255
        byte[] pixelBuffer = [50, 100, 150, 255];
        byte brightnessOffset = 30;

        // Act
        SimdBrightnessEngine.ApplyBrightnessSimdRgbOnly(pixelBuffer, brightnessOffset);

        // Assert
        pixelBuffer[0].Should().Be(80);  // Blue +30
        pixelBuffer[1].Should().Be(130); // Green +30
        pixelBuffer[2].Should().Be(180); // Red +30
        pixelBuffer[3].Should().Be(255); // Alpha
    }

    [Fact]
    public void ApplyBrightnessSimdRgbOnly_ShouldSaturateTo255_WhenOverflowOccurs()
    {
        // Arrange: Values ​​close to 255 to test saturation
        byte[] pixelBuffer = [240, 250, 200, 255];
        byte brightnessOffset = 30;

        // Act
        SimdBrightnessEngine.ApplyBrightnessSimdRgbOnly(pixelBuffer, brightnessOffset);

        // Assert
        pixelBuffer[0].Should().Be(255); // Saturation (240 + 30 = 270 -> 255)
        pixelBuffer[1].Should().Be(255); // Saturation (250 + 30 = 280 -> 255)
        pixelBuffer[2].Should().Be(230); // Normal
        pixelBuffer[3].Should().Be(255); // Alpha
    }       
}