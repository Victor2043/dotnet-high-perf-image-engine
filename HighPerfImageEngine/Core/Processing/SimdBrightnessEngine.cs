using System.Runtime.Intrinsics.X86;
using System.Runtime.Intrinsics;


namespace HighPerfImageEngine.Core.Processing
{
    public static class SimdBrightnessEngine
    {
        public static void ApplyBrightnessSimdRgbOnly(Span<byte> data, byte brightnessOffset)
        {
            int i = 0;

            if (Avx2.IsSupported && data.Length >= Vector256<byte>.Count)
            {
                // 1. Fill mask on the stack (32 bytes)
                Span<byte> maskSpan = stackalloc byte[32];
                for (int b = 0; b < 32; b++)
                {
                    maskSpan[b] = (b % 4 == 3) ? (byte)0 : brightnessOffset;
                }

                // 2. Explicit cast to ReadOnlySpan<byte> prevents compiler ambiguity
                ReadOnlySpan<byte> maskReadOnly = maskSpan;
                Vector256<byte> brightnessVector = Vector256.Create(maskReadOnly);

                int vectorSize = Vector256<byte>.Count;
                int loopLimit = data.Length - (data.Length % vectorSize);

                for (; i < loopLimit; i += vectorSize)
                {
                    ReadOnlySpan<byte> readBlock = data.Slice(i, vectorSize);
                    Vector256<byte> pixels = Vector256.Create(readBlock);

                    Vector256<byte> result = Avx2.AddSaturate(pixels, brightnessVector);

                    Span<byte> writeBlock = data.Slice(i, vectorSize);
                    result.CopyTo(writeBlock);
                }
            }

            // Scalar fallback processing for remaining buffer
            for (; i < data.Length; i++)
            {
                if (i % 4 != 3) // Ignore Alpha channel
                {
                    int sum = data[i] + brightnessOffset;
                    data[i] = sum > 255 ? (byte)255 : (byte)sum;
                }
            }
        }
    }
}
