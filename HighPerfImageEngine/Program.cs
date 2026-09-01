using HighPerfImageEngine.Core.Pipeline;
using Spectre.Console;

// ============================================================================
// 1. DIRECTORY AND ENVIRONMENT CONFIGURATION
// ============================================================================

AnsiConsole.Write(
              new FigletText(".NET High-Perf Engine")
                  .LeftJustified()
                  .Color(Color.Cyan1));

AnsiConsole.MarkupLine("[bold yellow]Starting direct processing engine via SkiaSharp + SIMD...[/]\n");

string baseDir = AppContext.BaseDirectory;

string solutionRoot = Path.GetFullPath(Path.Combine(baseDir, @"..\..\..\..\"));

string inputDir = Path.Combine(solutionRoot, "input_files");
string outputDir = Path.Combine(solutionRoot, "output_files");

Directory.CreateDirectory(inputDir);
Directory.CreateDirectory(outputDir);

var pipelineService = new ImagePipelineService();
pipelineService.ProcessImage(inputDir, outputDir);