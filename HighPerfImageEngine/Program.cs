using HighPerfImageEngine.Core.Pipeline;
using HighPerfImageEngine.Core.Ui;

// ============================================================================
// 1. DIRECTORY AND ENVIRONMENT CONFIGURATION
// ============================================================================

ConsoleUiService.RenderBanner();

string baseDir = AppContext.BaseDirectory;

string solutionRoot = Path.GetFullPath(Path.Combine(baseDir, @"..\..\..\..\"));

string inputDir = Path.Combine(solutionRoot, "input_files");
string outputDir = Path.Combine(solutionRoot, "output_files");

Directory.CreateDirectory(inputDir);
Directory.CreateDirectory(outputDir);

var pipelineService = new ImagePipelineService();
pipelineService.ProcessImage(inputDir, outputDir);