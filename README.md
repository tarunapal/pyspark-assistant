/************ README.md ************/

# PySpark Assistant

A VS Code extension that provides real-time feedback and optimization suggestions for PySpark development.

## Features

### 1. Real-time Code Analysis
- Detects common anti-patterns in PySpark code
- Suggests optimizations for DataFrame operations
- Flags potential memory issues before execution
- Identifies data skew and inefficient operations

### 2. Error Translation
- Converts cryptic Spark errors into clear, actionable advice
- Provides fix suggestions with code examples
- Links errors to relevant documentation
- Makes troubleshooting faster and more accessible

### 3. Spark UI Integration
- Simplified view of key Spark UI metrics within VS Code
- Intelligent navigation to relevant Spark UI tabs based on errors
- Performance visualization of job stages
- Quick access to executor metrics

### 4. Memory Management Assistant
- Monitors memory usage during notebook execution
- Alerts when approaching cluster limits
- Suggests partition/cache strategy improvements
- Provides recommendations to avoid OOM errors

## Installation

1. Open VS Code
2. Go to Extensions (Ctrl+Shift+X)
3. Search for "PySpark Assistant"
4. Click Install

## Usage

### Dashboard
Access the main dashboard by:
- Clicking the "PySpark" icon in the status bar
- Running the "PySpark Assistant: Open Dashboard" command from the command palette

### Code Analysis
The extension automatically analyzes your PySpark code in real-time. You can also:
- Right-click in the editor and select "PySpark Assistant: Analyze Current File"
- Use the "Analyze Current File" button on the dashboard

### Error Translation
- Copy a Spark error message
- Right-click and select "PySpark Assistant: Translate Error Message"
- Or use the "Translate Error Message" button on the dashboard
- Paste the error when prompted

### Spark UI Integration
- Use the "Connect to Spark UI" button on the dashboard
- Enter your Spark UI URL (default: http://localhost:4040)
- Once connected, you can navigate and view metrics directly in VS Code

## Requirements

- VS Code 1.60.0 or higher
- Python extension for VS Code
- PySpark environment

## Extension Settings

This extension contributes the following settings:

* `pysparkAssistant.enableRealTimeAnalysis`: Enable/disable real-time analysis of PySpark code
* `pysparkAssistant.sparkUIDefaultUrl`: Default URL for connecting to Spark UI
* `pysparkAssistant.memoryAlertThreshold`: Memory usage threshold for alerts (0.0-1.0)

## Known Issues

- Spark UI integration requires an active Spark session
- Some complex PySpark patterns may not be detected
- Extension does not modify code automatically

## Release Notes

### 0.1.0

- Initial release with core functionality
- Real-time code analysis
- Error translation
- Basic Spark UI integration
- Memory management recommendations

## Feedback & Contributions

Issues and pull requests are welcome on our [GitHub repository](https://github.com/yourorg/pyspark-assistant).

## License

MIT (err) {
                vscode.window.showErrorMessage(`Failed to connect to Spark UI: ${err}`);
            }
        }
    });
    
    // Register document change listeners
    const documentChangeListener = vscode.workspace.onDidChangeTextDocument(event => {
        if (/\.py$/.test(event.document.fileName) || 
            /\.ipynb$/.test(event.document.fileName)) {
            pysparkAnalyzer.analyzeChanges(event);
        }
    });
    
    // Jupyter cell execution listener
    const jupyterExecutionListener = vscode.notebooks.onDidChangeNotebookCells(event => {
        // Monitor notebook cell executions to track memory usage
        memoryManager.trackCellExecution(event);
    });
    
    // Register diagnostics collection
    const diagnosticCollection = vscode.languages.createDiagnosticCollection('pyspark-assistant');
    context.subscriptions.push(diagnosticCollection);
    
    // Register all disposables
    context.subscriptions.push(
        openDashboard,
        analyzeCurrentFile,
        translateError,
        connectToSparkUI,
        documentChangeListener,
        jupyterExecutionListener
    );
    
    // Initialize status bar
    const statusBar = vscode.window.createStatusBarItem(vscode.StatusBarAlignment.Right, 100);
    statusBar.text = "$(spark) PySpark";
    statusBar.tooltip = "PySpark Assistant";
    statusBar.command = 'pyspark-assistant.openDashboard';
    statusBar.show();
    context.subscriptions.push(statusBar);
    
    // Provide the pysparkAnalyzer reference to the error translator and memory manager
    pysparkAnalyzer.setErrorTranslator(errorTranslator);
    memoryManager.setPySparkAnalyzer(pysparkAnalyzer);
    
    // Automatically activate for Python/Jupyter files
    vscode.workspace.textDocuments.forEach(document => {
        if (/\.py$/.test(document.fileName) || /\.ipynb$/.test(document.fileName)) {
            pysparkAnalyzer.analyzeDocument(document);
        }
    });
}

export function deactivate() {
    // Clean up resources when extension is deactivated
}

function getErrorTranslationHtml(translation: any): string {
    return `
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Error Translation</title>
            <style>
                body {
                    font-family: var(--vscode-font-family);
                    padding: 20px;
                }
                .error-summary {
                    background-color: var(--vscode-editor-background);
                    border-left: 4px solid var(--vscode-errorForeground);
                    padding: 10px;
                    margin-bottom: 20px;
                }
                .explanation {
                    margin-bottom: 20px;
                }
                .solution {
                    background-color: var(--vscode-editor-background);
                    border-left: 4px solid var(--vscode-statusBarItem-prominentBackground);
                    padding: 10px;
                }
                h2 {
                    font-size: 1.2em;
                    margin-top: 1.5em;
                }
                pre {
                    background-color: var(--vscode-editor-background);
                    padding: 10px;
                    overflow: auto;
                }
            </style>
        </head>
        <body>
            <h1>Error Translation</h1>
            
            <h2>Original Error</h2>
            <pre>${translation.originalError}</pre>
            
            <h2>Summary</h2>
            <div class="error-summary">${translation.summary}</div>
            
            <h2>Detailed Explanation</h2>
            <div class="explanation">${translation.explanation}</div>
            
            <h2>Recommended Solutions</h2>
            <div class="solution">
                <p>${translation.solution}</p>
                ${translation.codeExample ? `<pre>${translation.codeExample}</pre>` : ''}
            </div>
            
            <h2>Related Documentation</h2>
            <ul>
                ${translation.links.map((link: { title: string, url: string }) => 
                    `<li><a href="${link.url}">${link.title}</a></li>`).join('')}
            </ul>
        </body>
        </html>
    `;
}