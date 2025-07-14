"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.activate = void 0;
const vscode = require("vscode");
const pysparkAnalyzer_1 = require("./pysparkAnalyzer");
const errorTranslator_1 = require("./errorTranslator");
const sparkUIIntegrator_1 = require("./sparkUIIntegrator");
const memoryManager_1 = require("./memoryManager");
const dashboardPanel_1 = require("./webviews/dashboardPanel");
function activate(context) {
    console.log('PySpark Assistant is now active');
    // Initialize core components
    const pysparkAnalyzer = new pysparkAnalyzer_1.PySparkAnalyzer();
    const errorTranslator = new errorTranslator_1.ErrorTranslator();
    const sparkUIIntegrator = new sparkUIIntegrator_1.SparkUIIntegrator();
    const memoryManager = new memoryManager_1.MemoryManager();
    // Set up analyzer dependencies
    pysparkAnalyzer.setErrorTranslator(errorTranslator);
    // Register the analyzer for Python files
    context.subscriptions.push(vscode.workspace.onDidChangeTextDocument(event => {
        if (event.document.languageId === 'python') {
            pysparkAnalyzer.analyzeChanges(event);
        }
    }));
    // Analyze all open Python files
    vscode.workspace.textDocuments.forEach(document => {
        if (document.languageId === 'python') {
            pysparkAnalyzer.analyzeDocument(document);
        }
    });
    // Create dashboard panel
    let dashboardPanel;
    // Register commands
    const openDashboard = vscode.commands.registerCommand('pyspark-assistant.openDashboard', () => {
        if (dashboardPanel) {
            dashboardPanel.reveal();
        }
        else {
            dashboardPanel_1.DashboardPanel.createOrShow(context.extensionUri);
        }
    });
    const analyzeCurrentFile = vscode.commands.registerCommand('pyspark-assistant.analyzeCurrentFile', async () => {
        const editor = vscode.window.activeTextEditor;
        if (editor) {
            const document = editor.document;
            const issues = await pysparkAnalyzer.analyzeDocument(document);
            if (issues.length > 0) {
                vscode.window.showInformationMessage(`Found ${issues.length} potential PySpark issue(s). See Problems panel.`);
            }
            else {
                vscode.window.showInformationMessage('No PySpark issues found.');
            }
        }
    });
    const translateError = vscode.commands.registerCommand('pyspark-assistant.translateError', async () => {
        const errorText = await vscode.window.showInputBox({
            prompt: 'Paste Spark error message',
            placeHolder: 'Error message to translate'
        });
        if (errorText) {
            const translation = errorTranslator.translateError(errorText);
            vscode.window.showInformationMessage(`Translated error: ${translation.summary}`);
            // Show detailed explanation in a separate webview
            const panel = vscode.window.createWebviewPanel('errorTranslation', 'Error Translation', vscode.ViewColumn.Beside, {});
            panel.webview.html = getErrorTranslationHtml(translation);
        }
    });
    const connectToSparkUI = vscode.commands.registerCommand('pyspark-assistant.connectToSparkUI', async () => {
        const sparkUIUrl = await vscode.window.showInputBox({
            prompt: 'Enter Spark UI URL',
            placeHolder: 'http://localhost:4040'
        });
        if (sparkUIUrl) {
            try {
                await sparkUIIntegrator.connect(sparkUIUrl);
                vscode.window.showInformationMessage(`Connected to Spark UI at ${sparkUIUrl}`);
                if (dashboardPanel) {
                    dashboardPanel.updateSparkUIStatus(true, sparkUIUrl);
                }
            }
            catch (error) {
                vscode.window.showErrorMessage(`Failed to connect to Spark UI: ${error.message}`);
            }
        }
    });
    // Register all commands
    context.subscriptions.push(openDashboard, analyzeCurrentFile, translateError, connectToSparkUI);
}
exports.activate = activate;
function getErrorTranslationHtml(translation) {
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
                    color: var(--vscode-editor-foreground);
                    padding: 20px;
                }
                .error-card {
                    background-color: var(--vscode-editor-background);
                    border-radius: 6px;
                    padding: 16px;
                    margin-bottom: 20px;
                }
                h2 {
                    margin-top: 0;
                    border-bottom: 1px solid var(--vscode-panel-border);
                    padding-bottom: 8px;
                }
            </style>
        </head>
        <body>
            <div class="error-card">
                <h2>Error Summary</h2>
                <p>${translation.summary}</p>
            </div>
            <div class="error-card">
                <h2>Detailed Explanation</h2>
                <p>${translation.explanation}</p>
            </div>
            <div class="error-card">
                <h2>Suggested Solution</h2>
                <p>${translation.solution}</p>
            </div>
        </body>
        </html>
    `;
}
//# sourceMappingURL=extension.js.map