import * as vscode from 'vscode';
import { PySparkAnalyzer } from './pysparkAnalyzer';
import { ErrorTranslator } from './errorTranslator';
import { SparkUIIntegrator } from './sparkUIIntegrator';
import { MemoryManager } from './memoryManager';
import { DashboardPanel } from './webviews/dashboardPanel';
import { PySparkCodeActionProvider } from './codeActionProvider';

export function activate(context: vscode.ExtensionContext) {
    console.log('PySpark Assistant is now active');

    // Initialize core components
    const pysparkAnalyzer = new PySparkAnalyzer();
    const errorTranslator = new ErrorTranslator();
    const sparkUIIntegrator = new SparkUIIntegrator();
    const memoryManager = new MemoryManager();
    
    // Set up analyzer dependencies
    pysparkAnalyzer.setErrorTranslator(errorTranslator);
    
    // Register the analyzer for Python files
    context.subscriptions.push(
        vscode.workspace.onDidChangeTextDocument(event => {
            if (event.document.languageId === 'python') {
                pysparkAnalyzer.analyzeChanges(event);
            }
        })
    );
    
    // Analyze all open Python files
    vscode.workspace.textDocuments.forEach(document => {
        if (document.languageId === 'python') {
            pysparkAnalyzer.analyzeDocument(document);
        }
    });

    // Create dashboard panel
    let dashboardPanel: DashboardPanel | undefined;
    
    // Register commands
    const openDashboard = vscode.commands.registerCommand('pyspark-assistant.openDashboard', () => {
        if (dashboardPanel) {
            dashboardPanel.reveal();
        } else {
            DashboardPanel.createOrShow(context.extensionUri);
        }
    });
    
    const analyzeCurrentFile = vscode.commands.registerCommand('pyspark-assistant.analyzeCurrentFile', async () => {
        const editor = vscode.window.activeTextEditor;
        if (editor) {
            const document = editor.document;
            const issues = await pysparkAnalyzer.analyzeDocument(document);
            
            if (issues.length > 0) {
                vscode.window.showInformationMessage(`Found ${issues.length} potential PySpark issue(s). See Problems panel.`);
            } else {
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
            const panel = vscode.window.createWebviewPanel(
                'errorTranslation',
                'Error Translation',
                vscode.ViewColumn.Beside,
                {}
            );
            
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
            } catch (error: any) {
                vscode.window.showErrorMessage(`Failed to connect to Spark UI: ${error.message}`);
            }
        }
    });

    // Register all commands
    context.subscriptions.push(
        openDashboard,
        analyzeCurrentFile,
        translateError,
        connectToSparkUI
    );

    context.subscriptions.push(
        vscode.languages.registerCodeActionsProvider('python', new PySparkCodeActionProvider(), {
            providedCodeActionKinds: PySparkCodeActionProvider.providedCodeActionKinds
        })
    );
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
