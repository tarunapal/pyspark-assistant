/************ webviews/dashboardPanel.ts ************/

import * as vscode from 'vscode';
import * as path from 'path';

export class DashboardPanel {
    public static currentPanel: DashboardPanel | undefined;
    
    private readonly _panel: vscode.WebviewPanel;
    private readonly _extensionUri: vscode.Uri;
    private _disposables: vscode.Disposable[] = [];
    
    private _sparkUIConnected: boolean = false;
    private _sparkUIUrl: string | undefined;
    
    public static createOrShow(extensionUri: vscode.Uri) {
        const column = vscode.window.activeTextEditor
            ? vscode.window.activeTextEditor.viewColumn
            : undefined;
            
        if (DashboardPanel.currentPanel) {
            DashboardPanel.currentPanel._panel.reveal(column);
            return;
        }
        
        const panel = vscode.window.createWebviewPanel(
            'pysparkAssistantDashboard',
            'PySpark Assistant Dashboard',
            column || vscode.ViewColumn.One,
            {
                enableScripts: true,
                localResourceRoots: [
                    vscode.Uri.joinPath(extensionUri, 'media')
                ]
            }
        );
        
        DashboardPanel.currentPanel = new DashboardPanel(panel, extensionUri);
    }
    
    constructor(panel: vscode.WebviewPanel, extensionUri: vscode.Uri) {
        this._panel = panel;
        this._extensionUri = extensionUri;
        
        this._update();
        
        this._panel.onDidDispose(() => this.dispose(), null, this._disposables);
        
        this._panel.webview.onDidReceiveMessage(
            message => {
                switch (message.command) {
                    case 'analyze':
                        vscode.commands.executeCommand('pyspark-assistant.analyzeCurrentFile');
                        return;
                    case 'connectToSparkUI':
                        vscode.commands.executeCommand('pyspark-assistant.connectToSparkUI');
                        return;
                    case 'translateError':
                        vscode.commands.executeCommand('pyspark-assistant.translateError');
                        return;
                }
            },
            null,
            this._disposables
        );
        
        // Update content every 10 seconds
        setInterval(() => {
            this._update();
        }, 10000);
    }
    
    public dispose() {
        DashboardPanel.currentPanel = undefined;
        
        this._panel.dispose();
        
        while (this._disposables.length) {
            const x = this._disposables.pop();
            if (x) {
                x.dispose();
            }
        }
    }
    
    public reveal() {
        this._panel.reveal();
    }
    
    public updateSparkUIStatus(connected: boolean, url?: string) {
        this._sparkUIConnected = connected;
        this._sparkUIUrl = url;
        this._update();
    }
    
    private _update() {
        const webview = this._panel.webview;
        this._panel.title = "PySpark Assistant Dashboard";
        this._panel.webview.html = this._getHtmlForWebview(webview);
    }
    
    private _getHtmlForWebview(webview: vscode.Webview) {
        return `
            <!DOCTYPE html>
            <html lang="en">
            <head>
                <meta charset="UTF-8">
                <meta name="viewport" content="width=device-width, initial-scale=1.0">
                <title>PySpark Assistant Dashboard</title>
                <style>
                    body {
                        font-family: var(--vscode-font-family);
                        color: var(--vscode-editor-foreground);
                        padding: 20px;
                    }
                    .dashboard-grid {
                        display: grid;
                        grid-template-columns: 1fr 1fr;
                        grid-gap: 20px;
                    }
                    .card {
                        background-color: var(--vscode-editor-background);
                        border-radius: 6px;
                        padding: 16px;
                        margin-bottom: 20px;
                    }
                    .card h2 {
                        margin-top: 0;
                        border-bottom: 1px solid var(--vscode-panel-border);
                        padding-bottom: 8px;
                    }
                    button {
                        background-color: var(--vscode-button-background);
                        color: var(--vscode-button-foreground);
                        border: none;
                        padding: 8px 12px;
                        border-radius: 4px;
                        cursor: pointer;
                        margin-right: 8px;
                        margin-bottom: 8px;
                    }
                    button:hover {
                        background-color: var(--vscode-button-hoverBackground);
                    }
                    .status {
                        display: inline-block;
                        padding: 4px 8px;
                        border-radius: 10px;
                        font-size: 12px;
                        margin-bottom: 8px;
                    }
                    .status.connected {
                        background-color: var(--vscode-testing-iconPassed);
                        color: white;
                    }
                    .status.disconnected {
                        background-color: var(--vscode-testing-iconFailed);
                        color: white;
                    }
                    .quick-tips {
                        background-color: var(--vscode-textBlockQuote-background);
                        padding: 10px;
                        border-left: 4px solid var(--vscode-textBlockQuote-border);
                        margin-bottom: 16px;
                    }
                    .tip-item {
                        margin-bottom: 8px;
                    }
                </style>
            </head>
            <body>
                <h1>PySpark Assistant Dashboard</h1>
                
                <div class="quick-tips">
                    <h3>Quick Tips:</h3>
                    <div class="tip-item">💡 Use <code>broadcast()</code> for joins with small DataFrames</div>
                    <div class="tip-item">💡 Filter data early in your processing pipeline</div>
                    <div class="tip-item">💡 Avoid <code>collect()</code> on large DataFrames</div>
                </div>
                
                <div class="dashboard-grid">
                    <div class="card">
                        <h2>Code Analysis</h2>
                        <p>Analyze your PySpark code to identify potential issues and get optimization recommendations.</p>
                        <button id="analyzeBtn">Analyze Current File</button>
                    </div>
                    
                    <div class="card">
                        <h2>Error Translation</h2>
                        <p>Translate cryptic Spark error messages into clear, actionable advice.</p>
                        <button id="translateBtn">Translate Error Message</button>
                    </div>
                    
                    <div class="card">
                        <h2>Spark UI Integration</h2>
                        <p>
                            Status: 
                            <span class="status ${this._sparkUIConnected ? 'connected' : 'disconnected'}">
                                ${this._sparkUIConnected ? 'Connected' : 'Disconnected'}
                            </span>
                        </p>
                        ${this._sparkUIConnected && this._sparkUIUrl 
                            ? `<p>Connected to: ${this._sparkUIUrl}</p>` 
                            : '<p>Connect to your Spark UI to monitor jobs and diagnose issues.</p>'}
                        <button id="connectBtn">Connect to Spark UI</button>
                        ${this._sparkUIConnected 
                            ? `<a href="${this._sparkUIUrl}" target="_blank"><button>Open in Browser</button></a>` 
                            : ''}
                    </div>
                    
                    <div class="card">
                        <h2>Memory Management</h2>
                        <p>Monitor and optimize memory usage in your Spark applications.</p>
                        <ul>
                            <li>Track executor memory usage</li>
                            <li>Get alerts before out-of-memory errors</li>
                            <li>Receive optimization recommendations</li>
                        </ul>
                    </div>
                </div>
                
                <script>
                    (function() {
                        const vscode = acquireVsCodeApi();
                        
                        document.getElementById('analyzeBtn').addEventListener('click', () => {
                            vscode.postMessage({
                                command: 'analyze'
                            });
                        });
                        
                        document.getElementById('translateBtn').addEventListener('click', () => {
                            vscode.postMessage({
                                command: 'translateError'
                            });
                        });
                        
                        document.getElementById('connectBtn').addEventListener('click', () => {
                            vscode.postMessage({
                                command: 'connectToSparkUI'
                            });
                        });
                    }())
                </script>
            </body>
            </html>
        `;
    }
}
