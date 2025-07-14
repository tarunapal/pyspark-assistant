/************ memoryManager.ts ************/

import * as vscode from 'vscode';
import { PySparkAnalyzer } from './pysparkAnalyzer';

export class MemoryManager {
    private pysparkAnalyzer: PySparkAnalyzer | undefined;
    private executorMemoryWarningThreshold: number = 0.8; // 80% memory usage triggers warning
    private executorMemoryUsage: Map<string, number> = new Map(); // executor-id -> memory usage (0-1)
    
    setPySparkAnalyzer(analyzer: PySparkAnalyzer) {
        this.pysparkAnalyzer = analyzer;
    }
    
    trackCellExecution(event: vscode.NotebookDocumentChangeEvent) {
        // This would ideally hook into Jupyter cell execution to monitor memory
        // Currently, VS Code extension API has limited support for this
        // We can implement basic pattern recognition instead
        
        for (const change of event.contentChanges) {
            // Get the cells from the notebook
            const cells = event.notebook.getCells();
            // Check for cells that were just executed
            const executedCells = cells.filter(cell => cell.metadata?.executionOrder !== undefined);
            
            for (const cell of executedCells) {
                this.analyzeCellForMemoryIssues(cell);
            }
        }
    }
    
    private analyzeCellForMemoryIssues(cell: vscode.NotebookCell) {
        const cellText = cell.document.getText();
        
        // Look for potential memory-intensive operations
        if (/\.collect\(\)|\.toPandas\(\)/.test(cellText) && 
            !/\.limit\(|\.sample\(/.test(cellText)) {
            
            vscode.window.showWarningMessage(
                "Memory-intensive operation detected. Consider using .limit() or .sample() before collecting data to driver.", 
                "Learn More"
            ).then(selection => {
                if (selection === "Learn More") {
                    vscode.env.openExternal(vscode.Uri.parse(
                        "https://spark.apache.org/docs/latest/tuning.html#memory-management-overview"
                    ));
                }
            });
        }
        
        // Look for join operations without broadcast hints on smaller DataFrames
        if (/\.join\(/.test(cellText) && !/broadcast\(/.test(cellText)) {
            vscode.window.showInformationMessage(
                "Join operation detected. Consider using broadcast hint for smaller DataFrame to optimize memory usage.", 
                "Learn More"
            ).then(selection => {
                if (selection === "Learn More") {
                    vscode.env.openExternal(vscode.Uri.parse(
                        "https://spark.apache.org/docs/latest/sql-performance-tuning.html#broadcast-hint-for-sql-queries"
                    ));
                }
            });
        }
    }
    
    updateExecutorMemoryUsage(executorId: string, memoryUsage: number) {
        this.executorMemoryUsage.set(executorId, memoryUsage);
        
        // Check if memory usage is over threshold
        if (memoryUsage > this.executorMemoryWarningThreshold) {
            vscode.window.showWarningMessage(
                `Executor ${executorId} is using ${Math.round(memoryUsage * 100)}% of its allocated memory, which may lead to OutOfMemoryError.`,
                "Show Recommendations"
            ).then(selection => {
                if (selection === "Show Recommendations") {
                    this.showMemoryRecommendations();
                }
            });
        }
    }
    
    private showMemoryRecommendations() {
        const panel = vscode.window.createWebviewPanel(
            'memoryRecommendations',
            'Memory Optimization Recommendations',
            vscode.ViewColumn.Beside,
            {}
        );
        
        panel.webview.html = `
            <!DOCTYPE html>
            <html>
            <head>
                <meta charset="UTF-8">
                <title>Memory Optimization Recommendations</title>
                <style>
                    body {
                        padding: 20px;
                        font-family: var(--vscode-font-family);
                        color: var(--vscode-editor-foreground);
                    }
                    h2 {
                        color: var(--vscode-editorLink-activeForeground);
                    }
                    .recommendation {
                        margin-bottom: 20px;
                        padding: 10px;
                        background-color: var(--vscode-editor-background);
                        border-left: 4px solid var(--vscode-editorLink-activeForeground);
                    }
                    code {
                        display: block;
                        background-color: var(--vscode-textCodeBlock-background);
                        padding: 10px;
                        margin: 10px 0;
                        font-family: var(--vscode-editor-font-family);
                    }
                </style>
            </head>
            <body>
                <h1>Memory Optimization Recommendations</h1>
                
                <div class="recommendation">
                    <h2>1. Filter Data Early</h2>
                    <p>Apply filters as early as possible in your processing pipeline to reduce the amount of data being processed.</p>
                    <code>
                    # Instead of this<br>
                    df = spark.read.parquet("large_dataset.parquet")<br>
                    result = df.join(other_df, "key").filter(conditions)<br>
                    <br>
                    # Do this<br>
                    df = spark.read.parquet("large_dataset.parquet").filter(conditions)<br>
                    result = df.join(other_df, "key")
                    </code>
                </div>
                
                <div class="recommendation">
                    <h2>2. Use Broadcast Joins for Small DataFrames</h2>
                    <p>When joining a large DataFrame with a small one, use broadcast hints to avoid shuffle and reduce memory pressure.</p>
                    <code>
                    from pyspark.sql.functions import broadcast<br>
                    <br>
                    # Instead of this<br>
                    result = large_df.join(small_df, "key")<br>
                    <br>
                    # Do this<br>
                    result = large_df.join(broadcast(small_df), "key")
                    </code>
                </div>
                
                <div class="recommendation">
                    <h2>3. Avoid collect() on Large DataFrames</h2>
                    <p>Use sampling, limiting, or aggregation instead of collecting entire large DataFrames to the driver.</p>
                    <code>
                    # Instead of this<br>
                    all_data = large_df.collect()<br>
                    <br>
                    # Do this<br>
                    sample_data = large_df.limit(1000).collect()<br>
                    # or<br>
                    sample_data = large_df.sample(False, 0.01).collect()
                    </code>
                </div>
                
                <div class="recommendation">
                    <h2>4. Cache Wisely</h2>
                    <p>Only cache DataFrames that are reused multiple times, and unpersist them when no longer needed.</p>
                    <code>
                    # Cache DataFrame that will be reused<br>
                    filtered_df = large_df.filter(conditions).cache()<br>
                    <br>
                    # Use it multiple times<br>
                    result1 = filtered_df.groupBy("col1").count()<br>
                    result2 = filtered_df.groupBy("col2").sum("value")<br>
                    <br>
                    # Unpersist when done<br>
                    filtered_df.unpersist()
                    </code>
                </div>
                
                <div class="recommendation">
                    <h2>5. Tune Partition Count</h2>
                    <p>Adjust the number of partitions to avoid too few (memory pressure) or too many (task overhead).</p>
                    <code>
                    # Increase partitions when processing large data<br>
                    df = df.repartition(200)<br>
                    <br>
                    # Reduce partitions when there are too many small partitions<br>
                    df = df.coalesce(10)
                    </code>
                </div>
                
                <div class="recommendation">
                    <h2>6. Configuration Settings</h2>
                    <p>Consider these Spark configuration settings to help with memory management:</p>
                    <code>
                    spark = SparkSession.builder \\<br>
                        .config("spark.executor.memory", "8g") \\<br>
                        .config("spark.driver.memory", "4g") \\<br>
                        .config("spark.memory.fraction", 0.8) \\<br>
                        .config("spark.memory.storageFraction", 0.5) \\<br>
                        .config("spark.sql.shuffle.partitions", 200) \\<br>
                        .getOrCreate()
                    </code>
                </div>
            </body>
            </html>
        `;
    }
}