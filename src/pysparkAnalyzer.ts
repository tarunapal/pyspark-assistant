/************ pysparkAnalyzer.ts ************/

import * as vscode from 'vscode';
import { ErrorTranslator } from './errorTranslator';

interface PerformanceImpact {
    currentEstimate: string;
    improvedEstimate: string;
    improvementFactor: number;
}

interface DataVolumeEstimate {
    rowCount: number;
    dataSize: string;
    memoryImpact: string;
    riskLevel: 'low' | 'medium' | 'high' | 'critical';
}

export class PySparkAnalyzer {
    private errorTranslator: ErrorTranslator | undefined;
    private diagnosticCollection: vscode.DiagnosticCollection;
    private _analyzeChangesTimeout: NodeJS.Timeout | undefined = undefined;
    
    constructor() {
        this.diagnosticCollection = vscode.languages.createDiagnosticCollection('pyspark-analyzer');
    }
    
    setErrorTranslator(translator: ErrorTranslator) {
        this.errorTranslator = translator;
    }
    
    async analyzeDocument(document: vscode.TextDocument): Promise<vscode.Diagnostic[]> {
        const text = document.getText();
        const diagnostics: vscode.Diagnostic[] = [];
        
        // Pattern detection for common PySpark issues
        this.detectCachingIssues(text, diagnostics, document);
        this.detectCartesianJoins(text, diagnostics, document);
        this.detectIneffcientOperations(text, diagnostics, document);
        this.detectMemoryIssues(text, diagnostics, document);
        this.detectDataSkew(text, diagnostics, document);
        
        // Update diagnostics
        this.diagnosticCollection.set(document.uri, diagnostics);
        
        return diagnostics;
    }
    
    analyzeChanges(event: vscode.TextDocumentChangeEvent) {
        // Throttled analysis on document changes
        // We'll use a simple debounce here
        if (this._analyzeChangesTimeout) {
            clearTimeout(this._analyzeChangesTimeout);
        }
        this._analyzeChangesTimeout = setTimeout(() => {
            this.analyzeDocument(event.document);
        }, 500);
    }
    
    private detectCachingIssues(text: string, diagnostics: vscode.Diagnostic[], document: vscode.TextDocument) {
        // Look for repeated DataFrame operations without caching
        const multipleActionPattern = /(\w+)\s*=.*\n.*\1\.(?:count|collect|show|take|toPandas)/g;
        let match;
        
        while ((match = multipleActionPattern.exec(text)) !== null) {
            const startPos = document.positionAt(match.index);
            const endPos = document.positionAt(match.index + match[0].length);
            const range = new vscode.Range(startPos, endPos);
            
            diagnostics.push({
                message: "Multiple actions on the same DataFrame without caching. Consider using .cache() or .persist() to avoid recomputation.",
                range,
                severity: vscode.DiagnosticSeverity.Warning,
                source: 'PySpark Assistant',
                code: 'missing-cache'
            });
        }
    }
    
    private estimatePerformanceImpact(operation: string, dataSize: number): PerformanceImpact {
        const impacts: { [key: string]: PerformanceImpact } = {
            'cartesian-join': {
                currentEstimate: 'O(n²) - Hours for large datasets',
                improvedEstimate: 'O(n log n) - Minutes with proper join conditions',
                improvementFactor: 120 // ~120x improvement
            },
            'collect-large-df': {
                currentEstimate: 'Minutes + High Memory Usage',
                improvedEstimate: 'Seconds with proper limiting',
                improvementFactor: 60 // ~60x improvement
            },
            'missing-cache': {
                currentEstimate: 'Multiple full recomputations (minutes each)',
                improvedEstimate: 'Single computation + memory lookup (seconds)',
                improvementFactor: 10 // ~10x improvement
            },
            'broadcast-hint': {
                currentEstimate: 'O(n) shuffle - Minutes',
                improvedEstimate: 'O(1) broadcast - Seconds',
                improvementFactor: 30 // ~30x improvement
            }
        };

        return impacts[operation] || {
            currentEstimate: 'Unknown',
            improvedEstimate: 'Potentially improved',
            improvementFactor: 1
        };
    }
    
    private estimateDataVolume(text: string): DataVolumeEstimate {
        // Look for common data loading patterns
        const patterns = {
            sparkRange: /spark\.range\((\d+)\)/,
            readCSV: /spark\.read\.csv\(['"](.*?)['"]\)/,
            readParquet: /spark\.read\.parquet\(['"](.*?)['"]\)/,
            readTable: /spark\.read\.table\(['"](.*?)['"]\)/,
            createDF: /toDF\(|createDataFrame\(/
        };

        let rowCount = 0;
        let dataSize = "Unknown";
        let memoryImpact = "Unknown";
        let riskLevel: 'low' | 'medium' | 'high' | 'critical' = 'low';

        // Check for explicit row counts
        const rangeMatch = text.match(patterns.sparkRange);
        if (rangeMatch) {
            rowCount = parseInt(rangeMatch[1]);
        }

        // Estimate based on operation type and data size
        if (rowCount > 0) {
            if (rowCount < 10000) {
                dataSize = "< 1MB";
                memoryImpact = "Minimal";
                riskLevel = 'low';
            } else if (rowCount < 100000) {
                dataSize = "~10MB";
                memoryImpact = "Moderate";
                riskLevel = 'medium';
            } else if (rowCount < 1000000) {
                dataSize = "~100MB";
                memoryImpact = "Significant";
                riskLevel = 'high';
            } else {
                dataSize = "> 1GB";
                memoryImpact = "Critical";
                riskLevel = 'critical';
            }
        }

        return { rowCount, dataSize, memoryImpact, riskLevel };
    }

    private getDataVolumeWarning(estimate: DataVolumeEstimate, operation: 'collect' | 'broadcast' | 'join'): string {
        const warnings = {
            collect: {
                high: `WARNING: Collecting large dataset (${estimate.dataSize}) to driver memory!`,
                critical: `CRITICAL: Attempting to collect very large dataset (${estimate.dataSize}) to driver!`
            },
            broadcast: {
                high: `Consider partitioning instead of broadcast for datasets > 100MB`,
                critical: `Broadcasting large datasets (${estimate.dataSize}) can cause OOM errors`
            },
            join: {
                high: "Large dataset join detected. Consider optimizing join strategy.",
                critical: "Very large dataset join detected. High risk of memory issues."
            }
        };

        return estimate.riskLevel === 'critical' ? 
            warnings[operation].critical : 
            warnings[operation].high;
    }
    
    private detectCartesianJoins(text: string, diagnostics: vscode.Diagnostic[], document: vscode.TextDocument) {
        const cartesianJoinPattern = /\.crossJoin\(/g;
        let match;
        
        while ((match = cartesianJoinPattern.exec(text)) !== null) {
            const startPos = document.positionAt(match.index);
            const endPos = document.positionAt(match.index + match[0].length);
            const range = new vscode.Range(startPos, endPos);
            
            const volumeEstimate = this.estimateDataVolume(text);
            const resultSize = Math.pow(volumeEstimate.rowCount, 2);
            const resultEstimate = {
                rowCount: resultSize,
                dataSize: resultSize > 1000000 ? "> 1GB" : "~100MB",
                memoryImpact: resultSize > 1000000 ? "Critical" : "High",
                riskLevel: resultSize > 1000000 ? 'critical' : 'high'
            };
            
            const impact = this.estimatePerformanceImpact('cartesian-join', 1000000);
            
            const diagnostic = new vscode.Diagnostic(
                range,
                `CRITICAL: Cartesian join detected!\n` +
                `Input Data Size: ${volumeEstimate.dataSize}\n` +
                `Estimated Result Size: ${resultEstimate.dataSize} (${resultEstimate.rowCount.toLocaleString()} rows)\n` +
                `Memory Impact: ${resultEstimate.memoryImpact}\n` +
                `Current Performance: ${impact.currentEstimate}\n` +
                `Potential Performance: ${impact.improvedEstimate}\n` +
                `Estimated Improvement: ${impact.improvementFactor}x faster with proper join conditions`,
                vscode.DiagnosticSeverity.Error
            );
            
            // Set the source and code
            diagnostic.source = 'PySpark Assistant';
            diagnostic.code = 'cartesian-join';
            
            // Make the diagnostic more visible with a code action
            diagnostic.tags = [vscode.DiagnosticTag.Unnecessary];
            
            diagnostics.push(diagnostic);
        }
        
        // Also check for joins without conditions
        const joinWithoutConditionPattern = /\.join\([^,]+(,\s*how\s*=\s*['"][^'"]+['"])?\)/g;
        while ((match = joinWithoutConditionPattern.exec(text)) !== null) {
            const startPos = document.positionAt(match.index);
            const endPos = document.positionAt(match.index + match[0].length);
            const range = new vscode.Range(startPos, endPos);
            
            diagnostics.push({
                message: "Join without condition detected. This will result in a cartesian product and may cause memory issues.",
                range,
                severity: vscode.DiagnosticSeverity.Error,
                source: 'PySpark Assistant',
                code: 'implicit-cartesian-join'
            });
        }
    }
    
    private detectIneffcientOperations(text: string, diagnostics: vscode.Diagnostic[], document: vscode.TextDocument) {
        // Detect collect() on large DataFrames
        const collectPattern = /\.collect\(\)/g;
        let match;
        
        while ((match = collectPattern.exec(text)) !== null) {
            const startPos = document.positionAt(match.index);
            const endPos = document.positionAt(match.index + match[0].length);
            const range = new vscode.Range(startPos, endPos);
            
            const impact = this.estimatePerformanceImpact('collect-large-df', 1000000);
            const volumeEstimate = this.estimateDataVolume(text);
            
            diagnostics.push({
                message: `collect() may cause out of memory errors on large DataFrames.\n` +
                        `Estimated Data Volume: ${volumeEstimate.dataSize}\n` +
                        `Memory Impact: ${volumeEstimate.memoryImpact}\n` +
                        `Current Performance: ${impact.currentEstimate}\n` +
                        `Potential Performance: ${impact.improvedEstimate}\n` +
                        `Estimated Improvement: ${impact.improvementFactor}x faster with take(), limit(), or sampling\n` +
                        `${this.getDataVolumeWarning(volumeEstimate, 'collect')}`,
                range,
                severity: volumeEstimate.riskLevel === 'critical' ? 
                    vscode.DiagnosticSeverity.Error : 
                    vscode.DiagnosticSeverity.Warning,
                source: 'PySpark Assistant',
                code: 'collect-large-df'
            });
        }
        
        // Detect toPandas() on large DataFrames
        const toPandasPattern = /\.toPandas\(\)/g;
        while ((match = toPandasPattern.exec(text)) !== null) {
            const startPos = document.positionAt(match.index);
            const endPos = document.positionAt(match.index + match[0].length);
            const range = new vscode.Range(startPos, endPos);
            
            diagnostics.push({
                message: "toPandas() brings all data to the driver and may cause out of memory errors. Consider limiting data size before conversion.",
                range,
                severity: vscode.DiagnosticSeverity.Warning,
                source: 'PySpark Assistant',
                code: 'to-pandas-large-df'
            });
        }
    }
    
    private detectMemoryIssues(text: string, diagnostics: vscode.Diagnostic[], document: vscode.TextDocument) {
        // Look for potential broadcast hash join opportunities
        const joinPattern = /(\w+)\.join\((\w+),.*\)/g;
        let match;
        
        while ((match = joinPattern.exec(text)) !== null) {
            // Check if we already have a broadcast hint
            const hasBroadcastHint = text.includes(`broadcast(${match[2]})`) || 
                                      text.includes(`F.broadcast(${match[2]})`);
            
            if (!hasBroadcastHint) {
                const startPos = document.positionAt(match.index);
                const endPos = document.positionAt(match.index + match[0].length);
                const range = new vscode.Range(startPos, endPos);
                
                const impact = this.estimatePerformanceImpact('broadcast-hint', 1000000);
                
                diagnostics.push({
                    message: `Consider using broadcast hint for smaller DataFrame to optimize join.\n` +
                            `Current Performance: ${impact.currentEstimate}\n` +
                            `Potential Performance: ${impact.improvedEstimate}\n` +
                            `Estimated Improvement: ${impact.improvementFactor}x faster with F.broadcast(smallerDF)`,
                    range,
                    severity: vscode.DiagnosticSeverity.Information,
                    source: 'PySpark Assistant',
                    code: 'broadcast-hint'
                });
            }
        }
    }
    
    private detectDataSkew(text: string, diagnostics: vscode.Diagnostic[], document: vscode.TextDocument) {
        // Look for potential data skew issues in groupBy operations
        const groupByPattern = /\.groupBy\(([^)]+)\)/g;
        let match;
        
        while ((match = groupByPattern.exec(text)) !== null) {
            const startPos = document.positionAt(match.index);
            const endPos = document.positionAt(match.index + match[0].length);
            const range = new vscode.Range(startPos, endPos);
            
            diagnostics.push({
                message: "GroupBy operation may cause data skew if the key distribution is uneven. Consider analyzing key distribution and using salting techniques if necessary.",
                range,
                severity: vscode.DiagnosticSeverity.Information,
                source: 'PySpark Assistant',
                code: 'potential-data-skew'
            });
        }
    }
}