import * as vscode from 'vscode';

export class PySparkCodeActionProvider implements vscode.CodeActionProvider {
  public static readonly providedCodeActionKinds = [
    vscode.CodeActionKind.QuickFix
  ];

  provideCodeActions(
    document: vscode.TextDocument,
    range: vscode.Range,
    context: vscode.CodeActionContext,
    token: vscode.CancellationToken
  ): vscode.CodeAction[] | undefined {
    const actions: vscode.CodeAction[] = [];

    for (const diagnostic of context.diagnostics) {
      if (diagnostic.code === 'collect-large-df') {
        const fix = new vscode.CodeAction(
          'Replace with limit(1000).collect()',
          vscode.CodeActionKind.QuickFix
        );
        // Replace just the .collect() part with .limit(1000).collect()
        const text = document.getText(diagnostic.range);
        const fixedText = text.replace(/\.collect\(\)/, '.limit(1000).collect()');
        fix.edit = new vscode.WorkspaceEdit();
        fix.edit.replace(document.uri, diagnostic.range, fixedText);
        fix.diagnostics = [diagnostic];
        fix.isPreferred = true;
        actions.push(fix);
      }
      if (diagnostic.code === 'to-pandas-large-df') {
        const fix = new vscode.CodeAction(
          'Replace with limit(1000).toPandas()',
          vscode.CodeActionKind.QuickFix
        );
        const text = document.getText(diagnostic.range);
        const fixedText = text.replace(/\.toPandas\(\)/, '.limit(1000).toPandas()');
        fix.edit = new vscode.WorkspaceEdit();
        fix.edit.replace(document.uri, diagnostic.range, fixedText);
        fix.diagnostics = [diagnostic];
        fix.isPreferred = true;
        actions.push(fix);
      }
      if (diagnostic.code === 'missing-cache') {
        // Try to add .cache() to the DataFrame assignment
        // Find the line where the DataFrame is assigned
        const line = document.lineAt(diagnostic.range.start.line).text;
        // If .cache() is not already present, add it
        if (!/\.cache\(\)/.test(line)) {
          const match = line.match(/^(\s*\w+\s*=\s*.+)$/);
          if (match) {
            const newLine = line.replace(/(\s*\w+\s*=\s*.+)$/, '$1.cache()');
            const fix = new vscode.CodeAction(
              'Add .cache() to DataFrame assignment',
              vscode.CodeActionKind.QuickFix
            );
            fix.edit = new vscode.WorkspaceEdit();
            fix.edit.replace(document.uri, new vscode.Range(diagnostic.range.start.line, 0, diagnostic.range.start.line, line.length), newLine);
            fix.diagnostics = [diagnostic];
            fix.isPreferred = true;
            actions.push(fix);
          }
        }
      }
      if (diagnostic.code === 'cartesian-join') {
        // Suggest adding a join condition
        const text = document.getText(diagnostic.range);
        // Replace .crossJoin( with .join( , "join_key") as a template
        const fixedText = text.replace(/\.crossJoin\(/, '.join(').replace(/\)$/, ', "join_key")');
        const fix = new vscode.CodeAction(
          'Replace with join and add join condition template',
          vscode.CodeActionKind.QuickFix
        );
        fix.edit = new vscode.WorkspaceEdit();
        fix.edit.replace(document.uri, diagnostic.range, fixedText);
        fix.diagnostics = [diagnostic];
        fix.isPreferred = true;
        actions.push(fix);
      }
      if (diagnostic.code === 'broadcast-hint') {
        // Suggest using F.broadcast() on the smaller DataFrame in join
        const text = document.getText(diagnostic.range);
        // Try to find the second argument of join and wrap it with F.broadcast()
        const fixedText = text.replace(/\.join\((\w+)/, '.join(F.broadcast($1)');
        const fix = new vscode.CodeAction(
          'Use F.broadcast() for small DataFrame in join',
          vscode.CodeActionKind.QuickFix
        );
        fix.edit = new vscode.WorkspaceEdit();
        fix.edit.replace(document.uri, diagnostic.range, fixedText);
        fix.diagnostics = [diagnostic];
        fix.isPreferred = true;
        actions.push(fix);
      }
    }

    return actions;
  }
} 