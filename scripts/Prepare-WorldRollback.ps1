# Mechanical asset variant generation only. Never edits the served World asset.
$ErrorActionPreference = 'Stop'
$source = 'C:\spaceos\space-bridge\static\world.html'
$destination = 'C:\spaceos\target\stage2b3-preflight\rollback-world.html'
$html = [IO.File]::ReadAllText($source)
$before = (Get-FileHash -LiteralPath $source).Hash
foreach ($function in @('openSemObjCreate','createSemanticObject','openSemObjEdit','saveSemanticObject','openSemRelCreate','createSemanticRelation')) {
    $pattern = '(function ' + $function + '\([^)]*\)\s*\{)'
    if ([regex]::Matches($html, $pattern).Count -ne 1) { throw "Ambiguous/missing function: $function" }
    $html = [regex]::Replace($html, $pattern, '$1' + "`n            return toast('Semantic editing unavailable: rollback Core does not support Semantic Commands.', true); // ROLLBACK_DISABLED")
}
# Disable static and dynamically generated semantic action buttons, preserving all other UI.
$html = [regex]::Replace($html, '<button([^>]*onclick="(?:openSemObjCreate|createSemanticObject|openSemObjEdit|saveSemanticObject|openSemRelCreate|createSemanticRelation)\([^>]*?)>', '<button disabled title="Semantic editing unavailable on rollback Core"$1>')
if (Test-Path -LiteralPath $destination) { throw 'Rollback artifact already exists; inspect it instead of overwriting' }
[IO.File]::WriteAllText($destination, $html, [Text.UTF8Encoding]::new($false))
if ((Get-FileHash -LiteralPath $source).Hash -ne $before) { throw 'Source changed during preparation' }
Get-FileHash -LiteralPath $source,$destination
