//! Static UI asset guard.
//!
//! The World/Shell UIs live in `space-bridge/static/*.html` as one large inline
//! `<script>` block. A single JavaScript syntax error there is fatal and silent:
//! the browser refuses to parse the whole block, `init()` never runs, and the UI
//! stays on "Connection: initializing…" with an empty world while every HTTP and
//! WebSocket endpoint keeps answering `200`. That is exactly what happened with
//! three adjacent string literals (`'Read failed:' ' + x`) in `world.html`.
//!
//! `cargo` cannot parse JavaScript, so this test uses a small dependency-free
//! lexer that understands strings, template literals, comments and regex
//! literals, and reports:
//!   * two literals with only whitespace between them (always a syntax error), and
//!   * unbalanced `()[]{}` outside of strings/comments/regexes.
//!
//! It is deliberately narrow: it guards the defect class that bit us without
//! pretending to be a full JS parser.

use std::fs;
use std::path::{Path, PathBuf};

/// Static assets directory of the bridge crate.
fn static_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("static")
}

/// Extract inline `<script>` bodies; `src=` scripts and non-JS types are skipped.
fn inline_scripts(html: &str) -> Vec<(usize, String)> {
    let lower = html.to_ascii_lowercase();
    let bytes = lower.as_bytes();
    let mut out = Vec::new();
    let mut pos = 0usize;

    while let Some(rel) = lower[pos..].find("<script") {
        let start = pos + rel;
        let Some(gt_rel) = lower[start..].find('>') else { break };
        let attrs_start = start + "<script".len();
        let attrs = &lower[attrs_start..start + gt_rel];
        let body_start = start + gt_rel + 1;
        let Some(end_rel) = lower[body_start..].find("</script") else { break };
        let body_end = body_start + end_rel;

        let is_js = !attrs.contains("src=")
            && (attrs.contains("javascript") || !attrs.contains("type=") || attrs.trim().is_empty());
        if is_js {
            let line = bytes[..body_start].iter().filter(|b| **b == b'\n').count() + 1;
            out.push((line, html[body_start..body_end].to_string()));
        }
        pos = body_start + end_rel + "</script".len();
    }
    out
}

/// True when a `/` seen in normal state starts a regex literal rather than a division.
fn regex_allowed(prev: Option<char>) -> bool {
    match prev {
        None => true,
        Some(c) => !(c.is_alphanumeric() || c == '_' || c == ')' || c == ']' || c == '}'),
    }
}

/// Lex one script body and report syntax-level defects with line numbers
/// (relative to the start of the script body).
fn check_script(body: &str) -> Vec<String> {
    let chars: Vec<char> = body.chars().collect();
    let mut diags = Vec::new();
    let mut line = 1usize;
    let mut i = 0usize;
    let mut prev_sig: Option<char> = None;
    // Set when the previous significant token was a literal (string/template/regex).
    let mut prev_was_literal = false;
    let mut stack: Vec<(char, usize)> = Vec::new();

    while i < chars.len() {
        let c = chars[i];

        if c == '\n' {
            line += 1;
            i += 1;
            continue;
        }
        if c.is_whitespace() {
            i += 1;
            continue;
        }

        // Comments.
        if c == '/' && i + 1 < chars.len() && chars[i + 1] == '/' {
            while i < chars.len() && chars[i] != '\n' {
                i += 1;
            }
            continue;
        }
        if c == '/' && i + 1 < chars.len() && chars[i + 1] == '*' {
            i += 2;
            while i + 1 < chars.len() && !(chars[i] == '*' && chars[i + 1] == '/') {
                if chars[i] == '\n' {
                    line += 1;
                }
                i += 1;
            }
            i = (i + 2).min(chars.len());
            continue;
        }

        // Literals.
        if c == '\'' || c == '"' || c == '`' {
            if prev_was_literal {
                diags.push(format!(
                    "line {line}: two literals in a row (missing operator?) near `{}`",
                    snippet(&chars, i)
                ));
            }
            let quote = c;
            let start_line = line;
            i += 1;
            while i < chars.len() {
                let d = chars[i];
                if d == '\\' {
                    i += 2;
                    continue;
                }
                if d == '\n' {
                    line += 1;
                    if quote != '`' {
                        diags.push(format!("line {start_line}: unterminated {quote} string"));
                        break;
                    }
                }
                if d == quote {
                    i += 1;
                    break;
                }
                i += 1;
            }
            prev_sig = Some(quote);
            prev_was_literal = true;
            continue;
        }

        // Regex literal or division.
        if c == '/' && regex_allowed(prev_sig) {
            let mut in_class = false;
            i += 1;
            while i < chars.len() {
                let d = chars[i];
                if d == '\\' {
                    i += 2;
                    continue;
                }
                if d == '[' {
                    in_class = true;
                } else if d == ']' {
                    in_class = false;
                } else if d == '/' && !in_class {
                    i += 1;
                    break;
                } else if d == '\n' {
                    break;
                }
                i += 1;
            }
            prev_sig = Some('/');
            prev_was_literal = true;
            continue;
        }

        // Brackets.
        if c == '(' || c == '[' || c == '{' {
            stack.push((c, line));
            prev_was_literal = false;
            prev_sig = Some(c);
            i += 1;
            continue;
        }
        if c == ')' || c == ']' || c == '}' {
            let open = match c {
                ')' => '(',
                ']' => '[',
                _ => '{',
            };
            match stack.pop() {
                Some((o, _)) if o == open => {}
                Some((o, oline)) => diags.push(format!(
                    "line {line}: `{c}` closes `{o}` opened on line {oline}"
                )),
                None => diags.push(format!("line {line}: unmatched `{c}`")),
            }
            prev_was_literal = false;
            prev_sig = Some(c);
            i += 1;
            continue;
        }

        prev_sig = Some(c);
        prev_was_literal = false;
        i += 1;
    }

    for (open, oline) in stack {
        diags.push(format!("line {oline}: `{open}` is never closed"));
    }
    diags
}

fn snippet(chars: &[char], at: usize) -> String {
    let end = (at + 24).min(chars.len());
    chars[at..end].iter().collect::<String>().replace('\n', " ")
}

/// Every inline script of every static asset must be free of the defects above.
#[test]
fn static_ui_inline_scripts_are_syntactically_sound() {
    let dir = static_dir();
    assert!(dir.is_dir(), "static dir not found: {}", dir.display());

    let mut checked_files = 0usize;
    let mut checked_scripts = 0usize;

    let mut assets: Vec<PathBuf> = fs::read_dir(&dir)
        .expect("read static dir")
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| {
            matches!(
                p.extension().and_then(|e| e.to_str()),
                Some("html") | Some("htm") | Some("js")
            )
        })
        .collect();
    assets.sort();

    for asset in assets {
        let text = fs::read_to_string(&asset).expect("read asset");
        let name = asset.file_name().unwrap().to_string_lossy().to_string();

        let scripts: Vec<(usize, String)> = if name.ends_with(".js") {
            vec![(1, text.clone())]
        } else {
            inline_scripts(&text)
        };

        for (start_line, body) in scripts {
            if body.trim().is_empty() {
                continue;
            }
            checked_scripts += 1;
            let diags = check_script(&body);
            assert!(
                diags.is_empty(),
                "{name} (script starting at file line {start_line}) has JavaScript syntax problems:\n  {}",
                diags.join("\n  ")
            );
        }
        checked_files += 1;
    }

    assert!(checked_files > 0, "no static assets found in {}", dir.display());
    assert!(checked_scripts > 0, "no inline scripts found in {}", dir.display());
    println!("checked {checked_scripts} script(s) across {checked_files} asset(s)");
}

/// The exact regression that broke the World UI must be caught.
#[test]
fn detector_catches_adjacent_string_literals() {
    let broken = r#"
        async function x() {
            try { await y(); }
            catch (e) { toast('Read failed:' ' + trunc(e.message, 120), true); }
        }
    "#;
    let diags = check_script(broken);
    assert!(
        diags.iter().any(|d| d.contains("two literals in a row")),
        "expected the adjacent-literal defect to be detected, got: {diags:?}"
    );

    let fixed = r#"
        async function x() {
            try { await y(); }
            catch (e) { toast('Read failed: ' + trunc(e.message, 120), true); }
        }
    "#;
    assert!(check_script(fixed).is_empty(), "fixed snippet must be clean");
}

/// Constructs that legitimately put quotes and slashes next to each other must not
/// be reported: regex literals inside `replace()`, division, escapes and templates.
#[test]
fn detector_ignores_legal_constructs() {
    let legal = r#"
        const esc = v => String(v == null ? '' : v).replace(/[&<>"']/g, ch => ({'&':'&amp;'}[ch]));
        const re = /[\\'"]/g;
        const q = name.replace(/'/g, "\\'");
        const half = total / 2, ratio = (a) / 3, arr = [1, 2][0] / 4;
        const t = `a 'quoted' thing and ${1 + 2} plus /slash`;
        const s = 'It\'s fine';
        const obj = { 'a': 1, "b": 2 };
        if (n > 0) { go(); } else { stop(); }
    "#;
    let diags = check_script(legal);
    assert!(diags.is_empty(), "legal JS must not be flagged, got: {diags:?}");
}
