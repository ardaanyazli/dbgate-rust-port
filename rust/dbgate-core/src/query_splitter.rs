//! SQL statement splitter.
//!
//! Rust port of the tokenizer-based `dbgate-query-splitter` npm package,
//! extracted from the hand-rolled splitter the SQLite driver used to split
//! scripts. A script is split on `;` terminators while quoted strings
//! (`' " ``), `--` line comments and `/* */` block comments protect inner
//! semicolons; returned items keep comments but drop the terminator.
//!
//! Node dialect flags `hashComments` and `supportNumericComments` are not
//! ported — every driver uses `'`-quoted strings with `--` / `/* */`
//! comments only.

/// Split a SQL script into statements, mirroring `dbgate-query-splitter`
/// with `'` as the string-escape char (used by every driver dialect):
/// trailing semicolons are dropped from items and empty statements skipped.
pub fn split_sql(sql: &str) -> Vec<String> {
    split_sql_dialect(sql, '\'')
}

/// Like [`split_sql`], with the dialect's quote-escape char parameterized.
/// A quote is escaped by doubling it (`''`), consumed in one step so the
/// escape never closes the string; Node's splitter treats the doubled quote
/// the same way (its escape branch fires only when the escape char differs
/// from the closing quote). Future-proofing hook — every driver passes `'`.
pub fn split_sql_dialect(sql: &str, string_escape_char: char) -> Vec<String> {
    let mut out = Vec::new();
    let mut current = String::new();
    let mut in_quote: Option<char> = None;
    let mut in_line_comment = false;
    let mut in_block_comment = false;
    let chars: Vec<char> = sql.chars().collect();
    let mut i = 0;

    while i < chars.len() {
        let c = chars[i];
        let next = chars.get(i + 1).copied();

        if in_line_comment {
            if c == '\n' {
                in_line_comment = false;
            }
            current.push(c);
            i += 1;
            continue;
        }
        if in_block_comment {
            if c == '*' && next == Some('/') {
                in_block_comment = false;
                current.push(c);
                current.push('/');
                i += 2;
                continue;
            }
            current.push(c);
            i += 1;
            continue;
        }
        if let Some(q) = in_quote {
            if string_escape_char == q && c == q && next == Some(q) {
                current.push(c);
                current.push(q);
                i += 2;
                continue;
            }
            current.push(c);
            if c == q {
                in_quote = None;
            }
            i += 1;
            continue;
        }

        match c {
            '\'' | '"' | '`' => {
                in_quote = Some(c);
                current.push(c);
                i += 1;
            }
            '-' if next == Some('-') => {
                in_line_comment = true;
                current.push(c);
                current.push('-');
                i += 2;
            }
            '/' if next == Some('*') => {
                in_block_comment = true;
                current.push(c);
                current.push('*');
                i += 2;
            }
            ';' => {
                let trimmed = current.trim().to_string();
                if !trimmed.is_empty() {
                    out.push(trimmed);
                }
                current.clear();
                i += 1;
            }
            _ => {
                current.push(c);
                i += 1;
            }
        }
    }

    let trimmed = current.trim().to_string();
    if !trimmed.is_empty() {
        out.push(trimmed);
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_multiple_statements_dropping_terminators() {
        assert_eq!(split_sql("SELECT 1; SELECT 2"), vec!["SELECT 1", "SELECT 2"]);
        assert_eq!(split_sql("SELECT 1; SELECT 2;"), vec!["SELECT 1", "SELECT 2"]);
    }

    #[test]
    fn keeps_statements_together_holding_semicolons_in_strings() {
        assert_eq!(split_sql("SELECT 'a;b'"), vec!["SELECT 'a;b'"]);
        assert_eq!(
            split_sql("SELECT 'a;''b'; SELECT 2"),
            vec!["SELECT 'a;''b'", "SELECT 2"]
        );
        assert_eq!(
            split_sql("SELECT \"a;b\"; SELECT 2"),
            vec!["SELECT \"a;b\"", "SELECT 2"]
        );
        assert_eq!(split_sql("SELECT `a;b`"), vec!["SELECT `a;b`"]);
    }

    #[test]
    fn keeps_semicolons_in_block_and_line_comments() {
        assert_eq!(
            split_sql("SELECT 1 /* ; */ ; SELECT 2"),
            vec!["SELECT 1 /* ; */", "SELECT 2"]
        );
        assert_eq!(
            split_sql("SELECT 1 -- ;\n; SELECT 2"),
            vec!["SELECT 1 -- ;", "SELECT 2"]
        );
    }

    #[test]
    fn drops_empty_statements_and_trailing_terminators() {
        assert_eq!(split_sql("SELECT 1;;SELECT 2"), vec!["SELECT 1", "SELECT 2"]);
        assert_eq!(split_sql(";SELECT 1"), vec!["SELECT 1"]);
        assert_eq!(split_sql("SELECT 1;"), vec!["SELECT 1"]);
    }

    #[test]
    fn escaped_quotes_do_not_close_the_string() {
        assert_eq!(split_sql("SELECT 'it''s'"), vec!["SELECT 'it''s'"]);
    }

    #[test]
    fn split_sql_delegates_to_dialect_with_single_quote_escape() {
        for sql in [
            "SELECT 1; SELECT 2",
            "SELECT 'a;b'",
            "SELECT 1 /* ; */ ; SELECT 2",
            "SELECT 'it''s'",
        ] {
            assert_eq!(split_sql(sql), split_sql_dialect(sql, '\''));
        }
    }
}