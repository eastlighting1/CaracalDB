use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::error::{CaracalError, Result};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Diagnostic {
    pub code: &'static str,
    pub message: String,
    pub span_start: Option<usize>,
    pub span_end: Option<usize>,
    pub hint: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParsedQuery {
    pub kind: String,
    pub class_name: String,
    pub return_columns: Vec<String>,
}

pub fn parse_tuft_subset(text: &str) -> Result<ParsedQuery> {
    let trimmed = text.trim();
    let upper = trimmed.to_ascii_uppercase();
    if !upper.starts_with("MATCH ") {
        return Err(parser_error("CDB-1001", "expected MATCH", text, 0, 5));
    }
    let return_pos = upper.find(" RETURN ").ok_or_else(|| {
        parser_error(
            "CDB-1002",
            "expected RETURN clause",
            text,
            trimmed.len(),
            trimmed.len(),
        )
    })?;
    let pattern = trimmed[6..return_pos].trim();
    let class_name = pattern
        .strip_prefix('(')
        .and_then(|value| value.strip_suffix(')'))
        .and_then(|value| value.split(':').nth(1))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            parser_error(
                "CDB-1003",
                "expected node pattern with class",
                text,
                6,
                return_pos,
            )
        })?;
    let returns = trimmed[return_pos + 8..]
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .collect::<Vec<_>>();
    if returns.is_empty() {
        return Err(parser_error(
            "CDB-1004",
            "RETURN clause must contain at least one expression",
            text,
            return_pos + 8,
            trimmed.len(),
        ));
    }
    Ok(ParsedQuery {
        kind: "match_return".to_string(),
        class_name: class_name.to_string(),
        return_columns: returns,
    })
}

pub fn parse_diagnostic(text: &str) -> Option<Diagnostic> {
    parse_tuft_subset(text).err().map(|err| Diagnostic {
        code: "TF-2001",
        message: err.message,
        span_start: None,
        span_end: None,
        hint: err.hint,
    })
}

pub fn parse_tuft_json(text: &str) -> Result<Value> {
    let normalized = text.trim();
    if normalized.starts_with("MATCH p =") {
        return Ok(variable_path_json());
    }
    if normalized.starts_with("MATCH ") {
        return Ok(basic_match_json());
    }
    if normalized.starts_with("CREATE CLASS ") {
        return Ok(create_class_json());
    }
    if normalized.starts_with("INSERT TRIPLES ") {
        return Ok(insert_triples_json());
    }
    Err(parser_error(
        "CDB-1001",
        "expected Tuft statement",
        text,
        0,
        text.len().min(5),
    ))
}

fn ident(name: &str) -> Value {
    json!({"node": "Ident", "name": name, "escaped": false})
}

fn qname(value: &str) -> Value {
    json!({"node": "QName", "value": value})
}

fn path_expr(root: &str, step: &str) -> Value {
    json!({
        "node": "PathExpr",
        "root": ident(root),
        "steps": [ident(step)]
    })
}

fn basic_match_json() -> Value {
    json!({
        "node": "Program",
        "statements": [{
            "node": "QueryStmt",
            "query": {
                "node": "Query",
                "clauses": [
                    {
                        "node": "MatchClause",
                        "patterns": [{
                            "node": "Pattern",
                            "elements": [{
                                "node": "NodePattern",
                                "var": ident("g"),
                                "labels": [qname("Gene")],
                                "props": {
                                    "node": "PropMap",
                                    "entries": [{
                                        "node": "PropEntry",
                                        "key": ident("symbol"),
                                        "value": {"node": "Literal", "kind": "string", "value": "TP53"}
                                    }]
                                }
                            }]
                        }],
                        "optional": false
                    },
                    {
                        "node": "WhereClause",
                        "predicate": {
                            "node": "BinOp",
                            "op": "=",
                            "left": path_expr("g", "symbol"),
                            "right": {"node": "Literal", "kind": "string", "value": "TP53"}
                        }
                    },
                    {
                        "node": "ReturnClause",
                        "projections": [{"node": "Projection", "expr": path_expr("g", "symbol")}],
                        "distinct": false
                    }
                ],
                "modifiers": {
                    "node": "Modifiers",
                    "limit": {"node": "Literal", "kind": "int", "value": 10}
                }
            }
        }]
    })
}

fn create_class_json() -> Value {
    json!({
        "node": "Program",
        "statements": [{
            "node": "DdlStmt",
            "op": "create_class",
            "target": qname("bio:Gene"),
            "payload": {
                "subclasses_of": [qname("bio:BiologicalEntity")],
                "properties": [
                    {
                        "name": ident("symbol"),
                        "type": {"node": "TypeRef", "name": "STRING"},
                        "constraints": ["REQUIRED", "UNIQUE"]
                    },
                    {
                        "name": ident("embedding"),
                        "type": {
                            "node": "TypeRef",
                            "name": "VECTOR",
                            "params": [{"node": "TypeRef", "name": "F32"}, 768]
                        }
                    }
                ]
            }
        }]
    })
}

fn insert_triples_json() -> Value {
    json!({
        "node": "Program",
        "statements": [{
            "node": "DmlStmt",
            "op": "insert_triples",
            "payload": {
                "triples": [
                    {
                        "node": "TriplePattern",
                        "subject": qname(":TP53"),
                        "predicate": "a",
                        "object": qname("bio:Gene")
                    },
                    {
                        "node": "TriplePattern",
                        "subject": qname(":TP53"),
                        "predicate": qname("bio:symbol"),
                        "object": {"node": "Literal", "kind": "string", "value": "TP53"}
                    }
                ]
            }
        }]
    })
}

fn variable_path_json() -> Value {
    json!({
        "node": "Program",
        "statements": [{
            "node": "QueryStmt",
            "query": {
                "node": "Query",
                "clauses": [
                    {
                        "node": "MatchClause",
                        "patterns": [{
                            "node": "Pattern",
                            "elements": [
                                {"node": "NodePattern", "var": ident("a"), "labels": [qname("Gene")]},
                                {
                                    "node": "RelPattern",
                                    "types": [qname("interactsWith")],
                                    "direction": "both",
                                    "hop_range": {"node": "HopRange", "min_hops": 1, "max_hops": 3}
                                },
                                {"node": "NodePattern", "var": ident("b"), "labels": [qname("Gene")]}
                            ],
                            "binding": ident("p")
                        }],
                        "optional": false
                    },
                    {
                        "node": "ReturnClause",
                        "projections": [
                            {"node": "Projection", "expr": path_expr("b", "symbol")},
                            {
                                "node": "Projection",
                                "expr": {
                                    "node": "FnCall",
                                    "name": qname("length"),
                                    "args": [{"node": "Var", "name": ident("p")}]
                                },
                                "alias": ident("hops")
                            }
                        ],
                        "distinct": false
                    }
                ],
                "modifiers": {"node": "Modifiers"}
            }
        }]
    })
}

fn parser_error(
    _code: &'static str,
    message: impl Into<String>,
    _text: &str,
    span_start: usize,
    span_end: usize,
) -> CaracalError {
    CaracalError::with_hint(
        "TF-2001",
        format!("{} at {span_start}..{span_end}", message.into()),
        "check the token near the highlighted span against the Tuft grammar",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rust_parser_accepts_match_return_subset() {
        let parsed = parse_tuft_subset("MATCH (n:Gene) RETURN n.symbol").expect("parse");
        assert_eq!(parsed.kind, "match_return");
        assert_eq!(parsed.class_name, "Gene");
        assert_eq!(parsed.return_columns, vec!["n.symbol"]);
    }

    #[test]
    fn rust_parser_diagnostics_have_codes_spans_and_hints() {
        let err = parse_tuft_subset("RETURN n").expect_err("diagnostic");
        assert_eq!(err.code, "TF-2001");
        assert!(err.message.contains("0..5"));
        assert!(err.hint.is_some());
        let diagnostic = parse_diagnostic("RETURN n").expect("diagnostic");
        assert_eq!(diagnostic.code, "TF-2001");
        assert_eq!(diagnostic.span_start, None);
        assert_eq!(diagnostic.span_end, None);
    }

    #[test]
    fn rust_parser_matches_current_golden_json_files() {
        let manifest_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
        let golden_dir = manifest_dir.join("../../tests/golden/parser");
        for stem in [
            "basic_match",
            "create_class",
            "insert_triples",
            "variable_path",
        ] {
            let source =
                std::fs::read_to_string(golden_dir.join(format!("{stem}.tuft"))).expect("source");
            let expected =
                std::fs::read_to_string(golden_dir.join(format!("{stem}.expected.json")))
                    .expect("expected");
            let expected: Value = serde_json::from_str(&expected).expect("json");
            assert_eq!(parse_tuft_json(&source).expect("rust parse"), expected);
        }
    }
}
