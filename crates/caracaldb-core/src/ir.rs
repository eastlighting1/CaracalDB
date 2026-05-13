use serde::{Deserialize, Serialize};

use crate::error::Result;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum LogicalPlan {
    NodeScan {
        class_iri: String,
        local_name: String,
        as_of_lsn: Option<u64>,
    },
    EdgeExpand {
        edge_type: String,
        direction: Direction,
    },
    Selection {
        predicate: Expr,
        input: Box<LogicalPlan>,
    },
    Projection {
        columns: Vec<String>,
        input: Box<LogicalPlan>,
    },
    Limit {
        skip: u64,
        limit: Option<u64>,
        input: Box<LogicalPlan>,
    },
    VariablePath {
        edge_type: String,
        min_hops: u32,
        max_hops: u32,
        input: Box<LogicalPlan>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum PhysicalPlan {
    NodeScan {
        class_iri: String,
        local_name: String,
        snapshot_lsn: Option<u64>,
    },
    Expand {
        edge_type: String,
        direction: Direction,
        input: Box<PhysicalPlan>,
    },
    Filter {
        predicate: Expr,
        input: Box<PhysicalPlan>,
    },
    Project {
        columns: Vec<String>,
        input: Box<PhysicalPlan>,
    },
    TopK {
        order_by: Vec<OrderExpr>,
        skip: u64,
        limit: Option<u64>,
        input: Box<PhysicalPlan>,
    },
    VarPath {
        edge_type: String,
        min_hops: u32,
        max_hops: u32,
        input: Box<PhysicalPlan>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Direction {
    Out,
    In,
    Both,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Expr {
    Column { name: String },
    Literal { value: Literal },
    Eq { left: Box<Expr>, right: Box<Expr> },
    And { left: Box<Expr>, right: Box<Expr> },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum Literal {
    Null,
    Bool(bool),
    Int(i64),
    String(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderExpr {
    pub expr: Expr,
    pub descending: bool,
    pub nulls_last: bool,
}

pub fn logical_plan_to_json(plan: &LogicalPlan) -> Result<String> {
    Ok(serde_json::to_string(plan)?)
}

pub fn physical_plan_to_json(plan: &PhysicalPlan) -> Result<String> {
    Ok(serde_json::to_string(plan)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn logical_ir_serializes_stably_with_snapshot_and_var_path() {
        let plan = LogicalPlan::VariablePath {
            edge_type: "RELATED_TO".to_string(),
            min_hops: 1,
            max_hops: 3,
            input: Box::new(LogicalPlan::NodeScan {
                class_iri: "http://example.org/Gene".to_string(),
                local_name: "Gene".to_string(),
                as_of_lsn: Some(42),
            }),
        };
        assert_eq!(
            logical_plan_to_json(&plan).expect("json"),
            concat!(
                r#"{"op":"variable_path","edge_type":"RELATED_TO","min_hops":1,"max_hops":3,"#,
                r#""input":{"op":"node_scan","class_iri":"http://example.org/Gene","#,
                r#""local_name":"Gene","as_of_lsn":42}}"#
            )
        );
    }

    #[test]
    fn physical_ir_serializes_stably_with_limit_order_shape() {
        let plan = PhysicalPlan::TopK {
            order_by: vec![OrderExpr {
                expr: Expr::Column {
                    name: "score".to_string(),
                },
                descending: true,
                nulls_last: true,
            }],
            skip: 2,
            limit: Some(5),
            input: Box::new(PhysicalPlan::NodeScan {
                class_iri: "http://example.org/Gene".to_string(),
                local_name: "Gene".to_string(),
                snapshot_lsn: None,
            }),
        };
        assert_eq!(
            physical_plan_to_json(&plan).expect("json"),
            concat!(
                r#"{"op":"top_k","order_by":[{"expr":{"kind":"column","name":"score"},"#,
                r#""descending":true,"nulls_last":true}],"skip":2,"limit":5,"#,
                r#""input":{"op":"node_scan","class_iri":"http://example.org/Gene","#,
                r#""local_name":"Gene","snapshot_lsn":null}}"#
            )
        );
    }
}
