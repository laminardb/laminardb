//! Attribute-Based Access Control (ABAC).
//!
//! Builds on RBAC by adding attribute-based conditions. An ABAC policy
//! contains rules that evaluate predicates over user attributes and
//! resource metadata.
//!
//! # Example
//!
//! ```text
//! allow if user.department == "engineering" AND resource.sensitivity < 3
//! ```

use std::collections::HashMap;

use crate::identity::{AuthError, Identity};

/// Comparison operators for attribute conditions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Operator {
    /// Equality check.
    Eq,
    /// Inequality check.
    NotEq,
    /// Less than (numeric comparison).
    Lt,
    /// Greater than (numeric comparison).
    Gt,
    /// Less than or equal (numeric comparison).
    LtEq,
    /// Greater than or equal (numeric comparison).
    GtEq,
    /// String contains check.
    Contains,
}

/// Source of an attribute value in a condition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AttributeSource {
    /// From the user's identity attributes.
    User(String),
    /// From the resource metadata.
    Resource(String),
    /// A literal string value.
    Literal(String),
}

/// A single condition comparing an attribute to a value.
#[derive(Debug, Clone)]
pub struct Condition {
    /// Left-hand side of the comparison.
    pub lhs: AttributeSource,
    /// Comparison operator.
    pub op: Operator,
    /// Right-hand side of the comparison.
    pub rhs: AttributeSource,
}

/// An ABAC rule: a set of conditions that must all hold (AND semantics).
#[derive(Debug, Clone)]
pub struct AbacRule {
    /// Human-readable name for this rule.
    pub name: String,
    /// Action this rule applies to (e.g., "SELECT", "INSERT", "*").
    pub action: String,
    /// Optional resource pattern (None = applies to all resources).
    pub resource_pattern: Option<String>,
    /// Conditions that must all evaluate to true.
    pub conditions: Vec<Condition>,
    /// Effect: allow or deny.
    pub effect: Effect,
}

/// The effect of a matching rule.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Effect {
    /// Allow the action.
    Allow,
    /// Deny the action.
    Deny,
}

/// ABAC policy: a collection of rules evaluated in order.
///
/// Evaluation strategy: first-match wins. If no rule matches, the default
/// effect is **deny**.
#[derive(Debug, Clone, Default)]
pub struct AbacPolicy {
    rules: Vec<AbacRule>,
}

impl AbacPolicy {
    /// Create an empty ABAC policy.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a rule to the policy.
    pub fn add_rule(&mut self, rule: AbacRule) {
        self.rules.push(rule);
    }

    /// Evaluate the policy for a given identity, action, and resource.
    ///
    /// `resource_attrs` provides metadata about the resource being accessed.
    pub fn evaluate(
        &self,
        identity: &Identity,
        action: &str,
        resource: Option<&str>,
        resource_attrs: &HashMap<String, String>,
    ) -> Result<(), AuthError> {
        // Superuser bypass
        if identity.is_superuser() {
            return Ok(());
        }

        for rule in &self.rules {
            // Check action match
            if rule.action != "*" && rule.action != action {
                continue;
            }

            // Check resource pattern match
            if let Some(pattern) = &rule.resource_pattern {
                match resource {
                    Some(r) if r == pattern || pattern == "*" => {}
                    _ => continue,
                }
            }

            // Evaluate all conditions
            let all_match = rule
                .conditions
                .iter()
                .all(|c| evaluate_condition(c, identity, resource_attrs));

            if all_match {
                return match rule.effect {
                    Effect::Allow => Ok(()),
                    Effect::Deny => Err(AuthError::AccessDenied(format!(
                        "denied by ABAC rule '{}' for user '{}'",
                        rule.name, identity.name
                    ))),
                };
            }
        }

        // Default deny
        Err(AuthError::AccessDenied(format!(
            "no ABAC rule matched for user '{}' action '{}' on {}",
            identity.name,
            action,
            resource.unwrap_or("(global)")
        )))
    }
}

/// Resolve an [`AttributeSource`] to a concrete string value.
fn resolve_attribute(
    source: &AttributeSource,
    identity: &Identity,
    resource_attrs: &HashMap<String, String>,
) -> Option<String> {
    match source {
        AttributeSource::User(key) => identity.attributes.get(key).cloned(),
        AttributeSource::Resource(key) => resource_attrs.get(key).cloned(),
        AttributeSource::Literal(val) => Some(val.clone()),
    }
}

/// Evaluate a single condition.
fn evaluate_condition(
    condition: &Condition,
    identity: &Identity,
    resource_attrs: &HashMap<String, String>,
) -> bool {
    let lhs = match resolve_attribute(&condition.lhs, identity, resource_attrs) {
        Some(v) => v,
        None => return false,
    };
    let rhs = match resolve_attribute(&condition.rhs, identity, resource_attrs) {
        Some(v) => v,
        None => return false,
    };

    match condition.op {
        Operator::Eq => lhs == rhs,
        Operator::NotEq => lhs != rhs,
        Operator::Contains => lhs.contains(&rhs),
        // Numeric comparisons: try parsing, fall back to string comparison
        Operator::Lt => compare_numeric_or_string(&lhs, &rhs, |a, b| a < b, |a, b| a < b),
        Operator::Gt => compare_numeric_or_string(&lhs, &rhs, |a, b| a > b, |a, b| a > b),
        Operator::LtEq => compare_numeric_or_string(&lhs, &rhs, |a, b| a <= b, |a, b| a <= b),
        Operator::GtEq => compare_numeric_or_string(&lhs, &rhs, |a, b| a >= b, |a, b| a >= b),
    }
}

/// Compare two values numerically if possible, otherwise as strings.
fn compare_numeric_or_string(
    lhs: &str,
    rhs: &str,
    num_cmp: fn(f64, f64) -> bool,
    str_cmp: fn(&str, &str) -> bool,
) -> bool {
    if let (Ok(l), Ok(r)) = (lhs.parse::<f64>(), rhs.parse::<f64>()) {
        num_cmp(l, r)
    } else {
        str_cmp(lhs, rhs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::AuthMethod;

    fn engineer_identity() -> Identity {
        Identity::new("alice", AuthMethod::Ldap)
            .with_attribute("department", "engineering")
            .with_attribute("clearance", "3")
            .with_attribute("tenant_id", "acme")
    }

    fn sales_identity() -> Identity {
        Identity::new("bob", AuthMethod::Ldap)
            .with_attribute("department", "sales")
            .with_attribute("clearance", "1")
            .with_attribute("tenant_id", "acme")
    }

    fn resource_attrs(sensitivity: &str) -> HashMap<String, String> {
        let mut m = HashMap::new();
        m.insert("sensitivity".to_string(), sensitivity.to_string());
        m
    }

    #[test]
    fn test_abac_allow_by_department() {
        let mut policy = AbacPolicy::new();
        policy.add_rule(AbacRule {
            name: "engineering-read".to_string(),
            action: "SELECT".to_string(),
            resource_pattern: None,
            conditions: vec![Condition {
                lhs: AttributeSource::User("department".to_string()),
                op: Operator::Eq,
                rhs: AttributeSource::Literal("engineering".to_string()),
            }],
            effect: Effect::Allow,
        });

        let eng = engineer_identity();
        let sales = sales_identity();
        let attrs = HashMap::new();

        assert!(policy.evaluate(&eng, "SELECT", None, &attrs).is_ok());
        assert!(policy.evaluate(&sales, "SELECT", None, &attrs).is_err());
    }

    #[test]
    fn test_abac_sensitivity_check() {
        let mut policy = AbacPolicy::new();
        policy.add_rule(AbacRule {
            name: "clearance-check".to_string(),
            action: "SELECT".to_string(),
            resource_pattern: Some("secret_data".to_string()),
            conditions: vec![Condition {
                lhs: AttributeSource::User("clearance".to_string()),
                op: Operator::GtEq,
                rhs: AttributeSource::Resource("sensitivity".to_string()),
            }],
            effect: Effect::Allow,
        });

        let eng = engineer_identity(); // clearance=3
        let sales = sales_identity(); // clearance=1

        let attrs = resource_attrs("2");
        assert!(policy
            .evaluate(&eng, "SELECT", Some("secret_data"), &attrs)
            .is_ok());
        assert!(policy
            .evaluate(&sales, "SELECT", Some("secret_data"), &attrs)
            .is_err());
    }

    #[test]
    fn test_abac_deny_rule() {
        let mut policy = AbacPolicy::new();
        policy.add_rule(AbacRule {
            name: "block-sales".to_string(),
            action: "*".to_string(),
            resource_pattern: Some("*".to_string()),
            conditions: vec![Condition {
                lhs: AttributeSource::User("department".to_string()),
                op: Operator::Eq,
                rhs: AttributeSource::Literal("sales".to_string()),
            }],
            effect: Effect::Deny,
        });

        let sales = sales_identity();
        let attrs = HashMap::new();
        let result = policy.evaluate(&sales, "SELECT", Some("trades"), &attrs);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("denied by ABAC"));
    }

    #[test]
    fn test_abac_superuser_bypass() {
        let policy = AbacPolicy::new(); // empty = default deny
        let su = Identity::new("root", AuthMethod::Anonymous).with_role("superuser");
        let attrs = HashMap::new();
        assert!(policy.evaluate(&su, "DROP", Some("trades"), &attrs).is_ok());
    }

    #[test]
    fn test_abac_first_match_wins() {
        let mut policy = AbacPolicy::new();
        // First rule: allow engineering
        policy.add_rule(AbacRule {
            name: "allow-eng".to_string(),
            action: "SELECT".to_string(),
            resource_pattern: None,
            conditions: vec![Condition {
                lhs: AttributeSource::User("department".to_string()),
                op: Operator::Eq,
                rhs: AttributeSource::Literal("engineering".to_string()),
            }],
            effect: Effect::Allow,
        });
        // Second rule: deny everyone
        policy.add_rule(AbacRule {
            name: "deny-all".to_string(),
            action: "*".to_string(),
            resource_pattern: None,
            conditions: vec![],
            effect: Effect::Deny,
        });

        let eng = engineer_identity();
        let attrs = HashMap::new();
        // Engineering matches first rule → allowed
        assert!(policy.evaluate(&eng, "SELECT", None, &attrs).is_ok());
    }
}
