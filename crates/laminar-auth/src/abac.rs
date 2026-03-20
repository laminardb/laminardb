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
                if pattern == "*" {
                    // Wildcard matches everything, including None (global actions).
                } else {
                    match resource {
                        Some(r) if r == pattern => {}
                        _ => continue,
                    }
                }
            }

            // Evaluate all conditions.
            // On error (missing/malformed attribute), fail closed:
            //   Deny rules trigger (deny when uncertain).
            //   Allow rules do not match (don't allow when uncertain).
            let mut all_match = true;
            let mut eval_error = false;
            for c in &rule.conditions {
                match evaluate_condition(c, identity, resource_attrs) {
                    Ok(true) => {}
                    Ok(false) => {
                        all_match = false;
                        break;
                    }
                    Err(_) => {
                        eval_error = true;
                        all_match = false;
                        break;
                    }
                }
            }

            // Fail closed: errors during Deny evaluation trigger the deny.
            if eval_error && rule.effect == Effect::Deny {
                return Err(AuthError::AccessDenied(format!(
                    "denied by ABAC rule '{}' for user '{}' (fail-closed on missing/malformed attribute)",
                    rule.name, identity.name
                )));
            }

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
///
/// Returns `Ok(true)` if the condition holds, `Ok(false)` if it does not,
/// or `Err` if an attribute is missing or a numeric comparison fails to parse.
/// The caller uses the error to implement fail-closed semantics on Deny rules.
fn evaluate_condition(
    condition: &Condition,
    identity: &Identity,
    resource_attrs: &HashMap<String, String>,
) -> Result<bool, AuthError> {
    let lhs = resolve_attribute(&condition.lhs, identity, resource_attrs).ok_or_else(|| {
        AuthError::AccessDenied(format!("missing attribute: {:?}", condition.lhs))
    })?;
    let rhs = resolve_attribute(&condition.rhs, identity, resource_attrs).ok_or_else(|| {
        AuthError::AccessDenied(format!("missing attribute: {:?}", condition.rhs))
    })?;

    let result = match condition.op {
        Operator::Eq => lhs == rhs,
        Operator::NotEq => lhs != rhs,
        Operator::Contains => lhs.contains(&rhs),
        Operator::Lt => compare_numeric(&lhs, &rhs, |a, b| a < b)?,
        Operator::Gt => compare_numeric(&lhs, &rhs, |a, b| a > b)?,
        Operator::LtEq => compare_numeric(&lhs, &rhs, |a, b| a <= b)?,
        Operator::GtEq => compare_numeric(&lhs, &rhs, |a, b| a >= b)?,
    };
    Ok(result)
}

/// Compare two values numerically.
///
/// Returns `Err` if either side is non-numeric, so the caller can fail closed.
fn compare_numeric(lhs: &str, rhs: &str, num_cmp: fn(f64, f64) -> bool) -> Result<bool, AuthError> {
    let l = lhs
        .parse::<f64>()
        .map_err(|_| AuthError::AccessDenied(format!("non-numeric attribute value: {lhs:?}")))?;
    let r = rhs
        .parse::<f64>()
        .map_err(|_| AuthError::AccessDenied(format!("non-numeric attribute value: {rhs:?}")))?;
    Ok(num_cmp(l, r))
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

    #[test]
    fn test_numeric_comparisons_return_error_for_non_numeric_values() {
        let identity = engineer_identity();
        let attrs = HashMap::new();
        let condition = Condition {
            lhs: AttributeSource::Literal("2foo".to_string()),
            op: Operator::Gt,
            rhs: AttributeSource::Literal("10".to_string()),
        };

        let result = evaluate_condition(&condition, &identity, &attrs);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("non-numeric"));
    }

    #[test]
    fn test_missing_attribute_returns_error() {
        let identity = engineer_identity();
        let attrs = HashMap::new();
        // User attribute "nonexistent" does not exist.
        let condition = Condition {
            lhs: AttributeSource::User("nonexistent".to_string()),
            op: Operator::Eq,
            rhs: AttributeSource::Literal("value".to_string()),
        };

        let result = evaluate_condition(&condition, &identity, &attrs);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("missing attribute"));
    }

    #[test]
    fn test_deny_rule_fail_closed_on_missing_attribute() {
        let mut policy = AbacPolicy::new();
        // Deny rule that checks an attribute that won't exist.
        policy.add_rule(AbacRule {
            name: "deny-missing-attr".to_string(),
            action: "*".to_string(),
            resource_pattern: None,
            conditions: vec![Condition {
                lhs: AttributeSource::User("security_level".to_string()),
                op: Operator::Lt,
                rhs: AttributeSource::Literal("5".to_string()),
            }],
            effect: Effect::Deny,
        });

        let identity = engineer_identity(); // no "security_level" attribute
        let attrs = HashMap::new();

        // Missing attribute on a Deny rule must fail closed (deny).
        let result = policy.evaluate(&identity, "SELECT", None, &attrs);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("fail-closed"));
    }

    #[test]
    fn test_allow_rule_does_not_match_on_missing_attribute() {
        let mut policy = AbacPolicy::new();
        // Allow rule that checks an attribute that won't exist.
        policy.add_rule(AbacRule {
            name: "allow-missing-attr".to_string(),
            action: "*".to_string(),
            resource_pattern: None,
            conditions: vec![Condition {
                lhs: AttributeSource::User("security_level".to_string()),
                op: Operator::GtEq,
                rhs: AttributeSource::Literal("5".to_string()),
            }],
            effect: Effect::Allow,
        });

        let identity = engineer_identity(); // no "security_level" attribute
        let attrs = HashMap::new();

        // Missing attribute on an Allow rule must not match (falls through to default deny).
        let result = policy.evaluate(&identity, "SELECT", None, &attrs);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("no ABAC rule matched"));
    }

    #[test]
    fn test_deny_rule_fail_closed_on_malformed_numeric() {
        let mut policy = AbacPolicy::new();
        // Deny rule with numeric comparison where user attribute is non-numeric.
        policy.add_rule(AbacRule {
            name: "deny-malformed".to_string(),
            action: "*".to_string(),
            resource_pattern: None,
            conditions: vec![Condition {
                lhs: AttributeSource::User("department".to_string()), // "engineering" — not a number
                op: Operator::Lt,
                rhs: AttributeSource::Literal("5".to_string()),
            }],
            effect: Effect::Deny,
        });

        let identity = engineer_identity();
        let attrs = HashMap::new();

        // Malformed numeric on a Deny rule must fail closed (deny).
        let result = policy.evaluate(&identity, "SELECT", None, &attrs);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("fail-closed"));
    }

    #[test]
    fn test_wildcard_resource_matches_none() {
        let mut policy = AbacPolicy::new();
        policy.add_rule(AbacRule {
            name: "deny-all-wildcard".to_string(),
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

        // resource: None with pattern "*" must match (global actions).
        let result = policy.evaluate(&sales, "SELECT", None, &attrs);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("denied by ABAC"));
    }

    /// Regression test for the "empty string bypass" attack:
    /// If both user.dept and resource.dept are missing, a naive implementation
    /// might default both to "" making them equal, granting access.
    /// Our implementation returns Err from resolve_attribute when either side
    /// is missing, so the Allow rule is skipped (fail-closed).
    #[test]
    fn test_both_attributes_missing_on_allow_does_not_grant_access() {
        let mut policy = AbacPolicy::new();
        policy.add_rule(AbacRule {
            name: "dept-match".to_string(),
            action: "SELECT".to_string(),
            resource_pattern: None,
            conditions: vec![Condition {
                // Compare user.dept == resource.dept
                // Both will be missing → must NOT match.
                lhs: AttributeSource::User("dept".to_string()),
                op: Operator::Eq,
                rhs: AttributeSource::Resource("dept".to_string()),
            }],
            effect: Effect::Allow,
        });

        // User has NO "dept" attribute, resource has NO "dept" attribute.
        let identity = Identity::new("eve", AuthMethod::Anonymous);
        let resource_attrs: HashMap<String, String> = HashMap::new();

        // Must be denied — the Allow rule must not match when attributes are missing.
        let result = policy.evaluate(&identity, "SELECT", None, &resource_attrs);
        assert!(
            result.is_err(),
            "both attributes missing must not grant access"
        );
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("no ABAC rule matched"),
            "should fall through to default deny, not match the Allow rule"
        );
    }

    /// Same scenario but with a Deny rule: both attributes missing should
    /// trigger the deny (fail-closed on Deny).
    #[test]
    fn test_both_attributes_missing_on_deny_triggers_deny() {
        let mut policy = AbacPolicy::new();
        // First: an Allow-all rule so we can see if the Deny fires.
        policy.add_rule(AbacRule {
            name: "deny-dept-match".to_string(),
            action: "SELECT".to_string(),
            resource_pattern: None,
            conditions: vec![Condition {
                lhs: AttributeSource::User("dept".to_string()),
                op: Operator::Eq,
                rhs: AttributeSource::Resource("dept".to_string()),
            }],
            effect: Effect::Deny,
        });

        let identity = Identity::new("eve", AuthMethod::Anonymous);
        let resource_attrs: HashMap<String, String> = HashMap::new();

        let result = policy.evaluate(&identity, "SELECT", None, &resource_attrs);
        assert!(
            result.is_err(),
            "both attributes missing on Deny must trigger deny"
        );
        assert!(
            result.unwrap_err().to_string().contains("fail-closed"),
            "must report fail-closed semantics"
        );
    }

    #[test]
    fn test_wildcard_resource_matches_specific_resource() {
        let mut policy = AbacPolicy::new();
        policy.add_rule(AbacRule {
            name: "allow-wildcard".to_string(),
            action: "SELECT".to_string(),
            resource_pattern: Some("*".to_string()),
            conditions: vec![Condition {
                lhs: AttributeSource::User("department".to_string()),
                op: Operator::Eq,
                rhs: AttributeSource::Literal("engineering".to_string()),
            }],
            effect: Effect::Allow,
        });

        let eng = engineer_identity();
        let attrs = HashMap::new();

        // Wildcard should also match specific resource names.
        assert!(policy
            .evaluate(&eng, "SELECT", Some("trades"), &attrs)
            .is_ok());
    }
}
