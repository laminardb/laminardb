//! Namespace-aware structured selectors.
//!
//! ```text
//! path      := selector ('|' selector)*
//! selector  := ['/'] step (('/' | '->') step)*
//! step      := '.' pred* | '..' | '**' | '*' | name pred* | '{' set '}' pred*
//!            | '@' name | '#text' | '#name' | '#type' | '#locator'
//! pred      := '[' name ']' | '[' '{' set '}' ']'
//! name      := [prefix ':'] local
//! ```
//!
//! Element names resolve through the profile's namespace table (an unprefixed element name uses
//! its default namespace); unprefixed attributes have no namespace. A leading `/` starts at the
//! document root; `|` concatenates the results of alternatives in order (FpML's
//! value-or-reference choices). `**` is descendant-or-self,
//! `[name]` keeps elements that have such a child (`[{set}]`: a child in the set), and `->`
//! follows the current element's `href`
//! to the element with that `id`. A value step (`@`, `#…`) may only end a selector; a selector
//! ending in an element yields that element's character data.

use std::collections::BTreeMap;

/// A parsed, not yet interned, selector.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ParsedSelector {
    pub(crate) steps: Vec<ParsedStep>,
    pub(crate) terminal: ParsedTerminal,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ParsedStep {
    Root,
    Child {
        test: ParsedTest,
        predicates: Vec<ParsedTest>,
    },
    Descendants,
    Parent,
    Deref,
    /// `.[name]…`: keep the current elements that have such children.
    Filter(Vec<ParsedTest>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ParsedTest {
    Name(ParsedName),
    Set(String),
    Any,
}

/// `(namespace, local)` after prefix resolution.
pub(crate) type ParsedName = (String, String);

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ParsedTerminal {
    Text,
    Attribute(ParsedName),
    Name,
    Type,
    Locator,
}

/// Parse a `|`-separated path into its alternatives.
pub(crate) fn parse_path(
    text: &str,
    namespaces: &BTreeMap<String, String>,
) -> Result<Vec<ParsedSelector>, String> {
    text.split('|')
        .map(|part| parse(part, namespaces))
        .collect()
}

/// Parse one selector, resolving prefixes with `namespaces` (`""` is the default element
/// namespace).
pub(crate) fn parse(
    text: &str,
    namespaces: &BTreeMap<String, String>,
) -> Result<ParsedSelector, String> {
    let mut text = text.trim();
    if text.is_empty() {
        return Err("selector is empty".into());
    }
    let mut steps = Vec::new();
    if let Some(rest) = text.strip_prefix('/') {
        steps.push(ParsedStep::Root);
        text = rest;
        if text.is_empty() {
            return Ok(ParsedSelector {
                steps,
                terminal: ParsedTerminal::Text,
            });
        }
    }
    let mut terminal = ParsedTerminal::Text;
    let mut rest = text;
    let mut first = true;
    while !rest.is_empty() {
        if !first {
            if let Some(after) = rest.strip_prefix("->") {
                steps.push(ParsedStep::Deref);
                rest = after;
                if rest.is_empty() {
                    break;
                }
            } else if let Some(after) = rest.strip_prefix('/') {
                rest = after;
            } else {
                return Err(format!("expected '/' or '->' before '{rest}'"));
            }
        }
        first = false;
        let end = rest
            .char_indices()
            .find(|(index, c)| *c == '/' || rest[*index..].starts_with("->"))
            .map_or(rest.len(), |(index, _)| index);
        let token = &rest[..end];
        rest = &rest[end..];
        if !matches!(terminal, ParsedTerminal::Text) {
            return Err(format!("'{token}' follows a value step"));
        }
        match token {
            "" => return Err("empty step".into()),
            "." => {}
            ".." => steps.push(ParsedStep::Parent),
            "**" => steps.push(ParsedStep::Descendants),
            "#text" => terminal = ParsedTerminal::Text,
            "#name" => terminal = ParsedTerminal::Name,
            "#type" => terminal = ParsedTerminal::Type,
            "#locator" => terminal = ParsedTerminal::Locator,
            _ if token.starts_with('#') => return Err(format!("unknown value step '{token}'")),
            _ if token.starts_with(".[") => steps.push(ParsedStep::Filter(predicates(
                token,
                &token[1..],
                namespaces,
            )?)),
            _ if token.starts_with('@') => {
                terminal = ParsedTerminal::Attribute(name(&token[1..], namespaces, false)?);
            }
            _ => steps.push(child(token, namespaces)?),
        }
        if token.starts_with('#') || token.starts_with('@') {
            // A value step must be last; enforced on the next iteration.
            if !rest.is_empty() {
                return Err(format!("'{token}' must be the last step"));
            }
        }
    }
    Ok(ParsedSelector { steps, terminal })
}

fn child(token: &str, namespaces: &BTreeMap<String, String>) -> Result<ParsedStep, String> {
    let (head, predicates) = match token.find('[') {
        Some(open) => (&token[..open], &token[open..]),
        None => (token, ""),
    };
    let test = if head == "*" {
        ParsedTest::Any
    } else {
        name_or_set(head, namespaces)?
    };
    Ok(ParsedStep::Child {
        test,
        predicates: self::predicates(token, predicates, namespaces)?,
    })
}

fn name_or_set(text: &str, namespaces: &BTreeMap<String, String>) -> Result<ParsedTest, String> {
    let Some(set) = text.strip_prefix('{').and_then(|set| set.strip_suffix('}')) else {
        return Ok(ParsedTest::Name(name(text, namespaces, true)?));
    };
    if set.is_empty() {
        return Err("empty set name".into());
    }
    Ok(ParsedTest::Set(set.to_owned()))
}

fn predicates(
    token: &str,
    mut rest: &str,
    namespaces: &BTreeMap<String, String>,
) -> Result<Vec<ParsedTest>, String> {
    let mut parsed = Vec::new();
    while !rest.is_empty() {
        let inner = rest
            .strip_prefix('[')
            .and_then(|after| after.split_once(']'))
            .ok_or_else(|| format!("malformed predicate in '{token}'"))?;
        parsed.push(name_or_set(inner.0, namespaces)?);
        rest = inner.1;
    }
    Ok(parsed)
}

fn name(
    text: &str,
    namespaces: &BTreeMap<String, String>,
    element: bool,
) -> Result<ParsedName, String> {
    let (prefix, local) = match text.split_once(':') {
        Some((prefix, local)) => (Some(prefix), local),
        None => (None, text),
    };
    if !is_ncname(local) {
        return Err(format!("'{text}' is not a valid XML name"));
    }
    let namespace = match prefix {
        Some(prefix) => namespaces
            .get(prefix)
            .cloned()
            .ok_or_else(|| format!("prefix '{prefix}' is not declared in [namespaces]"))?,
        None if element => namespaces.get("").cloned().unwrap_or_default(),
        None => String::new(),
    };
    Ok((namespace, local.to_owned()))
}

fn is_ncname(text: &str) -> bool {
    let mut chars = text.chars();
    chars
        .next()
        .is_some_and(|first| first.is_alphabetic() || first == '_')
        && chars.all(|c| c.is_alphanumeric() || matches!(c, '_' | '-' | '.'))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn namespaces() -> BTreeMap<String, String> {
        BTreeMap::from([
            (String::new(), "urn:fpml".to_owned()),
            ("ext".to_owned(), "urn:ext".to_owned()),
        ])
    }

    fn fpml(local: &str) -> ParsedName {
        ("urn:fpml".to_owned(), local.to_owned())
    }

    #[test]
    fn parses_paths_sets_predicates_references_and_values() {
        let parsed = parse(
            "**/trade[tradeHeader]/{product}/payerPartyReference->ext:alias/@scheme",
            &namespaces(),
        )
        .unwrap();
        assert_eq!(
            parsed.steps,
            vec![
                ParsedStep::Descendants,
                ParsedStep::Child {
                    test: ParsedTest::Name(fpml("trade")),
                    predicates: vec![ParsedTest::Name(fpml("tradeHeader"))]
                },
                ParsedStep::Child {
                    test: ParsedTest::Set("product".into()),
                    predicates: vec![]
                },
                ParsedStep::Child {
                    test: ParsedTest::Name(fpml("payerPartyReference")),
                    predicates: vec![]
                },
                ParsedStep::Deref,
                ParsedStep::Child {
                    test: ParsedTest::Name(("urn:ext".into(), "alias".into())),
                    predicates: vec![]
                },
            ]
        );
        assert_eq!(
            parsed.terminal,
            ParsedTerminal::Attribute((String::new(), "scheme".into()))
        );
        let absolute = parse_path("/header/messageId | ext:alias", &namespaces()).unwrap();
        assert_eq!(absolute.len(), 2);
        assert_eq!(absolute[0].steps[0], ParsedStep::Root);
        let reference = parse("partyReference->", &namespaces()).unwrap();
        assert_eq!(reference.steps.last(), Some(&ParsedStep::Deref));
        assert_eq!(parse(".", &namespaces()).unwrap().steps, vec![]);
        assert_eq!(
            parse(".[basketConstituent]", &namespaces()).unwrap().steps,
            vec![ParsedStep::Filter(vec![ParsedTest::Name(fpml(
                "basketConstituent"
            ))])]
        );
        assert_eq!(
            parse("trade[tradeHeader][{rates}]", &namespaces())
                .unwrap()
                .steps,
            vec![ParsedStep::Child {
                test: ParsedTest::Name(fpml("trade")),
                predicates: vec![
                    ParsedTest::Name(fpml("tradeHeader")),
                    ParsedTest::Set("rates".into())
                ]
            }]
        );
        assert_eq!(
            parse("{product}/#name", &namespaces()).unwrap().terminal,
            ParsedTerminal::Name
        );
    }

    #[test]
    fn rejects_malformed_selectors() {
        for selector in [
            "", "a//b", "@x/b", "#name/b", "u:a", "a[b", "{}", "#unknown", "1a", "a[*]", "a[{}]",
        ] {
            assert!(parse(selector, &namespaces()).is_err(), "{selector}");
        }
    }
}
