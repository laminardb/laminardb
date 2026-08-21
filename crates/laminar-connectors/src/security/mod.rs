//! Shared secret classification for catalog and checkpoint metadata.

const REDACTED: &str = "<redacted>";

fn normalized_key(key: &str) -> String {
    key.to_ascii_lowercase().replace(['.', '-'], "_")
}

/// Whether `value` is one exact environment reference without a default.
#[must_use]
pub fn is_env_reference(value: &str) -> bool {
    let Some(variable) = value
        .strip_prefix("${")
        .and_then(|value| value.strip_suffix('}'))
    else {
        return false;
    };
    let mut chars = variable.chars();
    chars
        .next()
        .is_some_and(|first| first == '_' || first.is_ascii_alphabetic())
        && chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
}

/// Whether a connector option name conventionally carries credential material.
#[must_use]
pub fn is_secret_option_key(key: &str) -> bool {
    let key = normalized_key(key);
    key.contains("password")
        || key.contains("passwd")
        || key.contains("secret")
        || key == "token"
        || key.ends_with("_token")
        || key.contains("credential")
        || key.contains("private_key")
        || key.contains("sasl_jaas")
        || key.contains("oauthbearer")
        || key.contains("api_key")
        || key.contains("account_key")
        || key.contains("session_key")
}

fn percent_decode_key(key: &str) -> String {
    let bytes = key.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' && index + 2 < bytes.len() {
            let hex = |byte: u8| match byte {
                b'0'..=b'9' => Some(byte - b'0'),
                b'a'..=b'f' => Some(byte - b'a' + 10),
                b'A'..=b'F' => Some(byte - b'A' + 10),
                _ => None,
            };
            if let (Some(high), Some(low)) = (hex(bytes[index + 1]), hex(bytes[index + 2])) {
                decoded.push((high << 4) | low);
                index += 3;
                continue;
            }
        }
        decoded.push(bytes[index]);
        index += 1;
    }
    String::from_utf8_lossy(&decoded).into_owned()
}

/// Whether a URI query key conventionally carries a signature or credential.
#[must_use]
pub fn is_sensitive_uri_query_key(key: &str) -> bool {
    let key = normalized_key(&percent_decode_key(key));
    key == "sig"
        || key.contains("signature")
        || key.contains("password")
        || key.contains("token")
        || key.contains("secret")
        || key.contains("credential")
        || key == "key"
        || key.ends_with("_key")
}

fn uri_segment_contains_secret(segment: &str, allow_reference: bool) -> bool {
    let Some((_, after_scheme)) = segment.split_once("://") else {
        return false;
    };
    let authority = after_scheme
        .split(['/', '?', '#'])
        .next()
        .unwrap_or_default();
    if let Some((userinfo, _)) = authority.rsplit_once('@') {
        let secret = userinfo
            .split_once(':')
            .map_or(userinfo, |(_, password)| password);
        if !(secret.is_empty() || allow_reference && is_env_reference(secret)) {
            return true;
        }
    }
    after_scheme
        .split_once('?')
        .map(|(_, query)| query.split('#').next().unwrap_or_default())
        .into_iter()
        .flat_map(|query| query.split(['&', ';']))
        .filter_map(|parameter| parameter.split_once('='))
        .any(|(key, secret)| {
            is_sensitive_uri_query_key(key)
                && !secret.is_empty()
                && !(allow_reference && is_env_reference(secret))
        })
}

/// Whether any URI in a comma/whitespace-delimited connector value embeds literal credentials.
#[must_use]
pub fn value_contains_uri_secret(value: &str, allow_reference: bool) -> bool {
    value
        .split(|ch: char| ch == ',' || ch.is_ascii_whitespace())
        .filter(|segment| segment.contains("://"))
        .any(|segment| uri_segment_contains_secret(segment, allow_reference))
}

fn sanitize_query(query: &str) -> String {
    let mut output = String::with_capacity(query.len());
    let mut start = 0;
    for (index, separator) in query.match_indices(['&', ';']) {
        sanitize_parameter(&query[start..index], &mut output);
        output.push_str(separator);
        start = index + separator.len();
    }
    sanitize_parameter(&query[start..], &mut output);
    output
}

fn sanitize_parameter(parameter: &str, output: &mut String) {
    let Some((key, value)) = parameter.split_once('=') else {
        output.push_str(parameter);
        return;
    };
    output.push_str(key);
    output.push('=');
    if is_sensitive_uri_query_key(key) && !value.is_empty() {
        output.push_str(REDACTED);
    } else {
        output.push_str(value);
    }
}

fn sanitize_uri_segment(segment: &str) -> String {
    let Some(scheme_end) = segment.find("://").map(|index| index + 3) else {
        return segment.to_string();
    };
    let authority_end = segment[scheme_end..]
        .find(['/', '?', '#'])
        .map_or(segment.len(), |index| scheme_end + index);
    let authority = &segment[scheme_end..authority_end];
    let mut output = String::with_capacity(segment.len());
    output.push_str(&segment[..scheme_end]);
    if let Some((_, host)) = authority.rsplit_once('@') {
        output.push_str(REDACTED);
        output.push('@');
        output.push_str(host);
    } else {
        output.push_str(authority);
    }

    let suffix = &segment[authority_end..];
    let Some(query_start) = suffix.find('?') else {
        output.push_str(suffix);
        return output;
    };
    output.push_str(&suffix[..=query_start]);
    let query_and_fragment = &suffix[query_start + 1..];
    let (query, fragment) = query_and_fragment
        .split_once('#')
        .map_or((query_and_fragment, None), |(query, fragment)| {
            (query, Some(fragment))
        });
    output.push_str(&sanitize_query(query));
    if let Some(fragment) = fragment {
        output.push('#');
        output.push_str(fragment);
    }
    output
}

/// Return a stable connector identity value with credential material removed.
#[must_use]
pub fn sanitize_identity_value(key: &str, value: &str) -> String {
    if is_secret_option_key(key) {
        return REDACTED.to_string();
    }
    let mut output = String::with_capacity(value.len());
    let mut start = 0;
    for (index, delimiter) in value
        .char_indices()
        .filter(|(_, ch)| *ch == ',' || ch.is_ascii_whitespace())
    {
        output.push_str(&sanitize_uri_segment(&value[start..index]));
        output.push(delimiter);
        start = index + delimiter.len_utf8();
    }
    output.push_str(&sanitize_uri_segment(&value[start..]));
    output
}

#[cfg(test)]
mod tests;
