//! Implementation of `#[derive(ConnectorConfig)]`.
//!
//! This macro generates `from_config()`, `validate()`, and `config_keys()` methods
//! for connector configuration structs, eliminating boilerplate.

use proc_macro2::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Error, Fields, Ident, Lit, Type};

/// Attribute information for a config field.
#[derive(Default)]
struct ConfigAttr {
    /// The config key name (e.g., "bootstrap.servers").
    key: Option<String>,
    /// Whether this field is required.
    required: bool,
    /// Default value as a string literal.
    default: Option<String>,
    /// Environment variable to read from.
    env: Option<String>,
    /// Description for documentation.
    description: Option<String>,
    /// Whether the value should be parsed as Duration from milliseconds.
    duration_ms: bool,
}

/// Field metadata extracted from struct definition.
struct FieldInfo {
    ident: Ident,
    ty: Type,
    config: ConfigAttr,
    is_option: bool,
}

/// Expand the ConnectorConfig derive macro.
pub fn expand_connector_config(input: DeriveInput) -> Result<TokenStream, Error> {
    let name = &input.ident;

    let fields = match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(named) => &named.named,
            _ => {
                return Err(Error::new_spanned(
                    &input,
                    "ConnectorConfig can only be derived for structs with named fields",
                ));
            }
        },
        _ => {
            return Err(Error::new_spanned(
                &input,
                "ConnectorConfig can only be derived for structs",
            ));
        }
    };

    let mut field_infos = Vec::new();

    for field in fields {
        let ident = field.ident.clone().unwrap();
        let ty = field.ty.clone();
        let is_option = is_option_type(&ty);

        let mut config = ConfigAttr::default();

        for attr in &field.attrs {
            if attr.path().is_ident("config") {
                parse_config_attr(attr, &mut config)?;
            }
        }

        // Default key to field name with underscores replaced by dots
        if config.key.is_none() {
            config.key = Some(ident.to_string().replace('_', "."));
        }

        field_infos.push(FieldInfo {
            ident,
            ty,
            config,
            is_option,
        });
    }

    let from_config_body = generate_from_config(&field_infos)?;
    let validate_body = generate_validate(&field_infos)?;
    let config_keys_body = generate_config_keys(&field_infos)?;

    let expanded = quote! {
        impl #name {
            /// Creates a new instance from a [`ConnectorConfig`].
            ///
            /// # Errors
            ///
            /// Returns `ConnectorError` if required keys are missing or values cannot be parsed.
            pub fn from_config(
                config: &laminar_connectors::config::ConnectorConfig,
            ) -> Result<Self, laminar_connectors::error::ConnectorError> {
                #from_config_body
            }

            /// Validates the configuration.
            ///
            /// # Errors
            ///
            /// Returns `ConnectorError` if validation fails.
            pub fn validate(&self) -> Result<(), laminar_connectors::error::ConnectorError> {
                #validate_body
                Ok(())
            }

            /// Returns the configuration key specifications for this config type.
            #[must_use]
            pub fn config_keys() -> Vec<laminar_connectors::config::ConfigKeySpec> {
                #config_keys_body
            }
        }
    };

    Ok(expanded)
}

/// Parse the `#[config(...)]` attribute.
fn parse_config_attr(attr: &syn::Attribute, config: &mut ConfigAttr) -> Result<(), Error> {
    attr.parse_nested_meta(|meta| {
        if meta.path.is_ident("key") {
            let value: Lit = meta.value()?.parse()?;
            if let Lit::Str(lit) = value {
                config.key = Some(lit.value());
            }
        } else if meta.path.is_ident("required") {
            config.required = true;
        } else if meta.path.is_ident("default") {
            let value: Lit = meta.value()?.parse()?;
            if let Lit::Str(lit) = value {
                config.default = Some(lit.value());
            }
        } else if meta.path.is_ident("env") {
            let value: Lit = meta.value()?.parse()?;
            if let Lit::Str(lit) = value {
                config.env = Some(lit.value());
            }
        } else if meta.path.is_ident("description") {
            let value: Lit = meta.value()?.parse()?;
            if let Lit::Str(lit) = value {
                config.description = Some(lit.value());
            }
        } else if meta.path.is_ident("duration_ms") {
            config.duration_ms = true;
        }
        Ok(())
    })
}

/// Generate the `from_config` method body.
fn generate_from_config(fields: &[FieldInfo]) -> Result<TokenStream, Error> {
    let field_parsers: Vec<TokenStream> = fields
        .iter()
        .map(|f| {
            let ident = &f.ident;
            let key = f.config.key.as_ref().unwrap();
            let is_option = f.is_option;
            let has_default = f.config.default.is_some();
            let has_env = f.config.env.is_some();
            let is_required = f.config.required;
            let duration_ms = f.config.duration_ms;

            // Build the value retrieval expression
            let get_value = if has_env {
                let env_var = f.config.env.as_ref().unwrap();
                quote! {
                    config.get(#key)
                        .map(String::from)
                        .or_else(|| std::env::var(#env_var).ok())
                }
            } else {
                quote! {
                    config.get(#key).map(String::from)
                }
            };

            // Build the default value expression
            let with_default = if has_default {
                let default_val = f.config.default.as_ref().unwrap();
                quote! {
                    #get_value.or_else(|| Some(#default_val.to_string()))
                }
            } else {
                get_value
            };

            // Generate the parsing logic based on field type
            if duration_ms {
                // Duration from milliseconds
                if is_option {
                    quote! {
                        let #ident = match #with_default {
                            Some(v) => {
                                let ms: u64 = v.parse().map_err(|e| {
                                    laminar_connectors::error::ConnectorError::ConfigurationError(
                                        format!("invalid value for '{}': {}", #key, e)
                                    )
                                })?;
                                Some(std::time::Duration::from_millis(ms))
                            }
                            None => None,
                        };
                    }
                } else if is_required || has_default {
                    quote! {
                        let #ident = {
                            let v = #with_default.ok_or_else(|| {
                                laminar_connectors::error::ConnectorError::MissingConfig(#key.to_string())
                            })?;
                            let ms: u64 = v.parse().map_err(|e| {
                                laminar_connectors::error::ConnectorError::ConfigurationError(
                                    format!("invalid value for '{}': {}", #key, e)
                                )
                            })?;
                            std::time::Duration::from_millis(ms)
                        };
                    }
                } else {
                    // Non-optional, non-required, no default - this is a compile error case
                    quote! {
                        compile_error!("Non-optional Duration field must be required or have a default");
                    }
                }
            } else if is_option {
                // Optional field
                let inner_type = extract_option_inner(&f.ty);
                if is_string_type(inner_type) {
                    quote! {
                        let #ident = #with_default;
                    }
                } else {
                    quote! {
                        let #ident = match #with_default {
                            Some(v) => Some(v.parse().map_err(|e| {
                                laminar_connectors::error::ConnectorError::ConfigurationError(
                                    format!("invalid value for '{}': {}", #key, e)
                                )
                            })?),
                            None => None,
                        };
                    }
                }
            } else if is_required || has_default {
                // Required field or field with default
                if is_string_type(&f.ty) {
                    quote! {
                        let #ident = #with_default.ok_or_else(|| {
                            laminar_connectors::error::ConnectorError::MissingConfig(#key.to_string())
                        })?;
                    }
                } else {
                    quote! {
                        let #ident = {
                            let v = #with_default.ok_or_else(|| {
                                laminar_connectors::error::ConnectorError::MissingConfig(#key.to_string())
                            })?;
                            v.parse().map_err(|e| {
                                laminar_connectors::error::ConnectorError::ConfigurationError(
                                    format!("invalid value for '{}': {}", #key, e)
                                )
                            })?
                        };
                    }
                }
            } else {
                // Non-optional without required or default - use Default::default()
                quote! {
                    let #ident = match #with_default {
                        Some(v) => v.parse().map_err(|e| {
                            laminar_connectors::error::ConnectorError::ConfigurationError(
                                format!("invalid value for '{}': {}", #key, e)
                            )
                        })?,
                        None => Default::default(),
                    };
                }
            }
        })
        .collect();

    let field_names: Vec<&Ident> = fields.iter().map(|f| &f.ident).collect();

    Ok(quote! {
        #(#field_parsers)*

        Ok(Self {
            #(#field_names),*
        })
    })
}

/// Generate the `validate` method body.
fn generate_validate(fields: &[FieldInfo]) -> Result<TokenStream, Error> {
    let validations: Vec<TokenStream> = fields
        .iter()
        .filter(|f| f.config.required && !f.is_option)
        .map(|f| {
            let key = f.config.key.as_ref().unwrap();
            let ident = &f.ident;

            if is_string_type(&f.ty) {
                quote! {
                    if self.#ident.is_empty() {
                        return Err(laminar_connectors::error::ConnectorError::ConfigurationError(
                            format!("'{}' cannot be empty", #key)
                        ));
                    }
                }
            } else {
                // For non-string types, we just check they were parsed (no additional validation)
                quote! {}
            }
        })
        .collect();

    Ok(quote! {
        #(#validations)*
    })
}

/// Generate the `config_keys` method body.
fn generate_config_keys(fields: &[FieldInfo]) -> Result<TokenStream, Error> {
    let keys: Vec<TokenStream> = fields
        .iter()
        .map(|f| {
            let key = f.config.key.as_ref().unwrap();
            let description = f.config.description.as_deref().unwrap_or("No description");
            let required = f.config.required;
            let default = &f.config.default;

            if let Some(default_val) = default {
                quote! {
                    laminar_connectors::config::ConfigKeySpec::optional(
                        #key,
                        #description,
                        #default_val,
                    )
                }
            } else if required {
                quote! {
                    laminar_connectors::config::ConfigKeySpec::required(
                        #key,
                        #description,
                    )
                }
            } else {
                // Optional without default
                quote! {
                    laminar_connectors::config::ConfigKeySpec {
                        key: #key.to_string(),
                        description: #description.to_string(),
                        required: false,
                        default: None,
                    }
                }
            }
        })
        .collect();

    Ok(quote! {
        vec![
            #(#keys),*
        ]
    })
}

/// Check if a type is `Option<T>`.
fn is_option_type(ty: &Type) -> bool {
    if let Type::Path(type_path) = ty {
        if let Some(segment) = type_path.path.segments.last() {
            return segment.ident == "Option";
        }
    }
    false
}

/// Extract the inner type from `Option<T>`.
fn extract_option_inner(ty: &Type) -> &Type {
    if let Type::Path(type_path) = ty {
        if let Some(segment) = type_path.path.segments.last() {
            if segment.ident == "Option" {
                if let syn::PathArguments::AngleBracketed(args) = &segment.arguments {
                    if let Some(syn::GenericArgument::Type(inner)) = args.args.first() {
                        return inner;
                    }
                }
            }
        }
    }
    ty
}

/// Check if a type is `String`.
fn is_string_type(ty: &Type) -> bool {
    if let Type::Path(type_path) = ty {
        if let Some(segment) = type_path.path.segments.last() {
            return segment.ident == "String";
        }
    }
    false
}
