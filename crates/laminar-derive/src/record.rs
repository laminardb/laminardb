//! Implementation of `#[derive(Record)]`.

use proc_macro2::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Error, Fields, Ident, Type};

/// Field metadata extracted from struct definition.
struct FieldInfo {
    ident: Ident,
    ty: Type,
    column_name: String,
    is_event_time: bool,
    is_nullable: bool,
    is_option: bool,
}

pub fn expand_record(input: DeriveInput) -> Result<TokenStream, Error> {
    let name = &input.ident;

    let fields = match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(named) => &named.named,
            _ => {
                return Err(Error::new_spanned(
                    &input,
                    "Record can only be derived for structs with named fields",
                ));
            }
        },
        _ => {
            return Err(Error::new_spanned(
                &input,
                "Record can only be derived for structs",
            ));
        }
    };

    let mut field_infos = Vec::new();

    for field in fields {
        let ident = field.ident.clone().unwrap();
        let ty = field.ty.clone();

        let mut column_name = ident.to_string();
        let mut is_event_time = false;
        let mut is_nullable = false;

        for attr in &field.attrs {
            if attr.path().is_ident("event_time") {
                is_event_time = true;
            } else if attr.path().is_ident("nullable") {
                is_nullable = true;
            } else if attr.path().is_ident("column") {
                let lit: syn::LitStr = attr.parse_args()?;
                column_name = lit.value();
            }
        }

        let is_option = is_option_type(&ty);
        if is_option {
            is_nullable = true;
        }

        field_infos.push(FieldInfo {
            ident,
            ty,
            column_name,
            is_event_time,
            is_nullable,
            is_option,
        });
    }

    let schema_fields = field_infos.iter().map(|f| {
        let col_name = &f.column_name;
        let nullable = f.is_nullable;
        let arrow_type = rust_type_to_arrow(&f.ty, f.is_option);
        quote! {
            arrow::datatypes::Field::new(#col_name, #arrow_type, #nullable)
        }
    });

    let array_builders = field_infos.iter().map(|f| {
        let ident = &f.ident;
        build_array_expr(ident, &f.ty, f.is_option)
    });

    // Code generation for to_record_batch_from_iter
    let vec_inits = field_infos.iter().map(|f| {
        let ident = &f.ident;
        quote! { let mut #ident = Vec::new(); }
    });

    let vec_pushes = field_infos.iter().map(|f| {
        let ident = &f.ident;
        quote! { #ident.push(record.#ident); }
    });

    let array_builders_from_vec = field_infos.iter().map(|f| {
        let ident = &f.ident;
        build_array_from_vec(ident, &f.ty, f.is_option)
    });

    // Find the event_time field
    let event_time_impl = field_infos
        .iter()
        .find(|f| f.is_event_time)
        .map(|f| {
            let ident = &f.ident;
            if f.is_option {
                quote! {
                    fn event_time(&self) -> Option<i64> {
                        self.#ident.map(|v| v as i64)
                    }
                }
            } else {
                quote! {
                    fn event_time(&self) -> Option<i64> {
                        Some(self.#ident as i64)
                    }
                }
            }
        })
        .unwrap_or_else(|| {
            quote! {
                fn event_time(&self) -> Option<i64> {
                    None
                }
            }
        });

    let expanded = quote! {
        impl laminar_core::streaming::Record for #name {
            fn schema() -> arrow::datatypes::SchemaRef {
                std::sync::Arc::new(arrow::datatypes::Schema::new(vec![
                    #(#schema_fields),*
                ]))
            }

            fn to_record_batch(&self) -> arrow::array::RecordBatch {
                arrow::array::RecordBatch::try_new(
                    <Self as laminar_core::streaming::Record>::schema(),
                    vec![
                        #(#array_builders),*
                    ],
                )
                .expect("Record derive: schema and arrays must match")
            }

            fn to_record_batch_from_iter<I>(records: I) -> arrow::array::RecordBatch
            where
                I: IntoIterator<Item = Self>,
            {
                #(#vec_inits)*

                for record in records {
                    #(#vec_pushes)*
                }

                arrow::array::RecordBatch::try_new(
                    <Self as laminar_core::streaming::Record>::schema(),
                    vec![
                        #(#array_builders_from_vec),*
                    ],
                )
                .expect("Record derive: schema and arrays must match")
            }

            #event_time_impl
        }
    };

    Ok(expanded)
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
fn extract_option_inner(ty: &Type) -> Option<&Type> {
    if let Type::Path(type_path) = ty {
        if let Some(segment) = type_path.path.segments.last() {
            if segment.ident == "Option" {
                if let syn::PathArguments::AngleBracketed(args) = &segment.arguments {
                    if let Some(syn::GenericArgument::Type(inner)) = args.args.first() {
                        return Some(inner);
                    }
                }
            }
        }
    }
    None
}

/// Convert a Rust type to an Arrow DataType token.
fn rust_type_to_arrow(ty: &Type, is_option: bool) -> TokenStream {
    if is_option {
        if let Some(inner) = extract_option_inner(ty) {
            return rust_type_to_arrow(inner, false);
        }
    }

    if let Type::Path(type_path) = ty {
        let type_str = type_path
            .path
            .segments
            .iter()
            .map(|s| s.ident.to_string())
            .collect::<Vec<_>>()
            .join("::");

        match type_str.as_str() {
            "bool" => quote! { arrow::datatypes::DataType::Boolean },
            "i8" => quote! { arrow::datatypes::DataType::Int8 },
            "i16" => quote! { arrow::datatypes::DataType::Int16 },
            "i32" => quote! { arrow::datatypes::DataType::Int32 },
            "i64" => quote! { arrow::datatypes::DataType::Int64 },
            "u8" => quote! { arrow::datatypes::DataType::UInt8 },
            "u16" => quote! { arrow::datatypes::DataType::UInt16 },
            "u32" => quote! { arrow::datatypes::DataType::UInt32 },
            "u64" => quote! { arrow::datatypes::DataType::UInt64 },
            "f32" => quote! { arrow::datatypes::DataType::Float32 },
            "f64" => quote! { arrow::datatypes::DataType::Float64 },
            "String" => quote! { arrow::datatypes::DataType::Utf8 },
            "Vec" => {
                // Check for Vec<u8> → Binary
                if let Some(segment) = type_path.path.segments.last() {
                    if let syn::PathArguments::AngleBracketed(args) = &segment.arguments {
                        if let Some(syn::GenericArgument::Type(Type::Path(inner_path))) =
                            args.args.first()
                        {
                            if inner_path.path.is_ident("u8") {
                                return quote! { arrow::datatypes::DataType::Binary };
                            }
                        }
                    }
                }
                quote! { compile_error!("Unsupported Vec type. Only Vec<u8> (Binary) is supported.") }
            }
            _ => {
                quote! { compile_error!(concat!("Unsupported type for Record derive: ", stringify!(#ty))) }
            }
        }
    } else {
        quote! { compile_error!("Unsupported type for Record derive") }
    }
}

/// Build the Arrow array expression for a field.
fn build_array_expr(ident: &Ident, ty: &Type, is_option: bool) -> TokenStream {
    if is_option {
        if let Some(inner) = extract_option_inner(ty) {
            return build_option_array_expr(ident, inner);
        }
    }

    if let Type::Path(type_path) = ty {
        let type_str = type_path
            .path
            .segments
            .iter()
            .map(|s| s.ident.to_string())
            .collect::<Vec<_>>()
            .join("::");

        match type_str.as_str() {
            "bool" => quote! {
                std::sync::Arc::new(arrow::array::BooleanArray::from(vec![self.#ident]))
            },
            "i8" => quote! {
                std::sync::Arc::new(arrow::array::Int8Array::from(vec![self.#ident]))
            },
            "i16" => quote! {
                std::sync::Arc::new(arrow::array::Int16Array::from(vec![self.#ident]))
            },
            "i32" => quote! {
                std::sync::Arc::new(arrow::array::Int32Array::from(vec![self.#ident]))
            },
            "i64" => quote! {
                std::sync::Arc::new(arrow::array::Int64Array::from(vec![self.#ident]))
            },
            "u8" => quote! {
                std::sync::Arc::new(arrow::array::UInt8Array::from(vec![self.#ident]))
            },
            "u16" => quote! {
                std::sync::Arc::new(arrow::array::UInt16Array::from(vec![self.#ident]))
            },
            "u32" => quote! {
                std::sync::Arc::new(arrow::array::UInt32Array::from(vec![self.#ident]))
            },
            "u64" => quote! {
                std::sync::Arc::new(arrow::array::UInt64Array::from(vec![self.#ident]))
            },
            "f32" => quote! {
                std::sync::Arc::new(arrow::array::Float32Array::from(vec![self.#ident]))
            },
            "f64" => quote! {
                std::sync::Arc::new(arrow::array::Float64Array::from(vec![self.#ident]))
            },
            "String" => quote! {
                std::sync::Arc::new(arrow::array::StringArray::from(vec![self.#ident.as_str()]))
            },
            "Vec" => {
                // Vec<u8> → BinaryArray
                quote! {
                    std::sync::Arc::new(arrow::array::BinaryArray::from_vec(vec![self.#ident.as_slice()]))
                }
            }
            _ => quote! { compile_error!("Unsupported type for Record derive array builder") },
        }
    } else {
        quote! { compile_error!("Unsupported type for Record derive array builder") }
    }
}

/// Build array expression for Option<T> fields.
fn build_option_array_expr(ident: &Ident, inner_ty: &Type) -> TokenStream {
    if let Type::Path(type_path) = inner_ty {
        let type_str = type_path
            .path
            .segments
            .iter()
            .map(|s| s.ident.to_string())
            .collect::<Vec<_>>()
            .join("::");

        match type_str.as_str() {
            "bool" => quote! {
                std::sync::Arc::new(arrow::array::BooleanArray::from(vec![self.#ident]))
            },
            "i8" => quote! {
                std::sync::Arc::new(arrow::array::Int8Array::from(vec![self.#ident]))
            },
            "i16" => quote! {
                std::sync::Arc::new(arrow::array::Int16Array::from(vec![self.#ident]))
            },
            "i32" => quote! {
                std::sync::Arc::new(arrow::array::Int32Array::from(vec![self.#ident]))
            },
            "i64" => quote! {
                std::sync::Arc::new(arrow::array::Int64Array::from(vec![self.#ident]))
            },
            "u8" => quote! {
                std::sync::Arc::new(arrow::array::UInt8Array::from(vec![self.#ident]))
            },
            "u16" => quote! {
                std::sync::Arc::new(arrow::array::UInt16Array::from(vec![self.#ident]))
            },
            "u32" => quote! {
                std::sync::Arc::new(arrow::array::UInt32Array::from(vec![self.#ident]))
            },
            "u64" => quote! {
                std::sync::Arc::new(arrow::array::UInt64Array::from(vec![self.#ident]))
            },
            "f32" => quote! {
                std::sync::Arc::new(arrow::array::Float32Array::from(vec![self.#ident]))
            },
            "f64" => quote! {
                std::sync::Arc::new(arrow::array::Float64Array::from(vec![self.#ident]))
            },
            "String" => quote! {
                std::sync::Arc::new(arrow::array::StringArray::from(vec![self.#ident.as_deref()]))
            },
            _ => quote! { compile_error!("Unsupported Option<T> type for Record derive") },
        }
    } else {
        quote! { compile_error!("Unsupported Option type for Record derive") }
    }
}

/// Build array expression from a Vec of values.
fn build_array_from_vec(ident: &Ident, ty: &Type, is_option: bool) -> TokenStream {
    if is_option {
        if let Some(inner) = extract_option_inner(ty) {
            return build_option_array_from_vec(ident, inner);
        }
    }

    if let Type::Path(type_path) = ty {
        let type_str = type_path
            .path
            .segments
            .iter()
            .map(|s| s.ident.to_string())
            .collect::<Vec<_>>()
            .join("::");

        match type_str.as_str() {
            "bool" => quote! {
                std::sync::Arc::new(arrow::array::BooleanArray::from(#ident))
            },
            "i8" => quote! {
                std::sync::Arc::new(arrow::array::Int8Array::from(#ident))
            },
            "i16" => quote! {
                std::sync::Arc::new(arrow::array::Int16Array::from(#ident))
            },
            "i32" => quote! {
                std::sync::Arc::new(arrow::array::Int32Array::from(#ident))
            },
            "i64" => quote! {
                std::sync::Arc::new(arrow::array::Int64Array::from(#ident))
            },
            "u8" => quote! {
                std::sync::Arc::new(arrow::array::UInt8Array::from(#ident))
            },
            "u16" => quote! {
                std::sync::Arc::new(arrow::array::UInt16Array::from(#ident))
            },
            "u32" => quote! {
                std::sync::Arc::new(arrow::array::UInt32Array::from(#ident))
            },
            "u64" => quote! {
                std::sync::Arc::new(arrow::array::UInt64Array::from(#ident))
            },
            "f32" => quote! {
                std::sync::Arc::new(arrow::array::Float32Array::from(#ident))
            },
            "f64" => quote! {
                std::sync::Arc::new(arrow::array::Float64Array::from(#ident))
            },
            "String" => quote! {
                std::sync::Arc::new(arrow::array::StringArray::from(#ident))
            },
            "Vec" => {
                // Vec<u8> → BinaryArray
                // Vec<Vec<u8>> needs conversion or iteration
                quote! {
                    std::sync::Arc::new(arrow::array::BinaryArray::from_iter_values(#ident.iter()))
                }
            }
            _ => quote! { compile_error!("Unsupported type for Record derive array builder") },
        }
    } else {
        quote! { compile_error!("Unsupported type for Record derive array builder") }
    }
}

/// Build array expression for Vec<Option<T>>.
fn build_option_array_from_vec(ident: &Ident, inner_ty: &Type) -> TokenStream {
    if let Type::Path(type_path) = inner_ty {
        let type_str = type_path
            .path
            .segments
            .iter()
            .map(|s| s.ident.to_string())
            .collect::<Vec<_>>()
            .join("::");

        match type_str.as_str() {
            "bool" => quote! {
                std::sync::Arc::new(arrow::array::BooleanArray::from(#ident))
            },
            "i8" => quote! {
                std::sync::Arc::new(arrow::array::Int8Array::from(#ident))
            },
            "i16" => quote! {
                std::sync::Arc::new(arrow::array::Int16Array::from(#ident))
            },
            "i32" => quote! {
                std::sync::Arc::new(arrow::array::Int32Array::from(#ident))
            },
            "i64" => quote! {
                std::sync::Arc::new(arrow::array::Int64Array::from(#ident))
            },
            "u8" => quote! {
                std::sync::Arc::new(arrow::array::UInt8Array::from(#ident))
            },
            "u16" => quote! {
                std::sync::Arc::new(arrow::array::UInt16Array::from(#ident))
            },
            "u32" => quote! {
                std::sync::Arc::new(arrow::array::UInt32Array::from(#ident))
            },
            "u64" => quote! {
                std::sync::Arc::new(arrow::array::UInt64Array::from(#ident))
            },
            "f32" => quote! {
                std::sync::Arc::new(arrow::array::Float32Array::from(#ident))
            },
            "f64" => quote! {
                std::sync::Arc::new(arrow::array::Float64Array::from(#ident))
            },
            "String" => quote! {
                std::sync::Arc::new(arrow::array::StringArray::from(#ident))
            },
            _ => quote! { compile_error!("Unsupported Option<T> type for Record derive") },
        }
    } else {
        quote! { compile_error!("Unsupported Option type for Record derive") }
    }
}
