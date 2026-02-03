//! Implementation of `#[derive(FromRecordBatch)]` and `#[derive(FromRow)]`.

use proc_macro2::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Error, Fields, Ident, Type};

/// Field metadata for deserialization.
struct FieldInfo {
    ident: Ident,
    ty: Type,
    column_name: String,
    is_option: bool,
}

/// Parse struct fields from a `DeriveInput`, returning field metadata.
///
/// Shared by both `FromRecordBatch` and `FromRow` derives.
fn parse_fields(input: &DeriveInput, macro_name: &str) -> Result<Vec<FieldInfo>, Error> {
    let fields = match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(named) => &named.named,
            _ => {
                return Err(Error::new_spanned(
                    input,
                    format!(
                        "{} can only be derived for structs with named fields",
                        macro_name
                    ),
                ));
            }
        },
        _ => {
            return Err(Error::new_spanned(
                input,
                format!("{} can only be derived for structs", macro_name),
            ));
        }
    };

    let mut field_infos = Vec::new();

    for field in fields {
        let ident = field.ident.clone().unwrap();
        let ty = field.ty.clone();
        let mut column_name = ident.to_string();

        for attr in &field.attrs {
            if attr.path().is_ident("column") {
                let lit: syn::LitStr = attr.parse_args()?;
                column_name = lit.value();
            }
        }

        let is_option = is_option_type(&ty);

        field_infos.push(FieldInfo {
            ident,
            ty,
            column_name,
            is_option,
        });
    }

    Ok(field_infos)
}
/// Generate the inherent `from_batch` and `from_batch_all` method implementations.
fn generate_inherent_methods(name: &syn::Ident, field_infos: &[FieldInfo]) -> TokenStream {
    let field_extractions = field_infos.iter().map(|f| {
        let ident = &f.ident;
        let col_name = &f.column_name;
        build_extraction(ident, &f.ty, col_name, f.is_option)
    });

    let field_names = field_infos.iter().map(|f| &f.ident);

    quote! {
        impl #name {
            /// Deserialize a single row from a `RecordBatch`.
            ///
            /// # Panics
            ///
            /// Panics if the column is missing or the type doesn't match.
            pub fn from_batch(batch: &arrow::array::RecordBatch, row: usize) -> Self {
                #(#field_extractions)*

                Self {
                    #(#field_names),*
                }
            }

            /// Deserialize all rows from a `RecordBatch`.
            pub fn from_batch_all(batch: &arrow::array::RecordBatch) -> Vec<Self> {
                (0..batch.num_rows()).map(|i| Self::from_batch(batch, i)).collect()
            }
        }
    }
}

/// Expand `#[derive(FromRecordBatch)]`.
///
/// Generates inherent `from_batch` and `from_batch_all` methods on the struct.
pub fn expand_from_record_batch(input: DeriveInput) -> Result<TokenStream, Error> {
    let name = &input.ident;
    let field_infos = parse_fields(&input, "FromRecordBatch")?;
    let inherent_impl = generate_inherent_methods(name, &field_infos);

    Ok(inherent_impl)
}

/// Expand `#[derive(FromRow)]`.
///
/// Generates the same inherent `from_batch` / `from_batch_all` methods as
/// `FromRecordBatch`, **plus** an `impl laminar_db::FromBatch for T` block
/// that delegates to those inherent methods.
pub fn expand_from_row(input: DeriveInput) -> Result<TokenStream, Error> {
    let name = &input.ident;
    let field_infos = parse_fields(&input, "FromRow")?;
    let inherent_impl = generate_inherent_methods(name, &field_infos);

    let trait_impl = quote! {
        impl laminar_db::FromBatch for #name {
            fn from_batch(
                batch: &arrow::array::RecordBatch,
                row: usize,
            ) -> Self {
                #name::from_batch(batch, row)
            }

            fn from_batch_all(
                batch: &arrow::array::RecordBatch,
            ) -> Vec<Self> {
                #name::from_batch_all(batch)
            }
        }
    };

    Ok(quote! {
        #inherent_impl
        #trait_impl
    })
}
fn is_option_type(ty: &Type) -> bool {
    if let Type::Path(type_path) = ty {
        if let Some(segment) = type_path.path.segments.last() {
            return segment.ident == "Option";
        }
    }
    false
}

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

/// Build the extraction code for a field from a `RecordBatch`.
fn build_extraction(ident: &Ident, ty: &Type, column_name: &str, is_option: bool) -> TokenStream {
    if is_option {
        if let Some(inner) = extract_option_inner(ty) {
            return build_option_extraction(ident, inner, column_name);
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

        let col_idx_let = quote! {
            let col_idx = batch.schema().index_of(#column_name)
                .unwrap_or_else(|_| panic!("Column '{}' not found in RecordBatch", #column_name));
        };

        match type_str.as_str() {
            "bool" => quote! {
                #col_idx_let
                let #ident = batch.column(col_idx)
                    .as_any().downcast_ref::<arrow::array::BooleanArray>()
                    .unwrap_or_else(|| panic!("Column '{}' is not BooleanArray", #column_name))
                    .value(row);
            },
            "i8" => build_primitive_extraction(ident, column_name, "Int8Array", "i8"),
            "i16" => build_primitive_extraction(ident, column_name, "Int16Array", "i16"),
            "i32" => build_primitive_extraction(ident, column_name, "Int32Array", "i32"),
            "i64" => build_primitive_extraction(ident, column_name, "Int64Array", "i64"),
            "u8" => build_primitive_extraction(ident, column_name, "UInt8Array", "u8"),
            "u16" => build_primitive_extraction(ident, column_name, "UInt16Array", "u16"),
            "u32" => build_primitive_extraction(ident, column_name, "UInt32Array", "u32"),
            "u64" => build_primitive_extraction(ident, column_name, "UInt64Array", "u64"),
            "f32" => build_primitive_extraction(ident, column_name, "Float32Array", "f32"),
            "f64" => build_primitive_extraction(ident, column_name, "Float64Array", "f64"),
            "String" => quote! {
                let col_idx = batch.schema().index_of(#column_name)
                    .unwrap_or_else(|_| panic!("Column '{}' not found in RecordBatch", #column_name));
                let #ident = batch.column(col_idx)
                    .as_any().downcast_ref::<arrow::array::StringArray>()
                    .unwrap_or_else(|| panic!("Column '{}' is not StringArray", #column_name))
                    .value(row)
                    .to_string();
            },
            "Vec" => quote! {
                let col_idx = batch.schema().index_of(#column_name)
                    .unwrap_or_else(|_| panic!("Column '{}' not found in RecordBatch", #column_name));
                let #ident = batch.column(col_idx)
                    .as_any().downcast_ref::<arrow::array::BinaryArray>()
                    .unwrap_or_else(|| panic!("Column '{}' is not BinaryArray", #column_name))
                    .value(row)
                    .to_vec();
            },
            _ => {
                quote! { compile_error!(concat!("Unsupported type for FromRecordBatch: ", stringify!(#ty))); }
            }
        }
    } else {
        quote! { compile_error!("Unsupported type for FromRecordBatch"); }
    }
}

fn build_primitive_extraction(
    ident: &Ident,
    column_name: &str,
    array_type: &str,
    _rust_type: &str,
) -> TokenStream {
    let array_ident = syn::Ident::new(array_type, proc_macro2::Span::call_site());
    quote! {
        let col_idx = batch.schema().index_of(#column_name)
            .unwrap_or_else(|_| panic!("Column '{}' not found in RecordBatch", #column_name));
        let #ident = batch.column(col_idx)
            .as_any().downcast_ref::<arrow::array::#array_ident>()
            .unwrap_or_else(|| panic!("Column '{}' is not {}", #column_name, #array_type))
            .value(row);
    }
}

fn build_option_extraction(ident: &Ident, inner_ty: &Type, column_name: &str) -> TokenStream {
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
                let col_idx = batch.schema().index_of(#column_name)
                    .unwrap_or_else(|_| panic!("Column '{}' not found in RecordBatch", #column_name));
                let arr = batch.column(col_idx)
                    .as_any().downcast_ref::<arrow::array::BooleanArray>()
                    .unwrap_or_else(|| panic!("Column '{}' is not BooleanArray", #column_name));
                let #ident = if arr.is_null(row) { None } else { Some(arr.value(row)) };
            },
            "i32" => build_option_primitive(ident, column_name, "Int32Array"),
            "i64" => build_option_primitive(ident, column_name, "Int64Array"),
            "u32" => build_option_primitive(ident, column_name, "UInt32Array"),
            "u64" => build_option_primitive(ident, column_name, "UInt64Array"),
            "f32" => build_option_primitive(ident, column_name, "Float32Array"),
            "f64" => build_option_primitive(ident, column_name, "Float64Array"),
            "String" => quote! {
                let col_idx = batch.schema().index_of(#column_name)
                    .unwrap_or_else(|_| panic!("Column '{}' not found in RecordBatch", #column_name));
                let arr = batch.column(col_idx)
                    .as_any().downcast_ref::<arrow::array::StringArray>()
                    .unwrap_or_else(|| panic!("Column '{}' is not StringArray", #column_name));
                let #ident = if arr.is_null(row) { None } else { Some(arr.value(row).to_string()) };
            },
            _ => quote! { compile_error!("Unsupported Option<T> for FromRecordBatch"); },
        }
    } else {
        quote! { compile_error!("Unsupported Option type for FromRecordBatch"); }
    }
}

fn build_option_primitive(ident: &Ident, column_name: &str, array_type: &str) -> TokenStream {
    let array_ident = syn::Ident::new(array_type, proc_macro2::Span::call_site());
    quote! {
        let col_idx = batch.schema().index_of(#column_name)
            .unwrap_or_else(|_| panic!("Column '{}' not found in RecordBatch", #column_name));
        let arr = batch.column(col_idx)
            .as_any().downcast_ref::<arrow::array::#array_ident>()
            .unwrap_or_else(|| panic!("Column '{}' is not {}", #column_name, #array_type));
        let #ident = if arr.is_null(row) { None } else { Some(arr.value(row)) };
    }
}
