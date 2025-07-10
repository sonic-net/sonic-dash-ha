use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput, LitStr};

/// Derive macro for sonic-db serialization/deserialization
#[proc_macro_derive(SonicDb, attributes(sonicdb))]
pub fn serde_sonicdb_derive(input: TokenStream) -> TokenStream {
    // Parse the input token stream
    let input = parse_macro_input!(input as DeriveInput);

    // Get the struct name
    let struct_name = &input.ident;

    // Process struct-level drive attributes
    let mut table_name: String = "".to_string();
    let mut key_separator: char = 'a';
    let mut db_name: String = "".to_string();
    let mut is_dpu: bool = false;
    for attr in &input.attrs {
        if attr.path().is_ident("sonicdb") {
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("table_name") {
                    let value = meta.value()?; // this parses the `=`
                    let s: LitStr = value.parse()?;
                    table_name = s.value();
                    Ok(())
                } else if meta.path.is_ident("key_separator") {
                    let value = meta.value()?; // this parses the `=`
                    let s: LitStr = value.parse()?;
                    let value_str = s.value();
                    let mut chars = value_str.chars();
                    if let (Some(c), None) = (chars.next(), chars.next()) {
                        key_separator = c;
                    } else {
                        return Err(meta.error("key_separator must be a single character"));
                    }
                    Ok(())
                } else if meta.path.is_ident("db_name") {
                    let value = meta.value()?; // this parses the `=`
                    let s: LitStr = value.parse()?;
                    db_name = s.value();
                    Ok(())
                } else if meta.path.is_ident("is_dpu") {
                    let value = meta.value()?;
                    let s: LitStr = value.parse()?;
                    let value_str = s.value();
                    is_dpu = match value_str.as_str() {
                        "true" | "True" | "1" => true,
                        "false" | "False" | "0" => false,
                        _ => return Err(meta.error("is_dpu must be a boolean (true/false)")),
                    };
                    Ok(())
                } else {
                    Err(meta.error("unknown attribute"))
                }
            })
            .unwrap();
        }
    }
    if table_name.is_empty() {
        panic!("Missing table_name attribute");
    }
    if db_name.is_empty() {
        panic!("Missing db_name attribute");
    }
    if key_separator == 'a' {
        panic!("Missing key_separator attribute");
    }

    let is_dpu_value = is_dpu;

    let expanded = quote! {
        impl swss_common::SonicDbTable for #struct_name {
            fn key_separator() -> char {
                #key_separator
            }

            fn table_name() -> &'static str {
                #table_name
            }

            fn db_name() -> &'static str {
                #db_name
            }

            fn is_dpu() -> bool {
                #is_dpu_value
            }
        }
    };

    // Return the generated code
    TokenStream::from(expanded)
}
