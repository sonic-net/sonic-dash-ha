use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, Data, DeriveInput, Fields, LitStr};

/// Derive macro for sonic-db serialization/deserialization
#[proc_macro_derive(SerdeSonicDb, attributes(serde_sonicdb))]
pub fn serde_sonicdb_derive(input: TokenStream) -> TokenStream {
    // Parse the input token stream
    let input = parse_macro_input!(input as DeriveInput);

    // Get the struct name
    let struct_name = &input.ident;

    // Process struct-level drive attributes
    let mut table_name: String = struct_name.to_string().to_uppercase();
    let mut key_separator: String = "|".to_string();
    for attr in &input.attrs {
        if attr.path().is_ident("serde_sonicdb") {
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("table_name") {
                    let value = meta.value()?; // this parses the `=`
                    let s: LitStr = value.parse()?; // this parses `"EarlGrey"`
                    table_name = s.value();
                    Ok(())
                } else if meta.path.is_ident("key_separator") {
                    let value = meta.value()?; // this parses the `=`
                    let s: LitStr = value.parse()?; // this parses `"EarlGrey"`
                    key_separator = s.value();
                    Ok(())
                } else {
                    Err(meta.error("unknown attribute"))
                }
            })
            .unwrap();
        }
    }

    let fields = match input.data {
        Data::Struct(data_struct) => match data_struct.fields {
            Fields::Named(fields) => fields.named,
            _ => panic!("serde_sonicdb can only be derived for structs with named fields"),
        },
        _ => panic!("serde_sonicdb can only be derived for structs"),
    };
    let mut serialize_keys = vec![];
    let mut from_keys = vec![];
    let mut serialize_fields = vec![];
    let mut deserialize_fields = vec![];
    for field in &fields {
        let field_ident = field.ident.as_ref().unwrap();
        let field_name = field_ident.to_string();
        let mut is_key = false;
        for attr in &field.attrs {
            if attr.path().is_ident("serde_sonicdb") {
                attr.parse_nested_meta(|meta| {
                    if meta.path.is_ident("key") {
                        serialize_keys.push(quote! { &self.#field_ident });
                        from_keys.push(quote! { #field_ident: parts.next().unwrap().to_string() });
                        is_key = true;
                        Ok(())
                    } else {
                        Err(meta.error("unknown attribute"))
                    }
                })
                .unwrap();
            }
        }

        if !is_key {
            serialize_fields.push(quote! {
                if let Some(field) = &self.#field_ident {
                    db.hset(&key, #field_name, &CxxString::new(field))?;
                } else {
                    db.hdel(&key, #field_name)?;
                }
            });
            deserialize_fields.push(quote! {
                if obj.contains_key(#field_name) {
                    self.#field_ident = Some(obj.get(#field_name).unwrap().to_string_lossy().into_owned());
                } else {
                    self.#field_ident = None;
                }
            });
            from_keys.push(quote! { #field_ident: None });
        }
    }

    let expanded = quote! {
        impl sonicdb_serde::SonicDbObject for #struct_name {
            fn get_key_separator() -> String {
                #key_separator.to_string()
            }
            fn get_table_name() -> String {
                #table_name.to_string()
            }

            fn get_key(&self) -> String {
                let mut keys = vec![#table_name,#(#serialize_keys),*];
                keys.join(&#key_separator)
            }

            fn from_keys(key: &str) -> Self {
                let mut parts = key.split(&#key_separator);
                // skip table name
                parts.next();

                Self {
                    #(#from_keys),*
                }
            }

            fn to_sonicdb(&self, db: &DbConnector) -> swss_common::Result<()> {
                let key = self.get_key();
                #(#serialize_fields)*
                Ok(())
            }

            fn from_sonicdb(&mut self, db: &DbConnector) -> swss_common::Result<()> {
                let key = self.get_key();
                let obj = db.hgetall(&key)?;
                #(#deserialize_fields)*
                Ok(())
            }
        }
    };

    // Return the generated code
    TokenStream::from(expanded)
}
