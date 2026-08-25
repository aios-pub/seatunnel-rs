/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//! Seatunnel proc-macros: source_factory, sink_factory, transform_factory.
//!
//! These macros generate boilerplate factory registration code for connectors.

extern crate proc_macro;
use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, ItemStruct};

/// Attribute arguments: `id = "xxx", category = "xxx"`.
#[derive(Default)]
struct Attrs {
    id: Option<String>,
    category: Option<String>,
}

impl Attrs {
    fn parse(tokens: proc_macro2::TokenStream) -> Self {
        let mut result = Self::default();
        let mut i = 0;
        let tokens_vec: Vec<proc_macro2::TokenTree> = tokens.into_iter().collect();
        while i < tokens_vec.len() {
            if let proc_macro2::TokenTree::Ident(ref ident) = tokens_vec[i] {
                let name = ident.to_string();
                if name == "id" || name == "category" {
                    // expect '=' next
                    if i + 2 < tokens_vec.len() {
                        if let proc_macro2::TokenTree::Punct(p) = &tokens_vec[i + 1] {
                            if p.as_char() == '=' {
                                if let proc_macro2::TokenTree::Literal(lit) = &tokens_vec[i + 2] {
                                    let s = lit.to_string();
                                    let val = s.trim_matches('"').to_string();
                                    if name == "id" {
                                        result.id = Some(val);
                                    } else {
                                        result.category = Some(val);
                                    }
                                }
                            }
                        }
                    }
                }
            }
            i += 1;
        }
        result
    }
}

/// Derive macro: generates a `Factory` impl for a source connector.
///
/// Usage:
/// ```ignore
/// #[source_factory(id = "fake")]
/// pub struct FakeSource { ... }
/// ```
#[proc_macro_attribute]
pub fn source_factory(attr: TokenStream, input: TokenStream) -> TokenStream {
    let attrs = Attrs::parse(attr.into());
    let input_struct = parse_macro_input!(input as ItemStruct);
    let name = &input_struct.ident;
    let id = attrs.id.unwrap_or_else(|| "unknown".to_string());
    let category = attrs.category.unwrap_or_else(|| "source".to_string());

    let expanded = quote! {
        #input_struct

        impl seatunnel_api::factory::Factory for #name {
            fn id(&self) -> &str {
                #id
            }
            fn category(&self) -> &str {
                #category
            }
            fn create(
                &self,
                _ctx: seatunnel_api::factory::FactoryContext,
            ) -> anyhow::Result<std::boxed::Box<dyn seatunnel_api::source::Source>> {
                Ok(std::boxed::Box::new(self.clone()))
            }
        }
    };
    TokenStream::from(expanded)
}

/// Derive macro: generates a `Factory` impl for a sink connector.
#[proc_macro_attribute]
pub fn sink_factory(attr: TokenStream, input: TokenStream) -> TokenStream {
    let attrs = Attrs::parse(attr.into());
    let input_struct = parse_macro_input!(input as ItemStruct);
    let name = &input_struct.ident;
    let id = attrs.id.unwrap_or_else(|| "unknown".to_string());
    let category = attrs.category.unwrap_or_else(|| "sink".to_string());

    let expanded = quote! {
        #input_struct

        impl seatunnel_api::factory::Factory for #name {
            fn id(&self) -> &str {
                #id
            }
            fn category(&self) -> &str {
                #category
            }
            fn create(
                &self,
                _ctx: seatunnel_api::factory::FactoryContext,
            ) -> anyhow::Result<std::boxed::Box<dyn seatunnel_api::sink::Sink>> {
                Ok(std::boxed::Box::new(self.clone()))
            }
        }
    };
    TokenStream::from(expanded)
}

/// Derive macro: generates a `Factory` impl for a transform connector.
#[proc_macro_attribute]
pub fn transform_factory(attr: TokenStream, input: TokenStream) -> TokenStream {
    let attrs = Attrs::parse(attr.into());
    let input_struct = parse_macro_input!(input as ItemStruct);
    let name = &input_struct.ident;
    let id = attrs.id.unwrap_or_else(|| "unknown".to_string());
    let category = attrs.category.unwrap_or_else(|| "transform".to_string());

    let expanded = quote! {
        #input_struct

        impl seatunnel_api::factory::Factory for #name {
            fn id(&self) -> &str {
                #id
            }
            fn category(&self) -> &str {
                #category
            }
            fn create(
                &self,
                _ctx: seatunnel_api::factory::FactoryContext,
            ) -> anyhow::Result<std::boxed::Box<dyn seatunnel_api::transform::Transform<Input = seatunnel_api::row::Row, Output = seatunnel_api::row::Row>>> {
                Ok(std::boxed::Box::new(self.clone()))
            }
        }
    };
    TokenStream::from(expanded)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_full_attrs() {
        let ts: proc_macro2::TokenStream = r#"id = "fake", category = "source""#.parse().unwrap();
        let attrs = Attrs::parse(ts);
        assert_eq!(attrs.id, Some("fake".to_string()));
        assert_eq!(attrs.category, Some("source".to_string()));
    }

    #[test]
    fn test_parse_id_only() {
        let ts: proc_macro2::TokenStream = r#"id = "kafka""#.parse().unwrap();
        let attrs = Attrs::parse(ts);
        assert_eq!(attrs.id, Some("kafka".to_string()));
        assert_eq!(attrs.category, None);
    }

    #[test]
    fn test_parse_empty() {
        let ts: proc_macro2::TokenStream = "".parse().unwrap();
        let attrs = Attrs::parse(ts);
        assert_eq!(attrs.id, None);
        assert_eq!(attrs.category, None);
    }

    #[test]
    fn test_parse_category_only() {
        let ts: proc_macro2::TokenStream = r#"category = "sink""#.parse().unwrap();
        let attrs = Attrs::parse(ts);
        assert_eq!(attrs.id, None);
        assert_eq!(attrs.category, Some("sink".to_string()));
    }
}
