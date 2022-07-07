use quote::{quote, ToTokens};
use syn::{braced, parse::Parse, parse_macro_input, DeriveInput};


#[proc_macro]
pub fn background_job(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let def = parse_macro_input!(input as BgJobsDef);
    def.into_token_stream().into()
}

struct BgJobsDef {
    _impl_token: syn::token::Impl,
    trait_ident: syn::Ident,
    _for_token: syn::token::For,
    struct_name: syn::Ident,
    _brace_token: syn::token::Brace,
    requests: syn::punctuated::Punctuated<Request, syn::Token![;]>,
}

impl Parse for BgJobsDef {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let content;
        Ok(Self {
            _impl_token: input.parse()?,
            trait_ident: input.parse()?,
            _for_token: input.parse()?,
            struct_name: input.parse()?,
            _brace_token: braced!(content in input),
            requests: content.parse_terminated(Request::parse)?,
        })
    }
}

struct Item(Request);
impl ToTokens for Item {
    fn to_tokens(&self, tokens: &mut proc_macro2::TokenStream) {
        let sig = &self.0;
        let name = &sig.name;

        tokens.extend(quote! {
            pub fn #name {
                todo!()
            }
        })
    }
}

impl ToTokens for BgJobsDef {
    fn to_tokens(&self, tokens: &mut proc_macro2::TokenStream) {
        let requests = self.requests.iter().cloned().map(Item);

        tokens.extend(quote!({
            #(#requests)*
        }))
    }
}

#[derive(Clone)]
struct Request {
    name: syn::Ident,
    _colon: syn::Token![:],
    input: syn::Ident,
    _arrow: syn::Token![->],
    result: syn::Ident,
}

impl Parse for Request {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        Ok(Self {
            name: input.parse()?,
            _colon: input.parse()?,
            input: input.parse()?,
            _arrow: input.parse()?,
            result: input.parse()?,
        })
    }
}
