use proc_macro2::TokenStream;
use quote::{quote, ToTokens};
use syn::{braced, parse::Parse, parse_macro_input};

#[proc_macro]
pub fn background_job(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let def = parse_macro_input!(input as TraitImpl);
    def.into_token_stream().into()
}

struct TraitImpl {
    _struct: syn::Token!(struct),
    iden: syn::Ident,
    _ang: syn::Token!(<),
    ctx: syn::Ident,
    _ang_2: syn::Token!(>),
    _brace_token: syn::token::Brace,
    requests: syn::punctuated::Punctuated<Request, syn::Token![,]>,
}

impl Parse for TraitImpl {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let content;
        Ok(Self {
            _struct: input.parse()?,
            iden: input.parse()?,
            _ang: input.parse()?,
            ctx: input.parse()?,
            _ang_2: input.parse()?,
            _brace_token: braced!(content in input),
            requests: content.parse_terminated(Request::parse)?,
        })
    }
}

struct FieldItem(Request, syn::Ident);
impl ToTokens for FieldItem {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let sig = &self.0;
        let name = &sig.name;
        let input = &sig.input;
        let ctx_name = &self.1;

        tokens.extend(quote! {
            #name: Box<dyn Fn(#ctx_name, #input) -> anyhow::Result<()>>,
        })
    }
}

struct MatchItem(Request);
impl ToTokens for MatchItem {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let sig = &self.0;
        let name = &sig.name;
        let type_name = &sig.input;

        tokens.extend(quote! {
            "#name" => {
                let payload = #type_name::from_bytes(payload);
                (handlers.#name)(ctx, payload)
            },
        })
    }
}

impl ToTokens for TraitImpl {
    fn to_tokens(&self, tokens: &mut proc_macro2::TokenStream) {
        let name = self.iden.clone();
        let ctx = self.ctx.clone();
        let fields = self
            .requests
            .iter()
            .cloned()
            .map(|i| FieldItem(i, ctx.clone()));
        let match_items = self.requests.iter().cloned().map(MatchItem);

        tokens.extend(quote! {
            pub struct #name {
                #(#fields)*
            }

            pub fn parse_message(handlers: #name, ctx: #ctx, ptype: String, payload: &[u8]) -> anyhow::Result<()> {
                match ptype.as_str() {
                    #(#match_items)*
                    _ => unreachable!()
                }
            }
        })
    }
}

#[derive(Clone)]
struct Request {
    name: syn::Ident,
    _colon: syn::Token![:],
    input: syn::Ident,
}

impl Parse for Request {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        Ok(Self {
            name: input.parse()?,
            _colon: input.parse()?,
            input: input.parse()?,
        })
    }
}
