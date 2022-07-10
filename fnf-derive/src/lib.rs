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
    // _ang: syn::Token!(<),
    // ctx: syn::Ident,
    // _ang_2: syn::Token!(>),
    _brace_token: syn::token::Brace,
    requests: syn::punctuated::Punctuated<Request, syn::Token![,]>,
}

impl Parse for TraitImpl {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let content;
        Ok(Self {
            _struct: input.parse()?,
            iden: input.parse()?,
            // _ang: input.parse()?,
            // ctx: input.parse()?,
            // _ang_2: input.parse()?,
            _brace_token: braced!(content in input),
            requests: content.parse_terminated(Request::parse)?,
        })
    }
}

impl ToTokens for TraitImpl {
    fn to_tokens(&self, tokens: &mut proc_macro2::TokenStream) {
        let name = self.iden.clone();
        //let ctx = self.ctx.clone();

        let impl_message = self.requests.iter().cloned().map(ImplMessage);
        let fields = self
            .requests
            .iter()
            .cloned()
            .map(FieldItem);
        let match_items = self.requests.iter().cloned().map(MatchArm);

        tokens.extend(quote! {
            use ::fnf_rs::JobParameter;

            #(#impl_message)*

            pub struct #name<C> {
                #(#fields)*
            }

            impl<C> ::fnf_rs::BgJobHandler<C> for #name<C> {
                fn dispatch(&self, ctx: C, ptype: String, payload: &[u8]) -> anyhow::Result<()> {
                    match ptype.as_str() {

                        #(#match_items)*

                        _ => unreachable!()
                    }
                }
            }
        })
    }
}

struct ImplMessage(Request);
impl ToTokens for ImplMessage {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let sig = &self.0;
        let type_name = &sig.input;
        let ptype = &sig.name;

        tokens.extend(quote! {
            impl ::fnf_rs::JobParameter for #type_name {
                fn to_bytes(&self) -> anyhow::Result<Vec<u8>> {
                    let result = ::fnf_rs::serde_json::to_vec(&self);
                    let result = ::fnf_rs::anyhow::Context::context(result, "unable to serialize");
                    Ok(result?)
                }

                fn from_bytes(payload: &[u8]) -> Self {
                    ::fnf_rs::serde_json::from_slice(payload).unwrap()
                }

                fn get_ptype(&self) -> String {
                    stringify!(#ptype).into()
                }
            }
        })
    }
}

struct FieldItem(Request);
impl ToTokens for FieldItem {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let sig = &self.0;
        let name = &sig.name;
        let input = &sig.input;

        tokens.extend(quote! {
            pub #name: Box<dyn Fn(&C, #input) -> anyhow::Result<()> + Send + Sync>,
        })
    }
}

struct MatchArm(Request);
impl ToTokens for MatchArm {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let sig = &self.0;
        let name = &sig.name;
        let type_name = &sig.input;

        tokens.extend(quote! {
            stringify!(#name) => {
                let payload = #type_name::from_bytes(payload);
                (self.#name)(&ctx, payload)
            },
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
