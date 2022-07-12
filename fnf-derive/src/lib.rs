use proc_macro2::TokenStream;
use quote::{format_ident, quote, ToTokens};
use syn::{braced, parse::Parse, parse_macro_input};

#[proc_macro]
pub fn background_job(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let def = parse_macro_input!(input as TraitImpl);
    def.into_token_stream().into()
}

struct TraitImpl {
    _struct: syn::Token!(struct),
    iden: syn::Ident,
    _brace_token: syn::token::Brace,
    requests: syn::punctuated::Punctuated<Request, syn::Token![,]>,
}

impl Parse for TraitImpl {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let content;
        Ok(Self {
            _struct: input.parse()?,
            iden: input.parse()?,
            _brace_token: braced!(content in input),
            requests: content.parse_terminated(Request::parse)?,
        })
    }
}

impl ToTokens for TraitImpl {
    fn to_tokens(&self, tokens: &mut proc_macro2::TokenStream) {
        let name = self.iden.clone();
        let context_name = format_ident!("{}Context", name);
        let context_name2 = context_name.clone();

        let impl_message = self.requests.iter().cloned().map(ImplMessage);
        let public_fields = self
            .requests
            .iter()
            .cloned()
            .map(|req| FieldItem::new(req, &context_name, OutputType::FieldPub));
        let fields_for_builder = self
            .requests
            .iter()
            .cloned()
            .map(|req| FieldItem::new(req, &context_name, OutputType::FieldPrivate));
        let uninitialized_fields = self
            .requests
            .iter()
            .cloned()
            .map(|req| FieldItem::new(req, &context_name, OutputType::UninitializedField));
        let builder_methods = self
            .requests
            .iter()
            .cloned()
            .map(|req| FieldItem::new(req, &context_name, OutputType::BuilderMethod));
        let builder_assignments = self
            .requests
            .iter()
            .cloned()
            .map(|req| FieldItem::new(req, &context_name, OutputType::Assignment));

        let match_items = self.requests.iter().cloned().map(MatchArm);

        let builder_type_name = format_ident!("{}Builder", name);

        tokens.extend(quote! {
            use ::fnf_rs::JobParameter;

            pub struct #context_name<C> {
                pub job: ::fnf_rs::BackgroundJobServerPublisher,
                pub app: C,
            }

            impl<C> #context_name<C> {
                pub fn enqueue(&self, message: impl ::fnf_rs::JobParameter) -> anyhow::Result<fnf_rs::JobId> {
                    self.job.enqueue(message)
                }
            }

            pub struct #builder_type_name<C> {
                ctx: C,
                id: String,
                amqp_address: String,

                #(#fields_for_builder)*
            }

            impl<C> #builder_type_name<C> {
                pub fn new(context: C, id: String, amqp_address: String) -> Self {
                    Self {
                        ctx: context,
                        id,
                        amqp_address,

                        #(#uninitialized_fields)*
                    }
                }

                #(#builder_methods)*

                pub fn build(self) -> anyhow::Result<fnf_rs::BackgroundJobServer<C, #name<C>>>
                where
                    C: Sync + Send + Clone + 'static,
                {
                    let publisher = fnf_rs::BackgroundJobServerPublisher::new(self.id.clone(), self.amqp_address.clone())?;
                    let ctx = #context_name {
                        job: publisher,
                        app: self.ctx,
                    };
                    let handler = #name {
                        ctx: ctx,
                        #(#builder_assignments)*
                    };

                    let publisher = fnf_rs::BackgroundJobServerPublisher::new(self.id, self.amqp_address)?;
                    BackgroundJobServer::start(handler, publisher)
                }
            }

            #(#impl_message)*

            pub struct #name<C> {
                pub ctx: #context_name2<C>,
                #(#public_fields)*
            }

            impl<C> ::fnf_rs::BgJobHandler<C> for #name<C> {
                fn dispatch(&self, ptype: String, payload: &[u8]) -> anyhow::Result<()> {
                    match ptype.as_str() {

                        #(#match_items)*

                        _ => unreachable!()
                    }
                }

                fn get_ctx(&self) -> &C {
                    &self.ctx.app
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

struct FieldItem {
    req: Request,
    out_type: OutputType,
    context_name: proc_macro2::Ident,
}
impl FieldItem {
    fn new(req: Request, context_name: &proc_macro2::Ident, out_type: OutputType) -> Self {
        Self {
            req,
            out_type,
            context_name: context_name.clone(),
        }
    }
}

enum OutputType {
    FieldPrivate,
    FieldPub,
    UninitializedField,
    BuilderMethod,
    Assignment,
}
impl ToTokens for FieldItem {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let sig = &self.req;
        let name = &sig.name;
        let builder_method_name = format_ident!("with_{}_handler", name);
        let input = &sig.input;
        let docs = format!("Register a handler for [`{}`].\nThis handler will be called when a job is enqueued with a payload of this type.", input);
        let context_name = self.context_name.clone();

        match self.out_type {
            OutputType::FieldPrivate => {
                tokens.extend(quote! {
                    #name: ::core::option::Option<Box<dyn Fn(&#context_name<C>, #input) -> anyhow::Result<()> + Send + Sync>>,
                })
            },
            OutputType::FieldPub => {
                tokens.extend(quote! {
                    pub #name: ::core::option::Option<Box<dyn Fn(&#context_name<C>, #input) -> anyhow::Result<()> + Send + Sync>>,
                })
            },
            OutputType::UninitializedField => {
                tokens.extend(quote! {
                    #name: ::core::option::Option::None,
                })
            },
            OutputType::BuilderMethod => {
                tokens.extend(quote! {
                    #[doc = #docs]
                    pub fn #builder_method_name<M>(mut self, handler: M) -> Self
                    where
                        M: Fn(&#context_name<C>, #input) -> anyhow::Result<()> + Send + Sync + 'static {
                        self.#name = Some(Box::new(handler));
                        self
                    }

                })
            },
            OutputType::Assignment => {
                tokens.extend(quote! {
                    #name: self.#name,
                })
            },
        }
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
                if let Some(handler) = &self.#name {
                    (handler)(&self.ctx, payload)
                } else {
                    unimplemented!("")
                }
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
