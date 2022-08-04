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
        let inner_type_name = format_ident!("{}Inner", context_name);

        let impl_message = self.requests.iter().cloned().map(ImplMessage);
        let public_fields = self
            .requests
            .iter()
            .cloned()
            .map(|req| FieldItem::new(req, &inner_type_name, OutputType::FieldPub));
        let fields_for_builder = self
            .requests
            .iter()
            .cloned()
            .map(|req| FieldItem::new(req, &inner_type_name, OutputType::FieldPrivate));
        let uninitialized_fields = self
            .requests
            .iter()
            .cloned()
            .map(|req| FieldItem::new(req, &inner_type_name, OutputType::UninitializedField));
        let builder_methods = self
            .requests
            .iter()
            .cloned()
            .map(|req| FieldItem::new(req, &inner_type_name, OutputType::BuilderMethod));
        let builder_assignments = self
            .requests
            .iter()
            .cloned()
            .map(|req| FieldItem::new(req, &inner_type_name, OutputType::Assignment));

        let match_items = self.requests.iter().cloned().map(MatchArm);

        let builder_type_name = format_ident!("{}Builder", name);

        tokens.extend(quote! {

            pub struct #context_name<C: Send + Sync> { // Deref to `inner`
                inner: std::sync::Arc<#inner_type_name<C>>,
            }

            pub struct #inner_type_name<C> { // Deref to `job`
                job: ::later::BackgroundJobServerPublisher,
                app: C,
            }

            impl<C> std::ops::Deref for #context_name<C>
            where
                C: Send + Sync,
            {
                type Target = #inner_type_name<C>;

                fn deref(&self) -> &Self::Target {
                    &self.inner
                }
            }

            impl<C> std::ops::Deref for #inner_type_name<C> {
                type Target = ::later::BackgroundJobServerPublisher;

                fn deref(&self) -> &Self::Target {
                    &self.job
                }
            }

            // Build the stub
            pub struct #builder_type_name<C>
            where
                C: Sync + Send + 'static,
            {
                ctx: C,
                id: String,
                amqp_address: String,
                storage: Box<dyn ::later::storage::Storage>,

                #(#fields_for_builder)*
            }

            impl<C> #builder_type_name<C>
            where
                C: Sync + Send + 'static,
                {
                pub fn new(context: C, id: String, amqp_address: String, storage: Box<dyn ::later::storage::Storage>) -> Self {
                    Self {
                        ctx: context,
                        id,
                        amqp_address,
                        storage,

                        #(#uninitialized_fields)*
                    }
                }

                /// Accept a simplified and ergonomic async function handler
                ///     Fn(Ctx<C>, Payload) -> impl Future<Output = anyhow::Result<()>>
                /// and map this to the complex/nasty stuff required internally to make the compiler happy.
                ///     Fn(Arc<CtxWrapper<C>>, Payload) -> Pin<Box<Future<Output = anyhow::Result<()>>>
                fn wrap_complex_handler<Payload, HandlerFunc, Fut>(
                    arc_ctx: std::sync::Arc<#inner_type_name<C>>,
                    payload: Payload,
                    handler: HandlerFunc,
                ) -> ::later::futures::future::BoxFuture<'static, Fut::Output>
                where
                    HandlerFunc: FnOnce(#context_name<C>, Payload) -> Fut + Send + 'static,
                    Payload: ::later::core::JobParameter + Send + 'static,
                    Fut: ::later::futures::future::Future<Output = anyhow::Result<()>> + Send,
                {
                    Box::pin(async move {
                        let ctx = #context_name {
                            inner: arc_ctx.clone(),
                        };
                        handler(ctx, payload).await
                    })
                }

                #(#builder_methods)*

                pub fn build(self) -> anyhow::Result<later::BackgroundJobServer<C, #name<C>>>
                {
                    let publisher = later::BackgroundJobServerPublisher::new(
                        self.id.clone(),
                        self.amqp_address.clone(),
                        self.storage,
                    )?;
                    let ctx_inner = #inner_type_name {
                        job: publisher,
                        app: self.ctx,
                    };
                    let handler = #name {
                        ctx: std::sync::Arc::new(ctx_inner),
                        #(#builder_assignments)*
                    };

                    ::later::BackgroundJobServer::start(handler)
                }
            }

            #[async_trait]
            impl<C> ::later::core::BgJobHandler<C> for #name<C>
            where
                C: Sync + Send + 'static,
            {
                async fn dispatch(&self, ptype: String, payload: &[u8]) -> anyhow::Result<()> {
                    use ::later::core::JobParameter;

                    match ptype.as_str() {

                        #(#match_items)*

                        _ => unreachable!()
                    }
                }

                fn get_ctx(&self) -> &C {
                    &self.ctx.app
                }

                fn get_publisher(&self) -> &::later::BackgroundJobServerPublisher {
                    &self.ctx.job
                }
            }

            pub struct #name<C>
            where
                C: Sync + Send + 'static,
            {
                pub ctx: std::sync::Arc<#inner_type_name<C>>,
                #(#public_fields)*
            }

            #(#impl_message)*
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
            impl ::later::core::JobParameter for #type_name {
                fn to_bytes(&self) -> anyhow::Result<Vec<u8>> {
                    let result = ::later::encoder::encode(&self);
                    let result = ::later::anyhow::Context::context(result, "unable to serialize");
                    Ok(result?)
                }

                fn from_bytes(payload: &[u8]) -> Self {
                    ::later::encoder::decode(payload).unwrap()
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
    inner_name: proc_macro2::Ident,
}
impl FieldItem {
    fn new(req: Request, inner_name: &proc_macro2::Ident, out_type: OutputType) -> Self {
        Self {
            req,
            out_type,
            inner_name: inner_name.clone(),
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
        let inner_name = self.inner_name.clone();

        match self.out_type {
            OutputType::FieldPrivate => {
                tokens.extend(quote! {
                    #name: ::core::option::Option<
                                Box<
                                    dyn Fn(
                                            std::sync::Arc<#inner_name<C>>,
                                            #input,
                                        )
                                            -> ::later::futures::future::BoxFuture<'static, anyhow::Result<()>>
                                        + Sync
                                        + Send,
                                >,
                            >,
                })
            },
            OutputType::FieldPub => {
                tokens.extend(quote! {
                    pub #name: ::core::option::Option<
                                    Box<
                                        dyn Fn(
                                                std::sync::Arc<#inner_name<C>>,
                                                #input,
                                            )
                                                -> ::later::futures::future::BoxFuture<'static, anyhow::Result<()>>
                                            + Sync
                                            + Send,
                                    >,
                                >,
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
                    pub fn #builder_method_name<M, Fut>(mut self, handler: M) -> Self
                    where
                        M: FnOnce(DeriveHandlerContext<C>, #input) -> Fut + Send + Sync + Copy + 'static,
                        Fut: ::later::futures::future::Future<Output = anyhow::Result<()>> + Send,
                        C: Sync + Send + 'static,
                    {
                        self.#name = Some(Box::new(move |ctx, payload| {
                            Self::wrap_complex_handler(ctx, payload, handler)
                        }));
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
                    (handler)(self.ctx.clone(), payload).await
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
