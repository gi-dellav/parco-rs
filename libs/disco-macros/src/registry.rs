use proc_macro2::TokenStream;
use quote::quote;
use syn::{
    Expr, Ident, Token, Visibility, braced,
    parse::{Parse, ParseStream},
    punctuated::Punctuated,
};

/// Parsed input for the `task_registry!` macro.
pub struct RegistryInput {
    vis: Visibility,
    name: Ident,
    tasks: Punctuated<Expr, Token![,]>,
}

impl Parse for RegistryInput {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let vis: Visibility = input.parse()?;
        let name: Ident = input.parse()?;
        let content;
        braced!(content in input);
        let tasks = content.parse_terminated(Expr::parse, Token![,])?;
        Ok(RegistryInput { vis, name, tasks })
    }
}

/// Parsed input for the `task_dispatcher!` macro.
pub struct DispatcherInput(pub Punctuated<Expr, Token![,]>);

impl Parse for DispatcherInput {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        Ok(DispatcherInput(Punctuated::parse_terminated(input)?))
    }
}

pub fn expand_registry(input: RegistryInput) -> TokenStream {
    let RegistryInput { vis, name, tasks } = input;
    let tasks: Vec<Expr> = tasks.into_iter().collect();

    quote! {
        #vis struct #name {
            inner: ::disco_foundation::task::TaskRegistryImpl,
        }

        impl #name {
            #vis fn new() -> Self {
                let mut registry = ::disco_foundation::task::TaskRegistryImpl::new();
                #( registry.register(#tasks); )*
                Self { inner: registry }
            }

            pub fn registry(&self) -> &::disco_foundation::task::TaskRegistryImpl {
                &self.inner
            }
        }

        impl ::core::default::Default for #name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl ::disco_foundation::task::TaskRegistry for #name {
            fn dispatch(
                &self,
                name: &str,
                payload: &[u8],
                ctx: &::disco_foundation::task::TaskContext,
            ) -> Result<Vec<u8>, ::disco_foundation::error::DispatchError> {
                ::disco_foundation::task::TaskRegistry::dispatch(&self.inner, name, payload, ctx)
            }

            fn contains(&self, name: &str) -> bool {
                ::disco_foundation::task::TaskRegistry::contains(&self.inner, name)
            }

            fn names(&self) -> Vec<&'static str> {
                ::disco_foundation::task::TaskRegistry::names(&self.inner)
            }

            fn cancellable(&self, name: &str) -> bool {
                ::disco_foundation::task::TaskRegistry::cancellable(&self.inner, name)
            }

            fn len(&self) -> usize {
                ::disco_foundation::task::TaskRegistry::len(&self.inner)
            }
        }
    }
}

pub fn expand_dispatcher(tasks: Punctuated<Expr, Token![,]>) -> TokenStream {
    let tasks: Vec<Expr> = tasks.into_iter().collect();

    quote! {{
        let mut registry = ::disco_foundation::task::TaskRegistryImpl::new();
        #( registry.register(#tasks); )*
        ::disco_foundation::task::LocalDispatcher::with_system_clock(registry)
    }}
}
