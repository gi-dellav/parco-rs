use proc_macro2::{Span, TokenStream};
use quote::quote;
use syn::{
    FnArg, GenericArgument, ImplItem, ItemImpl, LitStr, PathArguments, ReturnType, Token, Type,
    parse::{Parse, ParseStream},
    parse_quote,
};

/// Parsed arguments of the `#[task(...)]` attribute.
pub struct TaskArgs {
    krate: syn::Path,
    name: Option<LitStr>,
    idempotent: bool,
    cancellable: bool,
}

impl Parse for TaskArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut krate: syn::Path = parse_quote!(::disco_foundation);
        let mut name = None;
        let mut idempotent = false;
        let mut cancellable = false;

        while !input.is_empty() {
            let ident: syn::Ident = input.parse()?;
            match ident.to_string().as_str() {
                "idempotent" => idempotent = true,
                "cancellable" => cancellable = true,
                "name" => {
                    input.parse::<Token![=]>()?;
                    name = Some(input.parse()?);
                }
                "crate" => {
                    input.parse::<Token![=]>()?;
                    krate = input.parse()?;
                }
                other => {
                    return Err(syn::Error::new(
                        ident.span(),
                        format!(
                            "unknown task option `{other}`; expected `name`, `idempotent`, `cancellable` or `crate`"
                        ),
                    ));
                }
            }
            if input.peek(Token![,]) {
                input.parse::<Token![,]>()?;
            }
        }

        Ok(TaskArgs {
            krate,
            name,
            idempotent,
            cancellable,
        })
    }
}

pub fn expand(attr: TokenStream, item: TokenStream) -> syn::Result<TokenStream> {
    let args: TaskArgs = syn::parse2(attr)?;
    let item_impl: ItemImpl = syn::parse2(item)?;

    let method = item_impl
        .items
        .iter()
        .find_map(|item| match item {
            ImplItem::Fn(function) => Some(function),
            _ => None,
        })
        .ok_or_else(|| {
            syn::Error::new_spanned(&item_impl, "`#[task]` requires at least one method")
        })?;

    let method_ident = method.sig.ident.clone();
    let (output_ty, error_ty) = result_types(&method.sig.output)?;

    let has_receiver = method
        .sig
        .inputs
        .iter()
        .any(|arg| matches!(arg, FnArg::Receiver(_)));

    let input_ty = method
        .sig
        .inputs
        .iter()
        .find_map(|arg| match arg {
            FnArg::Typed(pat) => Some((*pat.ty).clone()),
            FnArg::Receiver(_) => None,
        })
        .ok_or_else(|| {
            syn::Error::new_spanned(
                &method.sig,
                "task method needs an input parameter and a `&TaskContext` parameter",
            )
        })?;

    let self_ty = &item_impl.self_ty;
    let name = args.name.unwrap_or_else(|| default_name(self_ty));
    let krate = &args.krate;
    let idempotent = args.idempotent;
    let cancellable = args.cancellable;

    let call = if has_receiver {
        quote!(self.#method_ident(input, ctx))
    } else {
        quote!(Self::#method_ident(input, ctx))
    };

    Ok(quote! {
        #item_impl

        impl #krate::task::Task for #self_ty {
            type Input = #input_ty;
            type Output = #output_ty;
            type Error = #error_ty;

            const NAME: &'static str = #name;
            const IDEMPOTENT: bool = #idempotent;
            const CANCELLABLE: bool = #cancellable;

            fn execute(
                &self,
                input: Self::Input,
                ctx: &#krate::task::TaskContext,
            ) -> Result<Self::Output, Self::Error> {
                #call
            }
        }
    })
}

fn default_name(self_ty: &Type) -> LitStr {
    if let Type::Path(type_path) = self_ty
        && let Some(segment) = type_path.path.segments.last()
    {
        return LitStr::new(&segment.ident.to_string(), segment.ident.span());
    }
    LitStr::new("Task", Span::call_site())
}

fn result_types(output: &ReturnType) -> syn::Result<(Type, Type)> {
    let ty = match output {
        ReturnType::Type(_, ty) => &**ty,
        ReturnType::Default => {
            return Err(syn::Error::new_spanned(
                output,
                "task method must return `Result<Output, Error>`",
            ));
        }
    };

    if let Type::Path(type_path) = ty
        && let Some(segment) = type_path.path.segments.last()
        && segment.ident == "Result"
        && let PathArguments::AngleBracketed(arguments) = &segment.arguments
    {
        let mut types = arguments.args.iter().filter_map(|argument| match argument {
            GenericArgument::Type(argument) => Some(argument.clone()),
            _ => None,
        });
        if let (Some(output), Some(error)) = (types.next(), types.next()) {
            return Ok((output, error));
        }
    }

    Err(syn::Error::new_spanned(
        ty,
        "task method must return `Result<Output, Error>`",
    ))
}
