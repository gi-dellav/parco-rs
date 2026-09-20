use proc_macro2::TokenStream;
use quote::quote;
use syn::{DeriveInput, LitInt, LitStr, parse_quote};

pub fn expand(input: &DeriveInput) -> syn::Result<TokenStream> {
    let mut krate: syn::Path = parse_quote!(::disco_foundation);
    let mut name: Option<LitStr> = None;
    let mut version: Option<LitInt> = None;

    for attr in &input.attrs {
        if !attr.path().is_ident("capability") {
            continue;
        }
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("name") {
                name = Some(meta.value()?.parse::<LitStr>()?);
            } else if meta.path.is_ident("version") {
                version = Some(meta.value()?.parse::<LitInt>()?);
            } else if meta.path.is_ident("crate") {
                krate = meta.value()?.parse()?;
            } else {
                return Err(meta
                    .error("unknown capability attribute; expected `name`, `version` or `crate`"));
            }
            Ok(())
        })?;
    }

    let ident = &input.ident;
    let name = name.unwrap_or_else(|| LitStr::new(&ident.to_string(), ident.span()));
    let version = version
        .map(|version| quote!(#version))
        .unwrap_or_else(|| quote!(1u32));

    Ok(quote! {
        impl #krate::capability::Capability for #ident {
            const NAME: &'static str = #name;
            const VERSION: u32 = #version;
        }
    })
}
