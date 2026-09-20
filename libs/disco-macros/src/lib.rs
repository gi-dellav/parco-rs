//! Procedural macros for defining Disco tasks, registries and capabilities.
//!
//! This crate is an implementation detail of `disco-foundation`, which
//! re-exports every macro here. Depend on `disco-foundation` rather than
//! `disco-macros` directly.

use proc_macro::TokenStream;

mod capability;
mod registry;
mod task;

/// Implements [`disco_foundation::task::Task`] for an existing type.
///
/// Applied to an `impl` block whose method describes the task body:
///
/// ```ignore
/// #[disco_foundation::task(name = "multiply", idempotent)]
/// impl MultiplyTask {
///     fn run(input: MulInput, ctx: &TaskContext) -> Result<MulOutput, MulError> {
///         Ok(MulOutput(input.value * 2))
///     }
/// }
/// ```
///
/// The method may take `&self` for stateful tasks. The name defaults to the
/// type name; `idempotent` and `cancellable` default to `false`.
#[proc_macro_attribute]
pub fn task(attr: TokenStream, item: TokenStream) -> TokenStream {
    task::expand(attr.into(), item.into())
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// Derives [`disco_foundation::capability::Capability`] for a marker type.
///
/// ```ignore
/// #[derive(Capability)]
/// #[capability(name = "gpu", version = 2)]
/// pub struct Gpu;
/// ```
#[proc_macro_derive(Capability, attributes(capability))]
pub fn derive_capability(item: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(item as syn::DeriveInput);
    capability::expand(&input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// Builds a [`disco_foundation::task::TaskRegistry`] from a set of task values.
///
/// ```ignore
/// disco_foundation::task_registry! {
///     pub MyRegistry {
///         MultiplyTask,
///         AddTask::new(),
///     }
/// }
/// ```
#[proc_macro]
pub fn task_registry(item: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(item as registry::RegistryInput);
    registry::expand_registry(input).into()
}

/// Builds a ready-to-use [`disco_foundation::task::LocalDispatcher`].
///
/// ```ignore
/// let dispatcher = disco_foundation::task_dispatcher![MultiplyTask];
/// ```
#[proc_macro]
pub fn task_dispatcher(item: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(item as registry::DispatcherInput);
    registry::expand_dispatcher(input.0).into()
}
