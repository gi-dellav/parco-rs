/// Defines a transparent newtype identifier backed by `uuid::Uuid`.
///
/// ```ignore
/// disco_foundation::uuid_id!(pub ClientId);
/// ```
#[macro_export]
macro_rules! uuid_id {
    (
        $(#[$meta:meta])*
        $vis:vis $name:ident $(,)?
    ) => {
        $(#[$meta])*
        #[derive(
            Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash,
            ::serde::Serialize, ::serde::Deserialize,
        )]
        $vis struct $name(pub ::uuid::Uuid);

        impl $name {
            pub fn new() -> Self {
                Self(::uuid::Uuid::now_v7())
            }
        }

        impl ::core::default::Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl ::core::fmt::Display for $name {
            fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                ::core::write!(f, "{}", self.0)
            }
        }
    };
}
