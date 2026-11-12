"""Shared compiler settings from Cargo.toml's workspace contract."""

RUST_FLAGS = [
    "-Cdebuginfo=2",
    "-Wunused_extern_crates",
    "-Wtrivial_numeric_casts",
    "-Wunstable_features",
    "-Wunused_import_braces",
]
