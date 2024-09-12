use std::{env, fs, path::PathBuf};

fn main() {
    let swss_common_path = fs::canonicalize(PathBuf::from(
        env::var("SWSS_COMMON_PATH").unwrap_or("../../../sonic-swss-common".into()),
    ))
    .unwrap();
    assert!(
        swss_common_path.exists(),
        "{swss_common_path:?} doesn't exist - set env var SWSS_COMMON_PATH to the root of the sonic-swss-common repo"
    );
    let libs_path = swss_common_path.join("common/.libs").to_str().unwrap().to_string();
    let include_path = swss_common_path.join("common/c-api");
    let header_paths = fs::read_dir(include_path)
        .unwrap()
        .map(|res_entry| res_entry.unwrap().path())
        .filter(|p| p.extension().map_or(false, |ext| ext == "h"))
        .map(|p| p.to_str().unwrap().to_string())
        .collect::<Vec<_>>();

    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap()).join("bindings.rs");
    bindgen::builder()
        .headers(&header_paths)
        .derive_partialeq(true)
        .generate()
        .unwrap()
        .write_to_file(out_path)
        .unwrap();

    println!("cargo:rustc-link-lib=dylib=swsscommon");
    println!("cargo:rustc-link-search=native={libs_path}");
    for h in header_paths {
        println!("cargo:rerun-if-changed={h}");
    }
}
