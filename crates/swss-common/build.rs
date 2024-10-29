use std::{
    env, fs,
    path::{Path, PathBuf},
};

fn main() {
    let mut bindgen = bindgen::builder();

    match env::var_os("SWSS_COMMON_REPO") {
        // if SWSS_COMMON_REPO is specified, we will build this library based on the files found in
        // the repo
        Some(path_string) => {
            // Get full path of repo
            let path = Path::new(&path_string);
            let path = fs::canonicalize(path).unwrap_or_else(|e| {
                let debug_path = if path.is_relative() {
                    env::current_dir().unwrap()
                } else {
                    PathBuf::new()
                }
                .join(path);
                panic!("{debug_path:?}: {e}");
            });
            assert!(path.exists(), "{path:?} doesn't exist");

            // Add libs path to linker search
            let libs_path = path.join("common/.libs").to_str().unwrap().to_string();
            println!("cargo:rustc-link-search=native={libs_path}");

            // include all of common/c-api/*.h
            let include_path = path.join("common/c-api");
            let header_paths = fs::read_dir(include_path)
                .unwrap()
                .map(|res_entry| res_entry.unwrap().path())
                .filter(|p| p.extension().map_or(false, |ext| ext == "h"))
                .map(|p| p.to_str().unwrap().to_string())
                .collect::<Vec<_>>();

            bindgen = bindgen.headers(&header_paths);

            for h in header_paths {
                println!("cargo:rerun-if-changed={h}");
            }
        }

        // otherwise, we will assume the libswsscommon-dev package is installed and files
        // will be found at the locations defined in <sonic-swss-common repo>/debian/libswsscommon-dev.install
        None => {
            eprintln!("NOTE: If you are compiling outside of sonic-buildimage, you must set SWSS_COMMON_REPO to the path of a built copy of the sonic-swss-common repo");
            const INCLUDE_DIR: &str = "/usr/include/swss/c-api";
            const HEADERS: &[&str] = &[
                "consumerstatetable.h",
                "dbconnector.h",
                "producerstatetable.h",
                "subscriberstatetable.h",
                "util.h",
                "zmqclient.h",
                "zmqconsumerstatetable.h",
                "zmqproducerstatetable.h",
                "zmqserver.h",
            ];
            let header_paths = HEADERS.iter().map(|h| format!("{INCLUDE_DIR}/{h}")).collect::<Vec<_>>();
            bindgen = bindgen.headers(header_paths);
        }
    };

    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap()).join("bindings.rs");
    bindgen
        .derive_partialeq(true)
        .generate()
        .unwrap()
        .write_to_file(out_path)
        .unwrap();

    println!("cargo:rustc-link-lib=dylib=swsscommon");
}
