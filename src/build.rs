//! Build script for the N-API project.
//!
//! This script uses the `napi-build` crate to compile the Rust code
//! into a Node.js addon, handling the necessary configuration for
//! creating a native module that can be loaded by Node.js.

extern crate napi_build;

/// The main function of the build script.
///
/// This function is executed by Cargo when building the crate. It calls
/// `napi_build::setup()` to configure the build for N-API compatibility,
/// which ensures that the compiled library can be correctly loaded and
/// used as a native Node.js module.
fn main() {
    napi_build::setup();
}
