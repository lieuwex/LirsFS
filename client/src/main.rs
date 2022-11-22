use clap::Parser;
use fuse_ops::HelloFS;
use std::{ffi::OsString, io::ErrorKind, path::PathBuf, process::Command};

mod fuse_ops;

#[derive(Parser)]
struct CliArgs {
    mount_point: PathBuf,
}

#[tokio::main]
async fn main() {
    env_logger::init();
    let CliArgs { mount_point } = CliArgs::parse();
    let options = ["-o", "ro", "-o", "fsname=hello"]
        .iter()
        .map(|s| OsString::from(&s))
        .collect::<Vec<OsString>>();

    ctrlc::set_handler(|| {
        Command::new("fusermount")
            .arg("-u")
            .arg("/tmp/fs")
            .spawn()
            .expect("Unmounting FUSE file system with `fusermount` failed");
    })
    .expect("Setting abort-handler failed");

    match async_fuse::mount(HelloFS, &mount_point, &options) {
        Ok(_) => {}
        Err(err) => {
        match err.kind() {
            ErrorKind::NotConnected => panic!("FUSE file system at {:#?} was not properly unmounted. Unmount using `fusermount -u {:#?}` and run the client again.", &mount_point, &mount_point),
            _ => panic!("Unknown error mounting FUSE file system")
        }}
    };
}
