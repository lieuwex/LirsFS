# LirsFS

## Introduction

`LirsFS` is a remote, distributed, strongy-consistent database with fault tolerance configurable per-file. It is written in Rust.

## Installing

Install Rust using e.g. [Rustup](https://rustup.rs/). `LirsFS` requires version >1.66 of Rust.

Compile using `cargo build`.

## Running

### Locally

The easiest way to run `LirsFS` locally is to use `scripts/init_test_files.sh`. It will generate one configuration per instance, for three instances in total (this amount can easily be adjusted).

Then in three terminals run `cargo run configX.toml` where X is replaced by {0,1,2} for every terminal.

### Remotely

Run the `scripts/init_db.sh` on each node in the cluster. Then create a configuration file (options are given below). Run the `LirsFS` binary on each node.

## Configuration options

The configuration file is in TOML-format. An example is given below:

```toml
# Name of this Raft cluster
cluster_name = "test"

# This node's id
node_id = 0

# Address on which to host the WebDAV server
webdav_addr = "[::]:8080"

# Prefix directory in which this node should save its files
file_dir = "./data/0/files"

# Path to SQLite database to store file metadata
file_registry = "./data/0/db/db.db"

# Path to the generated snapshot databases
file_registry_snapshot = "./data/0/db/snapshot.db"

# Path to the hardstate file, needed for Raft internally
hardstate_file = "./data/0/db/raft_hardstate"


# List of other nodes in the cluster
[[nodes]]
# This node's id
id = 0
# This node's address to receive RPCs
tarpc_addr = "127.0.0.1:2000"
# This node's address for ssh. Required for `rsync` copying.
ssh_addr = "127.0.0.1:21"

# Add as many nodes as there are in the cluster...
[[nodes]]
id = 1
tarpc_addr = "127.0.0.1:2001"
ssh_addr = "127.0.0.1:21"

```
