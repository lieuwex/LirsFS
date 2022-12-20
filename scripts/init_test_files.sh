#!/usr/bin/env bash

rm -rfv ./data/
rm -rfv ./logs/
rm -fv ./config*.toml

nodes=$(seq 0 2)

for node in $nodes; do
	mkdir -p "./data/$node/files/"
	mkdir -p "./data/$node/db/"

	ls ./sql/*.sql | while read file; do
		sqlite3 "./data/$node/db/db.db" < $file
	done

	cat <<EOF > "./config$node.toml"
cluster_name = "test"
node_id = $node
webdav_addr = "[::]:8080"
slowdown = true

file_dir = "./data/$node/files"

file_registry = "./data/$node/db/db.db"
file_registry_snapshot = "./data/$node/db/snapshot.db"
hardstate_file = "./data/$node/db/raft_hardstate"
EOF

	for j in $nodes; do
		cat <<EOF >> "./config$node.toml"

[[nodes]]
id = $j
tarpc_addr = "127.0.0.1:20$(printf '%02d' $j)"
ssh_addr = "127.0.0.1:21"
EOF
	done
done
