#!/usr/bin/env bash

rm -rfv ./dfs/data/
rm -rfv ./dfs/logs/
rm -fv ./dfs/config*.toml

nodes=$(seq 0 2)

for node in $nodes; do
	mkdir -p "./dfs/data/$node/files/"
	mkdir -p "./dfs/data/$node/db/"

	ls ./dfs/sql/*.sql | while read file; do
		sqlite3 "./dfs/data/$node/db/db.db" < $file
	done

	cat <<EOF > "./dfs/config$node.toml"
cluster_name = "test"
node_id = $node
webdav_addr = "[::]:8080"

file_dir = "./data/$node/files"

file_registry = "./data/$node/db/db.db"
file_registry_snapshot = "./data/$node/db/snapshot.db"
hardstate_file = "./data/$node/db/raft_hardstate"
EOF

	for j in $nodes; do
		cat <<EOF >> "./dfs/config$node.toml"

[[nodes]]
id = $j
tarpc_addr = "192.168.1.183:20$(printf '%02d' $j)"
ssh_addr = "192.168.1.183:21"
EOF
	done
done
