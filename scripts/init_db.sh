DB=/tmp/db/dev.db
touch $DB

ls ./dfs/sql/*.sql | while read file; do
    echo "Running $file"
    sqlite3 $DB < $file
done
