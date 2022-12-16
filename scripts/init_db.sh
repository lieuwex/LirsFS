DB_DIR=/tmp/db/
DB_NAME=dev.db
DB=${DB_DIR}/${DB_NAME}
mkdir -p $DB_DIR && \
touch $DB

ls ./dfs/sql/*.sql | while read file; do
    echo "Running $file"
    sqlite3 $DB < $file
done
