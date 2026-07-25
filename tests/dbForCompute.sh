#!/usr/bin/env bash

echo -e "Resolving paths "
mkdir -p ./data/test_env
touch ./data/test_env/computar_queries.sqlite


sqlite3 ./data/test_env/computar_queries.sqlite \
-cmd "CREATE TABLE IF NOT EXISTS state(state_id INTEGER PRIMARY KEY, name TEXT);" \
-cmd "CREATE TABLE IF NOT EXISTS department(
    department_id INTEGER PRIMARY KEY,
    name TEXT,
    location TEXT,
    state_id INTEGER,
    FOREIGN KEY (state_id)
        REFERENCES state (state_id)
        ON DELETE CASCADE
        ON UPDATE NO ACTION
);" \
-cmd "INSERT INTO state(name) VALUES('Tlaxcala');" \
-cmd "INSERT INTO department(location, state_id) VALUES('Chiautempan', 1);" \
.quit

echo -e "Database ready for testing..."