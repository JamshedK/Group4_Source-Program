import psycopg2
import json
import pprint

class Database:

    # Only primary and foreign keys
    tables = {
        'customer': ['c_custkey', 'c_nationkey'],
        'lineitem': ['l_orderkey', 'l_linenumber', 'l_partkey', 'l_suppkey'],
        'nation': ['n_nationkey', 'n_regionkey'],
        'orders': ['o_orderkey', 'o_custkey'],
        'part': ['p_partkey'],
        'partsupp': ['ps_partkey', 'ps_suppkey'],
        'region': ['r_regionkey'],
        'supplier': ['s_suppkey', 's_nationkey']
    }

    def __init__(self):
        self.connection_config = {
            "dbname": "benchbase",
            "user": "postgres",
            "password": "123456",
            "host": "127.0.0.1",
            "port": 5432
        }

    def _connect(self):
        return psycopg2.connect(**self.connection_config)

    """
        Action-related methods
    """
    def drop_index(self, column, table):
        index_name = f"idx_{column}"
        command = f'DROP INDEX IF EXISTS {index_name};'
        try:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(command)
                    print(f"Dropped index on ({table}) {column}")
        except Exception as ex:
            print(f"Didn't drop index on {column}, error: {ex}")

    def create_index(self, column, table):
        index_name = f"idx_{column}"
        command = f'CREATE INDEX {index_name} ON {table} ({column});'
        try:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(command)
        except Exception as ex:
            print(f"Didn't create index on {column}, error: {ex}")

    """
        State-related methods
    """
    def get_table_columns(self, table):
        query = """
            SELECT column_name FROM information_schema.columns
            WHERE table_name = %s;
        """
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (table,))
                all_columns = [row[0] for row in cur.fetchall()]
        return [col for col in all_columns if col not in self.tables[table]]

    def get_table_indexed_columns(self, table):
        query = """
            SELECT a.attname
            FROM pg_class t, pg_class i, pg_index ix, pg_attribute a
            WHERE
                t.oid = ix.indrelid
                AND i.oid = ix.indexrelid
                AND a.attrelid = t.oid
                AND a.attnum = ANY(ix.indkey)
                AND t.relkind = 'r'
                AND t.relname = %s;
        """
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (table,))
                return [row[0] for row in cur.fetchall() if row[0] not in self.tables[table]]

    def get_indexes_map(self):
        indexes_map = {}
        for table in self.tables:
            indexes_map[table] = {}
            indexed_columns = self.get_table_indexed_columns(table)
            table_columns = self.get_table_columns(table)
            for column in table_columns:
                indexes_map[table][column] = 1 if column in indexed_columns else 0
        return indexes_map

    """
        Environment-related methods
    """
    def reset_indexes(self):
        query = """
            SELECT indexname FROM pg_indexes
            WHERE schemaname = 'public' AND indexname LIKE 'idx_%';
        """
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                index_names = [row[0] for row in cur.fetchall()]
                for index in index_names:
                    try:
                        cur.execute(f"DROP INDEX IF EXISTS {index};")
                    except Exception as ex:
                        print(f"Failed to drop index {index}: {ex}")
        return True

    def analyze_tables(self):
        with self._connect() as conn:
            with conn.cursor() as cur:
                for table in self.tables:
                    cur.execute(f"ANALYZE {table};")
        print("Analyzed tables")
