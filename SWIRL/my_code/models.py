from typing import List
from functools import total_ordering
import importlib
import os
from database_handler import PostgresDatabaseConnector, TableGenerator

### Create objects to represent Indexes, Tables, Queries, and Workloads
### From index_selection_evaluation at  https://github.com/hyrise/index_selection_evaluation/tree/rl_index_selection

@total_ordering
class Index:
    def __init__(self, columns, estimated_size=None):
        if len(columns) == 0:
            raise ValueError("Index needs at least 1 column")
        self.columns = tuple(columns)
        # Store hypopg estimated size when `store_size=True` (whatif)
        self.estimated_size = estimated_size
        self.hypopg_name = None

    # Used to sort indexes
    def __lt__(self, other):
        if len(self.columns) != len(other.columns):
            return len(self.columns) < len(other.columns)

        return self.columns < other.columns

    def __repr__(self):
        columns_string = ",".join(map(str, self.columns))
        return f"I({columns_string})"

    def __eq__(self, other):
        if not isinstance(other, Index):
            return False

        return self.columns == other.columns

    def __hash__(self):
        return hash(self.columns)

    def _column_names(self):
        return [x.name for x in self.columns]

    def is_single_column(self):
        return True if len(self.columns) == 1 else False

    def table(self):
        assert (
            self.columns[0].table is not None
        ), "Table should not be None to avoid false positive comparisons."
        return self.columns[0].table

    def index_idx(self):
        columns = "_".join(self._column_names())
        return f"{self.table()}_{columns}_idx"

    def joined_column_names(self):
        return ",".join(self._column_names())

    def appendable_by(self, other):
        if not isinstance(other, Index):
            return False

        if self.table() != other.table():
            return False

        if not other.is_single_column():
            return False

        if other.columns[0] in self.columns:
            return False

        return True

    def subsumes(self, other):
        if not isinstance(other, Index):
            return False
        return self.columns[: len(other.columns)] == other.columns

    def prefixes(self):
        """Consider I(K;S). For any prefix K' of K (including K' = K if S is not
        empty), an index I_P = (K';Ø) is obtained.
        Returns a list of index prefixes ordered by decreasing width."""
        index_prefixes = []
        for prefix_width in range(len(self.columns) - 1, 0, -1):
            index_prefixes.append(Index(self.columns[:prefix_width]))
        return index_prefixes

class Workload:
    def __init__(self, queries, description=""):
        self.queries = queries
        self.budget = None
        self.description = description

    def indexable_columns(self, return_sorted=True):
        indexable_columns = set()
        for query in self.queries:
            indexable_columns |= set(query.columns)
        if not return_sorted:
            return indexable_columns
        return sorted(list(indexable_columns))

    def potential_indexes(self):
        return sorted([Index([c]) for c in self.indexable_columns()])

    def __repr__(self):
        ids = []
        fr = []
        for query in self.queries:
            ids.append(query.nr)
            fr.append(query.frequency)

        return f"Query IDs: {ids} with {fr}. {self.description} Budget: {self.budget}"


class Query:
    def __init__(self, query_id, query_text, columns=None, frequency=1):
        self.nr = query_id
        self.text = query_text
        self.frequency = frequency

        # Indexable columns
        if columns is None:
            self.columns = []
        else:
            self.columns = columns

    def __repr__(self):
        return f"Q{self.nr}"

    def __eq__(self, other):
        if not isinstance(other, Query):
            return False

        return self.nr == other.nr

    def __hash__(self):
        return hash(self.nr)
    
class Schema(object):
    def __init__(self, benchmark_name, scale_factor, filters={}):
        generating_connector = PostgresDatabaseConnector(None, autocommit=True)
        table_generator = TableGenerator(
            benchmark_name=benchmark_name.lower(), scale_factor=scale_factor, database_connector=generating_connector
        )

        self.database_name = table_generator.database_name()
        self.tables = table_generator.tables

        self.columns = []
        for table in self.tables:
            for column in table.columns:
                self.columns.append(column)

        filter_instance = TableNumRowsFilter(filters["TableNumRowsFilter"], self.database_name)
        self.columns = filter_instance.apply_filter(self.columns)

class TableNumRowsFilter(object):
    def __init__(self, threshold, database_name):
        self.threshold = threshold
        self.connector = PostgresDatabaseConnector(database_name, autocommit=True)
        self.connector.create_statistics()

    def apply_filter(self, columns):
        output_columns = []

        for column in columns:
            table_name = column.table.name
            table_num_rows = self.connector.exec_fetch(
                f"SELECT reltuples::bigint AS estimate FROM pg_class where relname='{table_name}'"
            )[0]

            if table_num_rows > self.threshold:
                output_columns.append(column)

        #logging.warning(f"Reduced columns from {len(columns)} to {len(output_columns)}.")

        return output_columns