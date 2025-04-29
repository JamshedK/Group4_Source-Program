import numpy as np
import json
import random
import itertools

from models import Query, Workload, Schema

### ADBMS Group 4 implementation of SWIRL algorithm
### original algorithm implemented by Kossman et al. at 
### https://github.com/hyrise/rl_index_selection?tab=readme-ov-file

class Experiment:
    def __init__(self):  
        # Parameters
        self.rnd = random.Random()
        self.rnd.seed(2)    # Set random seed for reproducibility
        self.np_rnd = np.random.default_rng(seed=2)  # Numpy random generator with fixed seed
        self.benchmark = "TPCH"  # Use TPCH benchmark
        self.QUERY_PATH = '../rl_index_selection/query_files/TPCH'  # Path to query files
        self.COLUMN_FILENAME = 'column_names.txt'  # Column file (unused here)
        
        # Query classes
        self.available_query_classes = [1, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 18, 19, 21, 22]
        
        self.varying_frequencies = True  # Whether to vary frequency of each query in workloads
        self.schema = Schema('TPCH', scale_factor=1, filters={ "TableNumRowsFilter": 10000 })  # Load schema

        # Experiment workload configuration
        self.training_instances = 1000
        self.test_instances = 5
        self.size = 19  # Number of queries in a workload
        self.budgets = [500, 1000, 1500, 2500, 3500, 5000, 6500, 8000, 10000]  # Budget options
        self.max_index_width = 3  # Max number of columns in an index

        # Runtime variables
        self.query_texts = []  # Query text list
        self.workload_columns = []  # All columns in the schema
        self.all_indexable_columns = []  # Columns eligible for indexing
        self.wl_training = []  # Training workloads
        self.wl_validation = [None]  # Validation workloads
        self.wl_testing = [None]  # Testing workloads

def main():
    # Main function to set up the experiment
    experiment = Experiment()

    ### Initialize workload
    print('Initializing workload...')
    experiment.workload_columns = experiment.schema.columns  # Load schema columns

    experiment.query_texts = _retrieve_query_texts(experiment)  # Load queries
    experiment.all_indexable_columns = _select_indexable_columns(experiment)  # Determine which columns can be indexed

    # Generate training, validation, and test workloads
    experiment.wl_training, experiment.wl_validation[0], experiment.wl_testing[0] = _generate_workloads(
        experiment, experiment.training_instances, experiment.test_instances, experiment.size
    )
    
    print('Done generating workloads!')

    # Assign budgets to each workload randomly
    _assign_budgets_to_workloads(experiment)

    # Generate permutations of indexable columns for index combinations
    experiment.all_indexable_columns = create_column_permutation_indexes(
        experiment.all_indexable_columns, experiment.max_index_width
    )

def _retrieve_query_texts(exp):
    # Load and preprocess query files for TPCH benchmark
    query_files = [
        open(f"{exp.QUERY_PATH}/TPCH_{file_number}.txt", "r")
        for file_number in range(1, 23)
    ]

    finished_queries = []

    for query_file in query_files:
        queries = query_file.readlines()[:1]  # Read only first query in each file
        queries = _preprocess_queries(queries)
        finished_queries.append(queries)
        query_file.close()

    assert len(finished_queries) == 22  # Ensure all 22 queries are loaded
    return finished_queries

def _preprocess_queries(queries):
    # Clean query text by removing LIMIT clauses
    processed_queries = []
    for query in queries:
        query = query.replace("limit 100", "")
        query = query.replace("limit 20", "")
        query = query.replace("limit 10", "")
        query = query.strip()
        processed_queries.append(query)
    return processed_queries

def _select_indexable_columns(exp, only_utilized_indexes=False):
    # Identify which columns in the workload schema are used in WHERE clauses of the available queries.
    
    # Ensure the query classes are stored as an immutable tuple
    exp.available_query_classes = tuple(exp.available_query_classes)

    # Create a list of uniform frequency (all set to 1) for each query class
    query_class_frequencies = tuple([1 for freq in range(len(exp.available_query_classes))])

    # Generate a list of Workload objects based on the query class/frequency pairs
    workloads = _workloads_from_tuples(exp, [(exp.available_query_classes, query_class_frequencies)])[0]

    # Determine which columns appear in WHERE clauses
    indexable_columns = workloads.indexable_columns()

    selected_columns = []
    global_column_id = 0  # Assign a unique ID to each selected column

    # Select and assign global IDs to columns that are indexable
    for column in exp.workload_columns:
        if column in indexable_columns:
            column.global_column_id = global_column_id
            global_column_id += 1
            selected_columns.append(column)

    return selected_columns

def _workloads_from_tuples(exp, tuples, unknown_query_probability=None):
    # Construct Workload objects from query class/frequency pairs
    
    workloads = []
    unknown_query_probability = "" if unknown_query_probability is None else unknown_query_probability

    for query_classes, query_class_frequencies in tuples:
        queries = []

        for query_class, frequency in zip(query_classes, query_class_frequencies):
            # Pick a random query text from the set of queries for this class
            query_text = random.choice(exp.query_texts[query_class - 1])
            query = Query(query_class, query_text, frequency=frequency)

            # Skip if query doesn't contain a WHERE clause
            if "where" not in query_text:
                continue

            # Extract indexable columns used in this query
            _store_indexable_columns(exp, query)

            if len(query.columns) == 0:
                print(query_text)  # Debug: show query text if no columns are found

            # Ensure each query has at least one column in WHERE clause
            assert len(query.columns) > 0, f"Query columns should have length > 0: {query.text}"
            queries.append(query)

        # Optionally simulate presence of previously unseen queries
        previously_unseen_queries = (
            round(unknown_query_probability * len(queries)) if unknown_query_probability != "" else 0
        )

        # Add the set of queries as a new Workload
        workloads.append(
            Workload(queries, description=f"Contains {previously_unseen_queries} previously unseen queries.")
        )

    return workloads

def _store_indexable_columns(exp, query):
    # Determine and store which schema columns are referenced in the WHERE clause of a query
    
    query_text = query.text
    assert "where" in query_text, f"Query without WHERE clause encountered: {query_text} in {query.nr}"

    # Split query into pre-WHERE and post-WHERE sections
    split = query_text.split("where")
    query_text_before_where = split[0]
    query_text_after_where = ' '.join(split[i] for i in range(1, len(split)))

    # Check if each column name appears in the WHERE clause *and* the table name appears before WHERE
    for column in exp.workload_columns:
        if column.name in query_text_after_where and f"{column.table.name} " in query_text_before_where:
            query.columns.append(column)

def _generate_workloads(exp, train_instances, test_instances, size, unknown_query_probability=None):
    # Generate unique workloads for training, validation, and testing

    required_unique_workloads = train_instances + test_instances + test_instances
    unique_workload_tuples = set()

    # Generate enough unique workloads until required number is met
    while required_unique_workloads > len(unique_workload_tuples):
        workload_tuple = _generate_random_workload(exp, size, unknown_query_probability)
        unique_workload_tuples.add(workload_tuple)

    # Randomly split into validation, test, and training workloads
    validation_tuples = exp.rnd.sample(unique_workload_tuples, test_instances)
    unique_workload_tuples -= set(validation_tuples)

    test_workload_tuples = exp.rnd.sample(unique_workload_tuples, test_instances)
    unique_workload_tuples -= set(test_workload_tuples)

    assert len(unique_workload_tuples) == train_instances
    train_workload_tuples = unique_workload_tuples

    assert (
        len(train_workload_tuples) + len(test_workload_tuples) + len(validation_tuples) == required_unique_workloads
    )

    # Convert tuples into actual Workload objects
    validation_workloads = _workloads_from_tuples(exp, validation_tuples, unknown_query_probability)
    test_workloads = _workloads_from_tuples(exp, test_workload_tuples, unknown_query_probability)
    train_workloads = _workloads_from_tuples(exp, train_workload_tuples, unknown_query_probability)

    return train_workloads, validation_workloads, test_workloads

def _generate_random_workload(exp, size, unknown_query_probability=None):
    # Randomly select a workload of specified size (i.e., number of query classes)

    assert size <= len(exp.available_query_classes)

    workload_query_classes = tuple(exp.rnd.sample(exp.available_query_classes, size))

    # Generate frequencies for each query class (either uniform or varying)
    if exp.varying_frequencies:
        query_class_frequencies = tuple(list(exp.np_rnd.integers(1, 10000, size)))
    else:
        query_class_frequencies = tuple([1 for frequency in range(size)])

    return (workload_query_classes, query_class_frequencies)

def _assign_budgets_to_workloads(exp):
    # Assign a random budget value to each workload in validation and test sets

    for workload_list in exp.wl_testing:
        for workload in workload_list:
            workload.budget = exp.rnd.choice(exp.budgets)

    for workload_list in exp.wl_validation:
        for workload in workload_list:
            workload.budget = exp.rnd.choice(exp.budgets)

def create_column_permutation_indexes(columns, max_index_width):
    # Create all possible permutations of columns up to 'max_index_width'

    result_column_combinations = []
    table_column_dict = {}

    # Group columns by their table
    for column in columns:
        if column.table not in table_column_dict:
            table_column_dict[column.table] = set()
        table_column_dict[column.table].add(column)

    # Generate permutations for each table's columns
    for length in range(1, max_index_width + 1):
        unique = set()
        count = 0
        for key, columns_per_table in table_column_dict.items():
            permutations = set(itertools.permutations(columns_per_table, length))
            unique |= permutations
            count += len(permutations)
        print(f"{length}-column indexes: {count}")
        result_column_combinations.append(list(unique))

    return result_column_combinations

if __name__ == "__main__":
    main()
