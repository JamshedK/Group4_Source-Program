import subprocess
import os
from pathlib import Path
import json
import math
import time
import shutil
from pg_database import Database
from TPCH import TPCH
from benchmark import Benchmark



# Run it directly
if __name__ == "__main__":
    db = Database()
    print(db.get_indexes_map())
    # print('Creating an index')
    # db.create_index("c_name", "customer")
    # print(db.get_indexes_map())
    # print('Resetting indexes')
    # db.reset_indexes()
    # print(db.get_indexes_map())
    # benchmark = Benchmark()
    # runt he benchmark
    # benchmark.run()