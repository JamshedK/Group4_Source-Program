# Smartix-RL

This README file explain the steps needed to tun SmatIX on postgres database with TPC-H

## Quick Start Guide

Follow the steps below to set up the environment and run the reinforcement learning script. These examples are tested on Linux and use PostgreSQL as the target database.

---

### Step 1: Clone the Repository

Start by cloning the repository and navigating to the `scripts` folder:

```shell
git clone https://github.com/JamshedK/smartix-rl.git
cd smartix-rl/scripts
```

---

### Step 2: Install BenchBase

BenchBase is a multi-database transactional and analytical benchmarking framework. To install it, run the following command:

```shell
./install_benchbase.sh postgres
```

The script will:
1. Install OpenJDK 21.
2. Clone the BenchBase repository.
3. Build and package BenchBase using the PostgreSQL profile.

---

### Step 3: Build BenchBase and Populate PostgreSQL Data

Once BenchBase is installed, you can build the benchmark and populate the PostgreSQL database with data using the following command:

```shell
./build_benchmark.sh postgres tpcc
```

This script will:
1. Navigate to the BenchBase directory.
2. Load the `tpcc` benchmark data into the specified PostgreSQL database.

---

### Step 4: Remove All Indexes Except Primary Keys

To optimize the environment for reinforcement learning tasks, remove all non-primary-key indexes from the database schema using the provided SQL script:

1. Open your PostgreSQL client (e.g., `psql`).
2. Run the following command to execute the script:
   ```shell
   psql -d postgres -f remove_all_indexes.sql
   ```

This will drop all indexes except for primary key constraints in the `public` schema.

---

### Step 5: Create a Virtual Environment

To isolate the Python dependencies for the reinforcement learning algorithm, create a virtual environment:

1. Navigate to the repository root:
   ```shell
   cd ..
   ```
2. Create and activate a virtual environment:
   ```shell
   python3 -m venv venv
   source venv/bin/activate
   ```
3. Install the required Python packages:
   ```shell
   pip install -r requirements.txt
   ```

---

### Step 6: Run the Reinforcement Learning Algorithm

Finally, execute the reinforcement learning algorithm using the `run_algorithm.sh` script. This will run the process in the background and log the output to `training.log`.

```shell
./run_algorithm.sh
```

---

This source code is similar to the source code uploaded by the authors, but we rewrote some of the main components such as pg_database.py and agent.py. 
