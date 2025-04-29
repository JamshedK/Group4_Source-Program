This repository contains the source code for Group 4 CS5513 course. The repository has the source code for three auto tuning index algorithms: rCOREIL, SWIRL, SmartIX. 

# rCOREIL
The key files in **rCOREIL** are located in the `trunk/` directory.
- In `rCOREIL/trunk/config/dbtune.cfg`, we have the configuration setup for the database.
- In `rCOREIL/trunk/config/lib/`, we installed key dependencies necessary for the code to function.
- In `rCOREIL/trunk/workloads/postgres/`, we store the workloads that can be passed as arguments to the code for evaluation.
- In `rCOREIL/trunk/src/sg/edu/nus/autotune/`, we have `PostgresDATA.java`, which contains the logic for PostgreSQL and other important configuration files.
- In `rCOREIL/trunk/tests/sg/edu/nus/autotune/`, we have key files for running the project (`Launcher.java`, `Execution.java`).
To compile rCOREIl, use :
```
javac --add-exports java.sql.rowset/com.sun.rowset=ALL-UNNAMED -cp ".:lib/*" src/edu/ucsc/dbtune/**/*.java src/sg/edu/nus/autotune/*.java tests/sg/edu/nus/**/*.java
```
To run rCOREIl use:
```
java-- add-exports java.sql.rowset/com.sun.rowset=ALL-UNNAMED -cp ".:lib/*:src:tests" sg.edu.nus.autotune.Launcher -a 1 -f postgres/my_workload.sql
```

# SWIRL
The SWIRL folder contains a copy of the original SWIRL algorithm written by Kossman et al in the rl_index_selection folder. Instructions to run this code are in their README.md. The original repo can be found at https://github.com/hyrise/rl_index_selection.git .

The code written by our group to recreate the algorithm is contained in the my_code folder. To run this code, run

```
python swirl.py
```

Python version 3.7 is required along with Tensorflow version 1.15 which can be downloaded from https://pypi.org/project/tensorflow/1.15.0/#files.

# SmartIX
The source we used for SmartIX is in the SmartIX folder. In the same folder, there is a README file with instructions on how to run the algorithm. We rewrote some of the main Python files. The source code only worked with MySQL database. We rewrote it to make it work with a Postgres database and used benchbase to compute the reward. The files we rewrote were pg_database.py and agent.py. We also added scripts folder that makes it easy to train the algorithm. 
