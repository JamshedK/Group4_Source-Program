This repository contains the source code for Group 4 CS5513 course. The repository has the source code for three auto tuning index algorithms: rCOREIL, SWIRL, SmartIX. 

# rCOREIL
The key files in rCOREIL are in the trunk.
In rCOREIL/trunk/config/dbtune.cfg, we have the configuration setup for the databse. 
In rCOREIL/trunk/config/lib, we installed key dependencies to the workings of the code
In rCOREIL/trunk/workloads/postgres, we have the workloads that can be passed to the code as arguments for evaluation 
In rCOREIL/trunk/src/sg/edu/nus/autotune, we have the PostgresDATA.java file which containes the logic that makes the code function with postgres instead of DB2 as well as other important configuration files
In rCOREIL/trunk/tests/sg/edu/nus/autotune, we have key files to run the project. This folder contains the launcher.java and execution.java which are responsible for running the program. 
To compile rCOREIl, use : javac --add-exports java.sql.rowset/com.sun.rowset=ALL-UNNAMED -cp ".:lib/*" src/edu/ucsc/dbtune/**/*.java src/sg/edu/nus/autotune/*.java tests/sg/edu/nus/**/*.java
To run rCOREIl use: java --add-exports java.sql.rowset/com.sun.rowset=ALL-UNNAMED -cp ".:lib/*:src:tests" sg.edu.nus.autotune.Launcher -a 1 -f postgres/my_workload.sql

# SWIRL
The SWIRL folder contains a copy of the original SWIRL algorithm written by Kossman et al in the rl_index_selection folder. Instructions to run this code are in their README.md. The original repo can be found at https://github.com/hyrise/rl_index_selection.git .

The code written by our group to recreate the algorithm is contained in the my_code folder. To run this code, run

```
python swirl.py
```

Python version 3.7 is required along with Tensorflow version 1.15 which can be downloaded from https://pypi.org/project/tensorflow/1.15.0/#files.

# SmartIX
The source we used for SmartIX is in the SmartIX folder. In the same folder, there is a README file with instructions on how to run the algorithm.  
