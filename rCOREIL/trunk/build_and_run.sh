#!/bin/bash

# 1. Compile all .java files
echo "Compiling..."
javac --add-exports java.sql.rowset/com.sun.rowset=ALL-UNNAMED \
    -cp ".:lib/*" \
    $(find src -name "*.java") $(find tests -name "*.java")

# 2. Check if compilation succeeded
if [ $? -eq 0 ]; then
    echo "Compilation successful!"

    # 3. Run the main class
    echo "Running Launcher..."
    java --add-exports java.sql.rowset/com.sun.rowset=ALL-UNNAMED \
        -cp ".:lib/*:src:tests" \
        sg.edu.nus.autotune.Launcher
else
    echo "Compilation failed. Fix errors above before running."
fi
