import subprocess
import os
from pathlib import Path
import json
import math
import time
import shutil
from pg_database import Database

class TPCH:

    def __init__(self):
        self.db = Database()
        self.benchmark = "TPCH"
        self.test = "tpch"

    def run_benchmark(test):
        target_path = "temp"
        benchmark_path = "benchbase/target/benchbase-postgres"

        # remove temp directory if it exists
        shutil.rmtree("temp", ignore_errors=True)

        # Make sure 'temp' directory exists
        os.makedirs(target_path, exist_ok=True)


        with open(os.path.join(target_path, "out.txt"), 'w') as output_file:
            process = subprocess.Popen(
                [
                    'java', '-jar', 'benchbase.jar', '-b', test,
                    '-c', f"config/postgres/sample_{test}_config.xml",
                    "--create=false", "--clear=false", "--load=false", "--execute=true",
                    "-d", os.path.join("..", "..", "..", target_path)
                ],
                cwd=benchmark_path,
                stdout=output_file,
                stderr=subprocess.STDOUT,
                text=True
            )
            ret = process.wait()
        # read files from target_path .summary.json and comptute QphH

        summary_file = os.path.join(target_path, f"{test}.summary.json")

        if ret != 0:
            raise RuntimeError(f"BenchBase failed with exit code {ret}. See: ")

            # === Find the first *.summary.json file ===
        target_path = Path(target_path)
        summary_files = list(target_path.glob("*.summary.json"))
        if not summary_files:
            raise FileNotFoundError("No .summary.json file found in temp/")

        summary_path = summary_files[0]  # pick the first one

        with open(summary_path) as f:
            summary = json.load(f)

        avg_latency_us = summary["Latency Distribution"]["Average Latency (microseconds)"]
        throughput_rps = summary["Throughput (requests/second)"]
        scale_factor = float(summary.get("scalefactor", 1))

        # Compute Power@Size, Throughput@Size, QphH
        latency_s = avg_latency_us / 1_000_000
        power = (3600 / latency_s) * scale_factor
        throughput = throughput_rps * 3600 * scale_factor
        qphh = math.sqrt(power * throughput)

        print(f"✅ Benchmark complete. Summary file: {summary_path.name}")
        print(f"📊 Power@Size      = {power:.2f}")
        print(f"📈 Throughput@Size = {throughput:.2f}")
        print(f"💡 QphH@Size        = {qphh:.2f}")
        print(f"⏱ Elapsed Time    = {summary['Elapsed Time (nanoseconds)'] / 1_000_000:.2f} ms")
        return qphh
