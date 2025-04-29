from TPCH import TPCH


class Benchmark:


    def __init__(self, benchmark = "TPCH"):
        self.benchmark = benchmark

    def run(self):
        # run the TPCH benchmark
        result = TPCH.run_benchmark("tpch")
        return result