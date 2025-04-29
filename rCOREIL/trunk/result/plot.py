import re
import matplotlib.pyplot as plt

# Read the file
with open('autotune_202504118_173208.txt', 'r') as file:
    content = file.read()

# Split by each main query block
query_blocks = re.split(r'===== Query (\d+) =====', content)

query_numbers = []
planning_times = []
execution_times = []

# Process every second block (after splitting)
for i in range(1, len(query_blocks), 2):
    query_num = int(query_blocks[i])
    block_text = query_blocks[i+1]

    planning_match = re.search(r'Planning Time: ([\d\.]+) ms', block_text)
    execution_match = re.search(r'Execution Time: ([\d\.]+) ms', block_text)

    if planning_match and execution_match:
        planning_time = float(planning_match.group(1))
        execution_time = float(execution_match.group(1))

        query_numbers.append(query_num)
        planning_times.append(planning_time)
        execution_times.append(execution_time)

# Sort based on query number
sorted_indices = sorted(range(len(query_numbers)), key=lambda x: query_numbers[x])
query_numbers = [query_numbers[i] for i in sorted_indices]
planning_times = [planning_times[i] for i in sorted_indices]
execution_times = [execution_times[i] for i in sorted_indices]

# Plotting
plt.figure(figsize=(10,5))
plt.plot(query_numbers, execution_times, label='Execution Time (ms)', marker='o')
plt.plot(query_numbers, planning_times, label='Planning Time (ms)', marker='x')
plt.xlabel('Query Number')
plt.ylabel('Time (ms)')
plt.title('Execution and Planning Times per Query')
plt.legend()
plt.grid(True)
plt.tight_layout()

# Save the plot as a PNG file
plt.savefig('query_times_per_query.png')
plt.show()
