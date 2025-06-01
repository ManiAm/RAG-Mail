import re
import matplotlib.pyplot as plt
import numpy as np

log_file = "./emails_dump_bak.txt"
num_bins = 80
output_file = "email_thread_length_distribution.png"

text_block_lengths = []
with open(log_file, "r", encoding="utf-8") as file:
    for line in file:
        match = re.search(r'"text_block_len":\s*(\d+)', line)
        if match:
            text_block_lengths.append(int(match.group(1)))

lengths = np.array(text_block_lengths)

print("Email Thread Text Block Length Statistics:")
print(f"  Total Threads        : {len(lengths)}")
print(f"  Mean Length          : {np.mean(lengths):,.0f} characters")
print(f"  Median Length        : {np.median(lengths):,.0f} characters")
print(f"  Standard Deviation   : {np.std(lengths):,.0f}")
print(f"  Min Length           : {np.min(lengths)}")
print(f"  Max Length           : {np.max(lengths)}")
print(f"  90th Percentile      : {np.percentile(lengths, 90):,.0f}")
print(f"  95th Percentile      : {np.percentile(lengths, 95):,.0f}")
print(f"  99th Percentile      : {np.percentile(lengths, 99):,.0f}")

# Trim outliers for visualization (95th percentile)
threshold = np.percentile(lengths, 95)
filtered_lengths = lengths[lengths <= threshold]

plt.figure(figsize=(10, 6))
plt.hist(filtered_lengths, bins=num_bins, edgecolor='black', color='steelblue')
plt.title("Distribution of Email Thread Text Lengths (Trimmed at 95th Percentile)")
plt.xlabel("Text Block Length (characters)")
plt.ylabel("Number of Threads")
plt.grid(True)
plt.tight_layout()

plt.savefig(output_file, dpi=300)
print(f"\nChart saved as: {output_file}")
