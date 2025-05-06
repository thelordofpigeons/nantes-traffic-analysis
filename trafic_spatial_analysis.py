# ======================================
# trafic_spatial_analysis_updated.py
# ======================================
# Analyzes spatial traffic patterns from the cleaned Parquet data.

# 1. Import libraries
import pandas as pd
import matplotlib.pyplot as plt
import sys
import os
import time

print("🚀 Starting Spatial Traffic Analysis...")
start_time = time.time()

# --- Configuration ---
# Define the path to the cleaned Parquet file
# !! Ensure this path is correct and points to the output of trafic_processing_master.py !!
cleaned_data_path = r"C:\Users\ASUS\Documents\Stage LS2N\nantes_traffic_archiver\dist\cleaned_traffic_data.parquet"

# --- Check if pyarrow is installed (needed for read_parquet) ---
try:
    import pyarrow
except ImportError:
    print("❌ Error: 'pyarrow' library not found.")
    print("   Please install it using: pip install pyarrow")
    sys.exit(1)

# 2. Load the cleaned dataset from Parquet
print(f"⏳ Loading cleaned data from: {cleaned_data_path}")
if not os.path.exists(cleaned_data_path):
    print(f"❌ Error: Cleaned data file not found at {cleaned_data_path}")
    print("   Please run the 'trafic_processing_master.py' script first.")
    sys.exit(1)

try:
    df = pd.read_parquet(cleaned_data_path, engine='pyarrow')
    print(f"✅ Loaded {len(df)} rows for spatial analysis.")
except Exception as e:
    print(f"❌ Error reading Parquet file: {e}")
    sys.exit(1)

# --- Data Verification (Optional but Recommended) ---
# Check if timestamp column was loaded correctly as datetime
print("\n🔍 Verifying data types (first 5 rows info):")
df.info(memory_usage='deep')
# If 'timestamp' is not datetime64[ns], uncomment the next line:
# print("⚠️ Timestamp column not loaded as datetime. Converting...")
# df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
# If 'flow' or 'speed' are objects/strings, they might need conversion:
# df['flow'] = pd.to_numeric(df['flow'], errors='coerce')
# df['speed'] = pd.to_numeric(df['speed'], errors='coerce')
# df = df.dropna(subset=['flow', 'speed']) # Drop rows where conversion failed

# ============================
# 3. Spatial Analysis
# ============================
print("\n📊 Performing Spatial Analysis...")

# 3.1 Top 10 Most Congested Streets (Highest Average Flow)
# Ensure 'flow' is numeric before grouping
if pd.api.types.is_numeric_dtype(df['flow']):
    print("   Calculating Top 10 Congested Streets by Flow...")
    top_congested = df.groupby('channel_name')['flow'].mean().sort_values(ascending=False).head(10)

    plt.figure(figsize=(12, 7)) # Slightly larger figure
    top_congested.plot(kind='bar', color='skyblue')
    plt.title('Top 10 Most Congested Streets (by Average Flow)')
    plt.xlabel('Street Name')
    plt.ylabel('Average Traffic Flow (vehicles/hour?)') # Add units if known
    plt.xticks(rotation=45, ha='right')
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.tight_layout() # Adjust layout
    plt.show()
else:
    print("⚠️ Skipping Top Congested Streets plot: 'flow' column is not numeric.")


# 3.2 Top 10 Fastest Streets (Highest Average Speed)
# Ensure 'speed' is numeric before grouping
if pd.api.types.is_numeric_dtype(df['speed']):
    print("   Calculating Top 10 Fastest Streets by Speed...")
    top_fastest = df.groupby('channel_name')['speed'].mean().sort_values(ascending=False).head(10)

    plt.figure(figsize=(12, 7))
    top_fastest.plot(kind='bar', color='mediumseagreen')
    plt.title('Top 10 Fastest Streets (by Average Speed)')
    plt.xlabel('Street Name')
    plt.ylabel('Average Speed (km/h)')
    plt.xticks(rotation=45, ha='right')
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.tight_layout()
    plt.show()
else:
     print("⚠️ Skipping Top Fastest Streets plot: 'speed' column is not numeric.")


# 3.3 Top 10 Slowest Streets (Lowest Average Speed)
# Ensure 'speed' is numeric before grouping
if pd.api.types.is_numeric_dtype(df['speed']):
    print("   Calculating Top 10 Slowest Streets by Speed...")
    # Filter out potential zero speeds if they are not meaningful for "slowest"
    # df_speed_positive = df[df['speed'] > 0] # Optional: depends on if 0 speed is valid data
    # top_slowest = df_speed_positive.groupby('channel_name')['speed'].mean().sort_values(ascending=True).head(10)
    top_slowest = df.groupby('channel_name')['speed'].mean().sort_values(ascending=True).head(10)


    plt.figure(figsize=(12, 7))
    top_slowest.plot(kind='bar', color='lightcoral')
    plt.title('Top 10 Slowest Streets (Potential Congestion Areas by Speed)')
    plt.xlabel('Street Name')
    plt.ylabel('Average Speed (km/h)')
    plt.xticks(rotation=45, ha='right')
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.tight_layout()
    plt.show()
else:
    print("⚠️ Skipping Top Slowest Streets plot: 'speed' column is not numeric.")

# ============================
# 4. End of Script
# ============================
end_time = time.time()
print(f"\n✅ Spatial analysis done and plots generated.")
print(f"⏱️ Total time: {end_time - start_time:.2f} seconds.")