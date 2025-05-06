# ============================
# trafic_analysis_updated.py
# ============================
# Analyzes temporal traffic patterns from the cleaned Parquet data.

# 1. Import libraries
import pandas as pd
import matplotlib.pyplot as plt
import sys
import os
import time

print("🚀 Starting Temporal Traffic Analysis...")
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
    print(f"✅ Loaded {len(df)} rows for temporal analysis.")
except Exception as e:
    print(f"❌ Error reading Parquet file: {e}")
    sys.exit(1)

# --- Data Verification (Optional but Recommended) ---
# Check if required columns exist and have suitable types
print("\n🔍 Verifying data types (first 5 rows info):")
df.info(memory_usage='deep')
# If 'timestamp' is not datetime64[ns], uncomment the next line:
# print("⚠️ Timestamp column not loaded as datetime. Converting...")
# df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
# If 'flow' is object/string, it might need conversion:
# df['flow'] = pd.to_numeric(df['flow'], errors='coerce')
# df = df.dropna(subset=['flow']) # Drop rows where conversion failed

# Check if time features were loaded correctly
required_cols = ['hour', 'day_of_week', 'is_weekend', 'flow']
missing_req_cols = [col for col in required_cols if col not in df.columns]
if missing_req_cols:
    print(f"❌ Error: Required columns missing from Parquet file: {missing_req_cols}")
    print("   Ensure 'trafic_processing_master.py' includes these columns.")
    sys.exit(1)
if not pd.api.types.is_numeric_dtype(df['flow']):
     print("⚠️ Warning: 'flow' column is not numeric. Analysis might fail.")
     # Optionally exit or attempt conversion

# ============================
# 2. Temporal Traffic Analysis
# ============================
print("\n📊 Performing Temporal Analysis...")

# 2.1 Average Flow per Hour
print("   Calculating Average Flow per Hour...")
hourly_flow = df.groupby('hour')['flow'].mean()

plt.figure(figsize=(12, 6))
plt.plot(hourly_flow.index, hourly_flow.values, marker='o', linestyle='-', color='dodgerblue')
plt.title('Average Traffic Flow per Hour of the Day')
plt.xlabel('Hour of the Day (0-23)')
plt.ylabel('Average Flow (vehicles/hour?)') # Add units if known
plt.grid(True, linestyle='--', alpha=0.7)
plt.xticks(range(0, 24))
plt.tight_layout()
plt.show()

# 2.2 Compare Weekdays vs Weekends
print("   Calculating Average Flow: Weekdays vs Weekends...")
weekday_flow = df[df['is_weekend'] == False].groupby('hour')['flow'].mean()
weekend_flow = df[df['is_weekend'] == True].groupby('hour')['flow'].mean()

plt.figure(figsize=(12, 6))
plt.plot(weekday_flow.index, weekday_flow.values, label='Weekdays (Mon-Fri)', marker='o', color='darkorange')
plt.plot(weekend_flow.index, weekend_flow.values, label='Weekends (Sat-Sun)', marker='s', linestyle='--', color='purple') # Different marker/style
plt.title('Average Traffic Flow per Hour: Weekdays vs Weekends')
plt.xlabel('Hour of the Day (0-23)')
plt.ylabel('Average Flow (vehicles/hour?)')
plt.grid(True, linestyle='--', alpha=0.7)
plt.xticks(range(0, 24))
plt.legend()
plt.tight_layout()
plt.show()

# 2.3 Traffic by Day of Week
print("   Calculating Average Flow by Day of Week...")
day_flow = df.groupby('day_of_week')['flow'].mean()

# Ensure proper day order for plotting
day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
# Use pd.Categorical for sorting
df['day_of_week'] = pd.Categorical(df['day_of_week'], categories=day_order, ordered=True)
day_flow = df.groupby('day_of_week', observed=False)['flow'].mean() # Use observed=False with Categorical

plt.figure(figsize=(10, 6))
day_flow.plot(kind='bar', color='teal')
plt.title('Average Traffic Flow by Day of Week')
plt.xlabel('Day of the Week')
plt.ylabel('Average Flow (vehicles/hour?)')
plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.xticks(rotation=45, ha='right')
plt.tight_layout()
plt.show()

# ============================
# 3. End of Script
# ============================
end_time = time.time()
print(f"\n✅ Temporal analysis done and plots generated.")
print(f"⏱️ Total time: {end_time - start_time:.2f} seconds.")