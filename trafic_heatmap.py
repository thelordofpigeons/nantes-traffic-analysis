# ============================
# trafic_heatmap_updated.py
# ============================
# Generates a traffic density heatmap using Seaborn,
# using pre-cleaned data and pre-calculated coordinates.

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import sys
import os
import time

print("🚀 Starting Traffic Heatmap Generation (Seaborn)...")
start_time = time.time()

# --- Configuration ---
# Define paths to the required Parquet files
# !! Ensure these paths are correct !!
coordinate_mapping_path = r"C:\Users\ASUS\Documents\Stage LS2N\nantes_traffic_archiver\dist\channel_coordinates.parquet"
cleaned_data_path = r"C:\Users\ASUS\Documents\Stage LS2N\nantes_traffic_archiver\dist\cleaned_traffic_data.parquet"

# --- Check if pyarrow is installed ---
try:
    import pyarrow
except ImportError:
    print("❌ Error: 'pyarrow' library not found.")
    print("   Please install it using: pip install pyarrow")
    sys.exit(1)

# 1. Load Coordinate Mapping
print(f"⏳ Loading coordinate mapping from: {coordinate_mapping_path}")
if not os.path.exists(coordinate_mapping_path):
    print(f"❌ Error: Coordinate mapping file not found at {coordinate_mapping_path}")
    print("   Please run the 'trafic_processing_master.py' script first.")
    sys.exit(1)
try:
    geo_mapping = pd.read_parquet(coordinate_mapping_path, engine='pyarrow')
    print(f"✅ Loaded {len(geo_mapping)} streets with coordinates from master file.")
except Exception as e:
    print(f"❌ Error reading coordinate mapping file: {e}")
    sys.exit(1)

# 2. Load Cleaned Traffic Data
print(f"⏳ Loading cleaned traffic data from: {cleaned_data_path}")
if not os.path.exists(cleaned_data_path):
    print(f"❌ Error: Cleaned data file not found at {cleaned_data_path}")
    print("   Please run the 'trafic_processing_master.py' script first.")
    sys.exit(1)
try:
    df = pd.read_parquet(cleaned_data_path, engine='pyarrow')
    print(f"✅ Loaded {len(df)} rows from cleaned dataset.")
except Exception as e:
    print(f"❌ Error reading cleaned data file: {e}")
    sys.exit(1)

# 3. Merge Traffic Data with Coordinates
print("🔄 Merging traffic data with coordinates...")
df = pd.merge(df, geo_mapping, on='channel_name', how='left')
print(f"   -> Merged data shape: {df.shape}")

# 4. Final Data Preparation for Heatmap
# Drop rows where merge failed (no coordinates) or flow is missing
# Note: Negative flows should already be handled by the master script.
initial_rows = len(df)
df = df.dropna(subset=['longitude', 'latitude', 'flow'])
rows_dropped = initial_rows - len(df)
if rows_dropped > 0:
     print(f"   -> Dropped {rows_dropped} rows due to missing coordinates or flow after merge.")

# Ensure column types are correct for plotting
df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
df['flow'] = pd.to_numeric(df['flow'], errors='coerce')
df = df.dropna(subset=['longitude', 'latitude', 'flow']) # Drop if coercion failed

if df.empty:
    print("❌ Error: No valid data remaining after merging and cleaning for the heatmap.")
    sys.exit(1)

print(f"✅ Data ready for heatmap: {len(df)} rows.")

# 5. Plot the Heatmap using Seaborn KDE
print("🎨 Generating heatmap plot...")
plt.figure(figsize=(12, 10)) # Adjusted size

try:
    sns.kdeplot(
        x=df['longitude'],
        y=df['latitude'],
        weights=df['flow'], # Use traffic flow as weights for density
        cmap="Reds",        # Red color map indicates intensity
        fill=True,          # Fill the contours
        thresh=0,           # Include all data points
        levels=50,          # Number of contour levels (adjust for performance/detail)
        bw_adjust=0.2       # Adjust bandwidth (lower = more localized peaks)
    )
    plt.title('Traffic Flow Density Heatmap (Nantes - Weighted by Flow)')
    plt.xlabel('Longitude')
    plt.ylabel('Latitude')
    plt.grid(True, linestyle='--', alpha=0.6)
    plt.tight_layout()
    plt.show()
    print("✅ Heatmap plot generated successfully.")

except Exception as e:
    print(f"❌ Error during heatmap generation: {e}")

# ============================
# 6. End of Script
# ============================
end_time = time.time()
print(f"\n⏱️ Total time: {end_time - start_time:.2f} seconds.")