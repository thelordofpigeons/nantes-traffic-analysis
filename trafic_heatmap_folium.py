# ==================================
# trafic_heatmap_folium_updated.py
# ==================================
# Generates an interactive traffic heatmap using Folium,
# using pre-cleaned data and pre-calculated coordinates.

import pandas as pd
import folium
from folium.plugins import HeatMap
import sys
import os
import time

print("🚀 Starting Interactive Traffic Heatmap Generation (Folium)...")
start_time = time.time()

# --- Configuration ---
# Define paths to the required Parquet files
# !! Ensure these paths are correct !!
coordinate_mapping_path = r"C:\Users\ASUS\Documents\Stage LS2N\nantes_traffic_archiver\dist\channel_coordinates.parquet"
cleaned_data_path = r"C:\Users\ASUS\Documents\Stage LS2N\nantes_traffic_archiver\dist\cleaned_traffic_data.parquet"

# Output path for the interactive HTML map
output_html_path = r"C:\Users\ASUS\Documents\Stage LS2N\nantes_traffic_archiver\dist\traffic_heatmap_nantes_interactive.html"

# Nantes center coordinates for map initialization
nantes_center = [47.2184, -1.5536]

# Heatmap Layer Parameters (Adjust as needed)
heatmap_radius = 10
heatmap_blur = 5
heatmap_max_zoom = 18 # Max zoom level for heatmap points
heatmap_gradient = None # Use default gradient, or specify e.g., {0.2: 'blue', 0.4: 'lime', 0.6: 'orange', 1: 'red'}

# --- Check if required libraries are installed ---
try:
    import pyarrow
    import folium
except ImportError as e:
    print(f"❌ Error: Required library not found: {e}")
    print("   Please install required libraries: pip install pandas pyarrow folium")
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

# Ensure column types are correct for HeatMap input
df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
df['flow'] = pd.to_numeric(df['flow'], errors='coerce')
df = df.dropna(subset=['latitude', 'longitude', 'flow']) # Drop if coercion failed

if df.empty:
    print("❌ Error: No valid data remaining after merging and cleaning for the heatmap.")
    sys.exit(1)

print(f"✅ Data ready for heatmap: {len(df)} rows.")


# 5. Create Folium Map
print("🗺️ Creating Folium base map...")
# Use a slightly lighter base map that works well with heatmaps
m = folium.Map(location=nantes_center, zoom_start=13, tiles="CartoDB positron")

# 6. Prepare data for Folium HeatMap
# Format: List of lists, where each inner list is [latitude, longitude, weight]
print("🔥 Preparing data points for heatmap layer...")
# Use .values for potentially faster access than iterrows
heat_data = df[['latitude', 'longitude', 'flow']].values.tolist()
print(f"   -> Prepared {len(heat_data)} points.")

# 7. Add HeatMap layer to the map
print("➕ Adding HeatMap layer...")
try:
    HeatMap(
        heat_data,
        radius=heatmap_radius,
        blur=heatmap_blur,
        max_zoom=heatmap_max_zoom,
        gradient=heatmap_gradient
    ).add_to(m)
    print("✅ HeatMap layer added.")
except Exception as e:
    print(f"❌ Error adding HeatMap layer: {e}")
    sys.exit(1)

# 8. Save the interactive map to an HTML file
print(f"💾 Saving interactive map to: {output_html_path}")
try:
    m.save(output_html_path)
    print(f"✅ Heatmap saved successfully!")
    print(f"   You can open this file in your web browser: {output_html_path}")
except Exception as e:
     print(f"❌ Error saving HTML map file: {e}")

# ============================
# 9. End of Script
# ============================
end_time = time.time()
print(f"\n⏱️ Total time: {end_time - start_time:.2f} seconds.")