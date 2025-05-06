# ============================
# view_parquet_metrics.py
# ============================

import pandas as pd
import os
import numpy as np
import time

# --- Configuration: Set the paths to your Parquet files ---
coordinate_mapping_path = r"C:\Users\ASUS\Documents\Stage LS2N\nantes_traffic_archiver\dist\channel_coordinates.parquet"
cleaned_data_path = r"C:\Users\ASUS\Documents\Stage LS2N\nantes_traffic_archiver\dist\cleaned_traffic_data.parquet"

# List of files to analyze
files_to_analyze = [
    ("Coordinate Mapping Data", coordinate_mapping_path),
    ("Cleaned Traffic Data", cleaned_data_path)
]

# --- Helper Function ---
def format_bytes(size_bytes):
    """Converts bytes to a human-readable format (KB, MB, GB)."""
    if size_bytes == 0:
        return "0B"
    size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    i = int(np.floor(np.log(size_bytes) / np.log(1024)))
    p = np.power(1024, i)
    s = round(size_bytes / p, 2)
    return f"{s} {size_name[i]}"

# --- Main Analysis Function ---
def analyze_parquet(file_description, file_path):
    """Loads a Parquet file and prints relevant metrics."""
    print(f"\n{'='*15} Analyzing: {file_description} {'='*15}")
    print(f"File Path: {file_path}")
    start_time = time.time()

    # 1. Check File Existence and Size
    if not os.path.exists(file_path):
        print("❌ ERROR: File not found.")
        print("-" * 50)
        return

    try:
        file_size = os.path.getsize(file_path)
        print(f"📊 File Size: {format_bytes(file_size)} ({file_size} bytes)")
    except Exception as e:
        print(f"⚠️ Warning: Could not get file size - {e}")

    # 2. Load Data with Pandas
    try:
        print("⏳ Loading data...")
        df = pd.read_parquet(file_path)
        load_time = time.time() - start_time
        print(f"✅ Data loaded successfully in {load_time:.2f} seconds.")
    except Exception as e:
        print(f"❌ ERROR: Could not read Parquet file: {e}")
        print("      (Ensure 'pyarrow' or 'fastparquet' is installed: pip install pyarrow)")
        print("-" * 50)
        return

    # 3. Basic Dimensions
    print(f"\n📊 Shape (Rows, Columns): {df.shape}")

    # 4. Data Sample
    print("\n📋 First 5 Rows (Head):")
    print(df.head())
    print("\n📋 Last 5 Rows (Tail):")
    print(df.tail())

    # 5. Data Types, Non-Null Counts, and Memory Usage
    print("\nℹ️ Dataframe Info (Dtypes, Non-Nulls, Memory):")
    # Capture info output to print it cleanly
    buffer = pd.io.common.StringIO()
    df.info(buf=buffer, memory_usage='deep') # deep=True gives more accurate memory
    info_str = buffer.getvalue()
    print(info_str)

    # 6. Missing Values Analysis
    print("\n❓ Missing Values (NaNs) per Column:")
    missing_values = df.isnull().sum()
    missing_percent = (missing_values / len(df)) * 100
    missing_df = pd.DataFrame({'Count': missing_values, 'Percentage': missing_percent})
    missing_df = missing_df[missing_df['Count'] > 0].sort_values(by='Percentage', ascending=False) # Show only columns with missing values

    if not missing_df.empty:
        print(missing_df.to_string(formatters={'Percentage': '{:.2f}%'.format}))
    else:
        print("   No missing values found.")

    # 7. Descriptive Statistics - Numerical Columns
    print("\n🔢 Descriptive Statistics (Numerical Columns):")
    # Select only numeric columns for describe()
    numeric_df = df.select_dtypes(include=np.number)
    if not numeric_df.empty:
        # Use formatting for better readability
        print(numeric_df.describe().to_string(float_format='{:,.2f}'.format))
    else:
        print("   No numerical columns found.")

    # 8. Descriptive Statistics - Categorical/Object Columns
    print("\n** Descriptive Statistics (Categorical/Object Columns):")
    # Select object, category, potentially boolean columns
    categorical_df = df.select_dtypes(include=['object', 'category', 'bool'])
    if not categorical_df.empty:
        try:
            print(categorical_df.describe().to_string())
        except Exception as e:
             print(f"   Could not generate categorical description: {e}") # Handle potential errors if describe fails
    else:
        print("   No categorical/object columns found.")

    # 9. Unique Value Counts for Key Columns (Example)
    print("\n🔑 Unique Value Counts (Examples):")
    key_cols = ['channel_name', 'traffic_state', 'day_of_week', 'hour']
    for col in key_cols:
        if col in df.columns:
            nunique = df[col].nunique()
            print(f"   - Column '{col}': {nunique} unique values")
            # Optionally show top few values if unique count isn't too high
            if nunique < 20:
                print(f"     Values: {df[col].unique()}")


    end_time = time.time()
    analysis_time = end_time - start_time
    print(f"\n⏱️ Total time for analyzing {file_description}: {analysis_time:.2f} seconds")
    print("-" * 50)


# --- Run the Analysis ---
if __name__ == "__main__":
    print("Starting Parquet File Analysis...")
    # Ensure pyarrow is available for Pandas
    try:
        pd.read_parquet(None, engine='pyarrow')
    except ImportError:
         print("\n⚠️ WARNING: 'pyarrow' library not found. Installing it is recommended for Parquet support.")
         print("         Attempting analysis without explicitly setting engine (might use fastparquet if installed).")
    except TypeError: # Handles the case where pd.read_parquet(None) is called
        pass # This is expected if pyarrow is installed, just confirming it's loadable


    for description, path in files_to_analyze:
        analyze_parquet(description, path)

    print("\nAnalysis finished.")