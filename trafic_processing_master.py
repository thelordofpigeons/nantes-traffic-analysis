# ============================
# trafic_processing_master.py
# ============================

# 1. Import libraries
import os
import pandas as pd
import numpy as np  # For NaN
import ast         # For safe evaluation of strings
import time

print("🚀 Starting Traffic Data Processing...")
start_time = time.time()

# ============================
# 2. Configuration
# ============================
# --- Define Paths (Modify these as needed) ---
# Folder containing the raw CSV snapshots
raw_folder_path = r"C:\Users\thelo\Documents\LS2N\nantes_traffic_archiver\archive"

# Output path for the master coordinate mapping file
coordinate_mapping_path = r"C:\Users\thelo\Documents\LS2N\nantes-traffic-analysis\results\master_coordinate_mapping.parquet"

# Output path for the final cleaned traffic data
cleaned_data_path = r"C:\Users\thelo\Documents\LS2N\nantes-traffic-analysis\results\cleaned_traffic_data.parquet"

# --- Columns to keep for main processing ---
# Explicitly list columns to keep, excluding geo ones
columns_to_keep = [
    'cha_id', 'cha_lib', 'cha_long', 'mf1_hd', 'mf1_debit',
    'mf1_taux', 'mf1_vit', 'tc1_temps', 'couleur_tp', 'etat_trafic'
]

# --- Columns to check for invalid placeholders (-1) ---
# These columns might contain -1 indicating missing/invalid data
numeric_cols_check_invalid = ['mf1_debit', 'mf1_taux', 'mf1_vit', 'tc1_temps']

# --- Columns critical for dropping NaNs ---
critical_cols_for_na = ['speed', 'flow', 'occupancy', 'timestamp'] # Use cleaned names

# ============================
# 3. Helper Functions
# ============================

def safe_literal_eval(x):
    """Safely evaluates a string literal (like a dict or list)."""
    if pd.isna(x):
        return None
    try:
        return ast.literal_eval(x)
    except (ValueError, SyntaxError, TypeError, MemoryError):
        # Handle potential errors during evaluation
        return None

def extract_coord(coord_dict, key='lon'):
    """Extracts 'lon' or 'lat' from a dictionary safely."""
    if isinstance(coord_dict, dict):
        return coord_dict.get(key)
    return None

# ============================
# 4. Part 1: Generate Master Coordinate File
# ============================
print("\n Métape 1: Génération du fichier de coordonnées maîtres...")
coord_start_time = time.time()

raw_files = [os.path.join(raw_folder_path, f) for f in os.listdir(raw_folder_path) if f.endswith('.csv') and os.path.isfile(os.path.join(raw_folder_path, f))]

if not raw_files:
    print(f"❌ Erreur: Aucun fichier CSV trouvé dans {raw_folder_path}")
    exit()

print(f"🔍 Trouvé {len(raw_files)} fichiers CSV bruts pour l'extraction des coordonnées.")

all_coords_list = []
processed_files_coord = 0
errors_coord = 0

for file in raw_files:
    try:
        # Read only necessary columns for coordinates
        temp_df = pd.read_csv(file, header=0, usecols=['cha_lib', 'geo_point_2d'], low_memory=False)
        temp_df = temp_df.dropna(subset=['cha_lib', 'geo_point_2d'])

        if not temp_df.empty:
            # Safely parse the geo_point_2d string
            coords_parsed = temp_df['geo_point_2d'].apply(safe_literal_eval)

            # Extract lon and lat
            temp_df['longitude'] = coords_parsed.apply(extract_coord, key='lon')
            temp_df['latitude'] = coords_parsed.apply(extract_coord, key='lat')

            # Keep only relevant columns and drop rows where extraction failed
            coords_extracted = temp_df[['cha_lib', 'longitude', 'latitude']].dropna()
            all_coords_list.append(coords_extracted)
        processed_files_coord += 1
    except FileNotFoundError:
        print(f"⚠️ Fichier non trouvé (peut-être supprimé pendant le processus): {file}")
        errors_coord += 1
    except Exception as e:
        print(f"⚠️ Erreur lors du traitement du fichier {os.path.basename(file)} pour les coordonnées: {e}")
        errors_coord += 1
    # Optional: Add progress indicator
    if (processed_files_coord + errors_coord) % 50 == 0:
         print(f"   ... traité {processed_files_coord + errors_coord}/{len(raw_files)} fichiers pour les coordonnées")


if not all_coords_list:
    print("❌ Erreur: Aucune donnée de coordonnées n'a pu être extraite des fichiers.")
    exit()

# Combine all extracted coordinates
print("   Concatenating coordinate data...")
coord_df = pd.concat(all_coords_list, ignore_index=True)

# Rename channel library column consistently
coord_df.rename(columns={'cha_lib': 'channel_name'}, inplace=True)

# Keep only the *first* valid coordinate pair found for each unique channel_name
print("   Dropping duplicate coordinate entries...")
master_coords = coord_df.drop_duplicates(subset=['channel_name'], keep='first')

# Final check for NaNs in coordinates
master_coords = master_coords.dropna(subset=['longitude', 'latitude'])

# Save the master coordinate file using Parquet
try:
    master_coords.to_parquet(coordinate_mapping_path, index=False)
    coord_elapsed = time.time() - coord_start_time
    print(f"✅ Fichier maître de coordonnées enregistré dans {coordinate_mapping_path}")
    print(f"   -> {len(master_coords)} entrées uniques de coordonnées de canaux trouvées.")
    print(f"   -> Temps écoulé pour les coordonnées: {coord_elapsed:.2f} secondes.")
except Exception as e:
    print(f"❌ Erreur lors de l'enregistrement du fichier de coordonnées maître: {e}")
    print(f"   Veuillez vérifier que vous disposez des autorisations d'écriture et de la bibliothèque 'pyarrow' (pip install pyarrow).")
    exit()


# ============================
# 5. Part 2: Process Main Traffic Data
# ============================
print("\n Métape 2: Traitement des données principales sur le trafic...")
data_start_time = time.time()

df_list = []
processed_files_data = 0
errors_data = 0

# We reuse the raw_files list from Part 1
for file in raw_files:
    try:
        # Read only the columns we want to keep
        temp_df = pd.read_csv(file, header=0, usecols=columns_to_keep, low_memory=False)
        df_list.append(temp_df)
        processed_files_data += 1
    except FileNotFoundError:
        print(f"⚠️ Fichier non trouvé (peut-être supprimé pendant le processus): {file}")
        errors_data += 1
    except ValueError as ve:
        print(f"⚠️ Erreur de valeur (probablement problème de colonne) dans {os.path.basename(file)}: {ve}")
        errors_data += 1
    except Exception as e:
        print(f"⚠️ Erreur lors du traitement du fichier {os.path.basename(file)} pour les données: {e}")
        errors_data += 1
    # Optional: Add progress indicator
    if (processed_files_data + errors_data) % 50 == 0:
        print(f"   ... traité {processed_files_data + errors_data}/{len(raw_files)} fichiers pour les données")


if not df_list:
    print("❌ Erreur: Aucune donnée de trafic n'a pu être lue.")
    exit()

# Concatenate everything into one big dataframe
print(f"   Concaténation de {len(df_list)} DataFrames...")
df = pd.concat(df_list, ignore_index=True)
print(f"   -> Données brutes concaténées: {len(df)} lignes.")

# ============================
# 6. Part 3: Clean the Main Traffic Data
# ============================
print("\n Métape 3: Nettoyage des données principales sur le trafic...")
cleaning_start_time = time.time()

# --- Rename columns cleanly ---
df.rename(columns={
    'cha_id': 'channel_id',
    'cha_lib': 'channel_name',
    'cha_long': 'channel_length',
    'mf1_hd': 'timestamp',
    'mf1_debit': 'flow',
    'mf1_taux': 'occupancy',
    'mf1_vit': 'speed',
    'tc1_temps': 'travel_time',
    'couleur_tp': 'color_code',
    'etat_trafic': 'traffic_state'
}, inplace=True)
print("   Renommé les colonnes.")

# --- Convert timestamp to datetime ---
print("   Conversion de la colonne timestamp en datetime...")
df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce') # Coerce errors to NaT

# --- Handle Invalid Placeholders (-1) ---
print("   Gestion des espaces réservés non valides (-1) dans les colonnes numériques...")
for col in ['flow', 'occupancy', 'speed', 'travel_time']:
    if col in df.columns:
         # Ensure column is numeric-like before replacing. Coerce errors.
        df[col] = pd.to_numeric(df[col], errors='coerce')
        # Replace -1 with NaN AFTER coercion, just in case -1 was a string
        df[col] = df[col].replace(-1, np.nan)
        print(f"      -> Remplacé -1 par NaN dans la colonne '{col}'.")

# --- Handle potentially negative flow values ---
# Decide if negative flow is invalid. Here we assume it is and keep only >= 0.
if 'flow' in df.columns:
    original_rows = len(df)
    df = df[df['flow'] >= 0]
    removed_rows = original_rows - len(df)
    if removed_rows > 0:
        print(f"   Supprimé {removed_rows} lignes avec un flux négatif (< 0).")

# --- Add time-related features ---
print("   Ajout de fonctionnalités temporelles (heure, jour de la semaine, week-end)...")
df['hour'] = df['timestamp'].dt.hour
df['day_of_week'] = df['timestamp'].dt.day_name()
df['is_weekend'] = df['day_of_week'].isin(['Saturday', 'Sunday'])

# --- Drop rows with missing CRITICAL values ---
print(f"   Suppression des lignes avec des valeurs NaN dans les colonnes critiques: {critical_cols_for_na}...")
initial_rows = len(df)
df = df.dropna(subset=critical_cols_for_na)
rows_dropped = initial_rows - len(df)
print(f"   -> Supprimé {rows_dropped} lignes en raison de valeurs critiques manquantes.")

cleaning_elapsed = time.time() - cleaning_start_time
print(f"✅ Nettoyage terminé. Temps écoulé: {cleaning_elapsed:.2f} secondes.")

# ================================
# 7. Part 4: Save Cleaned Data
# ================================
print("\n Métape 4: Sauvegarde des données nettoyées...")

try:
    df.to_parquet(cleaned_data_path, index=False)
    print(f"✅ Ensemble de données nettoyées enregistré dans {cleaned_data_path}")
except Exception as e:
    print(f"❌ Erreur lors de la sauvegarde du fichier de données nettoyées: {e}")
    print(f"   Veuillez vérifier que vous disposez des autorisations d'écriture et de la bibliothèque 'pyarrow' (pip install pyarrow).")
    exit()

# ================================
# 8. Final Summary
# ================================
total_elapsed = time.time() - start_time
print("\n================ Résumé Final ================")
print(f"✅ Chargé et traité {len(raw_files)} fichiers CSV bruts.")
print(f"✅ Fichier maître de coordonnées créé avec {len(master_coords)} canaux uniques.")
print(f"✅ Ensemble de données final nettoyé contient {len(df)} lignes.")
print("\nÉchantillon de données nettoyées:")
print(df.head())
print("\nInformations sur l'ensemble de données nettoyées:")
df.info()
print(f"\n⏱️ Temps total de traitement: {total_elapsed:.2f} secondes.")
print("🎉 Traitement terminé avec succès!")