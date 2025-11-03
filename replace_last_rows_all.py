import os
import pandas as pd

# === Directory where backups exist ===
normalized_dir = "/home/sunkari/Stock_price_predictor/normalized_datasets"

# === Columns to remove ===
cols_to_drop = ["sentiment_score", "Headlines", "Headline_List"]

# Loop through all files ending with _backup.csv
for file in os.listdir(normalized_dir):
    if file.endswith("_backup.csv"):
        backup_path = os.path.join(normalized_dir, file)

        # Get the original filename (remove _backup)
        new_filename = file.replace("_backup.csv", ".csv")
        new_path = os.path.join(normalized_dir, new_filename)

        try:
            # Read backup file
            df = pd.read_csv(backup_path)

            # Drop unwanted columns if they exist
            df = df.drop(columns=[c for c in cols_to_drop if c in df.columns], errors="ignore")

            # Delete current existing .csv (old updated version)
            if os.path.exists(new_path):
                os.remove(new_path)
                print(f"🗑️ Deleted old file: {new_path}")

            # Save the cleaned backup as the new main file
            df.to_csv(new_path, index=False)
            print(f"✅ Restored and cleaned: {new_path}")

            # Optionally remove the backup file too (uncomment if you want)
            # os.remove(backup_path)

        except Exception as e:
            print(f"❌ Error processing {file}: {e}")
