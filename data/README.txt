Drop your exported files here.

Expected:
- parcels.jsonl  (newline-delimited JSON of parcel features)
Later you can switch to:
- parcels.parquet (recommended for speed/size)

If you add or remove files, update /data/manifest.json by running:
python3 ../scripts/build_manifest.py
