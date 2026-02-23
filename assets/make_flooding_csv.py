import json
import csv
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
INFILE = ROOT / "data" / "petersburg_exports" / "13_Flooding_Hazard.jsonl"
OUTFILE = ROOT / "flooding_hazard.csv"


def g(attrs, k):
    v = attrs.get(k)
    return None if v in ("", None) else v


def esri_to_geojson(geom):
    if not geom or not isinstance(geom, dict):
        return None
    if "x" in geom and "y" in geom:
        return {"type": "Point", "coordinates": [geom["x"], geom["y"]]}
    if "points" in geom:
        return {"type": "MultiPoint", "coordinates": geom["points"]}
    if "paths" in geom:
        if len(geom["paths"]) == 1:
            return {"type": "LineString", "coordinates": geom["paths"][0]}
        return {"type": "MultiLineString", "coordinates": geom["paths"]}
    if "rings" in geom:
        return {"type": "Polygon", "coordinates": geom["rings"]}
    return None


with open(INFILE, "r", encoding="utf-8") as fin, open(OUTFILE, "w", newline="", encoding="utf-8") as fout:
    w = csv.writer(fout)
    w.writerow([
        "objectid",
        "zone_subty",
        "attributes",
        "geojson"
    ])

    for line in fin:
        if not line.strip():
            continue
        feat = json.loads(line)
        attrs = feat.get("attributes", {}) or {}
        geom = feat.get("geometry", {}) or {}

        geojson = None
        gj = esri_to_geojson(geom)
        if gj:
            geojson = json.dumps(gj, ensure_ascii=False)

        w.writerow([
            g(attrs, "OBJECTID"),
            g(attrs, "ZONE_SUBTY"),
            json.dumps(attrs, ensure_ascii=False),
            geojson
        ])

print("Wrote:", OUTFILE)
