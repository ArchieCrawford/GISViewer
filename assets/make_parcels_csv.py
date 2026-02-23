import json
import csv

INFILE = r"C:\Users\AceGr\Downloads\petersburg-index-site-realtor-ui\data\parcels.jsonl"
OUTFILE = r"parcels.csv"

def g(attrs, k): 
    v = attrs.get(k)
    return None if v in ("", None) else v

with open(INFILE, "r", encoding="utf-8") as fin, open(OUTFILE, "w", newline="", encoding="utf-8") as fout:
    w = csv.writer(fout)
    w.writerow([
        "objectid",
        "parcel_num",
        "par_id",
        "streetnumb",
        "streetname",
        "street",
        "city",
        "state",
        "zip",
        "owner",
        "zoning",
        "gis_acres",
        "year_built",
        "attributes",
        "geojson"
    ])

    for line in fin:
        if not line.strip():
            continue
        feat = json.loads(line)
        attrs = feat.get("attributes", {}) or {}
        geom = feat.get("geometry", {}) or {}
        rings = geom.get("rings")

        geojson = None
        if rings:
            geojson = json.dumps({"type": "Polygon", "coordinates": rings}, ensure_ascii=False)

        w.writerow([
            g(attrs, "OBJECTID_1"),
            g(attrs, "Parcel_Num"),
            g(attrs, "PAR_ID"),
            g(attrs, "StreetNumb"),
            g(attrs, "StreetName"),
            g(attrs, "STREET"),
            g(attrs, "City"),
            g(attrs, "State"),
            g(attrs, "Zip"),
            g(attrs, "Owner"),
            g(attrs, "Zoning"),
            g(attrs, "GIS_Acres"),
            g(attrs, "Year_Built"),
            json.dumps(attrs, ensure_ascii=False),
            geojson
        ])

print("Wrote:", OUTFILE)