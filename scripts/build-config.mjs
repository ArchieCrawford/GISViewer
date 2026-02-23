import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

function parseEnvFile(envPath) {
  if (!fs.existsSync(envPath)) return {};
  const text = fs.readFileSync(envPath, "utf8");
  const out = {};
  for (const line of text.split(/\r?\n/)) {
    if (!line || line.trim().startsWith("#")) continue;
    const idx = line.indexOf("=");
    if (idx === -1) continue;
    const key = line.slice(0, idx).trim();
    let val = line.slice(idx + 1).trim();
    if ((val.startsWith("\"") && val.endsWith("\"")) || (val.startsWith("'") && val.endsWith("'"))) {
      val = val.slice(1, -1);
    }
    out[key] = val;
  }
  return out;
}

const envFilePaths = [
  path.resolve(__dirname, "../.env"),
  path.resolve(__dirname, "./.env")
];
const envFileVars = envFilePaths.reduce((acc, p) => {
  const vars = parseEnvFile(p);
  return { ...acc, ...vars };
}, {});

function readEnv(...names) {
  for (const name of names) {
    const v = process.env[name] ?? envFileVars[name];
    if (v) return v;
  }
  return "";
}

function deriveProjectUrl(dbUrl) {
  if (!dbUrl) return "";
  try {
    const u = new URL(dbUrl);
    const host = u.hostname || "";
    if (!host.endsWith(".supabase.co")) return "";
    if (host.startsWith("db.")) {
      const ref = host.slice(3, host.indexOf(".supabase.co"));
      if (ref) return `https://${ref}.supabase.co`;
    }
    return `https://${host.replace(/^db\./, "").replace(".supabase.co", "")}.supabase.co`;
  } catch {
    return "";
  }
}

function normalizeProjectUrl(value) {
  if (!value) return "";
  const trimmed = value.trim();
  if (/^postgres(ql)?:\/\//i.test(trimmed)) {
    return deriveProjectUrl(trimmed);
  }
  return trimmed;
}

const projectUrl = normalizeProjectUrl(readEnv(
  "SUPABASE_URL",
  "PUBLIC_SUPABASE_URL",
  "NEXT_PUBLIC_SUPABASE_URL",
  "SUPABASE_PROJECT_URL"
));

const anonKey = readEnv(
  "SUPABASE_ANON_KEY",
  "PUBLIC_SUPABASE_ANON_KEY",
  "NEXT_PUBLIC_SUPABASE_ANON_KEY"
);

const schema = readEnv("SUPABASE_SCHEMA") || "public";
const savedViewsEnabled = readEnv("SUPABASE_SAVED_VIEWS_ENABLED");
const sqlEnabled = readEnv("SUPABASE_SQL_ENABLED");

const config = `window.SUPABASE_CONFIG = ${JSON.stringify({
  url: projectUrl || "https://YOUR_PROJECT.supabase.co",
  anonKey: anonKey || "YOUR_ANON_KEY",
  schema,
  layers: [
    {
      id: "parcels",
      label: "Parcels",
      table: "parcels_view",
      searchColumns: ["owner", "street", "parcel_num", "par_id"],
      defaultColumns: ["parcel_num", "par_id", "owner", "streetnumb", "streetname", "street", "city", "state", "zip", "zoning", "gis_acres", "year_built"],
      hiddenColumns: ["geom", "geom_geojson", "geom_esri"],
      zoningColumn: "zoning",
      acresColumn: "gis_acres",
      geometryColumn: "geom_geojson"
    },
    {
      id: "zoning",
      label: "Zoning",
      table: "zoning_view",
      searchColumns: ["zoning", "zonelabel"],
      defaultColumns: ["zoning", "zonelabel", "objectid"],
      hiddenColumns: ["geom", "geom_geojson", "geom_esri"],
      geometryColumn: "geom_geojson"
    },
    {
      id: "flooding_hazard",
      label: "Flooding Hazard",
      table: "flooding_hazard_view",
      searchColumns: ["zone_subty", "objectid"],
      defaultColumns: ["zone_subty", "objectid"],
      hiddenColumns: ["geom", "geom_geojson", "geom_esri"],
      geometryColumn: "geom_geojson"
    },
    {
      id: "city_boundary",
      label: "City Boundary",
      table: "city_boundary_view",
      searchColumns: ["location", "descriptio", "source"],
      defaultColumns: ["location", "descriptio", "source"],
      hiddenColumns: ["geom", "geom_geojson", "geom_esri"],
      geometryColumn: "geom_geojson"
    },
    {
      id: "subdivisions",
      label: "Subdivisions",
      table: "subdivisions_view",
      searchColumns: ["subdivisio", "subdivis_1"],
      defaultColumns: ["subdivisio", "subdivis_1", "objectid"],
      hiddenColumns: ["geom", "geom_geojson", "geom_esri"],
      geometryColumn: "geom_geojson"
    },
    {
      id: "federal_state_historic_districts",
      label: "Federal/State Historic Districts",
      table: "federal_state_historic_districts_view",
      searchColumns: ["district_n", "state"],
      defaultColumns: ["district_n", "state", "objectid"],
      hiddenColumns: ["geom", "geom_geojson", "geom_esri"],
      geometryColumn: "geom_geojson"
    },
    {
      id: "local_historic_districts",
      label: "Local Historic Districts",
      table: "local_historic_districts_view",
      searchColumns: ["district", "objectid"],
      defaultColumns: ["district", "objectid"],
      hiddenColumns: ["geom", "geom_geojson", "geom_esri"],
      geometryColumn: "geom_geojson"
    },
    {
      id: "national_register_properties",
      label: "National Register Properties",
      table: "national_register_properties_view",
      searchColumns: ["name", "address", "parcel_id"],
      defaultColumns: ["name", "address", "parcel_id", "historic_u", "build_date", "national_r", "nps_proper"],
      hiddenColumns: ["geom", "geom_geojson", "geom_esri"],
      geometryColumn: "geom_geojson"
    },
    {
      id: "enterprise_zone",
      label: "Enterprise Zone",
      table: "enterprise_zone_view",
      searchColumns: ["parcel_num", "par_id", "premise", "street"],
      defaultColumns: ["parcel_num", "par_id", "premise", "street"],
      hiddenColumns: ["geom", "geom_geojson", "geom_esri"],
      geometryColumn: "geom_geojson"
    },
    {
      id: "oppurtunity_zone",
      label: "Opportunity Zone",
      table: "oppurtunity_zone_view",
      searchColumns: ["zone", "objectid"],
      defaultColumns: ["zone", "objectid"],
      hiddenColumns: ["geom", "geom_geojson", "geom_esri"],
      geometryColumn: "geom_geojson"
    },
    {
      id: "elementary_school_zone",
      label: "Elementary School Zone",
      table: "elementary_school_zone_view",
      searchColumns: ["school", "source"],
      defaultColumns: ["school", "source", "objectid"],
      hiddenColumns: ["geom", "geom_geojson", "geom_esri"],
      geometryColumn: "geom_geojson"
    },
    {
      id: "assessment_neighborhoods",
      label: "Assessment Neighborhoods",
      table: "assessment_neighborhoods_view",
      searchColumns: ["neigh", "objectid"],
      defaultColumns: ["neigh", "objectid"],
      hiddenColumns: ["geom", "geom_geojson", "geom_esri"],
      geometryColumn: "geom_geojson"
    },
    {
      id: "future_land_use",
      label: "Future Land Use",
      table: "future_land_use_view",
      searchColumns: ["landuse", "objectid"],
      defaultColumns: ["landuse", "acreage", "objectid"],
      hiddenColumns: ["geom", "geom_geojson", "geom_esri"],
      geometryColumn: "geom_geojson"
    },
    {
      id: "trail_paths",
      label: "Trail Paths",
      table: "trail_paths_view",
      searchColumns: ["type", "objectid"],
      defaultColumns: ["type", "objectid"],
      hiddenColumns: ["geom", "geom_geojson", "geom_esri"],
      geometryColumn: "geom_geojson"
    },
    {
      id: "wards",
      label: "Wards",
      table: "wards_view",
      searchColumns: ["ward_numbe", "location"],
      defaultColumns: ["ward_numbe", "location", "source", "objectid"],
      hiddenColumns: ["geom", "geom_geojson", "geom_esri"],
      geometryColumn: "geom_geojson"
    },
    {
      id: "contours",
      label: "Contours",
      table: "contours_view",
      searchColumns: ["elevation", "docname", "linetype"],
      defaultColumns: ["elevation", "docname", "linetype", "source"],
      hiddenColumns: ["geom", "geom_geojson", "geom_esri"],
      geometryColumn: "geom_geojson"
    },
    {
      id: "parks_conserved_areas",
      label: "Parks / Conserved Areas",
      table: "parks_conserved_areas_view",
      searchColumns: ["parcel_num", "par_id", "premise", "street"],
      defaultColumns: ["parcel_num", "par_id", "premise", "street"],
      hiddenColumns: ["geom", "geom_geojson", "geom_esri"],
      geometryColumn: "geom_geojson"
    },
    {
      id: "chesapeake_bay_preservation_areas",
      label: "Chesapeake Bay Preservation Areas",
      table: "chesapeake_bay_preservation_areas_view",
      searchColumns: ["name", "preservati", "source"],
      defaultColumns: ["name", "preservati", "source", "objectid"],
      hiddenColumns: ["geom", "geom_geojson", "geom_esri"],
      geometryColumn: "geom_geojson"
    }
  ],
  savedViews: {
    enabled: savedViewsEnabled === "true" || savedViewsEnabled === "1",
    table: "saved_views"
  },
  sql: {
    enabled: sqlEnabled === "true" || sqlEnabled === "1",
    rpcName: "run_sql"
  }
}, null, 2)};\n`;

const outPath = path.resolve(__dirname, "../assets/config.js");
fs.writeFileSync(outPath, config, "utf8");

const hasPlaceholders = config.includes("YOUR_PROJECT") || config.includes("YOUR_ANON_KEY");
if (hasPlaceholders) {
  console.warn("[build-config] Missing Supabase env vars. Using placeholders in assets/config.js");
} else {
  console.log("[build-config] Wrote assets/config.js");
}
