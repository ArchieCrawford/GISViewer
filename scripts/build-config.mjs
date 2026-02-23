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
) || deriveProjectUrl(readEnv("SUPABASE_DATABASE_URL", "SUPABASE_DB_URL", "SUPABASE_CONNECTION_STRING")));

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
      table: "parcels",
      searchColumns: ["owner", "street", "parcel_num"],
      zoningColumn: "zoning",
      acresColumn: "gis_acres",
      geometryColumn: "geom"
    },
    {
      id: "zoning",
      label: "Zoning",
      table: "zoning",
      geometryColumn: "geom"
    },
    {
      id: "flooding_hazard",
      label: "Flooding Hazard",
      table: "flooding_hazard",
      geometryColumn: "geom"
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
