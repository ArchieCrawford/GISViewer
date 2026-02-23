import fs from "node:fs";
import path from "node:path";
import readline from "node:readline";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const DATA_DIR = process.argv[2]
  ? path.resolve(process.argv[2])
  : path.resolve(__dirname, "../data/petersburg_exports");
const OUT_DIR = process.argv[3]
  ? path.resolve(process.argv[3])
  : path.resolve(__dirname, "../data/import");

const RESERVED_COLUMNS = new Set([
  "id",
  "geom",
  "geom_geojson",
  "geom_esri"
]);

const RESERVED = new Set([
  "all", "analyse", "analyze", "and", "any", "array", "as", "asc", "asymmetric",
  "authorization", "between", "bigint", "binary", "bit", "boolean", "both", "case",
  "cast", "char", "character", "check", "collate", "column", "constraint", "create",
  "cross", "current_date", "current_role", "current_time", "current_timestamp",
  "current_user", "default", "deferrable", "desc", "distinct", "do", "else", "end",
  "except", "false", "fetch", "float", "for", "foreign", "from", "full", "grant",
  "group", "having", "ilike", "in", "initially", "inner", "insert", "int", "integer",
  "intersect", "into", "is", "isnull", "join", "leading", "left", "like", "limit",
  "localtime", "localtimestamp", "natural", "not", "notnull", "null", "numeric", "offset",
  "on", "only", "or", "order", "outer", "overlaps", "placing", "primary", "real",
  "references", "returning", "right", "select", "session_user", "similar", "smallint",
  "table", "then", "to", "trailing", "true", "union", "unique", "user", "using",
  "variadic", "verbose", "when", "where"
]);

function normalizeKey(key) {
  let s = String(key || "").toLowerCase();
  s = s.replace(/[^a-z0-9]+/g, "_");
  s = s.replace(/^_+|_+$/g, "");
  s = s.replace(/_+/g, "_");
  if (!s) s = "col";
  if (RESERVED_COLUMNS.has(s)) s = `attr_${s}`;
  if (/^[0-9]/.test(s)) s = `c_${s}`;
  if (RESERVED.has(s)) s = `c_${s}`;
  return s;
}

function normalizeTableName(filename) {
  const base = filename.replace(/\.jsonl$/i, "");
  const noPrefix = base.replace(/^\d+_/, "");
  let s = noPrefix.toLowerCase();
  s = s.replace(/[^a-z0-9]+/g, "_");
  s = s.replace(/^_+|_+$/g, "");
  s = s.replace(/_+/g, "_");
  if (!s) s = "dataset";
  if (/^[0-9]/.test(s)) s = `d_${s}`;
  return s;
}

function esriToGeoJSON(g) {
  if (!g || typeof g !== "object") return null;
  if (typeof g.x === "number" && typeof g.y === "number") {
    return { type: "Point", coordinates: [g.x, g.y] };
  }
  if (Array.isArray(g.points)) {
    return g.points.length ? { type: "MultiPoint", coordinates: g.points } : null;
  }
  if (Array.isArray(g.paths)) {
    if (!g.paths.length) return null;
    if (g.paths.length === 1) return { type: "LineString", coordinates: g.paths[0] };
    return { type: "MultiLineString", coordinates: g.paths };
  }
  if (Array.isArray(g.rings)) {
    if (!g.rings.length) return null;
    return { type: "Polygon", coordinates: g.rings };
  }
  return null;
}

function csvEscape(value) {
  if (value === null || value === undefined) return "";
  const s = String(value);
  if (s === "") return "";
  if (/[",\n\r]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

function valueKind(value) {
  if (value === null || value === undefined) return "empty";
  if (typeof value === "boolean") return "bool";
  if (typeof value === "number" && !Number.isNaN(value)) return "number";
  if (typeof value === "string") {
    if (!value.trim()) return "empty";
    return "text";
  }
  return "text";
}

async function scanFile(filePath) {
  const keyMap = new Map();
  const usedNames = new Set();
  const baseCounts = new Map();
  const stats = new Map();
  let geomKind = null;

  function uniqueName(base) {
    if (!usedNames.has(base)) {
      usedNames.add(base);
      baseCounts.set(base, 1);
      return base;
    }
    let i = baseCounts.get(base) || 1;
    let name = `${base}_${i}`;
    while (usedNames.has(name)) {
      i += 1;
      name = `${base}_${i}`;
    }
    usedNames.add(name);
    baseCounts.set(base, i + 1);
    return name;
  }

  function mapKey(key) {
    if (keyMap.has(key)) return keyMap.get(key);
    const base = normalizeKey(key);
    const name = uniqueName(base);
    keyMap.set(key, name);
    return name;
  }

  const rl = readline.createInterface({
    input: fs.createReadStream(filePath),
    crlfDelay: Infinity
  });

  for await (const line of rl) {
    const trimmed = line.trim();
    if (!trimmed) continue;
    let obj;
    try { obj = JSON.parse(trimmed); } catch { continue; }
    const attrs = obj.attributes || {};
    for (const [k, v] of Object.entries(attrs)) {
      const col = mapKey(k);
      const kind = valueKind(v);
      if (kind === "empty") continue;
      const entry = stats.get(col) || { hasText: false, hasNumber: false, hasBool: false };
      if (kind === "text") entry.hasText = true;
      if (kind === "number") entry.hasNumber = true;
      if (kind === "bool") entry.hasBool = true;
      stats.set(col, entry);
    }

    if (!geomKind) {
      const g = obj.geometry;
      if (g) {
        if (typeof g.x === "number" && typeof g.y === "number") geomKind = "point";
        else if (Array.isArray(g.paths)) geomKind = "line";
        else if (Array.isArray(g.rings)) geomKind = "polygon";
      }
    }
  }

  const columns = Array.from(keyMap.entries())
    .map(([sourceKey, name]) => {
      const st = stats.get(name) || { hasText: false, hasNumber: false, hasBool: false };
      let type = "text";
      if (!st.hasText && st.hasNumber && !st.hasBool) type = "numeric";
      else if (!st.hasText && !st.hasNumber && st.hasBool) type = "boolean";
      return { name, sourceKey, type };
    })
    .sort((a, b) => a.name.localeCompare(b.name));

  return { columns, geomKind };
}

async function writeCsv(filePath, outPath, columns) {
  await fs.promises.mkdir(path.dirname(outPath), { recursive: true });
  const out = fs.createWriteStream(outPath, "utf8");
  const header = [...columns.map(c => c.name), "geom_geojson", "geom_esri"].join(",");
  out.write(`${header}\n`);

  const rl = readline.createInterface({
    input: fs.createReadStream(filePath),
    crlfDelay: Infinity
  });

  for await (const line of rl) {
    const trimmed = line.trim();
    if (!trimmed) continue;
    let obj;
    try { obj = JSON.parse(trimmed); } catch { continue; }
    const attrs = obj.attributes || {};
    const values = columns.map(col => {
      const v = attrs[col.sourceKey];
      if (v === null || v === undefined) return "";
      if (typeof v === "string" && !v.trim()) return "";
      if (typeof v === "object") return JSON.stringify(v);
      return String(v);
    });
    const geomGeo = esriToGeoJSON(obj.geometry);
    const geomGeoStr = geomGeo ? JSON.stringify(geomGeo) : "";
    const geomEsriStr = obj.geometry ? JSON.stringify(obj.geometry) : "";
    values.push(geomGeoStr, geomEsriStr);
    const csvLine = values.map(csvEscape).join(",");
    out.write(`${csvLine}\n`);
  }

  out.end();
}

function buildSchemaSQL(tableName, columns) {
  const colsSql = columns
    .map(c => `  ${c.name} ${c.type}`)
    .join(",\n");
  return `create table if not exists public.${tableName} (\n` +
    `  id bigserial primary key,\n` +
    (colsSql ? `${colsSql},\n` : "") +
    `  geom_geojson jsonb,\n` +
    `  geom_esri jsonb,\n` +
    `  geom geometry generated always as (public.safe_geom_from_geojson(geom_geojson)) stored\n` +
    `);\n\n` +
    `create index if not exists ${tableName}_geom_gix on public.${tableName} using gist (geom);\n\n` +
    `drop view if exists public.${tableName}_view;\n` +
    `create view public.${tableName}_view as select * from public.${tableName};\n`;
}

function buildCopySQL(tableName, columns, csvPath) {
  const cols = columns.map(c => c.name).concat(["geom_geojson", "geom_esri"]).join(", ");
  return `\\copy public.${tableName} (${cols}) from '${csvPath}' with (format csv, header true);\n`;
}

async function main() {
  const files = (await fs.promises.readdir(DATA_DIR))
    .filter(f => f.toLowerCase().endsWith(".jsonl"))
    .sort();

  if (!files.length) {
    console.error(`[prepare-import] No .jsonl files found in ${DATA_DIR}`);
    process.exit(1);
  }

  await fs.promises.mkdir(OUT_DIR, { recursive: true });

  const manifest = [];
  let schemaSQL = "create extension if not exists postgis;\n\n" +
    `create or replace function public.safe_geom_from_geojson(j jsonb)\n` +
    `returns geometry\n` +
    `language plpgsql\n` +
    `immutable\n` +
    `as $$\n` +
    `begin\n` +
    `  if j is null then return null; end if;\n` +
    `  begin\n` +
    `    return ST_SetSRID(ST_GeomFromGeoJSON(j::text), 3857);\n` +
    `  exception when others then\n` +
    `    return null;\n` +
    `  end;\n` +
    `end;\n` +
    `$$;\n\n`;

  let copySQL = "";

  for (const file of files) {
    const filePath = path.join(DATA_DIR, file);
    const tableName = normalizeTableName(file);
    console.log(`[prepare-import] Scanning ${file} -> ${tableName}`);
    const { columns, geomKind } = await scanFile(filePath);
    const csvName = `${tableName}.csv`;
    const csvPath = path.join(OUT_DIR, csvName);
    console.log(`[prepare-import] Writing ${csvName}`);
    await writeCsv(filePath, csvPath, columns);
    schemaSQL += buildSchemaSQL(tableName, columns);
    copySQL += buildCopySQL(tableName, columns, path.relative(path.resolve(__dirname, ".."), csvPath).replace(/\\/g, "/"));
    manifest.push({
      file,
      table: tableName,
      geom: geomKind || "unknown",
      columns: columns.map(c => ({ name: c.name, source: c.sourceKey, type: c.type }))
    });
  }

  await fs.promises.writeFile(path.join(OUT_DIR, "schema.sql"), schemaSQL, "utf8");
  await fs.promises.writeFile(path.join(OUT_DIR, "load.sql"), copySQL, "utf8");
  await fs.promises.writeFile(path.join(OUT_DIR, "manifest.json"), JSON.stringify(manifest, null, 2), "utf8");
  console.log(`[prepare-import] Done. Output in ${OUT_DIR}`);
}

main().catch(err => {
  console.error("[prepare-import] Failed:", err);
  process.exit(1);
});
