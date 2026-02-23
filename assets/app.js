import { createClient } from "https://cdn.jsdelivr.net/npm/@supabase/supabase-js@2/+esm";

const STORE_VIEWS = "pg_views_v3";
const STORE_COLUMNS = "pg_columns_v1";
const COLUMN_PREFS_VERSION = 2;
const QUERY_LIMIT = 1000;

const els = {
  layerTabs: document.getElementById("layerTabs"),
  globalSearch: document.getElementById("globalSearch"),
  btnExport: document.getElementById("btnExport"),
  btnApply: document.getElementById("btnApply"),
  filters: document.getElementById("filters"),
  activeLayerMeta: document.getElementById("activeLayerMeta"),
  viewSelect: document.getElementById("viewSelect"),
  btnSaveView: document.getElementById("btnSaveView"),
  btnDeleteView: document.getElementById("btnDeleteView"),
  sql: document.getElementById("sql"),
  btnRun: document.getElementById("btnRun"),
  btnReset: document.getElementById("btnReset"),
  status: document.getElementById("status"),
  btnFit: document.getElementById("btnFit"),
  colFilter: document.getElementById("colFilter"),
  btnCols: document.getElementById("btnCols"),
  colManager: document.getElementById("colManager"),
  colList: document.getElementById("colList"),
  btnColsShowAll: document.getElementById("btnColsShowAll"),
  btnColsReset: document.getElementById("btnColsReset"),
  resultMeta: document.getElementById("resultMeta"),
  resultTable: document.getElementById("resultTable"),
  savedViewsHint: document.getElementById("savedViewsHint"),
  sqlHint: document.getElementById("sqlHint"),
  activeTableName: document.getElementById("activeTableName")
};

const CONFIG = window.SUPABASE_CONFIG || {};
const SUPABASE_URL = CONFIG.url || window.SUPABASE_URL || "";
const SUPABASE_ANON_KEY = CONFIG.anonKey || window.SUPABASE_ANON_KEY || "";
const SUPABASE_SCHEMA = CONFIG.schema || "public";
const SAVED_VIEWS = {
  enabled: Boolean(CONFIG.savedViews && CONFIG.savedViews.enabled),
  table: (CONFIG.savedViews && CONFIG.savedViews.table) || "saved_views"
};
const SQL = {
  enabled: Boolean(CONFIG.sql && CONFIG.sql.enabled),
  rpcName: (CONFIG.sql && CONFIG.sql.rpcName) || "run_sql"
};

let supabase;
let datasets = [];
let activeDataset = null;
let activeColumns = [];
let lastResult = null;
let columnPrefs = loadColumnPrefs();
let columnManagerCols = [];

let map;
let geoLayer;

function setStatus(s) {
  els.status.textContent = s;
}

function setUiEnabled(enabled) {
  els.btnApply.disabled = !enabled;
  els.btnRun.disabled = !enabled || !SQL.enabled;
  els.btnReset.disabled = !enabled;
  els.btnSaveView.disabled = !enabled;
  els.btnDeleteView.disabled = !enabled;
  els.btnExport.disabled = !enabled;
  els.btnFit.disabled = !enabled;
}

function loadViewsLocal() {
  try { return JSON.parse(localStorage.getItem(STORE_VIEWS) || "[]"); } catch { return []; }
}

function saveViewsLocal(v) {
  localStorage.setItem(STORE_VIEWS, JSON.stringify(v));
}

function loadColumnPrefs() {
  try { return JSON.parse(localStorage.getItem(STORE_COLUMNS) || "{}"); } catch { return {}; }
}

function saveColumnPrefs() {
  localStorage.setItem(STORE_COLUMNS, JSON.stringify(columnPrefs));
}

async function loadViews() {
  if (!SAVED_VIEWS.enabled) return loadViewsLocal();
  const { data, error } = await supabase
    .from(SAVED_VIEWS.table)
    .select("id,name,layer,sql,created_at")
    .order("name", { ascending: true });
  if (error) {
    setStatus(`Saved views unavailable: ${error.message}`);
    return [];
  }
  return (data || []).map(row => {
    const parsed = parseViewPayload(row.sql);
    return {
      id: row.id,
      name: row.name,
      datasetId: parsed?.datasetId || row.layer || null,
      filters: parsed?.filters || null,
      sql: parsed?.sql || row.sql || "",
      savedAt: row.created_at ? Date.parse(row.created_at) : null
    };
  });
}

async function renderViewsSelect() {
  const views = (await loadViews()).sort((a, b) => a.name.localeCompare(b.name));
  els.viewSelect.innerHTML = "";
  const opt0 = document.createElement("option");
  opt0.value = "";
  opt0.textContent = "Select a saved view…";
  els.viewSelect.appendChild(opt0);
  for (const v of views) {
    const opt = document.createElement("option");
    opt.value = v.id;
    opt.textContent = v.name;
    els.viewSelect.appendChild(opt);
  }
}

function escapeSqlLike(s) {
  return String(s).replace(/'/g, "''");
}

function escapeOrValue(s) {
  return String(s).replace(/,/g, " ").replace(/\s+/g, " ").trim();
}

async function initSupabase() {
  const hasPlaceholders = SUPABASE_URL.includes("YOUR_PROJECT") || SUPABASE_ANON_KEY.includes("YOUR_ANON_KEY");
  if (!SUPABASE_URL || !SUPABASE_ANON_KEY || hasPlaceholders) {
    setStatus("Missing Supabase config. Update /assets/config.js with your project URL and anon key.");
    setUiEnabled(false);
    return false;
  }
  supabase = createClient(SUPABASE_URL, SUPABASE_ANON_KEY, {
    db: { schema: SUPABASE_SCHEMA }
  });
  return true;
}

async function initMap() {
  map = L.map("map", { zoomControl: true }).setView([37.2279, -77.4019], 12);
  L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
    maxZoom: 20,
    attribution: "&copy; OpenStreetMap"
  }).addTo(map);
  geoLayer = L.geoJSON([], { style: { weight: 1 } }).addTo(map);
}

function normalizeLayer(d) {
  const id = d.id || d.table || d.label;
  const label = d.label || d.table || d.id;
  return {
    id,
    label,
    table: d.table,
    searchColumns: d.searchColumns,
    zoningColumn: d.zoningColumn,
    acresColumn: d.acresColumn,
    geometryColumn: d.geometryColumn,
    columns: d.columns,
    defaultColumns: d.defaultColumns,
    hiddenColumns: d.hiddenColumns,
    columnOrder: d.columnOrder
  };
}

async function fetchLayers() {
  if (Array.isArray(CONFIG.layers) && CONFIG.layers.length) {
    datasets = CONFIG.layers.map(normalizeLayer);
    return;
  }
  const res = await fetch("/data/manifest.json", { cache: "no-store" });
  if (!res.ok) throw new Error("Missing /data/manifest.json or CONFIG.layers");
  const data = await res.json();
  if (!Array.isArray(data)) throw new Error("manifest.json must be a list");
  datasets = data.filter(d => d.table).map(normalizeLayer);
}

function renderTabs() {
  els.layerTabs.innerHTML = "";
  const list = datasets.slice().sort((a, b) => a.label.localeCompare(b.label));
  for (const d of list) {
    const btn = document.createElement("div");
    btn.className = "tab";
    btn.textContent = d.label;
    btn.dataset.id = d.id;
    btn.addEventListener("click", async () => {
      await activateDataset(d.id, true);
    });
    els.layerTabs.appendChild(btn);
  }
}

function setActiveTab(id) {
  for (const el of els.layerTabs.querySelectorAll(".tab")) {
    el.classList.toggle("active", el.dataset.id === id);
  }
}

async function activateDataset(id, resetFilters, skipQuery = false) {
  const d = datasets.find(x => x.id === id);
  if (!d) return;
  activeDataset = d;
  setActiveTab(id);
  els.activeLayerMeta.textContent = `Loading ${d.label}…`;
  els.activeTableName.textContent = d.table || "—";
  setStatus(`Loading ${d.label}…`);

  activeColumns = await getColumns(d);
  buildFilterUI(activeColumns, d);
  renderColumnManager(activeColumns);

  if (resetFilters) {
    els.globalSearch.value = "";
    els.sql.value = buildSqlPreview();
  }

  if (!skipQuery) await runQueryFromFilters();
}

async function getColumns(dataset) {
  if (Array.isArray(dataset.columns) && dataset.columns.length) return dataset.columns;
  if (!dataset.table) return [];
  const { data, error } = await supabase.from(dataset.table).select("*").limit(1);
  if (error) {
    setStatus(`Failed to load columns: ${error.message}`);
    return [];
  }
  if (!data || !data.length) return [];
  return Object.keys(data[0]);
}

async function updateLayerMeta() {
  if (!activeDataset?.table) return;
  const { count, error } = await supabase
    .from(activeDataset.table)
    .select("*", { count: "estimated", head: true });
  if (error) {
    els.activeLayerMeta.textContent = `${activeDataset.label} loaded`;
    return;
  }
  const countLabel = typeof count === "number" ? `~${count}` : "n/a";
  els.activeLayerMeta.textContent = `${activeDataset.label} loaded · rows: ${countLabel}`;
}

function buildFilterUI(cols, dataset) {
  els.filters.innerHTML = "";

  const searchCols = resolveSearchColumns(cols, dataset);

  const row1 = document.createElement("div");
  row1.className = "filterRow";
  row1.innerHTML = `<div class="label">Text search (checks: ${searchCols.join(", ") || "first text columns"})</div>
    <input id="f_text" class="input" placeholder="e.g., 123 main, smith, 032-..." />`;
  els.filters.appendChild(row1);

  const zoningCol = resolveZoningColumn(cols, dataset);
  if (zoningCol) {
    const row = document.createElement("div");
    row.className = "filterRow";
    row.innerHTML = `<div class="label">${zoningCol}</div><select id="f_zone" class="select"><option value="">Any</option></select>`;
    els.filters.appendChild(row);
    populateSelectDistinct("f_zone", zoningCol, 120);
  }

  const acresCol = resolveAcresColumn(cols, dataset);
  if (acresCol) {
    const row = document.createElement("div");
    row.className = "filterRow";
    row.innerHTML = `<div class="label">${acresCol} (min / max)</div>
      <div class="row">
        <input id="f_min" class="input mono" placeholder="min" />
        <input id="f_max" class="input mono" placeholder="max" />
      </div>`;
    els.filters.appendChild(row);
  }

  const extra = document.createElement("div");
  extra.className = "small";
  extra.textContent = "Tip: use the search box + Apply. SQL is optional.";
  els.filters.appendChild(extra);
}

function resolveSearchColumns(cols, dataset) {
  if (dataset?.searchColumns && dataset.searchColumns.length) {
    return mapColumnNames(cols, dataset.searchColumns);
  }
  const preferred = [];
  for (const c of cols) {
    if (/addr|address|situs/i.test(c)) preferred.push(c);
  }
  for (const c of cols) {
    if (/owner|name/i.test(c) && preferred.length < 3) preferred.push(c);
  }
  for (const c of cols) {
    if (/parcel|pin|pid|map/i.test(c) && preferred.length < 3) preferred.push(c);
  }
  const uniq = Array.from(new Set(preferred));
  if (uniq.length) return uniq.slice(0, 3);
  return cols.slice(0, 3);
}

function resolveZoningColumn(cols, dataset) {
  if (dataset?.zoningColumn) {
    const match = findColumnName(cols, dataset.zoningColumn);
    if (match) return match;
  }
  return cols.find(c => /zone|zoning|district/i.test(c));
}

function resolveAcresColumn(cols, dataset) {
  if (dataset?.acresColumn) {
    const match = findColumnName(cols, dataset.acresColumn);
    if (match) return match;
  }
  return cols.find(c => /acre|acres|area/i.test(c) && !/shape/i.test(c));
}

function resolveGeometryColumn(cols, dataset) {
  if (dataset?.geometryColumn) {
    const match = findColumnName(cols, dataset.geometryColumn);
    if (match) return match;
  }
  return cols.find(c => /geom|geometry|shape/i.test(c));
}

function findColumnName(cols, name) {
  const target = String(name || "").toLowerCase();
  for (const c of cols) {
    if (c.toLowerCase() === target) return c;
  }
  return null;
}

function mapColumnNames(cols, names) {
  return (names || [])
    .map(name => findColumnName(cols, name))
    .filter(Boolean);
}

function getDatasetColumnPrefs(allCols) {
  const id = activeDataset?.id || "default";
  if (!columnPrefs[id] || columnPrefs[id].version !== COLUMN_PREFS_VERSION) {
    columnPrefs[id] = { order: [], hidden: [], version: COLUMN_PREFS_VERSION, seeded: false };
  }
  const prefs = columnPrefs[id];
  if (!prefs.seeded && Array.isArray(allCols) && allCols.length) {
    const seeded = applyDefaultColumnPrefs(allCols);
    prefs.order = seeded.order;
    prefs.hidden = seeded.hidden;
    prefs.seeded = true;
    saveColumnPrefs();
  }
  return prefs;
}

function applyDefaultColumnPrefs(allCols) {
  const defaultCols = Array.isArray(activeDataset?.defaultColumns)
    ? mapColumnNames(allCols, activeDataset.defaultColumns)
    : [];
  const hiddenCols = Array.isArray(activeDataset?.hiddenColumns)
    ? mapColumnNames(allCols, activeDataset.hiddenColumns)
    : [];
  const orderCols = Array.isArray(activeDataset?.columnOrder)
    ? mapColumnNames(allCols, activeDataset.columnOrder)
    : [];

  const baseOrder = orderCols.length ? orderCols : defaultCols;
  let order = buildColumnOrder(allCols, { order: baseOrder });
  order = moveGeometryToEnd(order);

  const hidden = new Set(hiddenCols);
  if (defaultCols.length) {
    for (const c of allCols) {
      if (!defaultCols.includes(c)) hidden.add(c);
    }
  }

  return { order, hidden: Array.from(hidden) };
}

function buildColumnOrder(allCols, prefs) {
  const base = Array.isArray(prefs.order) ? prefs.order.filter(c => allCols.includes(c)) : [];
  const rest = allCols.filter(c => !base.includes(c));
  if (base.length === 0) {
    const nonGeom = rest.filter(c => !isGeometryColumn(c));
    const geom = rest.filter(c => isGeometryColumn(c));
    return nonGeom.concat(geom);
  }
  return base.concat(rest);
}

function moveGeometryToEnd(cols) {
  const nonGeom = cols.filter(c => !isGeometryColumn(c));
  const geom = cols.filter(c => isGeometryColumn(c));
  return nonGeom.concat(geom);
}

function isGeometryColumn(name) {
  return /geom|geometry|shape|geom_geojson/i.test(name);
}

function getColumnState(allCols) {
  const prefs = getDatasetColumnPrefs(allCols);
  const order = buildColumnOrder(allCols, prefs);
  const hidden = new Set((prefs.hidden || []).filter(c => allCols.includes(c)));
  return { order, hidden };
}

function renderColumnManager(cols) {
  if (!els.colList) return;
  columnManagerCols = Array.isArray(cols) ? cols.slice() : [];
  const prefs = getDatasetColumnPrefs(columnManagerCols);
  const order = buildColumnOrder(columnManagerCols, prefs);
  const hiddenSet = new Set((prefs.hidden || []).filter(c => columnManagerCols.includes(c)));

  prefs.order = order;
  prefs.hidden = Array.from(hiddenSet);
  saveColumnPrefs();

  els.colList.innerHTML = "";
  for (const col of order) {
    const row = document.createElement("div");
    row.className = "colRow";
    row.draggable = true;
    row.dataset.col = col;

    const handle = document.createElement("span");
    handle.className = "colHandle";
    handle.textContent = "::";

    const checkbox = document.createElement("input");
    checkbox.type = "checkbox";
    checkbox.className = "colToggle";
    checkbox.checked = !hiddenSet.has(col);
    checkbox.addEventListener("change", () => {
      const prefsNow = getDatasetColumnPrefs(columnManagerCols);
      const hidden = new Set(prefsNow.hidden || []);
      if (checkbox.checked) hidden.delete(col);
      else hidden.add(col);
      prefsNow.hidden = Array.from(hidden);
      saveColumnPrefs();
      if (lastResult) renderTable(lastResult);
    });

    const label = document.createElement("span");
    label.className = "colLabel";
    label.textContent = col;

    row.append(handle, checkbox, label);

    row.addEventListener("dragstart", (e) => {
      row.classList.add("dragging");
      e.dataTransfer.effectAllowed = "move";
      e.dataTransfer.setData("text/plain", col);
    });

    row.addEventListener("dragend", () => {
      row.classList.remove("dragging");
    });

    row.addEventListener("dragover", (e) => {
      e.preventDefault();
      row.classList.add("dragOver");
    });

    row.addEventListener("dragleave", () => {
      row.classList.remove("dragOver");
    });

    row.addEventListener("drop", (e) => {
      e.preventDefault();
      row.classList.remove("dragOver");
      const from = e.dataTransfer.getData("text/plain");
      const to = row.dataset.col;
      if (!from || !to || from === to) return;
      const prefsNow = getDatasetColumnPrefs(columnManagerCols);
      const next = buildColumnOrder(columnManagerCols, prefsNow).filter(c => c !== from);
      const idx = next.indexOf(to);
      if (idx === -1) return;
      next.splice(idx, 0, from);
      prefsNow.order = next;
      saveColumnPrefs();
      renderColumnManager(columnManagerCols);
      if (lastResult) renderTable(lastResult);
    });

    els.colList.appendChild(row);
  }

  if (!els.colList.dataset.bound) {
    els.colList.addEventListener("dragover", (e) => e.preventDefault());
    els.colList.addEventListener("drop", (e) => {
      if (e.target.closest(".colRow")) return;
      const from = e.dataTransfer.getData("text/plain");
      if (!from) return;
      const prefsNow = getDatasetColumnPrefs(columnManagerCols);
      const next = buildColumnOrder(columnManagerCols, prefsNow).filter(c => c !== from);
      next.push(from);
      prefsNow.order = next;
      saveColumnPrefs();
      renderColumnManager(columnManagerCols);
      if (lastResult) renderTable(lastResult);
    });
    els.colList.dataset.bound = "1";
  }
}

async function populateSelectDistinct(selectId, col, limit) {
  try {
    if (!activeDataset?.table) return;
    const { data, error } = await supabase
      .from(activeDataset.table)
      .select(col)
      .not(col, "is", null)
      .limit(limit);
    if (error) return;
    const el = document.getElementById(selectId);
    if (!el) return;
    const uniq = new Set();
    for (const row of data || []) {
      const v = row[col];
      if (v === null || v === undefined) continue;
      uniq.add(String(v));
    }
    Array.from(uniq)
      .sort((a, b) => a.localeCompare(b))
      .forEach(v => {
        const opt = document.createElement("option");
        opt.value = v;
        opt.textContent = v;
        el.appendChild(opt);
      });
  } catch {}
}

function getFilterState() {
  return {
    global: (els.globalSearch.value || "").trim(),
    text: (document.getElementById("f_text")?.value || "").trim(),
    zone: (document.getElementById("f_zone")?.value || "").trim(),
    min: (document.getElementById("f_min")?.value || "").trim(),
    max: (document.getElementById("f_max")?.value || "").trim()
  };
}

function applyFilterState(state) {
  if (!state) return;
  els.globalSearch.value = state.global || "";
  const fText = document.getElementById("f_text");
  if (fText) fText.value = state.text || "";
  const fZone = document.getElementById("f_zone");
  if (fZone) fZone.value = state.zone || "";
  const fMin = document.getElementById("f_min");
  if (fMin) fMin.value = state.min || "";
  const fMax = document.getElementById("f_max");
  if (fMax) fMax.value = state.max || "";
}

function buildSqlPreview() {
  if (!activeDataset?.table) return "";
  const cols = activeColumns;
  const { global, text, zone, min, max } = getFilterState();
  const allText = [global, text].filter(Boolean).join(" ").trim();

  const searchCols = resolveSearchColumns(cols, activeDataset);
  const zoningCol = resolveZoningColumn(cols, activeDataset);
  const acresCol = resolveAcresColumn(cols, activeDataset);

  const whereParts = [];
  const minNum = Number(min);
  const maxNum = Number(max);
  if (allText && searchCols.length) {
    const like = `%${escapeSqlLike(allText)}%`;
    const ors = searchCols.map(c => `upper(cast(${c} as text)) like upper('${like}')`);
    whereParts.push(`(${ors.join(" or ")})`);
  }
  if (zone && zoningCol) whereParts.push(`${zoningCol} = '${escapeSqlLike(zone)}'`);
  if (acresCol) {
    if (min && !Number.isNaN(minNum)) whereParts.push(`cast(${acresCol} as numeric) >= ${minNum}`);
    if (max && !Number.isNaN(maxNum)) whereParts.push(`cast(${acresCol} as numeric) <= ${maxNum}`);
  }
  const where = whereParts.length ? `where ${whereParts.join(" and ")}` : "";
  return `select * from ${activeDataset.table} ${where} limit ${QUERY_LIMIT};`;
}

function buildQueryFromFilters() {
  const cols = activeColumns;
  const { global, text, zone, min, max } = getFilterState();
  const allText = [global, text].filter(Boolean).join(" ").trim();

  const searchCols = resolveSearchColumns(cols, activeDataset);
  const zoningCol = resolveZoningColumn(cols, activeDataset);
  const acresCol = resolveAcresColumn(cols, activeDataset);
  const minNum = Number(min);
  const maxNum = Number(max);

  let qb = supabase.from(activeDataset.table).select("*", { count: "estimated" }).limit(QUERY_LIMIT);

  if (allText && searchCols.length) {
    const like = `%${escapeOrValue(allText)}%`;
    const orFilters = searchCols.map(c => `${c}.ilike.${like}`);
    qb = qb.or(orFilters.join(","));
  }
  if (zone && zoningCol) qb = qb.eq(zoningCol, zone);
  if (acresCol) {
    if (min && !Number.isNaN(minNum)) qb = qb.gte(acresCol, minNum);
    if (max && !Number.isNaN(maxNum)) qb = qb.lte(acresCol, maxNum);
  }

  return qb;
}

function tableFromRows(rows) {
  const cols = [];
  const seen = new Set();
  for (const r of rows) {
    for (const k of Object.keys(r)) {
      if (!seen.has(k)) { seen.add(k); cols.push(k); }
    }
  }
  return { cols, rows };
}

function formatCellValue(v) {
  if (v === null || v === undefined) return "";
  if (typeof v === "object") {
    const json = JSON.stringify(v);
    return json.length > 180 ? `${json.slice(0, 177)}…` : json;
  }
  return String(v);
}

function renderTable(tbl) {
  const colFilter = (els.colFilter.value || "").trim().toLowerCase();
  const { order, hidden } = getColumnState(tbl.cols);
  let cols = order.filter(c => !hidden.has(c));
  if (colFilter) cols = cols.filter(c => c.toLowerCase().includes(colFilter));

  els.resultTable.innerHTML = "";
  const thead = document.createElement("thead");
  const trh = document.createElement("tr");
  for (const c of cols) {
    const th = document.createElement("th");
    th.textContent = c;
    trh.appendChild(th);
  }
  thead.appendChild(trh);
  els.resultTable.appendChild(thead);

  const tbody = document.createElement("tbody");
  const maxRows = Math.min(tbl.rows.length, 250);
  for (let i = 0; i < maxRows; i++) {
    const tr = document.createElement("tr");
    for (const c of cols) {
      const td = document.createElement("td");
      const v = tbl.rows[i][c];
      td.textContent = formatCellValue(v);
      tr.appendChild(td);
    }
    tbody.appendChild(tr);
  }
  els.resultTable.appendChild(tbody);

  els.resultMeta.textContent = `Rows: ${tbl.rows.length} (showing ${maxRows}) · Columns: ${tbl.cols.length}`;
}

function esriToGeoJSON(g) {
  if (!g || typeof g !== "object") return null;
  if (typeof g.x === "number" && typeof g.y === "number") {
    return { type: "Point", coordinates: [g.x, g.y] };
  }
  if (Array.isArray(g.points)) {
    return { type: "MultiPoint", coordinates: g.points };
  }
  if (Array.isArray(g.paths)) {
    if (g.paths.length === 1) return { type: "LineString", coordinates: g.paths[0] };
    return { type: "MultiLineString", coordinates: g.paths };
  }
  if (Array.isArray(g.rings)) {
    return { type: "Polygon", coordinates: g.rings };
  }
  return null;
}

function mapCoords(coords, fn) {
  if (!Array.isArray(coords)) return coords;
  if (typeof coords[0] === "number") return fn(coords);
  return coords.map(c => mapCoords(c, fn));
}

function maxAbsCoord(coords, acc = 0) {
  if (!Array.isArray(coords)) return acc;
  if (typeof coords[0] === "number") {
    const x = Math.abs(coords[0]);
    const y = Math.abs(coords[1]);
    return Math.max(acc, x, y);
  }
  let next = acc;
  for (const c of coords) next = maxAbsCoord(c, next);
  return next;
}

function isLikelyWebMercator(geom) {
  const maxAbs = maxAbsCoord(geom.coordinates, 0);
  return maxAbs > 180;
}

function mercatorToLonLat(coord) {
  const x = coord[0];
  const y = coord[1];
  const R = 6378137;
  const lon = (x / R) * (180 / Math.PI);
  const lat = (2 * Math.atan(Math.exp(y / R)) - Math.PI / 2) * (180 / Math.PI);
  return [lon, lat];
}

function normalizeGeometry(val) {
  if (!val) return null;
  if (typeof val === "string") {
    const trimmed = val.trim();
    if (!trimmed) return null;
    if (trimmed.startsWith("{") || trimmed.startsWith("[")) {
      try { return JSON.parse(trimmed); } catch { return null; }
    }
    return null;
  }
  if (typeof val === "object") {
    if (val.type && val.coordinates) return val;
    return esriToGeoJSON(val);
  }
  return null;
}

function extractGeometry(row, cols) {
  const preferred = resolveGeometryColumn(cols, activeDataset);
  const candidates = [];
  if (preferred) candidates.push(preferred);
  for (const c of cols) {
    if (candidates.includes(c)) continue;
    if (/geom_geojson/i.test(c)) candidates.push(c);
  }
  for (const c of cols) {
    if (candidates.includes(c)) continue;
    if (/geom|geometry|shape/i.test(c)) candidates.push(c);
  }

  for (const c of candidates) {
    if (!(c in row)) continue;
    const norm = normalizeGeometry(row[c]);
    if (!norm) continue;
    if (norm.coordinates && isLikelyWebMercator(norm)) {
      return { ...norm, coordinates: mapCoords(norm.coordinates, mercatorToLonLat) };
    }
    return norm;
  }
  return null;
}

function renderMap(tbl) {
  geoLayer.clearLayers();
  if (!tbl.cols.length) { els.btnFit.disabled = true; return { featureCount: 0, hasGeom: false }; }

  const geomCol = resolveGeometryColumn(tbl.cols, activeDataset);
  if (!geomCol) { els.btnFit.disabled = true; return { featureCount: 0, hasGeom: false }; }

  const features = [];
  const max = Math.min(tbl.rows.length, QUERY_LIMIT);
  for (let i = 0; i < max; i++) {
    const g = extractGeometry(tbl.rows[i], tbl.cols);
    if (!g) continue;
    features.push({ type: "Feature", properties: {}, geometry: g });
  }
  geoLayer.addData(features);
  els.btnFit.disabled = features.length === 0;
  return { featureCount: features.length, hasGeom: true };
}

async function runQueryFromFilters() {
  if (!activeDataset?.table) return;
  setStatus("Running query…");
  els.sql.value = buildSqlPreview();
  const qb = buildQueryFromFilters();
  const { data, error, count } = await qb;
  if (error) {
    setStatus(`Query failed: ${error.message}`);
    return;
  }
  const rows = data || [];
  const tbl = tableFromRows(rows);
  lastResult = tbl;
  renderTable(tbl);
  const mapInfo = renderMap(tbl);
  renderColumnManager(tbl.cols);
  const countLabel = typeof count === "number" ? ` (of ~${count})` : "";
  const { order, hidden } = getColumnState(tbl.cols);
  const visibleCount = order.filter(c => !hidden.has(c)).length;
  const colLabel = visibleCount === tbl.cols.length ? `${tbl.cols.length}` : `${visibleCount}/${tbl.cols.length}`;
  const mapLabel = mapInfo?.hasGeom && mapInfo.featureCount === 0 ? " · Map: no geometries parsed" : "";
  els.resultMeta.textContent = `Rows: ${rows.length}${countLabel} · Columns: ${colLabel}${mapLabel}`;
  els.btnExport.disabled = rows.length === 0;
  setStatus("Ready.");
  updateLayerMeta();
}

function toCsv(tbl) {
  const esc = (v) => {
    const s = formatCellValue(v);
    if (/[",\n\r]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
    return s;
  };
  const cols = tbl.cols;
  const lines = [];
  lines.push(cols.map(esc).join(","));
  for (const r of tbl.rows) lines.push(cols.map(c => esc(r[c])).join(","));
  return lines.join("\n");
}

function download(name, content, mime) {
  const blob = new Blob([content], { type: mime });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = name;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

function buildViewPayload() {
  return JSON.stringify({
    v: 1,
    datasetId: activeDataset?.id || null,
    table: activeDataset?.table || null,
    filters: getFilterState(),
    sql: buildSqlPreview()
  });
}

function parseViewPayload(sqlText) {
  try {
    const parsed = JSON.parse(sqlText);
    if (parsed && parsed.v === 1) return parsed;
  } catch {}
  return null;
}

async function saveView(name) {
  const payload = buildViewPayload();
  if (!SAVED_VIEWS.enabled) {
    const views = loadViewsLocal();
    const id = `${Date.now()}_${Math.random().toString(16).slice(2)}`;
    views.push({ id, name, datasetId: activeDataset?.id, filters: getFilterState(), sql: els.sql.value, savedAt: Date.now() });
    saveViewsLocal(views);
    await renderViewsSelect();
    els.viewSelect.value = id;
    setStatus("Saved view.");
    return;
  }

  const { data, error } = await supabase
    .from(SAVED_VIEWS.table)
    .insert({ name, layer: activeDataset?.id || null, sql: payload })
    .select("id")
    .single();
  if (error) {
    setStatus(`Save failed: ${error.message}`);
    return;
  }
  await renderViewsSelect();
  if (data?.id) els.viewSelect.value = data.id;
  setStatus("Saved view.");
}

async function deleteView(id) {
  if (!id) return;
  if (!SAVED_VIEWS.enabled) {
    const views = loadViewsLocal().filter(v => v.id !== id);
    saveViewsLocal(views);
    await renderViewsSelect();
    setStatus("Deleted view.");
    return;
  }

  const { error } = await supabase.from(SAVED_VIEWS.table).delete().eq("id", id);
  if (error) {
    setStatus(`Delete failed: ${error.message}`);
    return;
  }
  await renderViewsSelect();
  setStatus("Deleted view.");
}

async function runSql(sql) {
  if (!SQL.enabled) {
    setStatus("SQL workbench is disabled. Configure CONFIG.sql to enable it.");
    return;
  }
  setStatus("Running SQL…");
  const { data, error } = await supabase.rpc(SQL.rpcName, { sql });
  if (error) {
    setStatus(`SQL failed: ${error.message}`);
    return;
  }
  const rows = Array.isArray(data) ? data : [];
  const tbl = tableFromRows(rows);
  lastResult = tbl;
  renderTable(tbl);
  const mapInfo = renderMap(tbl);
  renderColumnManager(tbl.cols);
  els.btnExport.disabled = rows.length === 0;
  const { order, hidden } = getColumnState(tbl.cols);
  const visibleCount = order.filter(c => !hidden.has(c)).length;
  const colLabel = visibleCount === tbl.cols.length ? `${tbl.cols.length}` : `${visibleCount}/${tbl.cols.length}`;
  const mapLabel = mapInfo?.hasGeom && mapInfo.featureCount === 0 ? " · Map: no geometries parsed" : "";
  els.resultMeta.textContent = `Rows: ${rows.length} · Columns: ${colLabel}${mapLabel}`;
  setStatus("Ready.");
}

function wireEvents() {
  els.btnApply.addEventListener("click", async () => {
    await runQueryFromFilters();
  });

  els.btnRun.addEventListener("click", async () => {
    await runSql(els.sql.value);
  });

  els.btnReset.addEventListener("click", async () => {
    els.globalSearch.value = "";
    buildFilterUI(activeColumns, activeDataset);
    els.sql.value = buildSqlPreview();
    await runQueryFromFilters();
  });

  els.btnFit.addEventListener("click", () => {
    try { map.fitBounds(geoLayer.getBounds(), { padding: [20, 20] }); } catch {}
  });

  els.colFilter.addEventListener("input", () => {
    if (lastResult) renderTable(lastResult);
  });

  els.btnCols.addEventListener("click", () => {
    if (!els.colManager) return;
    els.colManager.classList.toggle("hidden");
    if (!els.colManager.classList.contains("hidden")) {
      const cols = lastResult?.cols || activeColumns;
      if (cols && cols.length) renderColumnManager(cols);
    }
  });

  els.btnColsShowAll.addEventListener("click", () => {
    const prefs = getDatasetColumnPrefs(columnManagerCols);
    prefs.hidden = [];
    saveColumnPrefs();
    if (lastResult) renderTable(lastResult);
    renderColumnManager(lastResult?.cols || activeColumns);
  });

  els.btnColsReset.addEventListener("click", () => {
    const prefs = getDatasetColumnPrefs(columnManagerCols);
    prefs.hidden = [];
    prefs.order = [];
    prefs.seeded = false;
    saveColumnPrefs();
    if (lastResult) renderTable(lastResult);
    renderColumnManager(lastResult?.cols || activeColumns);
  });

  els.btnExport.addEventListener("click", () => {
    if (!lastResult) return;
    const csv = toCsv(lastResult);
    const safe = (activeDataset?.label || "view").replace(/[^a-z0-9]+/gi, "_").toLowerCase();
    download(`${safe}.csv`, csv, "text/csv;charset=utf-8");
  });

  els.btnSaveView.addEventListener("click", async () => {
    const name = prompt("Name this view:");
    if (!name) return;
    await saveView(name);
  });

  els.btnDeleteView.addEventListener("click", async () => {
    const id = els.viewSelect.value;
    if (!id) return;
    await deleteView(id);
  });

  els.viewSelect.addEventListener("change", async () => {
    const id = els.viewSelect.value;
    if (!id) return;
    const views = await loadViews();
    const view = views.find(v => v.id === id);
    if (!view) return;
    if (view.datasetId && view.datasetId !== activeDataset?.id) {
      await activateDataset(view.datasetId, false, true);
    }
    if (view.filters) applyFilterState(view.filters);
    els.sql.value = view.sql || buildSqlPreview();
    await runQueryFromFilters();
  });

  els.globalSearch.addEventListener("keydown", async (e) => {
    if (e.key === "Enter") {
      await runQueryFromFilters();
    }
  });
}

async function autoStart() {
  try {
    setStatus("Initializing Supabase…");
    const ok = await initSupabase();
    if (!ok) return;
    await initMap();

    els.savedViewsHint.textContent = SAVED_VIEWS.enabled ? "Saved in Supabase (shared)." : "Saved on this device.";
    els.sqlHint.textContent = SQL.enabled ? "SQL workbench (server RPC)." : "SQL preview (optional).";

    await fetchLayers();
    renderTabs();
    await renderViewsSelect();
    wireEvents();

    const parcels = datasets.find(d => String(d.label || "").toLowerCase() === "parcels") || datasets[0];
    if (parcels) {
      await activateDataset(parcels.id, true);
      setStatus("Ready.");
      setUiEnabled(true);
    } else {
      setStatus("No layers configured. Update CONFIG.layers in /assets/config.js.");
    }
  } catch (e) {
    setStatus(String(e));
  }
}

window.addEventListener("DOMContentLoaded", autoStart);
