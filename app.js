/* Reading Log PWA (Option 1B - local-first)
   - IndexedDB storage
   - Library / Reading / Slug editor / Reports / Data import-export
   - CSV schema parity (22 columns, exact order)
*/

const SCHEMA_VERSION = "2025.2";
const CSV_COLUMNS = [
  "SchemaVersion","Title","Author","AuthorID","BookID","Format","Series","SeriesID",
  "Duration_h","Cost_h","Net_h","Tags","Notes","Status","DateStarted","DateFinished",
  "DayDelta","Scoreboard","LastEdited","CompositeKey","Franchise","Subseries"
];

const ROUTES = ["library","reading","reports","data"];
let state = { route:"library", query:"", selectedBookKey:null, selectedDraftKey:null };

function $(sel, root=document){ return root.querySelector(sel); }
function el(tag, attrs={}, children=[]){
  const n = document.createElement(tag);
  for(const [k,v] of Object.entries(attrs)){
    if(k==="class") n.className=v;
    else if(k==="html") n.innerHTML=v;
    else if(k.startsWith("on") && typeof v==="function") n.addEventListener(k.slice(2), v);
    else if(v!==null && v!==undefined) n.setAttribute(k, v);
  }
  for(const c of (Array.isArray(children)?children:[children])){
    if(c===null||c===undefined) continue;
    n.appendChild(typeof c==="string"?document.createTextNode(c):c);
  }
  return n;
}
function toast(msg){
  const t = $("#toast");
  t.textContent = msg;
  t.style.display = "block";
  clearTimeout(toast._to);
  toast._to = setTimeout(()=>t.style.display="none", 2200);
}


function ensureImportStatus(){
  let s = document.getElementById("import-status");
  if(!s){
    s = document.createElement("div");
    s.id="import-status";
    s.className="status";
    s.textContent="(status)";
    document.body.appendChild(s);
  }
  return s;
}

function todayISO(){
  const d = new Date();
  const y=d.getFullYear(), m=String(d.getMonth()+1).padStart(2,"0"), da=String(d.getDate()).padStart(2,"0");
  return `${y}-${m}-${da}`;
}
function nowISO(){
  const d=new Date();
  const y=d.getFullYear(), m=String(d.getMonth()+1).padStart(2,"0"), da=String(d.getDate()).padStart(2,"0");
  return `${y}-${m}-${da}`;
}

function parseHhMm(s){
  // Accept "25h17", "25:17", "25h17 ", "25 H 17" etc.
  if(!s) return null;
  const t = String(s).trim().toLowerCase().replace(/\s+/g,"");
  let m = t.match(/^(-?\d+)(?:h|:)(\d{1,2})$/);
  if(!m) return null;
  const h = parseInt(m[1],10);
  const mm = parseInt(m[2],10);
  if(Number.isNaN(h)||Number.isNaN(mm)||mm<0||mm>59) return null;
  return h*60 + mm;
}
function fmtHhMm(minutes){
  const h = Math.trunc(minutes/60);
  const m = Math.abs(minutes%60);
  return `${h}h${String(m).padStart(2,"0")}`;
}
function minutesDiff(startMin, endMin){
  if(startMin===null||endMin===null) return null;
  return endMin - startMin;
}
function round4(x){
  return Math.round((x + Number.EPSILON)*10000)/10000;
}
function netIcon(net){
  return (net>=0) ? "✅" : "❌";
}

// ----- IndexedDB -----
const DB_NAME="readinglog_local";
const DB_VER=1;
let db=null;

function idbOpen(){
  return new Promise((resolve,reject)=>{
    let req; try{ req = indexedDB.open(DB_NAME, DB_VER); }catch(e){ reject(new Error("IndexedDB blocked in this browser mode. Try normal Safari (not Private) and disable content blockers for this site.")); return; }
    req.onupgradeneeded = (e)=>{
      const d = req.result;
      if(!d.objectStoreNames.contains("books")){
        const s=d.createObjectStore("books",{keyPath:"bookKey"});
        s.createIndex("by_title","title",{unique:false});
      }
      if(!d.objectStoreNames.contains("entries")){
        const s=d.createObjectStore("entries",{keyPath:"compositeKey"});
        s.createIndex("by_bookKey","bookKey",{unique:false});
        s.createIndex("by_dateStarted","dateStarted",{unique:false});
      }
      if(!d.objectStoreNames.contains("drafts")){
        const s=d.createObjectStore("drafts",{keyPath:"draftKey"});
        s.createIndex("by_bookKey","bookKey",{unique:false});
        s.createIndex("by_active","active",{unique:false});
      }
      if(!d.objectStoreNames.contains("meta")){
        d.createObjectStore("meta",{keyPath:"key"});
      }
    };
    req.onsuccess=()=>{ db=req.result; resolve(db); };
    req.onerror=()=>reject(req.error);
  });
}

function tx(store, mode="readonly"){
  return db.transaction(store, mode).objectStore(store);
}
function idbGet(store, key){
  return new Promise((res,rej)=>{
    const r=tx(store).get(key);
    r.onsuccess=()=>res(r.result||null);
    r.onerror=()=>rej(r.error);
  });
}
function idbPut(store, val){
  return new Promise((res,rej)=>{
    const r=tx(store,"readwrite").put(val);
    r.onsuccess=()=>res(true);
    r.onerror=()=>rej(r.error);
  });
}
function idbDel(store, key){
  return new Promise((res,rej)=>{
    const r=tx(store,"readwrite").delete(key);
    r.onsuccess=()=>res(true);
    r.onerror=()=>rej(r.error);
  });
}
function idbAll(store, indexName=null, key=null){
  return new Promise((res,rej)=>{
    const s=tx(store);
    const out=[];
    const source = indexName ? s.index(indexName) : s;
    const r = (indexName && key!==null) ? source.openCursor(IDBKeyRange.only(key)) : source.openCursor();
    r.onsuccess=()=>{
      const cur=r.result;
      if(cur){ out.push(cur.value); cur.continue(); }
      else res(out);
    };
    r.onerror=()=>rej(r.error);
  });
}

// ----- Derivations from existing schema rules -----
function normalizeId(s){
  return String(s||"").trim().toLowerCase()
    .replace(/['’]/g,"")
    .replace(/[^a-z0-9]+/g,"_")
    .replace(/^_+|_+$/g,"");
}

function deriveSeries(franchise, subseries){
  if(franchise && subseries) return `${franchise} — ${subseries}`;
  if(franchise && !subseries) return franchise;
  return "";
}
function deriveSeriesId(franchise, subseries){
  if(!franchise) return "";
  if(franchise && !subseries) return normalizeId(franchise);
  return normalizeId(`${franchise} ${subseries}`);
}

function makeBookKey(bookID, format){
  return `${bookID}||${format}`;
}

async function ensureBookFromCSVRow(row){
  const bookID = row.BookID;
  const format = row.Format;
  const bookKey = makeBookKey(bookID, format);
  let b = await idbGet("books", bookKey);
  if(!b){
    b = {
      bookKey,
      bookID,
      format,
      title: row.Title||"",
      author: row.Author||"",
      authorID: row.AuthorID||normalizeId(row.Author||""),
      franchise: row.Franchise||"",
      subseries: row.Subseries||"",
      series: row.Series || deriveSeries(row.Franchise||"", row.Subseries||""),
      seriesID: row.SeriesID || deriveSeriesId(row.Franchise||"", row.Subseries||""),
    };
    await idbPut("books", b);
  }
  return b;
}

function csvEscape(v){
  const s = (v===null || v===undefined) ? "" : String(v);
  if(/[",\n]/.test(s)) return `"${s.replace(/"/g,'""')}"`;
  return `"${s}"`; // always quote for simplicity + stability
}

function parseCSV(text){
  // Simple CSV parser that supports quoted fields with "" escaping.
  const rows=[];
  let i=0, field="", row=[], inQ=false;
  while(i<text.length){
    const c=text[i];
    if(inQ){
      if(c==='"'){
        if(text[i+1]==='"'){ field+='"'; i+=2; continue; }
        inQ=false; i++; continue;
      }else{ field+=c; i++; continue; }
    }else{
      if(c==='"'){ inQ=true; i++; continue; }
      if(c===','){ row.push(field); field=""; i++; continue; }
      if(c==='\n'){
        row.push(field); field="";
        // drop empty last line
        if(row.length>1 || row[0].trim()!=="") rows.push(row);
        row=[]; i++; continue;
      }
      if(c==='\r'){ i++; continue; }
      field+=c; i++; continue;
    }
  }
  // last line
  if(field.length || row.length){
    row.push(field);
    if(row.length>1 || row[0].trim()!=="") rows.push(row);
  }
  return rows;
}

function buildRowObjFromCSV(values, header){
  const obj={};
  for(let j=0;j<header.length;j++){
    obj[header[j]] = (values[j] ?? "");
  }
  return obj;
}

async function importCSV(text){
  const rows=parseCSV(text);
  if(rows.length<2) throw new Error("CSV appears empty.");
  const header=rows[0].map(h=>h.trim());
  const expected=CSV_COLUMNS;
  const same = header.length===expected.length && header.every((h,idx)=>h===expected[idx]);
  if(!same){
    throw new Error("CSV header does not match expected 22-column schema/order.");
  }
  let added=0, skipped=0, deduped=0;
  for(let r=1;r<rows.length;r++){
    const obj=buildRowObjFromCSV(rows[r], header);
    // normalize key fields
    const bookKey = makeBookKey(obj.BookID, obj.Format);
    const compositeKey = obj.CompositeKey;
    if(!compositeKey){
      // If missing, regenerate from BookID (keeps parity best effort)
      obj.CompositeKey = obj.BookID;
    }
    const existing = await idbGet("entries", obj.CompositeKey);
    if(existing){
      // collision: keep existing; import duplicate as _copyN
      let n=2;
      let ck=`${obj.CompositeKey}_copy${n}`;
      while(await idbGet("entries", ck)) { n++; ck=`${obj.CompositeKey}_copy${n}`; }
      obj.CompositeKey = ck;
      deduped++;
    }
    obj.bookKey = bookKey;
    // ensure book exists
    await ensureBookFromCSVRow(obj);
    // persist entry
    await idbPut("entries", {
      compositeKey: obj.CompositeKey,
      bookKey,
      row: obj
    });
    added++;
  }
  return {added, skipped, deduped};
}

async function exportCSV(){
  const entries = await idbAll("entries");
  // stable ordering: by LastEdited then CompositeKey
  entries.sort((a,b)=>{
    const la=(a.row.LastEdited||""); const lb=(b.row.LastEdited||"");
    if(la<lb) return -1; if(la>lb) return 1;
    return (a.compositeKey<b.compositeKey)?-1:1;
  });
  const lines=[];
  lines.push(CSV_COLUMNS.map(csvEscape).join(","));
  for(const e of entries){
    const row=e.row;
    const line = CSV_COLUMNS.map(k=>csvEscape(row[k] ?? "")).join(",");
    lines.push(line);
  }
  return lines.join("\n") + "\n";
}

function downloadText(filename, text){
  const blob = new Blob([text], {type:"text/csv;charset=utf-8"});
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href=url; a.download=filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  setTimeout(()=>URL.revokeObjectURL(url), 1000);
}

// ----- UI Views -----
function setRoute(route){
  if(!ROUTES.includes(route)) route="library";
  state.route=route;
  for(const r of ROUTES){
    const b = document.getElementById(`tab-${r}`);
    if(b){
      b.setAttribute("aria-current", r===route ? "page" : "false");
    }
  }
  render();
}

function libraryRow(book, stats){
  const net = stats.netOverall;
  const netClass = net>=0 ? "ok" : "bad";
  const meta = `${book.format} • Reads: ${stats.reads} • Net: ${net.toFixed(2)} h ${netIcon(net)}`;
  const btn = el("button",{class:"btn primary", onClick: async ()=>{
    state.selectedBookKey = book.bookKey;
    await openBookDetail(book.bookKey);
  }},["Open"]);
  return el("div",{class:"row"},[
    el("div",{class:"title"},[book.title]),
    el("div",{class:"meta"},[book.author]),
    el("div",{class:"meta"},[meta]),
    el("div",{class:"actions"},[btn])
  ]);
}

function readingRow(draft, book){
  const openRanges = draft.timeRanges.filter(r=>r.end===null).length;
  const closedRanges = draft.timeRanges.filter(r=>r.end!==null).length;
  const rangeText = openRanges ? `${openRanges} open range` : (closedRanges? `${closedRanges} ranges` : "No ranges");
  const meta = `${book.format} • Active • ${rangeText}`;
  return el("div",{class:"row"},[
    el("div",{class:"title"},[book.title]),
    el("div",{class:"meta"},[book.author]),
    el("div",{class:"meta"},[meta]),
    el("div",{class:"actions"},[
      el("button",{class:"btn primary", onClick:()=>openSlug(draft.draftKey)},["Open slug"])
    ])
  ]);
}

async function computeBookStats(bookKey){
  const entries = await idbAll("entries","by_bookKey", bookKey);
  let reads=0, dur=0, cost=0;
  for(const e of entries){
    const row=e.row;
    const d=parseFloat(row.Duration_h||"0")||0;
    const c=parseFloat(row.Cost_h||"0")||0;
    dur+=d; cost+=c;
    if((row.Status||"").toLowerCase()==="finished") reads+=1;
  }
  const net = dur-cost;
  return {reads, duration:dur, cost, netOverall:net};
}

async function renderLibrary(){
  const root = el("div",{},[]);
  const header = el("div",{class:"section"},[
    el("div",{class:"hrow"},[
      el("h1",{},["Library"]),
      el("div",{class:"small"},["Tip: Search title/author. Tap Open for details."])
    ]),
    el("label",{},["Search"]),
    el("input",{class:"input", value: state.query, placeholder:"Search…", onInput:(e)=>{ state.query=e.target.value; render(); }}),
  ]);
  root.appendChild(header);

  const books = await idbAll("books");
  const q = state.query.trim().toLowerCase();
  const filtered = q ? books.filter(b => (b.title||"").toLowerCase().includes(q) || (b.author||"").toLowerCase().includes(q)) : books;
  filtered.sort((a,b)=> (a.title||"").localeCompare(b.title||""));

  const list = el("div",{class:"section"},[]);
  list.appendChild(el("div",{class:"hrow"},[
    el("h2",{},[`${filtered.length} book items`]),
    el("span",{class:"badge"},["Format collisions are intentional."])
  ]));

  for(const b of filtered.slice(0, 500)){ // guard for very large lists
    const stats = await computeBookStats(b.bookKey);
    list.appendChild(libraryRow(b, stats));
  }
  if(filtered.length>500){
    list.appendChild(el("div",{class:"small"},["Showing first 500 results. Narrow your search to see more."]));
  }
  root.appendChild(list);
  return root;
}

async function renderReading(){
  const root=el("div",{},[]);
  root.appendChild(el("div",{class:"section"},[
    el("div",{class:"hrow"},[
      el("h1",{},["Reading"]),
      el("div",{class:"small"},["Tap any row to continue logging. Drafts auto-save locally."])
    ])
  ]));

  const drafts = await idbAll("drafts","by_active");
  const active = drafts.filter(d=>d.active===true);
  active.sort((a,b)=> (a.lastEdited||"").localeCompare(b.lastEdited||""));
  const sec=el("div",{class:"section"},[]);
  sec.appendChild(el("h2",{},[`${active.length} active`]));

  for(const d of active){
    const book = await idbGet("books", d.bookKey);
    if(book) sec.appendChild(readingRow(d, book));
  }
  if(active.length===0){
    sec.appendChild(el("div",{class:"small"},["No active items. Start a reread from a Book Detail page (Duplicate / Start Reading)."]));
  }
  root.appendChild(sec);
  return root;
}

function kpiCard(label, value, cls=""){
  return el("div",{class:"kcard"},[
    el("div",{class:"klabel"},[label]),
    el("div",{class:`kvalue ${cls}`.trim()},[value])
  ]);
}

function dateInRange(d, from, to){
  if(!d) return false;
  return d>=from && d<=to;
}
function startOfWeekISO(dISO){
  // Monday as start
  const d = new Date(dISO+"T00:00:00");
  const day = (d.getDay()+6)%7; // Mon=0
  d.setDate(d.getDate()-day);
  return d.toISOString().slice(0,10);
}
function startOfMonthISO(dISO){ return dISO.slice(0,7)+"-01"; }
function startOfYearISO(dISO){ return dISO.slice(0,4)+"-01-01"; }

async function sumEntries(from, to){
  const all = await idbAll("entries");
  let dur=0, cost=0;
  for(const e of all){
    const row=e.row;
    const ds = row.DateStarted || row.DateFinished || "";
    if(dateInRange(ds, from, to)){
      dur += parseFloat(row.Duration_h||"0")||0;
      cost += parseFloat(row.Cost_h||"0")||0;
    }
  }
  const net = dur-cost;
  return {dur, cost, net};
}

async function renderReports(){
  const root=el("div",{},[]);
  const now = todayISO();
  const wkStart = startOfWeekISO(now);
  const moStart = startOfMonthISO(now);
  const yrStart = startOfYearISO(now);

  const today = await sumEntries(now, now);
  const week = await sumEntries(wkStart, now);
  const month = await sumEntries(moStart, now);
  const ytd = await sumEntries(yrStart, now);
  const life = await sumEntries("0000-01-01", "9999-12-31");

  root.appendChild(el("div",{class:"section"},[
    el("div",{class:"hrow"},[
      el("h1",{},["Reports"]),
      el("div",{class:"small"},["All numbers = sums of Duration_h / Cost_h / Net_h from your CSV rows."])
    ]),
    el("div",{class:"kpi"},[
      kpiCard("Today", `${today.dur.toFixed(2)}h / ${today.cost.toFixed(2)}h / ${today.net.toFixed(2)}h ${netIcon(today.net)}`, today.net>=0?"ok":"bad"),
      kpiCard("This Week", `${week.dur.toFixed(2)}h / ${week.cost.toFixed(2)}h / ${week.net.toFixed(2)}h ${netIcon(week.net)}`, week.net>=0?"ok":"bad"),
      kpiCard("This Month", `${month.dur.toFixed(2)}h / ${month.cost.toFixed(2)}h / ${month.net.toFixed(2)}h ${netIcon(month.net)}`, month.net>=0?"ok":"bad"),
      kpiCard("YTD", `${ytd.dur.toFixed(2)}h / ${ytd.cost.toFixed(2)}h / ${ytd.net.toFixed(2)}h ${netIcon(ytd.net)}`, ytd.net>=0?"ok":"bad"),
      kpiCard("Lifetime", `${life.dur.toFixed(2)}h / ${life.cost.toFixed(2)}h / ${life.net.toFixed(2)}h ${netIcon(life.net)}`, life.net>=0?"ok":"bad"),
    ])
  ]));

  // Custom report builder (simple)
  const custom = el("div",{class:"section"},[
    el("h2",{},["Custom date range"]),
    el("div",{class:"grid two"},[
      el("div",{},[
        el("label",{},["From (YYYY-MM-DD)"]),
        el("input",{class:"input", id:"cr-from", value: wkStart})
      ]),
      el("div",{},[
        el("label",{},["To (YYYY-MM-DD)"]),
        el("input",{class:"input", id:"cr-to", value: now})
      ]),
    ]),
    el("div",{class:"actions"},[
      el("button",{class:"btn primary", onClick: async ()=>{
        const f = $("#cr-from").value.trim();
        const t = $("#cr-to").value.trim();
        const s = await sumEntries(f,t);
        $("#cr-out").textContent = `Read time: ${s.dur.toFixed(2)} h\nCost:      ${s.cost.toFixed(2)} h\nNet:       ${s.net.toFixed(2)} h ${netIcon(s.net)}`;
      }},["Run report"])
    ]),
    el("pre",{id:"cr-out", class:"row", style:"white-space:pre-wrap"},[""])
  ]);
  root.appendChild(custom);

  // Unpaid list
  const unpaid = el("div",{class:"section"},[
    el("h2",{},["Books not yet paid off (overall)"]),
    el("div",{class:"small"},["Overall net is summed across all entries for each BookID+Format item."]),
  ]);

  const books = await idbAll("books");
  const unpaidItems=[];
  for(const b of books){
    const st = await computeBookStats(b.bookKey);
    if(st.netOverall < 0) unpaidItems.push({b, st});
  }
  unpaidItems.sort((a,b)=>a.st.netOverall-b.st.netOverall);

  if(unpaidItems.length===0){
    unpaid.appendChild(el("div",{class:"small"},["None. Everything is paid off (net ≥ 0)."]));
  } else {
    for(const it of unpaidItems.slice(0,200)){
      unpaid.appendChild(el("div",{class:"row"},[
        el("div",{class:"title"},[it.b.title]),
        el("div",{class:"meta"},[it.b.author]),
        el("div",{class:"meta"},[`${it.b.format} • Net: ${it.st.netOverall.toFixed(2)} h ❌`]),
        el("div",{class:"actions"},[
          el("button",{class:"btn primary", onClick:()=>openBookDetail(it.b.bookKey)},["Open"])
        ])
      ]));
    }
    if(unpaidItems.length>200) unpaid.appendChild(el("div",{class:"small"},["Showing first 200 unpaid items. Use Library search for more."]));
  }
  root.appendChild(unpaid);

  return root;
}

async function renderData(){
  const root=el("div",{},[]);
  root.appendChild(el("div",{class:"section"},[
    el("div",{class:"hrow"},[
      el("h1",{},["Data"]),
      el("div",{class:"small"},["Import your existing CSV once. Export any time. Append-only storage."])
    ]),
    el("div",{class:"grid two"},[
      el("div",{},[
        el("h2",{},["Import CSV"]),
        el("div",{class:"small"},["Must be your 22-column schema/order. After you pick a file, you should see its name below."]),
        el("input",{class:"input", id:"csvfile", type:"file", accept:".csv,text/csv"}),
        el("div",{id:"import-filename", class:"small"},["No file selected."]),
        el("div",{class:"actions"},[
          el("button",{class:"btn", onClick: async ()=>{
            // Self-test: import a tiny valid CSV with one row, so you can verify the importer is running.
            const header = CSV_COLUMNS.map(csvEscape).join(",");
            const rowObj = {};
            for(const k of CSV_COLUMNS) rowObj[k] = "";
            rowObj.SchemaVersion = SCHEMA_VERSION;
            rowObj.Title = "IMPORT SELF-TEST";
            rowObj.Author = "System";
            rowObj.AuthorID = "system";
            rowObj.BookID = "import_self_test";
            rowObj.Format = "Kindle";
            rowObj.Duration_h = "1.0000";
            rowObj.Cost_h = "0";
            rowObj.Net_h = "1";
            rowObj.Status = "Finished";
            rowObj.DateStarted = todayISO();
            rowObj.DateFinished = todayISO();
            rowObj.LastEdited = todayISO();
            rowObj.CompositeKey = "import_self_test";
            const row = CSV_COLUMNS.map(k=>csvEscape(rowObj[k] ?? "")).join(",");
            const csv = header + "
" + row + "
";
            ensureImportStatus().textContent = "Running self-test import…";
            try{
              const res = await importCSV(csv);
              ensureImportStatus().textContent = `Self-test OK. Imported ${res.added} row(s), deduped ${res.deduped}.`;
              toast("Self-test import OK.");
              state.query="";
              render();
            }catch(err){
              ensureImportStatus().textContent = `Self-test FAILED: ${err.message||String(err)}`;
              alert(err.message||String(err));
            }
          }},["Run import self-test"])
        ]),
        el("div",{class:"actions"},[
          el("button",{class:"btn primary", id:"btn-import", onClick: async ()=>{
            const input = $("#csvfile");
            const f = input?.files?.[0];
            if(!f){
              ensureImportStatus().textContent = "No file selected. Tap the file picker first.";
              toast("Pick a CSV file first.");
              return;
            }
            const btn = $("#btn-import");
            btn.disabled = true;
            ensureImportStatus().textContent = `Reading file: ${f.name} (${Math.round(f.size/1024)} KB)…`;
            try{
              const text = await f.text();
              ensureImportStatus().textContent = "Parsing CSV header…";
              const res = await importCSV(text);
              ensureImportStatus().textContent = `Imported ${res.added} rows (${res.deduped} deduped).`;
              toast(`Imported ${res.added} rows.`);
              state.query="";
              render();
            }catch(err){
              ensureImportStatus().textContent = `IMPORT ERROR: ${err.message||String(err)}`;
              alert(err.message||String(err));
            }finally{
              btn.disabled = false;
            }
          }},["Import"])
        ]),
        el("div",{class:"row"},[
          el("div",{class:"title"},["Import status"]),
          el("div",{id:"import-status", class:"meta", style:"white-space:pre-wrap"},["Idle."])
        ])
      ]),
      el("div",{},[
        el("h2",{},["Export CSV"]),
        el("div",{class:"small"},["Exports your current database in the same schema/order."]),
        el("div",{class:"actions"},[
          el("button",{class:"btn primary", onClick: async ()=>{
            const csv = await exportCSV();
            downloadText(`ReadingLog_export_${todayISO()}.csv`, csv);
          }},["Export now"])
        ]),
        el("div",{class:"hr"}),
        el("h2",{},["Danger zone"]),
        el("div",{class:"small"},["This clears local data on this device only."]),
        el("div",{class:"actions"},[
          el("button",{class:"btn danger", onClick: async ()=>{
            if(!confirm("Clear ALL local data on this device? This cannot be undone unless you re-import.")) return;
            indexedDB.deleteDatabase(DB_NAME);
            toast("Local database cleared. Reloading…");
            setTimeout(()=>location.reload(), 800);
          }},["Clear local database"])
        ])
      ])
    ])
  ]));

  // Update filename label when a file is picked
  setTimeout(()=>{
    const input = $("#csvfile");
    if(input){
      input.addEventListener("change", ()=>{
        const f = input.files?.[0];
        $("#import-filename").textContent = f ? `Selected: ${f.name} (${Math.round(f.size/1024)} KB)` : "No file selected.";
      });
    }
  }, 0);

  return root;
}

// ----- Book Detail + Slug -----
async function openBookDetail(bookKey){
  const book = await idbGet("books", bookKey);
  if(!book){ toast("Book not found."); return; }
  const stats = await computeBookStats(bookKey);
  const entries = await idbAll("entries","by_bookKey", bookKey);
  entries.sort((a,b)=>{
    const da=(a.row.DateStarted||""); const db=(b.row.DateStarted||"");
    if(da<db) return -1; if(da>db) return 1;
    return (a.compositeKey<b.compositeKey)?-1:1;
  });

  const modal = el("div",{class:"section", role:"dialog", "aria-label":"Book details"},[
    el("div",{class:"hrow"},[
      el("h1",{},[book.title]),
      el("button",{class:"btn", onClick:()=>{ modal.remove(); }},["Close"])
    ]),
    el("div",{class:"small"},[book.author]),
    el("div",{class:"hr"}),
    el("div",{class:"grid two"},[
      el("div",{},[
        el("div",{class:"badge"},[`${book.franchise || "No franchise"}${book.subseries?` — ${book.subseries}`:""}`]),
        el("div",{class:"small"},[`Format: ${book.format}`]),
        el("div",{class:"small"},[`Series: ${book.series}`]),
      ]),
      el("div",{},[
        el("div",{class:"small"},[`Reads (Finished): ${stats.reads}`]),
        el("div",{class:"small"},[`Total time: ${stats.duration.toFixed(2)} h`]),
        el("div",{class:"small"},[`Total cost: ${stats.cost.toFixed(2)} h`]),
        el("div",{class:"small"},[`Net (overall): ${stats.netOverall.toFixed(2)} h ${netIcon(stats.netOverall)} ${(stats.netOverall>=0)?"Paid off":"Not paid off"}`]),
      ])
    ]),
    el("div",{class:"actions"},[
      el("button",{class:"btn primary", onClick: async ()=>{
        // Duplicate / Start Reading: create or reuse draft
        const draftKey = await getOrCreateDraft(bookKey, {isReread:true});
        modal.remove();
        openSlug(draftKey);
      }},["Duplicate / Start Reading"]),
      el("button",{class:"btn", onClick: async ()=>{
        const draft = await findActiveDraftByBookKey(bookKey);
        if(draft){ modal.remove(); openSlug(draft.draftKey); }
        else toast("Not currently in Reading.");
      }},["Continue (if active)"])
    ]),
    el("div",{class:"hr"}),
    el("h2",{},["Reading History (rows)"]),
    el("div",{class:"small"},["This is a read-only view of your CSV rows for this book item."]),
  ]);

  for(const e of entries.slice(0,200)){
    const r = e.row;
    const dur = parseFloat(r.Duration_h||"0")||0;
    const cost = parseFloat(r.Cost_h||"0")||0;
    const net = parseFloat(r.Net_h||"0")||0;
    const dateRange = `${r.DateStarted||"?"} → ${r.DateFinished||"?"}`;
    modal.appendChild(el("div",{class:"row"},[
      el("div",{class:"title"},[`Status: ${r.Status||""}`]),
      el("div",{class:"meta"},[`Date range: ${dateRange}`]),
      el("div",{class:"meta"},[`Time: ${dur.toFixed(2)} h • Cost: ${cost.toFixed(2)} h • Net: ${net.toFixed(2)} h ${netIcon(net)}`]),
      el("div",{class:"meta"},[`CompositeKey: ${e.compositeKey}`]),
    ]));
  }
  if(entries.length>200) modal.appendChild(el("div",{class:"small"},["Showing first 200 rows. Export CSV for the full dataset."]));

  $("#app").prepend(modal);
  modal.scrollIntoView({behavior:"smooth", block:"start"});
}

async function findActiveDraftByBookKey(bookKey){
  const drafts = await idbAll("drafts","by_bookKey", bookKey);
  return drafts.find(d=>d.active===true) || null;
}

async function getOrCreateDraft(bookKey, opts={isReread:false}){
  const existing = await findActiveDraftByBookKey(bookKey);
  if(existing) return existing.draftKey;
  const book = await idbGet("books", bookKey);
  if(!book) throw new Error("Book not found.");
  const draftKey = `draft_${bookKey}_${Date.now()}`;
  const d = {
    draftKey,
    bookKey,
    active: true,
    dateStarted: todayISO(),
    dateFinished: "",
    status: "Unfinished",
    tags: "",
    notes: opts.isReread ? "reread" : "",
    // cost default: 0 for reread, else blank until user fills (or you can store purchase price elsewhere)
    cost_h: opts.isReread ? "0" : "",
    timeRanges: [],
    lastEdited: nowISO()
  };
  await idbPut("drafts", d);
  return draftKey;
}

async function openSlug(draftKey){
  const draft = await idbGet("drafts", draftKey);
  if(!draft){ toast("Draft not found."); return; }
  const book = await idbGet("books", draft.bookKey);
  if(!book){ toast("Book not found."); return; }

  const view = el("div",{class:"section"},[
    el("div",{class:"hrow"},[
      el("h1",{},["Slug editor"]),
      el("div",{class:"actions"},[
        el("button",{class:"btn", onClick:()=>setRoute("reading")},["Back to Reading"])
      ])
    ]),
    el("div",{class:"row"},[
      el("div",{class:"title"},[book.title]),
      el("div",{class:"meta"},[book.author]),
      el("div",{class:"meta"},[`${book.franchise || "No franchise"}${book.subseries?` — ${book.subseries}`:""} • Format: ${book.format}`]),
      el("div",{class:"meta"},[`Series: ${book.series}`]),
    ]),
  ]);

  // Status + dates + cost + tags/notes
  const form = el("div",{class:"row"},[]);
  form.appendChild(el("div",{class:"grid two"},[
    el("div",{},[
      el("label",{},["Status"]),
      (()=>{ 
        const s = el("select",{class:"input", id:"slug-status"},[
          el("option",{value:"Unfinished"},["Unfinished"]),
          el("option",{value:"Finished"},["Finished"]),
          el("option",{value:"Abandoned"},["Abandoned"]),
        ]);
        s.value = draft.status || "Unfinished";
        s.addEventListener("change", async ()=>{
          draft.status = s.value;
          // if finished/abandoned, set DateFinished to today by default
          if(draft.status !== "Unfinished" && !draft.dateFinished) draft.dateFinished = todayISO();
          if(draft.status === "Unfinished") draft.dateFinished = "";
          await saveDraft(draft);
          renderSlugSummary();
        });
        return s;
      })()
    ]),
    el("div",{},[
      el("label",{},["Cost (as hours-equivalent, e.g., 2.09)"]),
      (()=>{ 
        const i = el("input",{class:"input", id:"slug-cost", inputmode:"decimal", placeholder:"0 or 2.09", value: draft.cost_h ?? ""});
        i.addEventListener("input", async ()=>{
          draft.cost_h = i.value;
          await saveDraft(draft);
          renderSlugSummary();
        });
        return i;
      })()
    ]),
  ]));

  form.appendChild(el("div",{class:"grid two"},[
    el("div",{},[
      el("label",{},["DateStarted (YYYY-MM-DD)"]),
      (()=>{ 
        const i = el("input",{class:"input", id:"slug-ds", value: draft.dateStarted || todayISO()});
        i.addEventListener("input", async ()=>{
          draft.dateStarted = i.value.trim();
          await saveDraft(draft);
        });
        return i;
      })()
    ]),
    el("div",{},[
      el("label",{},["DateFinished (blank unless finished/abandoned)"]),
      (()=>{ 
        const i = el("input",{class:"input", id:"slug-df", value: draft.dateFinished || ""});
        i.addEventListener("input", async ()=>{
          draft.dateFinished = i.value.trim();
          await saveDraft(draft);
        });
        return i;
      })()
    ]),
  ]));

  form.appendChild(el("div",{class:"grid two"},[
    el("div",{},[
      el("label",{},["Tags (comma-separated)"]),
      (()=>{ 
        const i = el("input",{class:"input", id:"slug-tags", value: draft.tags || ""});
        i.addEventListener("input", async ()=>{
          draft.tags = i.value;
          await saveDraft(draft);
        });
        return i;
      })()
    ]),
    el("div",{},[
      el("label",{},["Notes"]),
      (()=>{ 
        const t = el("textarea",{class:"input", id:"slug-notes", rows:"2"},[draft.notes || ""]);
        t.addEventListener("input", async ()=>{
          draft.notes = t.value;
          await saveDraft(draft);
        });
        return t;
      })()
    ]),
  ]));

  view.appendChild(form);

  // Time ranges
  const rangesBox = el("div",{class:"row"},[
    el("div",{class:"hrow"},[
      el("h2",{},["Time Ranges"]),
      el("div",{class:"small"},["Enter as 25h17 or 25:17. Ranges are counters; not wall clock."])
    ]),
    el("div",{id:"ranges"},[])
  ]);

  const addBtn = el("button",{class:"btn", onClick: async ()=>{
    draft.timeRanges.push({start:null, end:null});
    await saveDraft(draft);
    renderRanges();
    toast("Range added.");
  }},["＋ Add Time Range"]);

  rangesBox.appendChild(el("div",{class:"actions"},[addBtn]));
  view.appendChild(rangesBox);

  // Summary + actions
  const summary = el("div",{class:"row"},[
    el("h2",{},["Validation summary"]),
    el("div",{id:"slug-summary", class:"small"},[""]),
    el("div",{class:"actions"},[
      el("button",{class:"btn primary", id:"btn-commit"},["Commit Entry"]),
      el("button",{class:"btn danger", onClick: async ()=>{
        if(!confirm("Remove from Reading list? Draft will be kept but marked inactive.")) return;
        draft.active=false;
        await saveDraft(draft);
        toast("Removed from Reading.");
        setRoute("reading");
      }},["Remove from Reading"])
    ])
  ]);
  view.appendChild(summary);

  $("#app").innerHTML="";
  $("#app").appendChild(view);

  function computeDurationHours(){
    let totalMin=0;
    let hasOpen=false;
    for(const r of draft.timeRanges){
      if(r.start===null) continue;
      if(r.end===null){ hasOpen=true; continue; }
      const diff = minutesDiff(r.start, r.end);
      if(diff!==null && diff>=0) totalMin += diff;
    }
    return {totalMin, hasOpen, durH: round4(totalMin/60)};
  }

  function renderRanges(){
    const box = $("#ranges");
    box.innerHTML="";
    draft.timeRanges.forEach((r, idx)=>{
      const startVal = (r.start===null) ? "" : fmtHhMm(r.start);
      const endVal = (r.end===null) ? "" : fmtHhMm(r.end);

      const startInput = el("input",{class:"input", value:startVal, placeholder:"25h17"});
      const endInput = el("input",{class:"input", value:endVal, placeholder:"28h59"});

      startInput.addEventListener("input", async ()=>{
        const pm = parseHhMm(startInput.value);
        r.start = pm;
        await saveDraft(draft);
        renderSlugSummary();
      });
      endInput.addEventListener("input", async ()=>{
        const pm = parseHhMm(endInput.value);
        r.end = pm;
        await saveDraft(draft);
        renderSlugSummary();
      });

      const delBtn = el("button",{class:"btn danger", onClick: async ()=>{
        draft.timeRanges.splice(idx,1);
        await saveDraft(draft);
        renderRanges();
        renderSlugSummary();
      }},["Delete"]);

      box.appendChild(el("div",{class:"rangeRow"},[
        el("div",{},[el("label",{},["Start"]), startInput]),
        el("div",{},[el("label",{},["End"]), endInput]),
        delBtn
      ]));
    });
    if(draft.timeRanges.length===0){
      box.appendChild(el("div",{class:"small"},["No ranges yet. Add one, then enter start/end."]));
    }
  }

  async function commitEntry(){
    const {durH, hasOpen} = computeDurationHours();
    if(hasOpen){ alert("You have an open range (missing end). Close it before committing."); return; }
    if(durH<=0){ alert("Duration is 0. Add at least one valid closed range before committing."); return; }
    const cost = parseFloat(draft.cost_h||"0");
    if(Number.isNaN(cost) || cost<0){ alert("Cost must be a non-negative number (e.g., 0 or 2.09)."); return; }

    const net = round4(durH - cost);
    const lastEdited = todayISO();

    // CompositeKey rule: default BookID; if collision, append _copyN
    const bookID = book.bookID;
    let compositeKey = bookID;
    // if you want stricter uniqueness: include dateStarted. But we keep your common pattern and let collisions become _copyN.
    if(await idbGet("entries", compositeKey)){
      let n=2;
      let ck=`${bookID}_copy${n}`;
      while(await idbGet("entries", ck)) { n++; ck=`${bookID}_copy${n}`; }
      compositeKey = ck;
    }

    // Derive Series/SeriesID from franchise/subseries (same as contract)
    const series = book.series || deriveSeries(book.franchise, book.subseries);
    const seriesID = book.seriesID || deriveSeriesId(book.franchise, book.subseries);

    // Build exact 22-column row object
    const row = {
      SchemaVersion: SCHEMA_VERSION,
      Title: book.title,
      Author: book.author,
      AuthorID: book.authorID || normalizeId(book.author),
      BookID: book.bookID,
      Format: book.format,
      Series: series,
      SeriesID: seriesID,
      Duration_h: durH.toFixed(4),
      Cost_h: String(cost),
      Net_h: String(net),
      Tags: draft.tags || "",
      Notes: draft.notes || "",
      Status: draft.status || "Unfinished",
      DateStarted: draft.dateStarted || todayISO(),
      DateFinished: (draft.status && draft.status!=="Unfinished") ? (draft.dateFinished || todayISO()) : "",
      DayDelta: "0",
      Scoreboard: "",
      LastEdited: lastEdited,
      CompositeKey: compositeKey,
      Franchise: book.franchise || "",
      Subseries: book.subseries || ""
    };

    await idbPut("entries", { compositeKey, bookKey: book.bookKey, row });
    toast(`Committed. Net ${net.toFixed(2)}h ${netIcon(net)}`);

    // If finished/abandoned, default to inactive (remove from Reading)
    if(row.Status !== "Unfinished"){
      draft.active=false;
    }
    // keep draft (append-only), but update lastEdited
    draft.lastEdited = lastEdited;
    await saveDraft(draft);
    setRoute("reports");
  }

  function renderSlugSummary(){
    const {durH, hasOpen, totalMin} = computeDurationHours();
    const cost = parseFloat(draft.cost_h||"0")||0;
    const net = round4(durH - cost);

    const lines = [
      `Duration: ${durH.toFixed(2)} h (${totalMin} minutes)`,
      `Cost:      ${cost.toFixed(2)} h`,
      `Net:       ${net.toFixed(2)} h ${netIcon(net)}`
    ];
    if(hasOpen) lines.unshift("⚠️ Open range detected (missing end). Commit disabled.");
    $("#slug-summary").textContent = lines.join("\n");

    const btn = $("#btn-commit");
    btn.disabled = hasOpen || durH<=0;
    btn.onclick = commitEntry;
  }

  async function saveDraft(d){
    d.lastEdited = todayISO();
    await idbPut("drafts", d);
  }

  renderRanges();
  renderSlugSummary();
}

// ----- App bootstrap -----
async function render(){
  const app = $("#app");
  app.innerHTML="";
  if(state.route==="library"){
    app.appendChild(await renderLibrary());
  } else if(state.route==="reading"){
    app.appendChild(await renderReading());
  } else if(state.route==="reports"){
    app.appendChild(await renderReports());
  } else if(state.route==="data"){
    app.appendChild(await renderData());
  }
  app.focus();
}

async function seedIfEmpty(){
  const meta = await idbGet("meta","seeded");
  if(meta) return;
  // no auto seed content, but mark seeded so we don't do this again
  await idbPut("meta",{key:"seeded", value:"1"});
}

async function main(){
  await idbOpen();
  await seedIfEmpty();

  // tabs
  for(const r of ROUTES){
    const b=document.getElementById(`tab-${r}`);
    b.addEventListener("click", ()=>setRoute(r));
  }

  // register service worker (offline)
  if("serviceWorker" in navigator){
    try{
      await navigator.serviceWorker.register("./sw.js");
    }catch(e){ /* ignore */ }
  }

  setRoute("library");
}

main();
