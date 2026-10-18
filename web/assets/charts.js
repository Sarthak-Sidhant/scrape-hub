// Small SVG chart kit for the hub's pages. Every chart: thin marks, one y-axis, recessive grid, a hover tooltip, and a table view
// (Hub.view(...)). Colours are CSS variables from hub.css, so light and dark both work.
const Hub = (() => {
  const esc = s => String(s ?? "").replace(/[&<>"]/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));
  const fmt = n => n == null || isNaN(n) ? "-" : Number(n).toLocaleString("en-IN", { maximumFractionDigits: 1 });
  const trim = x => String(+x.toFixed(x >= 100 ? 0 : x >= 10 ? 1 : 2));
  const compact = v => { const a = Math.abs(v); return a >= 1e6 ? trim(v / 1e6) + "M" : a >= 1e3 ? trim(v / 1e3) + "K" : trim(v) };
  const money = v => { const a = Math.abs(v); return a >= 1e7 ? (a >= 1e9 ? fmt(Math.round(v / 1e7)) : trim(v / 1e7)) + " Cr" : a >= 1e5 ? trim(v / 1e5) + " L" : a >= 1e3 ? trim(v / 1e3) + "K" : trim(v) };
  const inr = v => v == null ? "-" : "₹" + money(v);
  const pct = (v, d = 1) => v == null || isNaN(v) ? "-" : (v * 100).toFixed(d) + "%";
  const nice = m => { if (m <= 0) return 1; const p = Math.pow(10, Math.floor(Math.log10(m))), f = m / p; return (f <= 1 ? 1 : f <= 2 ? 2 : f <= 2.5 ? 2.5 : f <= 5 ? 5 : 10) * p };
  async function j(u) { const r = await fetch(u); if (r.status === 401) { location = "/login"; throw 0 } if (!r.ok) throw new Error(u + " " + r.status); return r.json() }

  // one tooltip per chart container; rows: [{color, value, label}] or strings
  function tip(el) {
    let t = el.querySelector(".tip");
    if (!t) { t = document.createElement("div"); t.className = "tip"; t.hidden = true; el.appendChild(t) }
    return {
      show(x, y, head, rows) {
        t.innerHTML = `<div class="th">${esc(head)}</div>` + rows.map(r => typeof r === "string" ? `<div class="tr"><span class="tn">${esc(r)}</span></div>` :
          `<div class="tr"><span class="key" style="background:${r.color || "transparent"}"></span><span class="tv">${esc(r.value)}</span><span class="tn">${esc(r.label)}</span></div>`).join("");
        t.hidden = false;
        const w = el.clientWidth, tw = t.offsetWidth, th = t.offsetHeight;
        t.style.left = Math.max(0, Math.min(w - tw, x + 14 > w - tw ? x - tw - 14 : x + 14)) + "px";
        t.style.top = Math.max(0, y - th - 8) + "px";
      }, hide() { t.hidden = true }
    };
  }
  const local = (el, ev, W) => { const r = el.getBoundingClientRect(); return { x: ev.clientX - r.left, y: ev.clientY - r.top, sx: (ev.clientX - r.left) * W / r.width } };
  function legend(series) { return `<div class="lg">${series.map(s => `<span><i style="background:var(${s.color})"></i>${esc(s.label)}</span>`).join("")}</div>` }
  // show every k-th x label so labels never overlap (about 6.5 px per character at 10 px monospace, plus a gap)
  const tickEvery = (labels, pw) => { const w = Math.max(...labels.map(l => String(l).length)) * 6.5 + 14; return Math.max(1, Math.ceil(labels.length / Math.max(1, Math.floor(pw / w)))) };
  const roundedTop = (x, y, w, h, r) => { r = Math.min(r, w / 2, h); return `M${x},${y + h}V${y + r}Q${x},${y} ${x + r},${y}H${x + w - r}Q${x + w},${y} ${x + w},${y + r}V${y + h}Z` };

  // stacked (or single) vertical bars. o: {labels[], series[{key,label,color}], values: (i,key)=>n, yFmt, height, share:false}
  function bars(el, o) {
    const W = Math.max(320, el.clientWidth || 600), H = o.height || 240, L = 52, R = 8, T = 10, B = 24, pw = W - L - R, ph = H - T - B, n = o.labels.length;
    const tot = o.labels.map((_, i) => o.series.reduce((a, s) => a + (o.values(i, s.key) || 0), 0));
    const max = o.share ? 1 : nice(Math.max(...tot, 1)), y = v => T + ph - v / max * ph, slot = pw / n, bw = Math.max(2, Math.min(34, slot * 0.72));
    let g = `<svg viewBox="0 0 ${W} ${H}" role="img" aria-label="${esc(o.aria || "bar chart")}">`;
    [0, .25, .5, .75, 1].forEach(f => { const yy = Math.round(y(max * f)) + .5; g += `<line x1="${L}" x2="${W - R}" y1="${yy}" y2="${yy}" stroke="var(--line)" ${f ? 'stroke-dasharray="2 3"' : ''}/><text x="${L - 6}" y="${yy + 3}" text-anchor="end" class="ax">${o.share ? Math.round(f * 100) + "%" : (o.yFmt || compact)(max * f)}</text>` });
    o.labels.forEach((lab, i) => {
      const x = L + i * slot + (slot - bw) / 2, t = tot[i] || 1; let cum = 0;
      const vis = o.series.filter(s => (o.values(i, s.key) || 0) > 0), top = vis[vis.length - 1];
      vis.forEach((s, k) => { const v = (o.values(i, s.key) || 0) / (o.share ? t : 1), y0 = y(cum), y1 = y(cum + v), h = Math.max(1, y0 - y1 - (k ? 2 : 0)); cum += v;
        g += s === top ? `<path d="${roundedTop(x, y1, bw, h, 4)}" fill="var(${s.color})"/>` : `<rect x="${x}" y="${y1}" width="${bw}" height="${h}" fill="var(${s.color})"/>` });
    });
    const every = tickEvery(o.labels, pw);
    o.labels.forEach((lab, i) => { if (i % every === 0) g += `<text x="${L + i * slot + slot / 2}" y="${H - 6}" text-anchor="middle" class="ax">${esc(lab)}</text>` });
    g += `<rect class="hit" x="${L}" y="${T}" width="${pw}" height="${ph}" fill="transparent"/></svg>`;
    el.innerHTML = g + (o.series.length > 1 ? legend(o.series) : "");
    const svg = el.querySelector("svg"), tp = tip(el);
    svg.querySelector(".hit").addEventListener("pointermove", ev => {
      const p = local(svg, ev, W), i = Math.max(0, Math.min(n - 1, Math.floor((p.sx - L) / slot)));
      const rows = o.series.slice().reverse().map(s => ({ color: `var(${s.color})`, value: o.share ? pct((o.values(i, s.key) || 0) / (tot[i] || 1)) : (o.tipFmt || fmt)(o.values(i, s.key) || 0), label: s.label }));
      if (o.series.length > 1 && !o.share) rows.push({ value: (o.tipFmt || fmt)(tot[i]), label: "total" });
      tp.show(p.x, p.y, (o.tipHead || (l => l))(o.labels[i], i), rows.concat(o.tipExtra ? o.tipExtra(i) : []));
    });
    svg.querySelector(".hit").addEventListener("pointerleave", () => tp.hide());
  }

  // lines over shared x labels. o: {labels[], series[{label,color,values[]}], yFmt, height, yMax}
  function lines(el, o) {
    const W = Math.max(320, el.clientWidth || 600), H = o.height || 220, L = 52, R = 12, T = 12, B = 24, pw = W - L - R, ph = H - T - B, n = o.labels.length;
    const all = o.series.flatMap(s => s.values).filter(v => v != null), max = o.yMax || nice(Math.max(...all, 0) * 1.05 || 1);
    const x = i => L + (n === 1 ? pw / 2 : i / (n - 1) * pw), y = v => T + ph - v / max * ph;
    let g = `<svg viewBox="0 0 ${W} ${H}" role="img" aria-label="${esc(o.aria || "line chart")}">`;
    [0, .25, .5, .75, 1].forEach(f => { const yy = Math.round(y(max * f)) + .5; g += `<line x1="${L}" x2="${W - R}" y1="${yy}" y2="${yy}" stroke="var(--line)" ${f ? 'stroke-dasharray="2 3"' : ''}/><text x="${L - 6}" y="${yy + 3}" text-anchor="end" class="ax">${(o.yFmt || compact)(max * f)}</text>` });
    o.series.forEach(s => {
      let d = "", pen = false;
      s.values.forEach((v, i) => { if (v == null) { pen = false; return } d += (pen ? "L" : "M") + x(i).toFixed(1) + "," + y(v).toFixed(1); pen = true });
      g += `<path d="${d}" fill="none" stroke="var(${s.color})" stroke-width="2" stroke-linejoin="round" stroke-linecap="round"/>`;
      if (n <= 24) s.values.forEach((v, i) => { if (v != null) g += `<circle cx="${x(i)}" cy="${y(v)}" r="3" fill="var(${s.color})" stroke="var(--panel)" stroke-width="2"/>` });
    });
    const every = tickEvery(o.labels, pw);
    o.labels.forEach((lab, i) => { if (i % every === 0) g += `<text x="${x(i)}" y="${H - 6}" text-anchor="middle" class="ax">${esc(lab)}</text>` });
    g += `<line class="xh" x1="0" x2="0" y1="${T}" y2="${T + ph}" stroke="var(--dim)" visibility="hidden"/><rect class="hit" x="${L - 6}" y="${T}" width="${pw + 12}" height="${ph}" fill="transparent"/></svg>`;
    el.innerHTML = g + (o.series.length > 1 ? legend(o.series) : "");
    const svg = el.querySelector("svg"), tp = tip(el), xh = svg.querySelector(".xh");
    svg.querySelector(".hit").addEventListener("pointermove", ev => {
      const p = local(svg, ev, W), i = Math.max(0, Math.min(n - 1, Math.round((p.sx - L) / (pw / Math.max(1, n - 1)))));
      xh.setAttribute("x1", x(i)); xh.setAttribute("x2", x(i)); xh.setAttribute("visibility", "visible");
      tp.show(p.x, p.y, o.labels[i], o.series.map(s => ({ color: `var(${s.color})`, value: s.values[i] == null ? "-" : (o.tipFmt || o.yFmt || fmt)(s.values[i]), label: s.label })).concat(o.tipExtra ? o.tipExtra(i) : []));
    });
    svg.querySelector(".hit").addEventListener("pointerleave", () => { tp.hide(); xh.setAttribute("visibility", "hidden") });
  }

  // heatmap: o {rows[], cols[], value:(r,c)=>n|null, fmt, head:(r,c)=>string, extra:(r,c)=>[rows]}
  function heat(el, o) {
    const W = Math.max(320, el.clientWidth || 600), L = 46, T = 18, R = 4, cw = (W - L - R) / o.cols.length, ch = Math.max(14, Math.min(22, cw * 0.7)), H = T + o.rows.length * ch + 4;
    const vals = []; o.rows.forEach((_, r) => o.cols.forEach((_, c) => { const v = o.value(r, c); if (v != null) vals.push(v) }));
    const max = Math.max(...vals, 1), steps = ["--q1", "--q2", "--q3", "--q4", "--q5", "--q6"];
    let g = `<svg viewBox="0 0 ${W} ${H}" role="img" aria-label="${esc(o.aria || "heatmap")}">`;
    o.cols.forEach((c, i) => g += `<text x="${L + i * cw + cw / 2}" y="12" text-anchor="middle" class="ax">${esc(c)}</text>`);
    o.rows.forEach((r, ri) => {
      g += `<text x="${L - 6}" y="${T + ri * ch + ch / 2 + 3}" text-anchor="end" class="ax">${esc(r)}</text>`;
      o.cols.forEach((c, ci) => { const v = o.value(ri, ci); if (v == null) return;
        const k = Math.min(5, Math.floor(Math.sqrt(v / max) * 6));
        g += `<rect data-r="${ri}" data-c="${ci}" x="${(L + ci * cw + 1).toFixed(1)}" y="${T + ri * ch + 1}" width="${(cw - 2).toFixed(1)}" height="${ch - 2}" rx="2" fill="var(${steps[k]})"/>` });
    });
    el.innerHTML = g + "</svg>" + `<div class="lg"><span class="dim">fewer</span>${steps.map(s => `<i style="background:var(${s});width:18px"></i>`).join("")}<span class="dim">more</span></div>`;
    const svg = el.querySelector("svg"), tp = tip(el);
    svg.addEventListener("pointermove", ev => { const r = ev.target.closest("rect[data-r]"); if (!r) return tp.hide();
      const ri = +r.dataset.r, ci = +r.dataset.c, p = local(svg, ev, W); tp.show(p.x, p.y, o.head(ri, ci), [{ value: (o.fmt || fmt)(o.value(ri, ci)), label: o.unit || "" }].concat(o.extra ? o.extra(ri, ci) : [])) });
    svg.addEventListener("pointerleave", () => tp.hide());
  }

  // squarified treemap. items [{label, sub, value, color, tip:[rows], head}]
  function treemap(el, items, o = {}) {
    const W = Math.max(320, el.clientWidth || 600), H = o.height || Math.round(Math.min(420, Math.max(260, W * 0.42)));
    const tot = items.reduce((a, b) => a + b.value, 0) || 1, area = W * H, rects = [];
    const data = items.map(it => ({ it, a: it.value / tot * area })).filter(d => d.a > 0);
    let box = { x: 0, y: 0, w: W, h: H }, row = [];
    const worst = (r, side) => { const s = r.reduce((a, b) => a + b.a, 0), mx = Math.max(...r.map(d => d.a)), mn = Math.min(...r.map(d => d.a)); return Math.max(side * side * mx / (s * s), s * s / (side * side * mn)) };
    const layout = r => { const s = r.reduce((a, b) => a + b.a, 0);
      if (box.w >= box.h) { const w = s / box.h; let y = box.y; r.forEach(d => { const h = d.a / w; rects.push({ ...d, x: box.x, y, w, h }); y += h }); box = { x: box.x + w, y: box.y, w: box.w - w, h: box.h } }
      else { const h = s / box.w; let x = box.x; r.forEach(d => { const w = d.a / h; rects.push({ ...d, x, y: box.y, w, h }); x += w }); box = { x: box.x, y: box.y + h, w: box.w, h: box.h - h } } };
    data.forEach(d => { const side = Math.min(box.w, box.h); if (!row.length || worst(row.concat(d), side) <= worst(row, side)) row.push(d); else { layout(row); row = [d] } });
    if (row.length) layout(row);
    let g = `<svg viewBox="0 0 ${W} ${H}" role="img" aria-label="${esc(o.aria || "treemap")}">`;
    rects.forEach((r, i) => {
      const x = r.x + 1, y = r.y + 1, w = Math.max(0, r.w - 2), h = Math.max(0, r.h - 2);
      g += `<rect data-i="${i}" x="${x.toFixed(1)}" y="${y.toFixed(1)}" width="${w.toFixed(1)}" height="${h.toFixed(1)}" rx="4" fill="${r.it.color}"/>`;
      if (w > 54 && h > 26) { const fs = Math.max(10, Math.min(15, Math.sqrt(w * h) / 9));
        g += `<text x="${x + 6}" y="${y + fs + 3}" font-size="${fs}" font-weight="700" fill="#0b0f14" pointer-events="none">${esc(r.it.label.length * fs * .62 > w - 10 ? r.it.label.slice(0, Math.max(3, Math.floor((w - 10) / (fs * .62)))) + "…" : r.it.label)}</text>`;
        if (h > fs * 2 + 14) g += `<text x="${x + 6}" y="${y + fs * 2 + 7}" font-size="${Math.max(9, fs * .78)}" fill="#0b0f14" fill-opacity=".8" pointer-events="none">${esc(r.it.sub || "")}</text>` }
    });
    el.innerHTML = g + "</svg>";
    const svg = el.querySelector("svg"), tp = tip(el);
    svg.addEventListener("pointermove", ev => { const t = ev.target.closest("rect[data-i]"); if (!t) return tp.hide(); const r = rects[+t.dataset.i], p = local(svg, ev, W); tp.show(p.x, p.y, r.it.head || r.it.label, r.it.tip || []) });
    svg.addEventListener("pointerleave", () => tp.hide());
    svg.addEventListener("click", ev => { const t = ev.target.closest("rect[data-i]"); if (t && o.onClick) o.onClick(rects[+t.dataset.i].it) });
  }

  // chart / table switch for a section: draw() renders the chart into el, table() returns table HTML
  function view(section, el, draw, table) {
    let mode = "chart";
    const bar = section.querySelector(".seg.view");
    const render = () => { if (mode === "chart") draw(); else el.innerHTML = `<div class="tblwrap">${table()}</div>` };
    if (bar) { bar.innerHTML = `<button aria-pressed="true" data-m="chart">Chart</button><button aria-pressed="false" data-m="table">Table</button>`;
      bar.addEventListener("click", e => { const b = e.target.closest("[data-m]"); if (!b) return; mode = b.dataset.m; bar.querySelectorAll("button").forEach(x => x.setAttribute("aria-pressed", x.dataset.m === mode)); render() }) }
    render();
    let rz; addEventListener("resize", () => { clearTimeout(rz); rz = setTimeout(() => { if (mode === "chart") draw() }, 150) });
  }
  const ago = t => { if (!t) return "-"; const s = Math.max(0, (Date.now() - new Date(t)) / 1000); return s < 90 ? Math.round(s) + "s" : s < 5400 ? Math.round(s / 60) + "m" : s < 172800 ? Math.round(s / 3600) + "h" : Math.round(s / 86400) + "d" };
  return { esc, fmt, compact, money, inr, pct, nice, j, tip, bars, lines, heat, treemap, view, ago };
})();
