// Emits the frozen logo assets from the same recipe as lab.html.
// Usage: node generate.mjs   (writes pher-mark.svg and pher-favicon.svg here)
import { writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

function trail({ n = 5, r0 = 40, ratio = 0.72, gap = 0.25, Rc = 150, startDeg = 195 }) {
  const radii = Array.from({ length: n }, (_, i) => r0 * Math.pow(ratio, i));
  const pts = [];
  let th = startDeg * Math.PI / 180;
  pts.push([Rc * Math.cos(th), Rc * Math.sin(th)]);
  for (let i = 0; i < n - 1; i++) {
    const d = (radii[i] + radii[i + 1]) * (1 + gap);
    th += 2 * Math.asin(d / (2 * Rc));
    pts.push([Rc * Math.cos(th), Rc * Math.sin(th)]);
  }
  let x0 = Infinity, y0 = Infinity, x1 = -Infinity, y1 = -Infinity;
  pts.forEach(([x, y], i) => {
    x0 = Math.min(x0, x - radii[i]); y0 = Math.min(y0, y - radii[i]);
    x1 = Math.max(x1, x + radii[i]); y1 = Math.max(y1, y + radii[i]);
  });
  const pad = 0.08 * Math.max(x1 - x0, y1 - y0);
  const vb = [x0 - pad, y0 - pad, (x1 - x0) + 2 * pad, (y1 - y0) + 2 * pad]
    .map(v => v.toFixed(2)).join(" ");
  const circles = pts.map(([x, y], i) =>
    `  <circle cx="${x.toFixed(2)}" cy="${y.toFixed(2)}" r="${radii[i].toFixed(2)}"/>`
  ).join("\n");
  return `<svg viewBox="${vb}" fill="currentColor" style="color:#000"\n` +
         `     xmlns="http://www.w3.org/2000/svg">\n${circles}\n</svg>\n`;
}

const here = dirname(fileURLToPath(import.meta.url));
writeFileSync(join(here, "pher-mark.svg"), trail({}));
writeFileSync(join(here, "pher-favicon.svg"),
  trail({ n: 3, ratio: 0.68, Rc: 110, startDeg: 200 }));
writeFileSync(join(here, "pher-favicon-amber.svg"),
  trail({ n: 3, ratio: 0.68, Rc: 110, startDeg: 200 })
    .replace("color:#000", "color:#e8a33d"));
console.log("wrote pher-mark.svg, pher-favicon.svg, pher-favicon-amber.svg");
