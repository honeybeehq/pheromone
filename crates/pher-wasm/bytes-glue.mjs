import { readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";

const [pkgDir, version, bindgenVersion] = process.argv.slice(2);

function find(source, marker, file, from = 0) {
  const at = source.indexOf(marker, from);
  if (at < 0) {
    throw new Error(`${file}: wasm-bindgen output changed shape; cannot find ${JSON.stringify(marker)}`);
  }
  return at;
}

function cutBetween(source, start, end, file) {
  const from = find(source, start, file);
  const to = find(source, end, file, from);
  return source.slice(0, from) + source.slice(to);
}

function cutToEnd(source, start, file) {
  return source.slice(0, find(source, start, file)).trimEnd() + "\n";
}

const asyncExport = "export { initSync, __wbg_init as default };";
const gluePath = join(pkgDir, "pher_wasm.js");
let glue = readFileSync(gluePath, "utf8");
glue = cutBetween(glue, "async function __wbg_load(", "function initSync(", gluePath);
glue = cutBetween(glue, "async function __wbg_init(", asyncExport, gluePath);
glue = glue.slice(0, find(glue, asyncExport, gluePath)) + "export { initSync };\n";
writeFileSync(gluePath, glue);

const typesPath = join(pkgDir, "pher_wasm.d.ts");
let types = readFileSync(typesPath, "utf8");
types = cutBetween(types, "export type InitInput =", "export interface InitOutput", typesPath);
types = cutToEnd(types, "/**\n * If `module_or_path`", typesPath);
writeFileSync(typesPath, types);

const manifest = {
  name: "@pheromone/wasm",
  version,
  private: true,
  description: "Pheromone tiers 1-2 matcher as WebAssembly; instantiate from bytes with initSync({ module })",
  type: "module",
  main: "pher_wasm.js",
  types: "pher_wasm.d.ts",
  files: ["pher_wasm.js", "pher_wasm.d.ts", "pher_wasm_bg.wasm", "pher_wasm_bg.wasm.d.ts"],
  wasmBindgen: bindgenVersion,
  sideEffects: false,
};
writeFileSync(join(pkgDir, "package.json"), JSON.stringify(manifest, null, 2) + "\n");
