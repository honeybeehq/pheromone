# pher-wasm size and memory budget

CI step `pher-wasm size and memory budget` runs `node --expose-gc budget.mjs` after
`build.sh`. It prints every number below and fails when any gate is exceeded. RSS is
`process.memoryUsage().rss` after three forced GCs. MB means 1,000,000 bytes.

| Gate | Budget | Measured from → to |
| --- | --- | --- |
| Module size | ≤ 1.5 MB | `pkg/pher_wasm_bg.wasm` on disk |
| Total RSS growth | ≤ 24 MB | glue loaded, module not instantiated → instantiated, 256 warm-up rules plus 4,096 distinct rules parsed, `evaluate` once per rule against one envelope |
| Marginal RSS growth | ≤ 8 MB | after instantiation and the 256-rule warm-up → the 4,096 rules parsed and evaluated |

The total gate covers what a host pays to embed the matcher. The marginal gate catches
per-rule state growth. A mutation that leaks the parsed subscription plus 2 KB per
`parse` measured +15.1 MB marginal and +30.1 MB total (macOS), so it failed both gates.
The job also prints WASM linear-memory growth over the 4,096 rules, the exact figure
for state held inside the module. It is 0 bytes today.

These are revisions of the design's proposed budgets (module ≤ 1.5 MB, RSS ≤ 8 MB at
4,096 rules; Apiary `docs/design/event-bus/design.md` §12.1). The first proposal
measured the total against 8 MB, which V8's one-time compile cost alone exceeds.

## Measurements (Node 24, binaryen 132, wasm-bindgen 0.2.127)

| | Linux CI (ubuntu-24.04) | macOS (arm64) |
| --- | --- | --- |
| Module, before the shrink | 1.558 MB | 1.558 MB |
| Module, now | 0.992 MB | 0.993 MB |
| Instantiate | +2.5 MB | +2.4 MB |
| 256-rule warm-up | +9.7 MB | +12.0 MB |
| 4,096 rules (marginal) | +4.6 MB | +4.9 to 5.3 MB |
| Total | 15.3 to 17.4 MB | 19.3 to 20.7 MB |

The module shrinks through the `wasm-release` cargo profile (opt-level `z`, fat LTO, one
codegen unit, `panic = "abort"`) plus `wasm-opt -Oz`. binaryen is pinned with an integrity
hash in `package-lock.json` and installed by `npm ci`. The largest single part left is
the `regex` crate's Unicode tables (about 0.3 MB of data), kept so `matches` behaves
the same as native.

## Why most of the growth is a fixed V8 cost

- WASM linear memory stays at 1.5 MB. It grows 0 bytes across the 4,096 measured rules.
- `evaluate` parses every rule again, yet the evaluate pass adds only about 0.3 to
  0.7 MB.
- 4,096 copies of one rule cost the same as 4,096 distinct rules.
- 64 rules already cost about 10 MB (macOS).
- Compiling the whole module eagerly (`--no-wasm-lazy-compilation`) costs 36 MB, and
  `--liftoff-only` costs 14 MB. The growth is compiled code plus tier-up, and it scales
  with code size, not with rule count.
- Tier-up continues past 64 calls. On macOS, the marginal growth for 4,096 rules is
  about 9 MB after a 64-rule warm-up, 5.2 MB after 256, and 3.1 MB after 1,024. The
  harness uses 256.
- The marginal number includes the 4,096 parse results the harness keeps as JS strings
  (about 1.25 MB of characters). Without keeping them, it falls by about 1.7 MB.
