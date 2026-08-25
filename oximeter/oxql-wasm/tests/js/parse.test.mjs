import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import path from "node:path";
import test from "node:test";
import { pathToFileURL } from "node:url";

const outputDirectory = process.env.OXQL_WASM_OUTPUT;
assert.ok(outputDirectory, "OXQL_WASM_OUTPUT must name the wasm-pack output directory");

const bindings = await import(pathToFileURL(path.join(outputDirectory, "oxql.js")));
const wasm = await readFile(path.join(outputDirectory, "oxql_bg.wasm"));
await bindings.default({ module_or_path: wasm });

const corpusPath = new URL("../../../oxql/test-data/query-corpus.json", import.meta.url);
const corpus = JSON.parse(await readFile(corpusPath, "utf8"));

for (const entry of corpus) {
  test(entry.name, () => {
    const result = bindings.parse(entry.query);
    assert.equal(result.diagnostics.length === 0, entry.accepted);
  });
}

test("returns referenced timeseries", () => {
  const result = bindings.parse("{ get vm:memory_used; get vm:cpu_busy } | join");
  assert.deepEqual(result.referencedTimeseries, ["vm:cpu_busy", "vm:memory_used"]);
});

test("returns structured length diagnostics", () => {
  const result = bindings.parse(`get a:b${" | last 1".repeat(4096)}`);
  assert.equal(result.diagnostics[0].code, "query-too-long");
  assert.equal(result.diagnostics[0].phase, "parse");
});
