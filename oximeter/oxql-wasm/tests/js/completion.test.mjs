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

const corpusPath = new URL("../../../oxql/test-data/completion-corpus.json", import.meta.url);
const corpus = JSON.parse(await readFile(corpusPath, "utf8"));

for (const entry of corpus) {
  test(`completion: ${entry.source}`, () => {
    const result = bindings.completionContext(entry.source, entry.source.length);
    assert.equal(result.site, entry.expectedSite);
    assert.deepEqual(result.referencedTimeseries, entry.expectedNames);
  });
}

test("completion offsets use JavaScript string positions", () => {
  const source = 'get a:b | filter name == "é" || sl';
  const result = bindings.completionContext(source, source.length);
  assert.equal(result.site, "filter-identifier");
  assert.equal(source.slice(result.replacement.start, result.replacement.end), "sl");
});
