import { execFileSync } from "node:child_process";
import { readFile, rename, rm } from "node:fs/promises";
import { fileURLToPath } from "node:url";

const wasm = new URL("../dist/web/mcpx_web_bg.wasm", import.meta.url);
const temporary = new URL("../dist/web/mcpx_web_bg.optimized.wasm", import.meta.url);
const optimizer = fileURLToPath(new URL("bin/wasm-opt", import.meta.resolve("binaryen")));
try {
  // Preserve the input's feature set; --all-features can emit unsupported Wasm.
  execFileSync(process.execPath, [optimizer, fileURLToPath(wasm), "-Oz",
    "--strip-debug", "--strip-producers", "-o", fileURLToPath(temporary)], { stdio: "inherit" });
  if (!WebAssembly.validate(await readFile(temporary))) {
    throw new Error("Optimized Web Wasm is not supported by the build's Node runtime");
  }
  await rename(temporary, wasm);
} finally {
  await rm(temporary, { force: true });
}
