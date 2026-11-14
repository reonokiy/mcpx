import { transpile, writeFiles } from "@bytecodealliance/jco-transpile";
const profile=process.argv[2]??"debug";
if(!["debug","release"].includes(profile))throw new Error("Expected debug or release profile");
const { files } = await transpile(`target/wasm32-wasip2/${profile}/mcpx_component.wasm`, {
  outDir: "dist/component", name: "mcpx_component", emitTypescriptDeclarations: true,
});
await writeFiles(files);
