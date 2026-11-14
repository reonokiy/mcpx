import { chromium } from 'playwright';
import { readFile } from 'node:fs/promises';
import path from 'node:path';

async function render(svgPath, outputPath) {
    const absoluteSvgPath = path.resolve(svgPath);
    const svg = await readFile(absoluteSvgPath, 'utf8');
    const width = Number(svg.match(/<svg[^>]*\bwidth="(\d+(?:\.\d+)?)"/)?.[1]) || 1280;
    const height = Number(svg.match(/<svg[^>]*\bheight="(\d+(?:\.\d+)?)"/)?.[1]) || 640;

    const browser = await chromium.launch();
    try {
        const page = await browser.newPage({
            viewport: { width, height },
            deviceScaleFactor: 1,
        });
        await page.goto(`file://${absoluteSvgPath}`);
        await page.screenshot({ path: outputPath });
        console.log(`Successfully rendered: ${absoluteSvgPath} -> ${outputPath} (${width}x${height})`);
    } finally {
        await browser.close();
    }
}

const svgFile = process.argv[2] || 'docs/assets/banner.svg';
const outputFile = process.argv[3] || '/tmp/render_output.png';

render(svgFile, outputFile).catch((error) => {
    console.error(error);
    process.exitCode = 1;
});
