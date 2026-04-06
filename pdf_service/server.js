// DizelFinance PDF Service — Node.js + Puppeteer
// Запуск: node server.js
// PORT: 3001

const express    = require("express");
const puppeteer  = require("puppeteer");
const path       = require("path");
const fs         = require("fs");

const app  = express();
const PORT = process.env.PDF_PORT || 3001;

app.use(express.json({ limit: "10mb" }));

// ── Роуты ─────────────────────────────────────────────────────────────────────

app.get("/health", (req, res) => res.json({ status: "ok" }));

/**
 * POST /generate
 * Body: { template: "monthly"|"quarterly"|"yearly"|"networth"|"comparative", data: {...} }
 * Returns: PDF binary
 */
app.post("/generate", async (req, res) => {
  const { template, data } = req.body;

  if (!template || !data) {
    return res.status(400).json({ error: "Missing template or data" });
  }

  const templateFile = path.join(__dirname, "templates", `${template}.html`);
  if (!fs.existsSync(templateFile)) {
    return res.status(404).json({ error: `Template ${template} not found` });
  }

  try {
    const html = buildHtml(templateFile, data);
    const pdf  = await renderPdf(html);
    res.set({
      "Content-Type":        "application/pdf",
      "Content-Disposition": `attachment; filename="dizelfinance_${template}.pdf"`,
    });
    res.send(pdf);
  } catch (err) {
    console.error("PDF generation error:", err);
    res.status(500).json({ error: err.message });
  }
});

// ── Helpers ───────────────────────────────────────────────────────────────────

function buildHtml(templateFile, data) {
  let html = fs.readFileSync(templateFile, "utf-8");
  // Вставляем данные как JSON в window.__DATA__
  const dataScript = `<script>window.__DATA__ = ${JSON.stringify(data)};</script>`;
  html = html.replace("</head>", `${dataScript}\n</head>`);
  return html;
}

async function renderPdf(html) {
  const browser = await puppeteer.launch({
    headless: "new",
    args: [
      "--no-sandbox",
      "--disable-setuid-sandbox",
      "--disable-dev-shm-usage",
    ],
  });
  const page = await browser.newPage();
  await page.setContent(html, { waitUntil: "networkidle0" });

  // Ждём пока отрендерятся Chart.js графики
  await page.waitForTimeout(1000);

  const pdf = await page.pdf({
    format:            "A4",
    printBackground:   true,
    margin:            { top: "20mm", right: "16mm", bottom: "20mm", left: "16mm" },
  });
  await browser.close();
  return pdf;
}

app.listen(PORT, () => console.log(`📄 PDF Service → http://localhost:${PORT}`));
