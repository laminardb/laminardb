#!/usr/bin/env node
/**
 * build-index.mjs — LaminarDB RAG indexer
 *
 * Fetches your GitHub repo, chunks source files intelligently, embeds
 * each chunk via Cloudflare Workers AI, and upserts into Vectorize.
 *
 * Usage:
 *   node build-index.mjs              # incremental (skip unchanged files)
 *   node build-index.mjs --full       # wipe index and reindex everything
 *
 * Required env vars:
 *   CF_ACCOUNT_ID      — Cloudflare account ID (dashboard → right sidebar)
 *   CF_API_TOKEN       — Cloudflare API token (needs: Vectorize + Workers AI)
 *
 * Optional:
 *   GITHUB_TOKEN       — 5000 req/hr instead of 60
 */

import { createHash } from "crypto";
import { writeFileSync, readFileSync, existsSync } from "fs";

// ── CONFIG ────────────────────────────────────────────────────────────────────
const REPO_OWNER   = "laminardb";
const REPO_NAME    = "laminardb";
const BRANCH       = "main";
const INDEX_NAME   = "laminardb-docs";

const CHUNK_TARGET_CHARS = 600;   // target chunk size
const CHUNK_OVERLAP_CHARS = 80;   // overlap between consecutive chunks
const EMBED_BATCH_SIZE = 50;      // texts per CF AI embedding call (max 100)
const UPSERT_BATCH_SIZE = 200;    // vectors per Vectorize upsert call

// Files always indexed, in priority order
const PRIORITY_FILES = [
  "README.md",
  "ARCHITECTURE.md",
  "docs/sql-syntax.md",
  "docs/windows.md",
  "docs/joins.md",
  "docs/connectors.md",
  "docs/checkpointing.md",
  "docs/python.md",
  "docs/getting-started.md",
];

// Rust source directories to index (others are skipped)
const RUST_INCLUDE_PREFIXES = [
  "src/sql/",
  "src/window/",
  "src/join/",
  "src/connector/",
  "src/sink/",
  "src/checkpoint/",
  "src/engine/",
  "src/ring/",
  "src/source/",
  "src/",
  "laminardb-python/src/",
];

const SKIP_PATTERNS = [
  /^target\//,  /\/target\//,
  /\.lock$/,    /^\.github\//,
  /\/tests?\//i, /\/benches?\//i,
  /\/fixtures?\//i, /build\.rs$/,
  /\/examples?\//i,
];
// ─────────────────────────────────────────────────────────────────────────────

const CF_ACCOUNT_ID = process.env.CF_ACCOUNT_ID;
const CF_API_TOKEN  = process.env.CF_API_TOKEN;
const GITHUB_TOKEN  = process.env.GITHUB_TOKEN;
const FULL_REINDEX  = process.argv.includes("--full");

if (!CF_ACCOUNT_ID || !CF_API_TOKEN) {
  console.error("✗ Missing CF_ACCOUNT_ID or CF_API_TOKEN env vars");
  console.error("  export CF_ACCOUNT_ID=... CF_API_TOKEN=...");
  process.exit(1);
}

const CF_BASE = `https://api.cloudflare.com/client/v4/accounts/${CF_ACCOUNT_ID}`;
const GH_BASE = "https://api.github.com";

// ── GitHub helpers ────────────────────────────────────────────────────────────

function ghHeaders() {
  const h = {
    "Accept": "application/vnd.github+json",
    "User-Agent": "laminardb-rag-indexer",
  };
  if (GITHUB_TOKEN) h["Authorization"] = `Bearer ${GITHUB_TOKEN}`;
  return h;
}

async function ghFetch(path) {
  const res = await fetch(`${GH_BASE}${path}`, { headers: ghHeaders() });
  if (!res.ok) {
    const rem = res.headers.get("x-ratelimit-remaining");
    if (rem === "0") throw new Error(
      `GitHub rate limit hit. Set GITHUB_TOKEN for 5000 req/hr.`
    );
    throw new Error(`GitHub API ${res.status} on ${path}`);
  }
  return res.json();
}

async function fetchTree() {
  const data = await ghFetch(
    `/repos/${REPO_OWNER}/${REPO_NAME}/git/trees/${BRANCH}?recursive=1`
  );
  if (data.truncated) console.warn("⚠ GitHub tree truncated — very large repo");
  return data.tree.filter(f => f.type === "blob").map(f => ({
    path: f.path,
    sha: f.sha,
    size: f.size,
  }));
}

async function fetchFileContent(path) {
  const data = await ghFetch(
    `/repos/${REPO_OWNER}/${REPO_NAME}/contents/${path}?ref=${BRANCH}`
  );
  if (data.encoding === "base64") {
    return Buffer.from(data.content.replace(/\n/g, ""), "base64").toString("utf-8");
  }
  return data.content ?? "";
}

// ── Chunking ──────────────────────────────────────────────────────────────────

/**
 * Chunk a markdown file at heading and paragraph boundaries.
 * Each chunk inherits the heading context it lives under.
 */
function chunkMarkdown(path, content) {
  const chunks = [];
  const lines = content.split("\n");
  let currentHeading = "";
  let buffer = [];

  function flush() {
    const text = buffer.join("\n").trim();
    if (text.length < 40) { buffer = []; return; }

    // If over target, split on double-newlines
    if (text.length <= CHUNK_TARGET_CHARS) {
      chunks.push({ text, heading: currentHeading });
      buffer = [];
      return;
    }

    const paragraphs = text.split(/\n{2,}/);
    let current = "";
    for (const para of paragraphs) {
      if (current.length + para.length > CHUNK_TARGET_CHARS && current.length > 0) {
        chunks.push({ text: current.trim(), heading: currentHeading });
        // Overlap: keep last paragraph as start of next
        current = current.split(/\n{2,}/).slice(-1)[0] + "\n\n" + para;
      } else {
        current = current ? current + "\n\n" + para : para;
      }
    }
    if (current.trim().length > 40) {
      chunks.push({ text: current.trim(), heading: currentHeading });
    }
    buffer = [];
  }

  for (const line of lines) {
    const headingMatch = line.match(/^(#{1,3})\s+(.+)/);
    if (headingMatch) {
      flush();
      currentHeading = headingMatch[2].trim();
      buffer.push(line);
    } else {
      buffer.push(line);
      if (buffer.join("\n").length > CHUNK_TARGET_CHARS * 1.5) {
        flush();
      }
    }
  }
  flush();

  return chunks.map((c, i) => ({
    ...c,
    file: path,
    type: "md",
    chunkIdx: i,
  }));
}

/**
 * Chunk a Rust file at function/impl/struct/enum boundaries.
 * Falls back to line-count splitting for files without clear boundaries.
 */
function chunkRust(path, content) {
  // Strip plain // comments, keep doc comments
  const cleaned = content
    .split("\n")
    .filter(line => {
      const t = line.trimStart();
      if (t.startsWith("///") || t.startsWith("//!")) return true;
      if (t.startsWith("//")) return false;
      return true;
    })
    .join("\n")
    .replace(/\n{3,}/g, "\n\n");

  const chunks = [];

  // Split at top-level item boundaries
  const itemBoundary = /^(pub\s+)?(fn|impl|struct|enum|trait|type|const|static|mod)\b/m;
  const lines = cleaned.split("\n");

  let buffer = [];
  let currentItem = "";
  let braceDepth = 0;

  function flushBuffer() {
    const text = buffer.join("\n").trim();
    if (text.length < 60) { buffer = []; return; }

    if (text.length <= CHUNK_TARGET_CHARS + CHUNK_OVERLAP_CHARS) {
      chunks.push({ text, item: currentItem });
    } else {
      // Split large items by line groups
      const subLines = text.split("\n");
      let sub = "";
      for (const line of subLines) {
        if (sub.length + line.length > CHUNK_TARGET_CHARS && sub.length > 0) {
          chunks.push({ text: sub.trim(), item: currentItem });
          // Overlap: last few lines
          const prevLines = sub.split("\n").slice(-3).join("\n");
          sub = prevLines + "\n" + line;
        } else {
          sub = sub ? sub + "\n" + line : line;
        }
      }
      if (sub.trim().length > 60) chunks.push({ text: sub.trim(), item: currentItem });
    }
    buffer = [];
  }

  for (const line of lines) {
    // Track brace depth to detect top-level boundaries
    for (const ch of line) {
      if (ch === "{") braceDepth++;
      else if (ch === "}") braceDepth--;
    }

    const isTopLevel = braceDepth <= 1 && itemBoundary.test(line);

    if (isTopLevel && buffer.length > 0) {
      flushBuffer();
      const match = line.match(/(?:pub\s+)?(\w+)\s+(\w+)/);
      currentItem = match ? `${match[1]} ${match[2]}` : "";
    }

    buffer.push(line);

    // Force flush on very large accumulations
    if (buffer.join("\n").length > CHUNK_TARGET_CHARS * 3) {
      flushBuffer();
    }
  }
  flushBuffer();

  return chunks.map((c, i) => ({
    text: c.text,
    heading: c.item || "",
    file: path,
    type: "rs",
    chunkIdx: i,
  }));
}

function chunkFile(path, content) {
  if (path.endsWith(".md") || path.endsWith(".mdx")) {
    return chunkMarkdown(path, content);
  }
  if (path.endsWith(".rs")) {
    return chunkRust(path, content);
  }
  // Plain text: simple sliding window
  const chunks = [];
  let start = 0;
  while (start < content.length) {
    const end = Math.min(start + CHUNK_TARGET_CHARS, content.length);
    const text = content.slice(start, end).trim();
    if (text.length > 40) chunks.push({ text, heading: "", file: path, type: "txt", chunkIdx: chunks.length });
    start += CHUNK_TARGET_CHARS - CHUNK_OVERLAP_CHARS;
  }
  return chunks;
}

// Stable ID: hash of (file path + chunk index). Short enough for Vectorize (max 64 chars).
function chunkId(chunk) {
  const key = `${chunk.file}::${chunk.chunkIdx}`;
  return createHash("sha256").update(key).digest("hex").slice(0, 32);
}

// ── Cloudflare AI embeddings ──────────────────────────────────────────────────

async function embedBatch(texts) {
  const res = await fetch(
    `${CF_BASE}/ai/run/@cf/baai/bge-small-en-v1.5`,
    {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${CF_API_TOKEN}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ text: texts }),
    }
  );

  if (!res.ok) {
    const err = await res.text();
    throw new Error(`CF AI embedding failed ${res.status}: ${err}`);
  }

  const data = await res.json();
  // Returns { result: { shape: [n, 384], data: [[...], ...] } }
  return data.result.data; // array of float arrays
}

// ── Vectorize upsert ──────────────────────────────────────────────────────────

async function upsertVectors(vectors) {
  // Format as NDJSON: one JSON object per line
  const ndjson = vectors
    .map(v => JSON.stringify({
      id: v.id,
      values: v.values,
      metadata: {
        file: v.file,
        type: v.type,
        heading: v.heading,
        text: v.text,
        // Truncate text in metadata to stay under 10KB per vector
        // (600 char chunks are well within this)
      },
    }))
    .join("\n");

  const res = await fetch(
    `${CF_BASE}/vectorize/v2/indexes/${INDEX_NAME}/upsert`,
    {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${CF_API_TOKEN}`,
        "Content-Type": "application/x-ndjson",
      },
      body: ndjson,
    }
  );

  if (!res.ok) {
    const err = await res.text();
    throw new Error(`Vectorize upsert failed ${res.status}: ${err}`);
  }

  return res.json();
}

async function deleteAllVectors() {
  // Vectorize V2 doesn't have a "delete all" — we rely on upsert overwriting.
  // For a true full reindex, delete and recreate the index via wrangler CLI.
  console.log("  Note: for a clean reindex, run: npx wrangler vectorize delete laminardb-docs && npx wrangler vectorize create laminardb-docs --dimensions=384 --metric=cosine");
  console.log("  (upsert will overwrite existing vectors with the same ID)");
}

// ── File selection ────────────────────────────────────────────────────────────

function shouldSkip(path) {
  return SKIP_PATTERNS.some(p => p.test(path));
}

function isEligibleRust(path) {
  return path.endsWith(".rs") &&
    RUST_INCLUDE_PREFIXES.some(prefix => path.startsWith(prefix));
}

function isDoc(path) {
  return path.endsWith(".md") || path.endsWith(".mdx");
}

// ── State: track which file SHAs we've already indexed ───────────────────────

const STATE_FILE = ".index-state.json";

function loadState() {
  if (!existsSync(STATE_FILE)) return {};
  try { return JSON.parse(readFileSync(STATE_FILE, "utf-8")); } catch { return {}; }
}

function saveState(state) {
  writeFileSync(STATE_FILE, JSON.stringify(state, null, 2), "utf-8");
}

// ── Main ──────────────────────────────────────────────────────────────────────

async function main() {
  console.log(`LaminarDB RAG indexer${FULL_REINDEX ? " (full reindex)" : " (incremental)"}\n`);

  if (FULL_REINDEX) await deleteAllVectors();

  const state = FULL_REINDEX ? {} : loadState();

  // 1. Fetch file tree
  console.log("Fetching file tree from GitHub...");
  const tree = await fetchTree();
  console.log(`  ${tree.length} total files\n`);

  // 2. Select files to index
  const toIndex = [];

  // Priority docs first
  for (const p of PRIORITY_FILES) {
    const entry = tree.find(f => f.path === p);
    if (!entry) continue;
    if (!FULL_REINDEX && state[p] === entry.sha) {
      console.log(`  skip (unchanged): ${p}`);
      continue;
    }
    toIndex.push(entry);
  }

  // All other eligible files
  for (const entry of tree) {
    if (shouldSkip(entry.path)) continue;
    if (PRIORITY_FILES.includes(entry.path)) continue; // already handled
    if (!isDoc(entry.path) && !isEligibleRust(entry.path)) continue;

    if (!FULL_REINDEX && state[entry.path] === entry.sha) continue;
    toIndex.push(entry);
  }

  console.log(`\nFiles to index: ${toIndex.length}\n`);
  if (toIndex.length === 0) {
    console.log("Nothing changed. Run with --full to reindex everything.");
    return;
  }

  // 3. Fetch content, chunk, embed, upsert
  let totalChunks = 0;
  let totalVectors = 0;

  // Process files in groups to avoid hammering GitHub API
  const GROUP_SIZE = 5;
  for (let i = 0; i < toIndex.length; i += GROUP_SIZE) {
    const group = toIndex.slice(i, i + GROUP_SIZE);

    // Fetch content in parallel within group
    const contents = await Promise.allSettled(
      group.map(async entry => ({
        entry,
        content: await fetchFileContent(entry.path),
      }))
    );

    // Chunk all files in this group
    const allChunks = [];
    for (const result of contents) {
      if (result.status === "rejected") {
        console.warn(`  ✗ fetch failed: ${result.reason.message}`);
        continue;
      }
      const { entry, content } = result.value;
      const chunks = chunkFile(entry.path, content);
      totalChunks += chunks.length;
      allChunks.push(...chunks.map(c => ({ ...c, sha: entry.sha })));
      process.stdout.write(`  chunked: ${entry.path} → ${chunks.length} chunks\n`);
    }

    // Embed in batches
    const vectors = [];
    for (let j = 0; j < allChunks.length; j += EMBED_BATCH_SIZE) {
      const batch = allChunks.slice(j, j + EMBED_BATCH_SIZE);
      const texts = batch.map(c => `[${c.file}] ${c.heading ? c.heading + ": " : ""}${c.text}`);

      process.stdout.write(`  embedding batch ${j + 1}–${Math.min(j + EMBED_BATCH_SIZE, allChunks.length)} of ${allChunks.length}...`);
      const embeddings = await embedBatch(texts);
      process.stdout.write(` done\n`);

      for (let k = 0; k < batch.length; k++) {
        const chunk = batch[k];
        vectors.push({
          id: chunkId(chunk),
          values: embeddings[k],
          file: chunk.file,
          type: chunk.type,
          heading: chunk.heading,
          text: chunk.text,
        });
      }

      // Brief pause between embedding calls
      if (j + EMBED_BATCH_SIZE < allChunks.length) {
        await new Promise(r => setTimeout(r, 300));
      }
    }

    // Upsert to Vectorize in batches
    for (let j = 0; j < vectors.length; j += UPSERT_BATCH_SIZE) {
      const batch = vectors.slice(j, j + UPSERT_BATCH_SIZE);
      process.stdout.write(`  upserting ${batch.length} vectors to Vectorize...`);
      await upsertVectors(batch);
      process.stdout.write(` done\n`);
      totalVectors += batch.length;

      if (j + UPSERT_BATCH_SIZE < vectors.length) {
        await new Promise(r => setTimeout(r, 500));
      }
    }

    // Update state with new SHAs
    for (const { entry } of contents.filter(r => r.status === "fulfilled").map(r => r.value)) {
      state[entry.path] = entry.sha;
    }
    saveState(state);

    // Pause between file groups (GitHub rate limiting)
    if (i + GROUP_SIZE < toIndex.length) {
      await new Promise(r => setTimeout(r, 400));
    }
  }

  console.log(`
✓ Indexing complete
  Files processed : ${toIndex.length}
  Chunks created  : ${totalChunks}
  Vectors upserted: ${totalVectors}
  State saved to  : ${STATE_FILE}

Next: wrangler deploy
`);
}

main().catch(e => {
  console.error("\n✗ Indexing failed:", e.message);
  process.exit(1);
});
