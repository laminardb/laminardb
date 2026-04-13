/**
 * LaminarDB RAG chatbot — Cloudflare Worker
 *
 * At query time:
 *  1. Embed the user's question via Workers AI (bge-small-en)
 *  2. Query Vectorize for the 8 most relevant chunks
 *  3. Build a focused context from those chunks (~3KB vs old 150KB)
 *  4. Call Anthropic claude-haiku with the relevant context only
 *
 * Secrets:
 *   wrangler secret put ANTHROPIC_API_KEY
 *
 * Bindings (set in wrangler.toml):
 *   AI        — Workers AI (for embedding)
 *   VECTORIZE — Vectorize index
 */

const CORS_HEADERS = {
  "Access-Control-Allow-Origin": "*", // lock to your domain in production
  "Access-Control-Allow-Methods": "POST, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type",
};

const BASE_SYSTEM_PROMPT = `You are the LaminarDB documentation assistant. LaminarDB is an open-source embedded streaming SQL engine written in Rust — "SQLite for stream processing".

Answer based strictly on the source code and documentation excerpts provided below. If the answer isn't covered in the excerpts, say so clearly. Keep answers technical and concise — the audience is data engineers, quant engineers, and ML engineers. For code examples, prefer Rust unless the user asks for Python.`;

export default {
  async fetch(request, env) {
    if (request.method === "OPTIONS") {
      return new Response(null, { headers: CORS_HEADERS });
    }

    if (request.method !== "POST") {
      return new Response("Method not allowed", { status: 405 });
    }

    let body;
    try {
      body = await request.json();
    } catch {
      return new Response("Invalid JSON", { status: 400 });
    }

    const { messages } = body;
    if (!Array.isArray(messages) || messages.length === 0) {
      return new Response("messages array required", { status: 400 });
    }

    // The last user message is what we search for
    const lastUserMessage = [...messages].reverse().find(m => m.role === "user");
    if (!lastUserMessage) {
      return new Response("No user message found", { status: 400 });
    }

    // ── Step 1: Embed the query ───────────────────────────────────────────────
    let queryVector;
    try {
      const embedResult = await env.AI.run(
        "@cf/baai/bge-small-en-v1.5",
        { text: [lastUserMessage.content] }
      );
      queryVector = embedResult.data[0];
    } catch (e) {
      console.error("Embedding failed:", e);
      return new Response("Embedding error", { status: 502, headers: CORS_HEADERS });
    }

    // ── Step 2: Query Vectorize for top-K relevant chunks ────────────────────
    let retrievedChunks = [];
    try {
      const results = await env.VECTORIZE.query(queryVector, {
        topK: 8,
        returnMetadata: "all",
      });

      retrievedChunks = results.matches
        .filter(m => m.score > 0.3)     // discard low-similarity matches
        .sort((a, b) => b.score - a.score)
        .map(m => m.metadata);
    } catch (e) {
      console.error("Vectorize query failed:", e);
      // Degrade gracefully — answer without context rather than failing
      retrievedChunks = [];
    }

    // ── Step 3: Build focused context from retrieved chunks ──────────────────
    const contextBlock = buildContext(retrievedChunks);
    const systemPrompt = BASE_SYSTEM_PROMPT + contextBlock;

    // ── Step 4: Call Anthropic ────────────────────────────────────────────────
    const trimmedMessages = messages.slice(-10); // cap conversation history

    const anthropicRes = await fetch("https://api.anthropic.com/v1/messages", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-api-key": env.ANTHROPIC_API_KEY,
        "anthropic-version": "2023-06-01",
      },
      body: JSON.stringify({
        model: "claude-haiku-4-5-20251001",
        max_tokens: 1024,
        system: systemPrompt,
        messages: trimmedMessages,
      }),
    });

    if (!anthropicRes.ok) {
      const err = await anthropicRes.text();
      console.error("Anthropic error:", err);
      return new Response("Upstream error", { status: 502, headers: CORS_HEADERS });
    }

    const data = await anthropicRes.json();
    const reply = data.content?.[0]?.text ?? "No response";

    // Return reply + sources so the widget can show "based on: src/sql/planner.rs"
    const sources = [...new Set(retrievedChunks.map(c => c.file).filter(Boolean))];

    return new Response(
      JSON.stringify({ reply, sources }),
      { headers: { ...CORS_HEADERS, "Content-Type": "application/json" } }
    );
  },
};

/**
 * Assemble the context block from retrieved chunks.
 * Groups chunks by file and adds headings for readability.
 */
function buildContext(chunks) {
  if (chunks.length === 0) {
    return "\n\n[No relevant documentation found in the index for this query.]";
  }

  // Group by file
  const byFile = new Map();
  for (const chunk of chunks) {
    const file = chunk.file ?? "unknown";
    if (!byFile.has(file)) byFile.set(file, []);
    byFile.get(file).push(chunk);
  }

  let context = "\n\n## Relevant source excerpts\n";
  for (const [file, fileChunks] of byFile) {
    const ext = file.split(".").pop();
    const fence = ext === "rs" ? "rust" : ext === "md" ? "" : ext;
    context += `\n### ${file}\n`;
    for (const chunk of fileChunks) {
      if (chunk.heading) context += `*${chunk.heading}*\n`;
      context += `\`\`\`${fence}\n${chunk.text.trim()}\n\`\`\`\n`;
    }
  }

  return context;
}
