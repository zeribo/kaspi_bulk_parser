#!/usr/bin/env node
/*
generate_kaspi_links.js

Usage:
  1) Save your input array of category URLs into `categories.json` (an array of strings).
  2) Set your OpenAI API key in the environment: export OPENAI_API_KEY="sk-..."
  3) Install dependencies: npm install axios
  4) Run: node generate_kaspi_links.js categories.json new_links.json

What it does:
  - Reads an array of URLs from the input JSON file.
  - Extracts the `:category:...` token from the `q` parameter of each URL (decoded).
  - Calls OpenAI (chat completion) once per category to generate 5 short Russian search phrases related to that category.
  - For each generated phrase it creates a new URL identical to the original but with the `text` query param set to the URL-encoded phrase and with a fresh `requestId`.
  - Writes the final list of generated URLs into an output JSON file as a flat array.

Notes:
  - The script uses the Chat Completions endpoint (model `gpt-3.5-turbo`). You may change the model if you have access to others.
  - It attempts simple retries on failures and supports an adjustable concurrency level.
*/

const fs = require('fs');
const path = require('path');
const axios = require('axios');
const crypto = require('crypto');

if (!process.env.OPENAI_API_KEY) {
  console.error('ERROR: Please set OPENAI_API_KEY in your environment.');
  process.exit(1);
}

const INPUT_FILE = process.argv[2] || 'categories.json';
const OUTPUT_FILE = process.argv[3] || 'new_links.json';
const CONCURRENCY = Number(process.env.CONCURRENCY) || 5; // safe default
const PHRASES_PER_CATEGORY = 5;
const OPENAI_URL = 'https://api.openai.com/v1/chat/completions';
const OPENAI_MODEL = 'gpt-3.5-turbo';

function randomRequestId() {
  return crypto.randomBytes(16).toString('hex');
}

function extractCategoryFromQ(qDecoded) {
  // qDecoded is already decodeURIComponent(q), so spaces appear as real spaces
  // We capture everything between ':category:' and the next ':' (spaces allowed)
  // Example:
  //  ":category:First meal:availableInZones:Magnum_ZONE1" -> "First meal"
  const m = qDecoded.match(/:category:([^:]+)/i);
  if (m) return m[1].trim();
  return null;
}

async function generatePhrasesForCategory(category, n = PHRASES_PER_CATEGORY) {
  // Prompt ChatGPT to return a JSON array of n short Russian phrases related to the category.
  const system = `You are a helpful assistant that returns ONLY a JSON array of ${n} short Russian search phrases related to the given category. Each phrase should be 1–4 words max, relevant to the category, and natural for user search queries (e.g., "крафтовое пиво"). Output must be valid JSON array only (no explanation).`;
  const user = `Category: "${category}". Return ${n} unique short Russian search phrases as a JSON array.`;

  const body = {
    model: OPENAI_MODEL,
    messages: [
      { role: 'system', content: system },
      { role: 'user', content: user }
    ],
    temperature: 0.8,
    max_tokens: 200
  };

  try {
    const resp = await axios.post(OPENAI_URL, body, {
      headers: {
        'Authorization': `Bearer ${process.env.OPENAI_API_KEY}`,
        'Content-Type': 'application/json'
      },
      timeout: 60000
    });

    const txt = resp.data.choices?.[0]?.message?.content?.trim();
    if (!txt) throw new Error('empty response from OpenAI');

    // Try parsing as JSON first
    try {
      const arr = JSON.parse(txt);
      if (Array.isArray(arr)) return arr.slice(0, n).map(s => String(s).trim());
    } catch (err) {
      // not valid JSON; try to extract an array-like chunk using a regex
    }

    // Fallback: use a simple regex to extract lines that look like items
    const lines = txt.split(/\r?\n/).map(l => l.trim()).filter(Boolean);
    const candidates = [];
    for (const line of lines) {
      // remove leading bullets or numbers like "1.", "-", "•"
      const cleaned = line.replace(/^\s*[-•\d\.\)]\s*/, '').trim();
      if (cleaned) candidates.push(cleaned);
      if (candidates.length >= n) break;
    }

    if (candidates.length) return candidates.slice(0, n);

    // Last resort: split by commas and hope for the best
    const byCommas = txt.split(',').map(s => s.trim()).filter(Boolean);
    if (byCommas.length) return byCommas.slice(0, n);

    throw new Error('cannot parse OpenAI output');
  } catch (err) {
    throw err;
  }
}

async function processUrl(originalUrl) {
  try {
    const url = new URL(originalUrl);
    const q = url.searchParams.get('q') || '';
    const qDecoded = decodeURIComponent(q);
    const category = extractCategoryFromQ(qDecoded) || qDecoded || 'товары';

    // Ask OpenAI to make n phrases
    const phrases = await retry(() => generatePhrasesForCategory(category, PHRASES_PER_CATEGORY), 2);

    const generatedLinks = phrases.map(phrase => {
      // Use encodeURIComponent so spaces are encoded as %20 (not '+')
      const encodedPhrase = encodeURIComponent(phrase);
      // Work with the original URL string to avoid URLSearchParams turning spaces into '+'
      let u = originalUrl;
      if (/[?&]text=[^&]*/.test(u)) {
        u = u.replace(/([?&])text=[^&]*/, `$1text=${encodedPhrase}`);
      } else {
        u += (u.includes('?') ? '&' : '?') + `text=${encodedPhrase}`;
      }

      const rid = randomRequestId();
      if (/[?&]requestId=[^&]*/.test(u)) {
        u = u.replace(/([?&])requestId=[^&]*/, `$1requestId=${rid}`);
      } else {
        u += `&requestId=${rid}`;
      }

      return u;
    });

    return generatedLinks;
  } catch (err) {
    console.error('Error processing URL:', originalUrl, err.message || err);
    return [];
  }
}

async function retry(fn, retries = 2, delayMs = 1000) {
  let lastErr;
  for (let i = 0; i <= retries; i++) {
    try {
      return await fn();
    } catch (err) {
      lastErr = err;
      const wait = delayMs * Math.pow(2, i);
      console.warn(`Attempt ${i + 1} failed: ${err.message || err}. ${i < retries ? `Retrying in ${wait}ms...` : 'No more retries.'}`);
      if (i < retries) await new Promise(r => setTimeout(r, wait));
    }
  }
  throw lastErr;
}

async function main() {
  const inputPath = path.resolve(process.cwd(), INPUT_FILE);
  if (!fs.existsSync(inputPath)) {
    console.error('Input file not found:', inputPath);
    process.exit(2);
  }

  const raw = fs.readFileSync(inputPath, 'utf8');
  let urls;
  try {
    urls = JSON.parse(raw);
    if (!Array.isArray(urls)) throw new Error('input must be a JSON array of URLs');
  } catch (err) {
    console.error('Failed to parse input JSON:', err.message || err);
    process.exit(3);
  }

  const out = [];

  // Simple concurrency pool
  let idx = 0;
  const workers = new Array(CONCURRENCY).fill(null).map(async () => {
    while (true) {
      const i = idx++;
      if (i >= urls.length) break;
      const u = urls[i];
      process.stdout.write(`Processing ${i + 1}/${urls.length}\r`);
      try {
        const generated = await processUrl(u);
        for (const g of generated) out.push(g);
      } catch (err) {
        console.error('\nFailed to generate for:', u, err.message || err);
      }
    }
  });

  await Promise.all(workers);

  // Write output
  const outPath = path.resolve(process.cwd(), OUTPUT_FILE);
  fs.writeFileSync(outPath, JSON.stringify(out, null, 2), 'utf8');
  console.log(`\nDone. Generated ${out.length} links. Output written to ${outPath}`);
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(99);
});
