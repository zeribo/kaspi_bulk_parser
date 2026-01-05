'use strict';

const axios = require('axios');
const fs = require('fs');
const { v4: uuidv4 } = require('uuid');

// ==========================================
// CONFIGURATION
// ==========================================
const CATEGORY_URLS = require('./new_links.json'); // provide your categories.json
const OUTPUT_FILE = 'products.json';
const CHECKPOINT_INTERVAL = 500;   // save every N items
const REQUEST_DELAY = 150;         // ms delay between batches
const MAX_RETRIES = 4;
const CONCURRENCY = 8;            // categories processed in parallel
const PAGE_PARALLEL = 3;          // fetch up to 3 pages in parallel per category
const MAX_PAGE = 300;             // stop paginating after this page number (set to null to disable)

// merchant/static fields matching the requested output
const MERCHANT_ID = 'Magnum';
const MERCHANT_NAME = 'MAGNUM';
const SOURCE = 'kaspi';
const DEFAULT_SUB_CATEGORY = 'fruits_berries';
const DEFAULT_MEASURE = 'граммы';
const DEFAULT_CURRENCY = 'KZT';

// ==========================================
// AXIOS CONFIGURATION
// ==========================================
const axiosConfig = {
  timeout: 15000,
  headers: {
    'User-Agent':
      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36',
    Referer: 'https://kaspi.kz/',
    Accept: 'application/json, text/plain, */*',
    'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
  },
  validateStatus: status => status >= 200 && status < 500, // let code handle non-200
};

// ==========================================
// THREAD-SAFE STORAGE
// ==========================================
const seenIds = new Set();
const allResults = [];
let totalFound = 0;

// save partial results (checkpoint)
const saveCheckpoint = () => {
  fs.writeFileSync(OUTPUT_FILE, JSON.stringify(allResults, null, 2), 'utf8');
  console.log(`[CHECKPOINT] Saved ${allResults.length} items`);
};

// add product only if unique
const addProduct = product => {
  if (!seenIds.has(product.id)) {
    seenIds.add(product.id);
    allResults.push(product);
    totalFound++;
    if (totalFound % CHECKPOINT_INTERVAL === 0) saveCheckpoint();
    return true;
  }
  return false;
};

// sleep helper
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

// ==========================================
// FETCH WITH RETRY + BACKOFF
// ==========================================
const fetchWithRetry = async (url, retries = MAX_RETRIES) => {
  let attempt = 0;
  while (attempt < retries) {
    attempt++;
    try {
      const response = await axios.get(url, axiosConfig);
      // Handle rate limiting specially
      if (response.status === 429) {
        const backoff = Math.min(5000, 500 * attempt);
        console.warn(`[WARN] 429 rate limit for ${url}. Backing off ${backoff}ms (attempt ${attempt})`);
        await sleep(backoff);
        continue;
      }
      if (response.status >= 200 && response.status < 300) return response.data;
      // For other 4xx/5xx we backoff and retry a limited number of times
      if (response.status >= 400) {
        const backoff = 500 * attempt;
        console.warn(`[WARN] HTTP ${response.status} for ${url}. Backing off ${backoff}ms (attempt ${attempt})`);
        await sleep(backoff);
        continue;
      }
    } catch (err) {
      const backoff = 500 * attempt;
      console.warn(`[WARN] Error fetching ${url}: ${err.message}. Backing off ${backoff}ms (attempt ${attempt})`);
      await sleep(backoff);
    }
  }
  console.error(`[ERROR] Failed to fetch ${url} after ${retries} attempts`);
  return null;
};

// ==========================================
// UTIL: safe int parse
// ==========================================
const toIntSafe = v => {
  if (v == null) return 0;
  const n = Number(v);
  return Number.isNaN(n) ? 0 : Math.trunc(n);
};

// ==========================================
// DETERMINE IF ITEM IS MAGNUM (your filter)
// ==========================================
const isMagnumProduct = item => {
  const stickers = item.stickers || [];
  const hasMagnumSticker = stickers.includes('magnum_offer_available');

  const merchants = item.majorMerchants || [];
  const merchantNames = Array.isArray(merchants)
    ? merchants.map(m => (typeof m === 'string' ? m : (m.name || '') )).filter(Boolean)
    : [];
  const hasMagnumMerchant = merchantNames.some(n => n.toLowerCase().includes('magnum'));

  return hasMagnumSticker || hasMagnumMerchant;
};

// ==========================================
// BUILD PRODUCT OBJECT (matches the output you provided)
// ==========================================
const buildProductObject = (item) => {
  const now = new Date().toISOString();
  const id = item.id ? String(item.id) : uuidv4();

  const previewImages = Array.isArray(item.previewImages) ? item.previewImages : [];
  const images = previewImages
    .map(img => img.large || img.url || null)
    .filter(Boolean);

  return {
    _id: uuidv4(),
    mercant_id: MERCHANT_ID,
    mercant_name: MERCHANT_NAME,

    product_id: null,
    id: id,

    title: item.title || null,
    description: null,

    url: item.shopLink ? `https://kaspi.kz${item.shopLink}` : (item.url || null),
    url_picture: images.length ? images[0] : null,

    category_full_path: item.category ? item.category.join(' > ') : null,
    brand: item.brand || null,
    sub_category: null,

    time_scrap: now,
    measure: DEFAULT_MEASURE,
    city: null,

    matched_uuid: null,

    price: toIntSafe(item.unitPrice || item.price || 0),
    originalPrice: toIntSafe(
      item.unitSalePrice ||
      item.unitPriceBeforeDiscount ||
      item.unitPrice ||
      item.price ||
      0
    ),

    discount: toIntSafe(item.discount || 0),
    currency: DEFAULT_CURRENCY,

    images: images,

    inStock: item.inStock == null ? true : Boolean(item.inStock),
    stockQuantity: null,

    weight: null,
    volume: null,

    characteristics: [],

    rating: item.rating == null ? null : item.rating,
    reviewCount: toIntSafe(item.reviewsQuantity || 0),

    productUrl: item.shopLink ? `https://kaspi.kz${item.shopLink}` : (item.url || null),
    productId: id,

    parsedAt: now,
    lastUpdated: now,

    source: SOURCE,
    isActive: true,

    parsingErrors: [],

    createdAt: now,
    updatedAt: now,

    __v: 0
  };
};

// ==========================================
// PROCESS A SINGLE CATEGORY (page always starts at 0)
// ==========================================
const processCategory = async (categoryUrl, categoryIndex, totalCategories) => {
  // decode for logs
  const categoryName = decodeURIComponent((categoryUrl.match(/category%3A([^&]+)/) || [null, `Category ${categoryIndex}`])[1] || `Category ${categoryIndex}`);
  console.log(`[${categoryIndex}/${totalCategories}] Starting: ${categoryName}`);

  // ALWAYS START AT PAGE 0 (user requested)
  let page = 0;

  // Determine base contains page parameter to replace. If not found, we'll append ?page=...
  const hasPageParam = /page=\d+/.test(categoryUrl);

  let categoryMatches = 0;
  let keepGoing = true;

  while (keepGoing) {
    // stop if page already past MAX_PAGE
    if (MAX_PAGE != null && page > MAX_PAGE) break;

    // build a small batch of page URLs to fetch in parallel (respect MAX_PAGE)
    const pageUrls = [];
    for (let i = 0; i < PAGE_PARALLEL; i++) {
      const p = page + i;
      if (MAX_PAGE != null && p > MAX_PAGE) break;
      let paginatedUrl;
      if (hasPageParam) {
        paginatedUrl = categoryUrl.replace(/page=\d+/, `page=${p}`);
      } else {
        paginatedUrl = categoryUrl.includes('?') ? `${categoryUrl}&page=${p}` : `${categoryUrl}?page=${p}`;
      }
      // ensure unique requestId
      paginatedUrl = paginatedUrl.replace(/requestId=[^&]+/, `requestId=${uuidv4()}`);
      // if requestId absent, append one to avoid caching
      if (!/requestId=/.test(paginatedUrl)) {
        paginatedUrl += (paginatedUrl.includes('?') ? '&' : '?') + `requestId=${uuidv4()}`;
      }
      pageUrls.push({ url: paginatedUrl, pageNumber: p });
    }

    // if no pages to fetch -> stop
    if (pageUrls.length === 0) break;

    // fetch them in parallel
    const fetchPromises = pageUrls.map(pu => fetchWithRetry(pu.url));
    const responses = await Promise.all(fetchPromises);

    // process responses; if all responses are empty or null -> stop
    let anyData = false;
    for (let i = 0; i < responses.length; i++) {
      const data = responses[i];
      if (!data || !Array.isArray(data.data) || data.data.length === 0) {
        continue;
      }
      anyData = true;
      for (const item of data.data) {
        try {
          if (!item) continue;
          if (isMagnumProduct(item)) {
            const product = buildProductObject(item);
            if (addProduct(product)) {
              categoryMatches++;
            }
          }
        } catch (err) {
          // keep parsing even if a single item fails
          console.warn(`[WARN] Failed processing an item in category ${categoryName}: ${err.message}`);
        }
      }
    }

    // move page pointer forward by the number of pages fetched in this batch
    page += pageUrls.length;

    // if no data found in this parallel batch -> stop
    if (!anyData) keepGoing = false;

    // small delay between batches to avoid aggressive hitting
    await sleep(REQUEST_DELAY);
  }

  console.log(`[${categoryIndex}/${totalCategories}] Done: ${categoryName} - ${categoryMatches} items`);
  return categoryMatches;
};

// ==========================================
// CONCURRENCY RUNNER (limits how many categories run in parallel)
// ==========================================
const runWithConcurrency = async (tasks, concurrency) => {
  const results = [];
  const executing = new Set();

  for (const task of tasks) {
    const p = Promise.resolve().then(() => task());
    results.push(p);

    executing.add(p);
    const remove = () => executing.delete(p);
    p.then(remove).catch(remove);

    if (executing.size >= concurrency) {
      // wait for one of the running tasks to finish
      await Promise.race(executing);
    }
  }

  return Promise.all(results);
};

// ==========================================
// MAIN
// ==========================================
async function startParsing() {
  console.log(`Starting Kaspi Parser (concurrency=${CONCURRENCY}, pageParallel=${PAGE_PARALLEL}, MAX_PAGE=${MAX_PAGE === null ? 'none' : MAX_PAGE})`);
  console.log(`${CATEGORY_URLS.length} categories to process`);
  const startTime = Date.now();

  try {
    const tasks = CATEGORY_URLS.map((url, index) => {
      return () => processCategory(url, index + 1, CATEGORY_URLS.length);
    });

    await runWithConcurrency(tasks, CONCURRENCY);
  } catch (error) {
    console.error('Critical Error:', error && error.message ? error.message : error);
  } finally {
    // final save
    fs.writeFileSync(OUTPUT_FILE, JSON.stringify(allResults, null, 2), 'utf8');
    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
    console.log('='.repeat(60));
    console.log(`Done! Saved ${allResults.length} unique items to ${OUTPUT_FILE}`);
    console.log(`Total time: ${elapsed}s`);
    console.log('='.repeat(60));
  }
}

startParsing();
