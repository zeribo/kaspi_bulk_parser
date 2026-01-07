import axios from 'axios';
import fs from 'fs';
import { AXIOS_CONFIG, CONFIG, CITY_MAP } from './config.js';

// ==========================================
// STORAGE
// ==========================================
export const seenIds = new Set();
export const allResults = [];
let totalFound = 0;

export const saveCheckpoint = () => {
  fs.writeFileSync(CONFIG.OUTPUT_FILE, JSON.stringify(allResults, null, 2), 'utf8');
  console.log(`[CHECKPOINT] Saved ${allResults.length} items`);
};

export const addProduct = product => {
  if (!seenIds.has(product.id)) {
    seenIds.add(product.id);
    allResults.push(product);
    totalFound++;
    if (totalFound % CONFIG.CHECKPOINT_INTERVAL === 0) saveCheckpoint();
    return true;
  }
  return false;
};

export const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

// ==========================================
// CITY DETECTION
// ==========================================
export const extractCityFromUrl = (url) => {
  const match = url.match(/[&?]c=(\d+)/);
  if (match && match[1]) {
    return CITY_MAP[match[1]] || null;
  }
  return null;
};

// ==========================================
// EXTRACT MEASURE AND WEIGHT/VOLUME FROM TITLE
// ==========================================
export const extractMeasureAndWeight = (title) => {
  if (!title) return { measure: null, weight: null, volume: null };

  // Common measure patterns - use \s* to handle any whitespace including non-breaking spaces
  const measurePatterns = [
    { regex: /(\d+(?:[.,]\d+)?)\s*мл(?:\s|$|\.)/iu, measure: 'миллилитры', type: 'volume' },
    { regex: /(\d+(?:[.,]\d+)?)\s*л(?:\s|$|\.)/iu, measure: 'литры', type: 'volume' },
    { regex: /(\d+(?:[.,]\d+)?)\s*г(?:\s|$|\.)/iu, measure: 'граммы', type: 'weight' },
    { regex: /(\d+(?:[.,]\d+)?)\s*кг(?:\s|$|\.)/iu, measure: 'килограммы', type: 'weight' },
    { regex: /(\d+(?:[.,]\d+)?)\s*mg(?:\s|$|\.)/iu, measure: 'миллиграммы', type: 'weight' },
    { regex: /(\d+(?:[.,]\d+)?)\s*шт(?:\s|$|\.)/iu, measure: 'штуки', type: 'count' },
  ];

  for (const pattern of measurePatterns) {
    const match = title.match(pattern.regex);
    if (match) {
      const value = parseFloat(match[1].replace(',', '.'));
      
      if (pattern.type === 'volume') {
        return {
          measure: pattern.measure,
          weight: null,
          volume: value
        };
      } else if (pattern.type === 'weight') {
        return {
          measure: pattern.measure,
          weight: value,
          volume: null
        };
      } else {
        return {
          measure: pattern.measure,
          weight: value,
          volume: value
        };
      }
    }
  }

  return { measure: null, weight: null, volume: null };
};

// ==========================================
// FIX PRODUCT URL
// ==========================================
export const fixProductUrl = (url) => {
  if (!url) return null;
  return url.replace('https://kaspi.kz/p/', 'https://kaspi.kz/shop/p/');
};

// ==========================================
// FETCH WITH RETRY
// ==========================================
export const fetchWithRetry = async (url, retries = CONFIG.MAX_RETRIES) => {
  let attempt = 0;
  while (attempt < retries) {
    attempt++;
    try {
      const response = await axios.get(url, AXIOS_CONFIG);
      if (response.status === 429) {
        const backoff = Math.min(5000, 500 * attempt);
        console.warn(`[WARN] 429 rate limit. Backing off ${backoff}ms (attempt ${attempt})`);
        await sleep(backoff);
        continue;
      }
      if (response.status >= 200 && response.status < 300) return response.data;
      if (response.status >= 400) {
        const backoff = 500 * attempt;
        console.warn(`[WARN] HTTP ${response.status}. Backing off ${backoff}ms (attempt ${attempt})`);
        await sleep(backoff);
        continue;
      }
    } catch (err) {
      const backoff = 500 * attempt;
      console.warn(`[WARN] Error fetching: ${err.message}. Backing off ${backoff}ms (attempt ${attempt})`);
      await sleep(backoff);
    }
  }
  console.error(`[ERROR] Failed to fetch after ${retries} attempts`);
  return null;
};

// ==========================================
// UTILS
// ==========================================
export const toIntSafe = v => {
  if (v == null) return 0;
  const n = Number(v);
  return Number.isNaN(n) ? 0 : Math.trunc(n);
};

export const isMagnumProduct = item => {
  const stickers = item.stickers || [];
  const hasMagnumSticker = stickers.includes('magnum_offer_available');

  const merchants = item.majorMerchants || [];
  const merchantNames = Array.isArray(merchants)
    ? merchants.map(m => (typeof m === 'string' ? m : (m.name || '') )).filter(Boolean)
    : [];
  const hasMagnumMerchant = merchantNames.some(n => n.toLowerCase().includes('magnum'));

  return hasMagnumSticker || hasMagnumMerchant;
};