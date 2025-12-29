const fs = require('fs');
const csv = require('csv-parser');
const { v4: uuidv4 } = require('uuid');
const { Worker, isMainThread, parentPort, workerData } = require('worker_threads');
const os = require('os');

// ==========================================
// CONFIGURATION
// ==========================================
const INPUT_SCRAPE_FILE = 'products.json';
const INPUT_CSV_REFERENCE = 'PRODUCTS_TO_MATCH.csv';
const OUTPUT_FILE = 'mapped_data.json';
const CITY = 'almaty';
const SOURCE = 'kaspi_parser';
const NUM_WORKERS = Math.max(1, os.cpus().length - 1); // Leave 1 core free

// ==========================================
// PROGRESS BAR
// ==========================================
class ProgressBar {
  constructor(total, barLength = 40) {
    this.total = total;
    this.current = 0;
    this.barLength = barLength;
    this.startTime = Date.now();
  }

  update(current) {
    this.current = current;
    const percent = Math.round((current / this.total) * 100);
    const filled = Math.round((current / this.total) * this.barLength);
    const empty = this.barLength - filled;
    const bar = '█'.repeat(filled) + '░'.repeat(empty);
    
    const elapsed = (Date.now() - this.startTime) / 1000;
    const rate = current / elapsed;
    const eta = rate > 0 ? Math.round((this.total - current) / rate) : 0;
    
    process.stdout.write(`\r[${bar}] ${percent}% | ${current}/${this.total} | ${rate.toFixed(1)}/s | ETA: ${eta}s   `);
  }

  done() {
    const elapsed = ((Date.now() - this.startTime) / 1000).toFixed(1);
    console.log(`\n✓ Completed in ${elapsed}s`);
  }
}

// ==========================================
// HELPERS (shared)
// ==========================================
const extractWeightFromTitle = (title) => {
  if (!title) return null;
  const match = title.match(/(\d+(?:\.\d+)?)\s*(г|кг|мл|л|гр|кгр|млр|лр)/i);
  if (match) {
    let unit = match[2];
    if (unit === 'гр') unit = 'г';
    if (unit === 'кгр') unit = 'кг';
    if (unit === 'млр') unit = 'мл';
    if (unit === 'лр') unit = 'л';
    return `${match[1]} ${unit}`;
  }
  return null;
};

const getMeasure = (weightStr) => {
  if (!weightStr) return "шт";
  if (weightStr.includes('кг') || weightStr.includes('kg')) return 'кг';
  if (weightStr.includes('г') || weightStr.includes('g')) return 'г';
  if (weightStr.includes('мл') || weightStr.includes('ml')) return 'мл';
  if (weightStr.includes('л') || weightStr.includes('l')) return 'л';
  return "шт";
};

const normalize = (str) => {
  if (!str) return '';
  return str.toLowerCase().replace(/[^\p{L}\p{N}\s]/gu, '').replace(/\s+/g, ' ').trim();
};

const createNGrams = (str, n = 3) => {
  const normalized = normalize(str);
  const grams = new Set();
  for (let i = 0; i <= normalized.length - n; i++) {
    grams.add(normalized.substring(i, i + n));
  }
  return grams;
};

const jaccardSimilarity = (set1, set2) => {
  let intersection = 0;
  for (const gram of set1) {
    if (set2.has(gram)) intersection++;
  }
  const union = set1.size + set2.size - intersection;
  return union === 0 ? 0 : intersection / union;
};

const wordOverlapScore = (str1, str2) => {
  const words1 = new Set(normalize(str1).split(' ').filter(w => w.length > 2));
  const words2 = new Set(normalize(str2).split(' ').filter(w => w.length > 2));
  let matches = 0;
  for (const w of words1) {
    if (words2.has(w)) matches++;
  }
  const total = Math.max(words1.size, words2.size);
  return total === 0 ? 0 : matches / total;
};

// ==========================================
// WORKER THREAD CODE
// ==========================================
if (!isMainThread) {
  const { items, indexedRef } = workerData;
  
  // Reconstruct Sets from arrays
  const reconstructedRef = indexedRef.map(ref => ({
    ...ref,
    ngrams: new Set(ref.ngrams)
  }));

  const findBestMatch = (query, indexedRef, threshold = 0.3) => {
    const queryNgrams = createNGrams(query);
    const queryNorm = normalize(query);
    
    let bestMatch = null;
    let bestScore = 0;
    const candidates = [];
    
    for (const ref of indexedRef) {
      const ngramScore = jaccardSimilarity(queryNgrams, ref.ngrams);
      if (ngramScore > 0.15) {
        candidates.push({ ref, ngramScore });
      }
    }
    
    candidates.sort((a, b) => b.ngramScore - a.ngramScore);
    const topCandidates = candidates.slice(0, 50);
    
    for (const { ref, ngramScore } of topCandidates) {
      const wordScore = wordOverlapScore(queryNorm, ref.normalized);
      const combinedScore = (ngramScore * 0.4 + wordScore * 0.6);
      
      if (combinedScore > bestScore && combinedScore >= threshold) {
        bestScore = combinedScore;
        bestMatch = ref.item;
      }
    }
    
    return { match: bestMatch, score: bestScore };
  };

  const now = new Date().toISOString();
  const results = [];

  for (const item of items) {
    const { match: bestMatch, score: matchScore } = findBestMatch(item.name, reconstructedRef);
    
    let weightString = '';
    const titleWeight = extractWeightFromTitle(item.name);
    if (titleWeight) {
      weightString = titleWeight;
    } else if (bestMatch && bestMatch.weight) {
      weightString = bestMatch.weight;
    } else {
      let weightVal = item.weight || 0;
      if (weightVal > 0) {
        weightString = `${Math.round(weightVal * 1000)} г`;
      }
    }

    const measure = getMeasure(weightString);
    const price = item.regularPrice || 0;
    const originalPrice = item.unitPriceBeforeDiscount || 0;
    const discount = item.discount || 0;

    results.push({
      _id: uuidv4(),
      mercant_id: uuidv4(),
      mercant_name: "Magnum",
      product_id: item.id,
      id: item.id,
      title: item.name,
      description: bestMatch ? bestMatch.description : null,
      url: item.link,
      url_picture: item.image,
      category_full_path: item.fullCategoryPath,
      brand: item.brand,
      sub_category: item.fullCategoryPath ? item.fullCategoryPath.split(' > ').pop() : null,
      time_scrap: now,
      measure: measure,
      city: CITY,
      price: price,
      originalPrice: originalPrice,
      discount: discount,
      currency: "KZT",
      inStock: true,
      weight: weightString,
      reviewCount: item.reviewsQuantity || 0,
      productUrl: item.link,
      productId: item.id,
      parsedAt: now,
      lastUpdated: now,
      source: SOURCE,
      isActive: true,
      createdAt: now,
      updatedAt: now,
      matched_csv_title: bestMatch ? bestMatch.name : "UNMATCHED",
      match_confidence: bestMatch ? Math.round(matchScore * 100) : 0,
      matched_uuid: bestMatch ? bestMatch.uuid : null,
      best_match: bestMatch ? "match" : "none",
      mappingCreatedAt: now
    });

    parentPort.postMessage({ type: 'progress', count: 1 });
  }

  parentPort.postMessage({ type: 'done', results });
  process.exit(0);
}

// ==========================================
// MAIN THREAD
// ==========================================
const loadReferenceData = () => {
  return new Promise((resolve, reject) => {
    const results = [];
    fs.createReadStream(INPUT_CSV_REFERENCE)
      .pipe(csv())
      .on('data', (data) => results.push(data))
      .on('end', () => resolve(results))
      .on('error', (error) => reject(error));
  });
};

const loadScrapedData = () => {
  if (!fs.existsSync(INPUT_SCRAPE_FILE)) {
    console.error('Error: products.json not found.');
    process.exit(1);
  }
  return JSON.parse(fs.readFileSync(INPUT_SCRAPE_FILE, 'utf8'));
};

const buildIndex = (referenceData) => {
  console.log('Building search index...');
  return referenceData.map((item) => {
    const combinedText = [item.name, item.name_kk, item.name_origin].filter(Boolean).join(' ');
    const ngrams = createNGrams(combinedText);
    return {
      item,
      ngrams: Array.from(ngrams), // Convert Set to Array for worker transfer
      normalized: normalize(combinedText)
    };
  });
};

const runWorker = (items, indexedRef) => {
  return new Promise((resolve, reject) => {
    const worker = new Worker(__filename, {
      workerData: { items, indexedRef }
    });

    let results = [];
    worker.on('message', (msg) => {
      if (msg.type === 'progress') {
        // Will be handled by main thread
        process.emit('workerProgress', msg.count);
      } else if (msg.type === 'done') {
        results = msg.results;
      }
    });
    worker.on('error', reject);
    worker.on('exit', (code) => {
      if (code !== 0) reject(new Error(`Worker exited with code ${code}`));
      else resolve(results);
    });
  });
};

const main = async () => {
  console.log(`\n🚀 Parallel Product Mapper (${NUM_WORKERS} workers)\n`);
  console.time('Total time');

  console.log('Loading Reference Data...');
  const referenceData = await loadReferenceData();

  console.log('Loading Scraped Data...');
  const scrapedData = loadScrapedData();

  console.log(`Reference: ${referenceData.length} | Scraped: ${scrapedData.length}\n`);

  const indexedRef = buildIndex(referenceData);

  // Split work among workers
  const chunkSize = Math.ceil(scrapedData.length / NUM_WORKERS);
  const chunks = [];
  for (let i = 0; i < scrapedData.length; i += chunkSize) {
    chunks.push(scrapedData.slice(i, i + chunkSize));
  }

  console.log(`Splitting into ${chunks.length} chunks...\n`);

  // Progress tracking
  const progressBar = new ProgressBar(scrapedData.length);
  let processed = 0;
  process.on('workerProgress', (count) => {
    processed += count;
    progressBar.update(processed);
  });

  // Run all workers in parallel
  const workerPromises = chunks.map(chunk => runWorker(chunk, indexedRef));
  const resultsArrays = await Promise.all(workerPromises);

  progressBar.done();

  // Combine results
  const finalData = resultsArrays.flat();
  const matched = finalData.filter(r => r.best_match === 'match').length;

  fs.writeFileSync(OUTPUT_FILE, JSON.stringify(finalData, null, 2), 'utf8');

  console.log(`\n📊 Matched: ${matched}/${finalData.length} (${Math.round(matched/finalData.length*100)}%)`);
  console.log(`📁 Output: ${OUTPUT_FILE}`);
  console.timeEnd('Total time');
};

main().catch(console.error);
