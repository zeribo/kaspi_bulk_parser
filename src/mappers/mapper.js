const fs = require('fs');
const csv = require('csv-parser');
const { v4: uuidv4 } = require('uuid');
const Fuse = require('fuse.js');

// ==========================================
// CONFIGURATION
// ==========================================
const INPUT_SCRAPE_FILE = 'products.json';
const INPUT_CSV_REFERENCE = 'PRODUCTS_TO_MATCH.csv';
const OUTPUT_FILE = '../../mapped_data.json';

const CITY = 'almaty';
const SOURCE = 'kaspi_parser';

// ==========================================
// HELPERS
// ==========================================

const extractWeightFromTitle = (title) => {
    if (!title) return null;
    const match = title.match(/(\d+(?:\.\d+)?)\s*(г|кг|мл|л|гр|кгр|млр|лр)/i);
    if (match) {
        let unit = match[2];
        if(unit === 'гр') unit = 'г';
        if(unit === 'кгр') unit = 'кг';
        if(unit === 'млр') unit = 'мл';
        if(unit === 'лр') unit = 'л';
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

// ==========================================
// DATA LOADING
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
        console.error('Error: products.json not found. Run npm run scrape first.');
        process.exit(1);
    }
    return JSON.parse(fs.readFileSync(INPUT_SCRAPE_FILE, 'utf8'));
};

// ==========================================
// MAPPING LOGIC
// ==========================================

const transformData = async () => {
    console.log('Loading Reference Data...');
    const referenceData = await loadReferenceData();
    
    console.log('Loading Scraped Data...');
    const scrapedData = await loadScrapedData();

    console.log(`Reference: ${referenceData.length} items. Scraped: ${scrapedData.length} items.`);
    
    // Initialize Fuse.js for fuzzy matching
    // OPTIMIZATION 1: Stricter threshold = Faster, more accurate search
    // OPTIMIZATION 2: ignoreLocation = true (Speed boost)
    const fuse = new Fuse(referenceData, {
        keys: ['name', 'name_kk', 'name_origin'],
        threshold: 0.3, 
        includeScore: true,
        ignoreLocation: true, // Ignores spaces in words
        shouldSort: true 
    });

    const now = new Date().toISOString();
    const finalData = [];

    console.log('Matching and transforming...');
    
    // OPTIMIZATION 3: Progress Indicator
    for (let i = 0; i < scrapedData.length; i++) {
        const item = scrapedData[i];
        
        // Print progress every 50 items
        if (i > 0 && i % 50 === 0) {
            console.log(`   Processing... ${i}/${scrapedData.length}`);
        }
        
        // 1. Fuzzy Search
        // OPTIMIZATION 4: Limit results to 1 (Stop searching after first match - Huge Speedup)
        const searchResults = fuse.search(item.name, { limit: 1 });
        const bestMatch = searchResults.length > 0 ? searchResults[0].item : null;
        const matchScore = searchResults.length > 0 ? searchResults[0].score : null;

        // 2. Handle Weight Logic (Priority 1: Title, 2: CSV, 3: API)
        let weightString = '';
        const titleWeight = extractWeightFromTitle(item.name);
        
        if (titleWeight) {
            weightString = titleWeight;
        } else if (bestMatch && bestMatch.weight) {
            weightString = bestMatch.weight; 
        } else {
            let weightVal = item.weight || 0;
            if (weightVal > 0) {
                const grams = Math.round(weightVal * 1000);
                weightString = `${grams} г`;
            }
        }

        const measure = getMeasure(weightString);

        // 3. Determine Prices
        const price = item.regularPrice || 0;
        const originalPrice = item.unitPriceBeforeDiscount || 0; 
        const discount = item.discount || 0; 
        
        // 4. Construct Final Object
        const record = {
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
            // Matching Metadata
            matched_csv_title: bestMatch ? bestMatch.name : "UNMATCHED",
            match_confidence: bestMatch ? Math.round((1 - matchScore) * 100) : 0,
            matched_uuid: bestMatch ? bestMatch.uuid : null,
            best_match: bestMatch ? "match" : "none",
            mappingCreatedAt: now
        };

        finalData.push(record);
    }

    // Write to JSON File
    fs.writeFileSync(OUTPUT_FILE, JSON.stringify(finalData, null, 2), 'utf8');
    console.log(`\nDone! Mapped ${finalData.length} items to ${OUTPUT_FILE}`);
};

transformData().catch(console.error);