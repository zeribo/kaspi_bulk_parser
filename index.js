const puppeteer = require('puppeteer');
const fs = require('fs');

// ==========================================
// CONFIGURATION
// ==========================================

// 1. MAX PAGES TO PARSE
const MAX_PAGES = 10;

// 2. PASTE YOUR CATEGORY LINKS HERE
const CATEGORY_URLS = [
    'https://kaspi.kz/yml/product-view/pl/results?page=1&q=%3Acategory%3AChild%20goods%3AavailableInZones%3AMagnum_ZONE1&text&sort=relevance&qs&requestId=456805b252b33b8dfd18b4a39ca0ebf7&ui=d&i=-1&c=750000000'
    // Add more category links here...
];

// 3. TARGET ZONES (Found in the category JSON)
const TARGET_ZONES = [
    "Magnum_ZONE8",
    "magnum_f_zone",
    "Magnum_ZONE1",
    "Magnum_ZONE2",
    "Magnum_ZONE16"
];

const OUTPUT_FILE = 'products.json';

// ==========================================
// HELPER FUNCTIONS
// ==========================================

/**
 * Helper to fetch JSON with proper Headers to avoid 403
 */
async function fetchJson(browser, url) {
    const page = await browser.newPage();
    try {
        // CRITICAL: Set these headers to mimic a real browser request
        await page.setExtraHTTPHeaders({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36',
            'Referer': 'https://kaspi.kz/', 
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7'
        });
        
        const response = await page.goto(url, { waitUntil: 'networkidle0', timeout: 30000 });
        
        if (!response || response.status() !== 200) {
            console.error(`Failed to load ${url}. Status: ${response ? response.status() : 'No Response'}`);
            return null;
        }

        return await response.json();
    } catch (error) {
        console.error(`Error fetching ${url}:`, error.message);
        return null;
    } finally {
        await page.close();
    }
}

// ==========================================
// MAIN LOGIC
// ==========================================

async function startParsing() {
    console.log('Starting parser...');
    const browser = await puppeteer.launch({ headless: 'new' });
    const allResults = [];

    try {
        for (const categoryUrl of CATEGORY_URLS) {
            console.log(`\n Processing Category: ${categoryUrl}`);
            
            let page = 1;
            let hasProducts = true;

            // Loop while there are products AND we haven't hit the page limit
            while (hasProducts && page <= MAX_PAGES) {
                // Update the page number in the URL
                const paginatedUrl = categoryUrl.replace(/page=\d+/, `page=${page}`);
                
                console.log(`   Fetching Page ${page}...`);
                
                const listData = await fetchJson(browser, paginatedUrl);

                if (!listData || !listData.data || listData.data.length === 0) {
                    console.log(`   No products found on page ${page}. Moving to next category.`);
                    hasProducts = false;
                    break;
                }

                console.log(`   Found ${listData.data.length} items. Filtering by Zones...`);

                for (const item of listData.data) {
                    
                    // 1. FILTER LOGIC: Check if the item has ANY of the target delivery zones
                    const itemZones = item.deliveryZones || [];
                    const hasTargetZone = itemZones.some(zone => TARGET_ZONES.includes(zone));

                    if (hasTargetZone) {
                        // 2. Extract Data
                        const product = {
                            id: item.id,
                            name: item.title,
                            brand: item.brand,
                            // Price Logic
                            regularPrice: item.unitPriceBeforeDiscount || item.unitPrice,
                            promotionalPrice: (item.unitSalePrice < item.unitPrice) ? item.unitSalePrice : null,
                            
                            // Image & Link
                            link: `https://kaspi.kz${item.shopLink}`,
                            image: item.previewImages && item.previewImages.length > 0 ? item.previewImages[0].large : null,

                            // Categories
                            fullCategoryPath: item.category ? item.category.join(' > ') : null,

                            // These fields are often MISSING in the JSON, so we default to null
                            weight: item.weight || null, 
                            description: null, 
                            countryOfOrigin: null 
                        };

                        allResults.push(product);
                        console.log(`      [MATCH] ${item.title} - ${product.regularPrice} KZT`);
                    }
                }

                page++;
                // Small delay to be polite to the server
                await new Promise(resolve => setTimeout(resolve, 500));
            }

            if (page > MAX_PAGES) {
                console.log(`   Reached limit of ${MAX_PAGES} pages.`);
            }
        }
    } catch (error) {
        console.error('Critical Error:', error);
    } finally {
        await browser.close();
        
        // Save to file
        fs.writeFileSync(OUTPUT_FILE, JSON.stringify(allResults, null, 2), 'utf8');
        console.log(`\nParsing Complete! Saved ${allResults.length} items to ${OUTPUT_FILE}`);
    }
}

startParsing();