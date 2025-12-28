const puppeteer = require('puppeteer');
const fs = require('fs');

// ==========================================
// CONFIGURATION
// ==========================================

// 1. MAX PAGES TO PARSE
const MAX_PAGES = 10;

// 2. PASTE YOUR CATEGORY LINKS HERE
const CATEGORY_URLS = [
    'https://kaspi.kz/yml/product-view/pl/results?page=1&q=%3Acategory%3AChild%20goods%3AavailableInZones%3AMagnum_ZONE1&text&sort=relevance&qs&requestId=456805b252b33b8dfd18b4a39ca0ebf7&ui=d&i=-1&c=750000000',
    'https://kaspi.kz/yml/product-view/pl/results?page=1&q=%3Acategory%3ASmartphones%20and%20gadgets%3AavailableInZones%3AMagnum_ZONE1&text&sort=relevance&qs&requestId=d47e85d1cf90960eb88139ee80ec3f47&ui=d&i=-1&c=750000000',
    'https://kaspi.kz/yml/product-view/pl/results?page=1&q=%3Acategory%3AHome%20equipment%3AavailableInZones%3AMagnum_ZONE1&text&sort=relevance&qs&requestId=d9545879440e7b6dc92daf07c2458529&ui=d&i=-1&c=750000000',
    'https://kaspi.kz/yml/product-view/pl/results?page=1&q=%3Acategory%3ATV_Audio%3AavailableInZones%3AMagnum_ZONE1&text&sort=relevance&qs&requestId=4ad0cdc67b7e90f08d12fb778cc30f3a&ui=d&i=-1&c=750000000',
    'https://kaspi.kz/yml/product-view/pl/results?page=1&q=%3Acategory%3AComputers%3AavailableInZones%3AMagnum_ZONE1&text&sort=relevance&qs&requestId=a6bea0a08269e972e228a1be78b2af71&ui=d&i=-1&c=750000000',
    'https://kaspi.kz/yml/product-view/pl/results?page=1&q=%3Acategory%3AFurniture%3AavailableInZones%3AMagnum_ZONE1&text&sort=relevance&qs&requestId=8b7b54b5a1d15f1c3197491744dc1e02&ui=d&i=-1&c=750000000',
    'https://kaspi.kz/yml/product-view/pl/results?page=1&q=%3Acategory%3ABeauty%20care%3AavailableInZones%3AMagnum_ZONE1&text&sort=relevance&qs&requestId=ccc434ab2cb3db42f0c821e2a68850ca&ui=d&i=-1&c=750000000',
    'https://kaspi.kz/yml/product-view/pl/results?page=1&q=%3Acategory%3APharmacy%3AavailableInZones%3AMagnum_ZONE1&text&sort=relevance&qs&requestId=2ad0f0079772658cb69e297bbd6baf82&ui=d&i=-1&c=750000000',
    'https://kaspi.kz/yml/product-view/pl/results?page=1&q=%3Acategory%3AConstruction%20and%20repair%3AavailableInZones%3AMagnum_ZONE1&text&sort=relevance&qs&requestId=b1118cd24aa56a35360f2d478d3a9cd8&ui=d&i=-1&c=750000000',
    'https://kaspi.kz/yml/product-view/pl/results?page=1&q=%3Acategory%3ASports%20and%20outdoors%3AavailableInZones%3AMagnum_ZONE1&text&sort=relevance&qs&requestId=40d4088ec2e71fc8a8d6a574be3bf79c&ui=d&i=-1&c=750000000',
    'https://kaspi.kz/yml/product-view/pl/results?page=1&q=%3Acategory%3ALeisure%3AavailableInZones%3AMagnum_ZONE1&text&sort=relevance&qs&requestId=0555e8a945ae18947a58b3a284abe7f9&ui=d&i=-1&c=750000000',
    'https://kaspi.kz/yml/product-view/pl/results?page=1&q=%3Acategory%3ACar%20goods%3AavailableInZones%3AMagnum_ZONE1&text&sort=relevance&qs&requestId=5f07d539592b6558caff5932fde93a40&ui=d&i=-1&c=750000000',
    'https://kaspi.kz/yml/product-view/pl/results?page=1&q=%3Acategory%3AJewelry%20and%20Bijouterie%3AavailableInZones%3AMagnum_ZONE1&text&sort=relevance&qs&requestId=feb21977a5097c16be75b4921cbed476&ui=d&i=-1&c=750000000',
    'https://kaspi.kz/yml/product-view/pl/results?page=1&q=%3Acategory%3AFashion%20accessories%3AavailableInZones%3AMagnum_ZONE1&text&sort=relevance&qs&requestId=daa77525ab4bd4ee4fb0d05139ecf735&ui=d&i=-1&c=750000000',
    'https://kaspi.kz/yml/product-view/pl/results?page=1&q=%3Acategory%3AFashion%3AavailableInZones%3AMagnum_ZONE1&text&sort=relevance&qs&requestId=aeb29a2b04e1b8e9fbc7e2f5d105b1ad&ui=d&i=-1&c=750000000',
    'https://kaspi.kz/yml/product-view/pl/results?page=1&q=%3Acategory%3AShoes%3AavailableInZones%3AMagnum_ZONE1&text&sort=relevance&qs&requestId=8ceea6b2b60da9a22dd769e3223456c1&ui=d&i=-1&c=750000000',
    'https://kaspi.kz/yml/product-view/pl/results?page=1&q=%3Acategory%3AHome%3AavailableInZones%3AMagnum_ZONE1&text&sort=relevance&qs&requestId=ed00cf2d45d7aa2d98457fdfaa1f548f&ui=d&i=-1&c=750000000',
    'https://kaspi.kz/yml/product-view/pl/results?page=1&q=%3Acategory%3AGifts%20and%20party%20supplies%3AavailableInZones%3AMagnum_ZONE1&text&sort=relevance&qs&requestId=fb2cc3ed8e59cc2322d93407028396f3&ui=d&i=-1&c=750000000',
    'https://kaspi.kz/yml/product-view/pl/results?page=1&q=%3Acategory%3AOffice%20and%20school%20supplies%3AavailableInZones%3AMagnum_ZONE1&text&sort=relevance&qs&requestId=b4aa8479d57254a7d891383e927aedf0&ui=d&i=-1&c=750000000',
    'https://kaspi.kz/yml/product-view/pl/results?page=1&q=%3Acategory%3APet%20goods%3AavailableInZones%3AMagnum_ZONE1&text&sort=relevance&qs&requestId=2ced373bd9f9b73253e76457742a1912&ui=d&i=-1&c=750000000'

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
            
            let page = 0;
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