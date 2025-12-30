const axios = require('axios');
const fs = require('fs');
const { v4: uuidv4 } = require('uuid'); 

// ==========================================
// CONFIGURATION
// ==========================================
const CATEGORY_URLS = require('./categories.json');

const OUTPUT_FILE = 'products.json';
const CHECKPOINT_INTERVAL = 500;
const REQUEST_DELAY = 300;
const MAX_RETRIES = 3;

// ==========================================
// AXIOS CONFIGURATION
// ==========================================
const axiosConfig = {
    timeout: 15000,
    headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36',
        'Referer': 'https://kaspi.kz/', 
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7'
    }
};

// ==========================================
// DEDUPLICATION
// ==========================================
const seenIds = new Set();

// ==========================================
// FILTER FUNCTION
// ==========================================
const isMagnumProduct = (item) => {
    const stickers = item.stickers || [];
    const hasMagnumSticker = stickers.includes('magnum_offer_available');
    
    const merchants = item.majorMerchants || [];
    const hasMagnumMerchant = merchants.includes('Magnum');
    
    return hasMagnumSticker || hasMagnumMerchant;
};

// ==========================================
// CHECKPOINT SAVE
// ==========================================
const saveCheckpoint = (data) => {
    fs.writeFileSync(OUTPUT_FILE, JSON.stringify(data, null, 2), 'utf8');
    console.log(`   💾 [CHECKPOINT] Saved ${data.length} items`);
};

// ==========================================
// FETCH WITH RETRY
// ==========================================
const fetchWithRetry = async (url, retries = MAX_RETRIES) => {
    for (let attempt = 1; attempt <= retries; attempt++) {
        try {
            const response = await axios.get(url, axiosConfig);
            if (response.status === 200) return response.data;
        } catch (err) {
            console.error(`   ⚠️ Attempt ${attempt}/${retries} failed: ${err.message}`);
            if (attempt < retries) {
                const backoff = attempt * 1000;
                console.log(`   Waiting ${backoff}ms before retry...`);
                await new Promise(r => setTimeout(r, backoff));
            }
        }
    }
    return null;
};

// ==========================================
// MAIN LOGIC
// ==========================================
async function startParsing() {
    console.log('🚀 Starting Kaspi Magnum Parser...\n');
    const allResults = [];
    let totalCategories = CATEGORY_URLS.length;
    let currentCategory = 0;

    try {
        for (const categoryUrl of CATEGORY_URLS) {
            currentCategory++;
            console.log(`\n📂 [${currentCategory}/${totalCategories}] Processing Category...`);
            
            let page = 0;
            let hasProducts = true;
            let consecutiveEmpty = 0;

            while (hasProducts) {
                let paginatedUrl = categoryUrl.replace(/page=\d+/, `page=${page}`);
                paginatedUrl = paginatedUrl.replace(/requestId=[^&]+/, `requestId=${uuidv4()}`);

                console.log(`   📄 Page ${page}...`);
                
                const listData = await fetchWithRetry(paginatedUrl);
                
                if (!listData || !listData.data || listData.data.length === 0) {
                    consecutiveEmpty++;
                    if (consecutiveEmpty >= 2) {
                        console.log(`   ✅ Category complete (${page} pages)`);
                        hasProducts = false;
                    }
                    break;
                }
                
                consecutiveEmpty = 0;
                let pageMatches = 0;

                for (const item of listData.data) {
                    if (seenIds.has(item.id)) continue;
                    
                    if (isMagnumProduct(item)) {
                        seenIds.add(item.id);
                        
                        const product = {
                            id: item.id,
                            name: item.title,
                            brand: item.brand,
                            regularPrice: item.unitPrice || 0,
                            originalPrice: item.unitSalePrice || 0,
                            link: `https://kaspi.kz${item.shopLink}`,
                            image: item.previewImages && item.previewImages.length > 0 ? item.previewImages[0].large : null,
                            fullCategoryPath: item.category ? item.category.join(' > ') : null,
                            weight: item.weight || null, 
                            priceMinusBonus: item.priceMinusBonus || 0,
                            reviewsQuantity: item.reviewsQuantity || 0,
                            unitPriceBeforeDiscount: item.unitPriceBeforeDiscount || 0,
                            discount: item.discount || 0
                        };

                        allResults.push(product);
                        pageMatches++;
                        
                        if (allResults.length % CHECKPOINT_INTERVAL === 0) {
                            saveCheckpoint(allResults);
                        }
                    }
                }
                
                console.log(`      Found ${pageMatches} new Magnum items (Total: ${allResults.length})`);

                page++;
                await new Promise(r => setTimeout(r, REQUEST_DELAY));
            }
        }
    } catch (error) {
        console.error('\n❌ Critical Error:', error.message);
    } finally {
        fs.writeFileSync(OUTPUT_FILE, JSON.stringify(allResults, null, 2), 'utf8');
        console.log(`\n✅ Done! Saved ${allResults.length} unique items to ${OUTPUT_FILE}`);
    }
}

startParsing();
