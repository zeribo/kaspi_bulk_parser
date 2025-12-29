require('dotenv').config();
const mongoose = require('mongoose');
const fs = require('fs');

// ==========================================
// CONFIGURATION (Using Env Vars)
// ==========================================

const MONGO_USER = process.env.MONGO_LOGIN;
const MONGO_PASS = process.env.MONGO_PASSWORD;
const MONGO_HOST = process.env.MONGO_HOST || 'localhost';
const MONGO_PORT = process.env.MONGO_PORT || 27017;
const MONGO_DB = process.env.MONGO_DATABASE;
const MONGO_AUTH_DB = process.env.MONGO_AUTH_DATABASE;

const MONGO_URI = `mongodb://${MONGO_USER}:${MONGO_PASS}@${MONGO_HOST}:${MONGO_PORT}/${MONGO_DB}?authSource=${MONGO_AUTH_DB}`;

const INPUT_FILE = './mapped_data.json';

// ==========================================
// SCHEMA DEFINITION
// ==========================================

const ProductSchema = new mongoose.Schema({
    external_id: { type: String, unique: true }, 
    mercant_id: String,
    mercant_name: String,
    product_id: String,
    id: String,
    title: String,
    description: String,
    url: String,
    url_picture: String,
    category_full_path: String,
    brand: String,
    sub_category: String,
    time_scrap: Date,
    measure: String,
    city: String,
    price: Number,
    originalPrice: Number,
    discount: Number,
    currency: String,
    inStock: Boolean,
    weight: String,
    reviewCount: Number,
    productUrl: String,
    productId: String,
    parsedAt: Date,
    lastUpdated: Date,
    source: String,
    isActive: Boolean,
    createdAt: Date,
    updatedAt: Date,
    // Matching Metadata
    matched_csv_title: String,
    match_confidence: Number,
    matched_uuid: String,
    best_match: String,
    mappingCreatedAt: Date
});

const Product = mongoose.model('Product', ProductSchema);

// ==========================================
// MAIN FUNCTION
// ==========================================

async function loadToMongo() {
    try {
        console.log(`Connecting to MongoDB at ${MONGO_HOST}...`);
        await mongoose.connect(MONGO_URI);
        console.log('Connected successfully.');

        if (!fs.existsSync(INPUT_FILE)) {
            console.error(`Error: ${INPUT_FILE} not found. Run 'npm run map' first.`);
            process.exit(1);
        }

        console.log(`Reading ${INPUT_FILE}...`);
        const rawData = JSON.parse(fs.readFileSync(INPUT_FILE, 'utf8'));

        // 2. Transform Data (FIX: Destructure to exclude _id)
        // { _id, ...rest } separates the UUID from the rest of the object
        const dataToInsert = rawData.map(item => {
            const { _id, ...rest } = item;
            return {
                ...rest, // Insert all fields EXCEPT _id
                external_id: _id // Add UUID back with correct name
            };
        });

        // 3. Clean old data
        console.log('Cleaning old database records...');
        await Product.deleteMany({}); 

        // 4. Insert Data
        console.log(`Inserting ${dataToInsert.length} records...`);
        const result = await Product.insertMany(dataToInsert);

        console.log(`\nSuccess! Inserted ${result.length} items into MongoDB.`);

    } catch (error) {
        console.error('Error loading to MongoDB:', error.message);
    } finally {
        await mongoose.disconnect();
        console.log('Disconnected from MongoDB.');
    }
}

loadToMongo();