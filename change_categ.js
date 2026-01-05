const fs = require('fs');

// Configuration
const INPUT_FILE = 'categories.json';
const OUTPUT_FILE = 'categories_modified.json';

// What to change (based on your example)
const OLD_ZONE = 'Magnum_ZONE1';
const NEW_ZONE = 'Magnum_ZONE5';
const OLD_CITY_CODE = '750000000';
const NEW_CITY_CODE = '710000000';

// Function to modify a single URL
function modifyCategoryUrl(url) {
  // Replace the zone
  let modifiedUrl = url.replace(OLD_ZONE, NEW_ZONE);
  
  // Replace the city code
  modifiedUrl = modifiedUrl.replace(`c=${OLD_CITY_CODE}`, `c=${NEW_CITY_CODE}`);
  
  return modifiedUrl;
}

// Main function
function modifyCategoryUrls() {
  try {
    // Read the input JSON file
    const data = fs.readFileSync(INPUT_FILE, 'utf8');
    const urls = JSON.parse(data);
    
    if (!Array.isArray(urls)) {
      console.error('Error: Input file should contain an array of URLs');
      process.exit(1);
    }
    
    console.log(`Found ${urls.length} URLs to process`);
    
    // Modify each URL
    const modifiedUrls = urls.map(url => {
      if (typeof url !== 'string') {
        console.warn('Warning: Skipping non-string URL:', url);
        return url;
      }
      return modifyCategoryUrl(url);
    });
    
    // Save the modified URLs
    fs.writeFileSync(OUTPUT_FILE, JSON.stringify(modifiedUrls, null, 2), 'utf8');
    
    console.log(`✅ Successfully modified ${modifiedUrls.length} URLs`);
    console.log(`✅ Changes made:`);
    console.log(`   - Zone: ${OLD_ZONE} → ${NEW_ZONE}`);
    console.log(`   - City code: ${OLD_CITY_CODE} → ${NEW_CITY_CODE}`);
    console.log(`✅ Saved to: ${OUTPUT_FILE}`);
    
    // Show some examples
    console.log('\n📋 Example modifications:');
    for (let i = 0; i < Math.min(3, urls.length); i++) {
      console.log(`\nOriginal: ${urls[i]}`);
      console.log(`Modified: ${modifiedUrls[i]}`);
    }
    
  } catch (error) {
    console.error('❌ Error:', error.message);
    process.exit(1);
  }
}

// Run the script
modifyCategoryUrls();