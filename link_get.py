#!/usr/bin/env python3
import json
import urllib.parse

# Your exact file path
INPUT_FILE = "/home/adil/repos/kaspi_bulk_parser/ids.json"
OUTPUT_FILE = "/home/adil/repos/kaspi_bulk_parser/generated_urls.json"

def generate_urls():
    try:
        # 1. Load your JSON file
        with open(INPUT_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        urls = []
        
        # 2. Recursive function to find leaf categories
        def find_leaf_categories(items):
            for item in items:
                # If item has subcategories, go deeper
                if 'items' in item and item['items']:
                    find_leaf_categories(item['items'])
                # If no subcategories, this is a leaf - generate URL
                else:
                    if 'id' in item and item['id'].startswith(':category:'):
                        # Extract category name from ID
                        category = item['id'].replace(':category:', '')
                        # URL encode
                        encoded = urllib.parse.quote(category)
                        # Create URL
                        url = f"https://kaspi.kz/yml/product-view/pl/results?page=1&q=%3Acategory%3A{encoded}%3AavailableInZones%3AMagnum_ZONE1&text&sort=relevance&qs&requestId=c82660e6e6385aad26625b93309e246e&ui=d&i=-1&c=750000000"
                        urls.append(url)
        
        # 3. Start processing
        if isinstance(data, dict) and 'items' in data:
            find_leaf_categories(data['items'])
        elif isinstance(data, list):
            find_leaf_categories(data)
        
        # 4. Output results
        print(json.dumps(urls, indent=2))
        
        # 5. Save to file
        with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
            json.dump(urls, f, indent=2, ensure_ascii=False)
        
        print(f"\n✅ Generated {len(urls)} URLs")
        print(f"✅ Input: {INPUT_FILE}")
        print(f"✅ Output: {OUTPUT_FILE}")
        
    except FileNotFoundError:
        print(f"❌ Error: File not found at {INPUT_FILE}")
    except json.JSONDecodeError as e:
        print(f"❌ Error: Invalid JSON in {INPUT_FILE}")
        print(f"Details: {e}")

if __name__ == "__main__":
    generate_urls()