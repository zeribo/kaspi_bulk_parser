#!/usr/bin/env python3
import json
import re

def extract_measurement(title):
    """Extract weight or volume from title."""
    # Patterns for weight (grams/kilograms)
    weight_patterns = [
        r'(\d+(?:\.\d+)?)\s*г\b',      # e.g., 150 г
        r'(\d+(?:\.\d+)?)\s*g\b',      # e.g., 150g
        r'(\d+(?:\.\d+)?)\s*кг\b',     # e.g., 1.5 кг
        r'(\d+(?:\.\d+)?)\s*kg\b',     # e.g., 1.5kg
    ]
    
    # Patterns for volume (liters/milliliters)
    volume_patterns = [
        r'(\d+(?:\.\d+)?)\s*л\b',      # e.g., 0.5 л
        r'(\d+(?:\.\d+)?)\s*l\b',      # e.g., 0.5l
        r'(\d+(?:\.\d+)?)\s*мл\b',     # e.g., 500 мл
        r'(\d+(?:\.\d+)?)\s*ml\b',     # e.g., 500ml
    ]
    
    # Try weight patterns first
    for pattern in weight_patterns:
        match = re.search(pattern, title, re.IGNORECASE)
        if match:
            value = match.group(1)
            unit = match.group(0).replace(value, '').strip()
            if 'кг' in unit or 'kg' in unit:
                return f"{value} кг", 'weight'
            else:
                return f"{value} г", 'weight'
    
    # Try volume patterns
    for pattern in volume_patterns:
        match = re.search(pattern, title, re.IGNORECASE)
        if match:
            value = match.group(1)
            unit = match.group(0).replace(value, '').strip()
            if 'мл' in unit or 'ml' in unit:
                return f"{value} мл", 'volume'
            else:
                return f"{value} л", 'volume'
    
    return None, None

def clean_products(input_file, output_file):
    """Main cleaning function."""
    with open(input_file, 'r', encoding='utf-8') as f:
        products = json.load(f)
    
    cleaned_products = []
    extracted_measurements = 0
    city_changed_count = 0
    
    for product in products:
        cleaned = product.copy()
        title = cleaned.get('title', '')
        
        # Check if weight and volume are null/empty
        if (not cleaned.get('weight') or cleaned.get('weight') in [None, 'null', '']) and \
           (not cleaned.get('volume') or cleaned.get('volume') in [None, 'null', '']):
            
            measurement, measurement_type = extract_measurement(title)
            
            if measurement:
                if measurement_type == 'weight':
                    cleaned['weight'] = measurement
                elif measurement_type == 'volume':
                    cleaned['volume'] = measurement
                extracted_measurements += 1
        
        # Set city to "Almaty" for all products
        cleaned['city'] = "Almaty"
        city_changed_count += 1
        
        # Add to cleaned list
        cleaned_products.append(cleaned)
    
    # Save cleaned products
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(cleaned_products, f, indent=2, ensure_ascii=False)
    
    print(f"✅ Cleaned {len(cleaned_products)} products")
    print(f"✅ Extracted measurements for {extracted_measurements} products")
    print(f"✅ Set city to 'Almaty' for {city_changed_count} products")
    print(f"✅ Saved to {output_file}")

if __name__ == "__main__":
    # Your file paths
    input_path = "/home/adil/repos/kaspi_bulk_parser/products.json"
    output_path = "/home/adil/repos/kaspi_bulk_parser/products_cleaned.json"
    
    clean_products(input_path, output_path)