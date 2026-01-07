import { v4 as uuidv4 } from 'uuid';
import { MERCHANT } from './config.js';
import { extractMeasureAndWeight, fixProductUrl, toIntSafe } from './utils.js';

export const buildProductObject = (item, city) => {
  const now = new Date().toISOString();
  const id = item.id ? String(item.id) : uuidv4();

  const previewImages = Array.isArray(item.previewImages) ? item.previewImages : [];
  const images = previewImages
    .map(img => img.large || img.url || null)
    .filter(Boolean);

  // Extract measure and weight/volume from title
  const { measure, weight, volume } = extractMeasureAndWeight(item.title);

  // Fix product URLs to include /shop
  const baseUrl = item.shopLink ? `https://kaspi.kz${item.shopLink}` : (item.url || null);
  const fixedUrl = fixProductUrl(baseUrl);

  return {
    _id: uuidv4(),
    mercant_id: MERCHANT.ID,
    mercant_name: MERCHANT.NAME,

    product_id: null,
    id: id,

    title: item.title || null,
    description: null,

    url: fixedUrl,
    url_picture: images.length ? images[0] : null,

    category_full_path: item.category ? item.category.join(' > ') : null,
    brand: item.brand || null,
    sub_category: null,

    time_scrap: now,
    measure: measure,
    city: city,

    matched_uuid: null,

    price: toIntSafe(item.unitPrice || item.price || 0),
    originalPrice: toIntSafe(
      item.unitSalePrice ||
      item.unitPriceBeforeDiscount ||
      item.unitPrice ||
      item.price ||
      0
    ),

    discount: toIntSafe(item.discount || 0),
    currency: MERCHANT.CURRENCY,

    images: images,

    inStock: item.inStock == null ? true : Boolean(item.inStock),
    stockQuantity: null,

    weight: weight,
    volume: volume,

    characteristics: [],

    rating: item.rating == null ? null : item.rating,
    reviewCount: toIntSafe(item.reviewsQuantity || 0),

    productUrl: fixedUrl,
    productId: id,

    parsedAt: now,
    lastUpdated: now,

    source: MERCHANT.SOURCE,
    isActive: true,

    parsingErrors: [],

    createdAt: now,
    updatedAt: now,

    __v: 0
  };
};