export const CONFIG = {
  OUTPUT_FILE: './output/products.json',
  CHECKPOINT_INTERVAL: 500,
  REQUEST_DELAY: 150,
  MAX_RETRIES: 4,
  CONCURRENCY: 8,
  PAGE_PARALLEL: 3,
  MAX_PAGE: 300,
};

export const MERCHANT = {
  ID: 'Magnum',
  NAME: 'MAGNUM',
  SOURCE: 'kaspi',
  CURRENCY: 'KZT',
};

export const CITY_MAP = {
  '750000000': 'Almaty',
  '710000000': 'Astana',
};

export const AXIOS_CONFIG = {
  timeout: 15000,
  headers: {
    'User-Agent':
      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36',
    Referer: 'https://kaspi.kz/',
    Accept: 'application/json, text/plain, */*',
    'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
  },
  validateStatus: status => status >= 200 && status < 500,
};