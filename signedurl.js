const { Storage } = require('@google-cloud/storage');
require('dotenv').config();

if (!process.env.GCS_KEY_BASE64) {
  throw new Error('Missing GCS_KEY_BASE64 in environment variables');
}

// Decode Base64 service account JSON
const saKey = JSON.parse(
  Buffer.from(process.env.GCS_KEY_BASE64, 'base64').toString('utf8')
);

// Setup GCS client
const storage = new Storage({
  credentials: saKey,
  projectId: process.env.GCS_PROJECT_ID,
});

const bucket = storage.bucket(process.env.GCS_BUCKET_NAME);

/**
 * Generate a signed GET URL for a GCS object.
 * @param {string} gcsPath - Object path inside bucket
 * @param {number} [ttlMs=300000] - Expiry in ms (default 5 min)
 */
const getSignedUrl = async (gcsPath, ttlMs = 5 * 60 * 1000) => {
  const [url] = await bucket.file(gcsPath).getSignedUrl({
    action: 'read',
    expires: Date.now() + ttlMs,
  });
  return url;
};

module.exports = { getSignedUrl };
