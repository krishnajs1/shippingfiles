const { Storage } = require('@google-cloud/storage');

require('dotenv').config();

if (!process.env.GCS_KEY_BASE64) {
    throw new Error('Missing GCS_KEY_BASE64 in environment variables');
}

// Decode Base64 → JSON
console.log(process.env)
const saKey = JSON.parse(
    Buffer.from(process.env.GCS_KEY_BASE64, 'base64').toString('utf8')
);

// Setup GCS client
const storage = new Storage({
    credentials: saKey,
    projectId: process.env.GCS_PROJECT_ID
});

const bucket = storage.bucket(process.env.GCS_BUCKET_NAME);

const getSignedUrl = async (gcsPath) => {
    const [url] = await bucket.file(gcsPath).getSignedUrl({
        action: 'read',
        expires: Date.now() + 5 * 60 * 1000 // 5 min expiry
    });
    return url;
};

module.exports = {  getSignedUrl };
