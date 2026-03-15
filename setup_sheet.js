/**
 * One-time setup script: Create Google Sheet with 4 tabs for Flipkart Scraper.
 * Uses existing service account from flipkart-tracker project.
 */
const fs = require('fs');
const https = require('https');
const crypto = require('crypto');
const path = require('path');

const SA_KEY_PATH = path.join(__dirname, '..', 'flipkart-tracker', 'service-account-key.json');
const sa = JSON.parse(fs.readFileSync(SA_KEY_PATH, 'utf8'));

// Share with this email so you can access the sheet
const OWNER_EMAIL = 'himanshu.s@myfrido.com';

function base64url(data) {
  return Buffer.from(data).toString('base64')
    .replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/g, '');
}

function getToken() {
  return new Promise((resolve, reject) => {
    const now = Math.floor(Date.now() / 1000);
    const header = base64url(JSON.stringify({ alg: 'RS256', typ: 'JWT' }));
    const payload = base64url(JSON.stringify({
      iss: sa.client_email,
      scope: 'https://www.googleapis.com/auth/spreadsheets https://www.googleapis.com/auth/drive',
      aud: 'https://oauth2.googleapis.com/token',
      iat: now, exp: now + 3600
    }));
    const sig = crypto.createSign('RSA-SHA256')
      .update(header + '.' + payload)
      .sign(sa.private_key, 'base64')
      .replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/g, '');
    const jwt = header + '.' + payload + '.' + sig;
    const postData = 'grant_type=urn:ietf:params:oauth:grant-type:jwt-bearer&assertion=' + jwt;

    const req = https.request({
      hostname: 'oauth2.googleapis.com', path: '/token', method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded', 'Content-Length': Buffer.byteLength(postData) }
    }, res => {
      let d = ''; res.on('data', c => d += c);
      res.on('end', () => {
        const p = JSON.parse(d);
        if (p.access_token) resolve(p.access_token);
        else reject(new Error('Token failed: ' + d.substring(0, 200)));
      });
    });
    req.on('error', reject);
    req.write(postData); req.end();
  });
}

function apiCall(method, hostname, apiPath, body, token) {
  return new Promise((resolve, reject) => {
    const bodyStr = body ? JSON.stringify(body) : null;
    const headers = { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json' };
    if (bodyStr) headers['Content-Length'] = Buffer.byteLength(bodyStr);

    const req = https.request({ hostname, path: apiPath, method, headers }, res => {
      let d = ''; res.on('data', c => d += c);
      res.on('end', () => {
        if (res.statusCode >= 400) reject(new Error(`API ${res.statusCode}: ${d.substring(0, 300)}`));
        else resolve(JSON.parse(d || '{}'));
      });
    });
    req.on('error', reject);
    if (bodyStr) req.write(bodyStr);
    req.end();
  });
}

async function main() {
  console.log('Getting access token...');
  const token = await getToken();
  console.log('Authenticated as:', sa.client_email);

  // Step 1: Create blank spreadsheet via Drive API
  console.log('Creating Google Sheet via Drive API...');
  const driveBody = {
    name: 'Flipkart Scraper — Frido',
    mimeType: 'application/vnd.google-apps.spreadsheet'
  };
  const driveResult = await apiCall('POST', 'www.googleapis.com',
    '/drive/v3/files', driveBody, token);
  const sheetId = driveResult.id;
  const sheetUrl = `https://docs.google.com/spreadsheets/d/${sheetId}`;
  console.log('Sheet created!');
  console.log('  ID:', sheetId);
  console.log('  URL:', sheetUrl);

  // Step 2: Rename default "Sheet1" to "FSN Master" and add 3 more tabs
  console.log('Configuring tabs...');
  const meta = await apiCall('GET', 'sheets.googleapis.com',
    `/v4/spreadsheets/${sheetId}?fields=sheets.properties`, null, token);
  const defaultSheetId = meta.sheets[0].properties.sheetId;

  await apiCall('POST', 'sheets.googleapis.com',
    `/v4/spreadsheets/${sheetId}:batchUpdate`, {
      requests: [
        { updateSheetProperties: { properties: { sheetId: defaultSheetId, title: 'FSN Master' }, fields: 'title' } },
        { addSheet: { properties: { title: 'Latest Snapshot' } } },
        { addSheet: { properties: { title: 'Historical Log' } } },
        { addSheet: { properties: { title: 'OOS Alerts' } } },
      ]
    }, token);
  console.log('4 tabs created: FSN Master, Latest Snapshot, Historical Log, OOS Alerts');

  // Write FSN Master headers
  console.log('Writing FSN Master headers...');
  const headers = [
    'Product Title', 'Seller SKU Id', 'Sub-category', 'Flipkart Serial Number',
    'Listing ID', 'Listing Status', 'MRP', 'Your Selling Price',
    'Fulfillment By', 'System Stock count'
  ];
  await apiCall('PUT', 'sheets.googleapis.com',
    `/v4/spreadsheets/${sheetId}/values/${encodeURIComponent("'FSN Master'!A1:J1")}?valueInputOption=RAW`,
    { values: [headers] }, token);
  console.log('FSN Master headers written');

  // Share with owner
  console.log(`Sharing with ${OWNER_EMAIL}...`);
  await apiCall('POST', 'www.googleapis.com',
    `/drive/v3/files/${sheetId}/permissions`,
    { type: 'user', role: 'writer', emailAddress: OWNER_EMAIL },
    token);
  console.log('Shared successfully!');

  // Output for setting secrets
  console.log('\n=== SETUP INFO ===');
  console.log('GOOGLE_SHEET_ID=' + sheetId);
  console.log('Sheet URL: ' + sheetUrl);
  console.log('\nNext steps:');
  console.log('1. Open the sheet URL above');
  console.log('2. Paste your Flipkart seller panel data into the "FSN Master" tab (starting row 2)');
  console.log('3. Set GOOGLE_SHEET_ID as a GitHub Secret');
}

main().catch(e => { console.error('Error:', e.message); process.exit(1); });
