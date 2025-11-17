/**
 * Election DB API v3.2 - SQLite Direct Search Edition
 *
 * - Direct SQLite search without loading all data into memory
 * - Maintains same API interface and JSON response format
 * - Supports partial search with Arabic normalization
 * - Queue system for managing concurrent requests
 * - Professional English logging with request/response tracking
 */

const express = require("express");
const bodyParser = require("body-parser");
const fs = require("fs");
const path = require("path");
const Database = require("better-sqlite3");

// ==================== Configuration ====================
const PORT = process.env.PORT || 3000;
const API_KEY = process.env.API_KEY || "supersecretkey";
const LOG_FILE = "./log.txt";

const DB_PATH = process.env.DB_PATH || path.join(__dirname, "data", "data.db");
const DB_TABLE = process.env.DB_TABLE || "Sheet1";

// Cache settings
const CACHE_TTL_MS = 60 * 1000; // 60 seconds
const cache = new Map();

// Queue settings
const MAX_CONCURRENT = Number(process.env.MAX_CONCURRENT || 10);
let activeRequests = 0;
const requestQueue = [];

// Request tracking
let requestCounter = 0;

// ==================== Helper Functions ====================
function log(message) {
  const line = `[${new Date().toISOString()}] ${message}\n`;
  console.log(line.trim());
  try {
    fs.appendFileSync(LOG_FILE, line, "utf8");
  } catch (e) {
    console.error("Log write error:", e.message);
  }
}

function generateRequestId() {
  return `REQ-${Date.now()}-${(++requestCounter).toString().padStart(6, '0')}`;
}

function normalizeArabic(str = "") {
  return String(str)
      .replace(/[\u200e\u200f\u202a-\u202e\u2066-\u2069]/g, "") // Remove directional marks
      .replace(/[\u0610-\u061a\u064b-\u065f\u0670\u06d6-\u06ed]/g, "") // Remove diacritics
      .replace(/[أإآا]/g, "ا") // Normalize alef variations
      .replace(/[ىی]/g, "ي") // Normalize yaa variations
      .replace(/ؤ/g, "و") // Normalize waw with hamza
      .replace(/ئ/g, "ي") // Normalize yaa with hamza
      .replace(/ة/g, "ه"); // Normalize taa marbouta
}

function normalizeText(str = "") {
  return normalizeArabic(str)
      .replace(/[ـ]/g, "") // Remove tatweel
      .replace(/[^\p{Letter}\p{Number}\s]/gu, "") // Keep only letters, numbers, spaces
      .replace(/\s+/g, " ")
      .trim()
      .toLowerCase();
}

// ==================== Database Management ====================
let db = null;
let NAME_KEY = null;
let TOTAL_RECORDS = 0;

function initDatabase() {
  log(`INFO: Initializing database connection to: ${DB_PATH}`);

  if (!fs.existsSync(DB_PATH)) {
    throw new Error(`Database file not found: ${DB_PATH}`);
  }

  // Open read-only connection
  db = new Database(DB_PATH, { readonly: true });
  log("INFO: SQLite database connection established");

  // Get total record count
  const countStmt = db.prepare(`SELECT COUNT(*) as count FROM ${DB_TABLE}`);
  TOTAL_RECORDS = countStmt.get().count;
  log(`INFO: Total records in table '${DB_TABLE}': ${TOTAL_RECORDS}`);

  // Detect name column from first record
  const firstRow = db.prepare(`SELECT * FROM ${DB_TABLE} LIMIT 1`).get();
  if (!firstRow) {
    throw new Error(`Table '${DB_TABLE}' contains no data`);
  }

  NAME_KEY = detectNameColumn(firstRow);
  if (!NAME_KEY) {
    throw new Error(
        `Failed to detect name column in table '${DB_TABLE}'. Ensure column exists with name like 'الأسم', 'الاسم', or 'name'`
    );
  }

  log(`INFO: Database initialized successfully | Name column: '${NAME_KEY}' | Total records: ${TOTAL_RECORDS}`);
}

function detectNameColumn(row) {
  const keys = Object.keys(row);
  if (!keys.length) return null;

  const possible = ["الاسم", "الأسم", "اسم", "name", "full name"];

  for (const key of keys) {
    const n = normalizeText(key);
    for (const p of possible) {
      if (n.includes(normalizeText(p))) {
        return key;
      }
    }
  }

  const fallback = keys.find((k) => normalizeText(k).includes("اسم"));
  return fallback || null;
}

// ==================== Direct SQLite Search ====================
function searchByName(name, maxResults = 5, requestId = '') {
  if (!db || !NAME_KEY) {
    throw new Error("Database not ready");
  }

  const queryNorm = normalizeText(name);
  if (!queryNorm) {
    return { totalMatches: 0, results: [] };
  }

  const cacheKey = `${queryNorm}::${maxResults}`;
  const now = Date.now();

  // Check cache
  const cached = cache.get(cacheKey);
  if (cached && now - cached.ts < CACHE_TTL_MS) {
    log(`INFO: [${requestId}] Cache hit for query: '${name}'`);
    return cached.data;
  }

  log(`INFO: [${requestId}] Executing database search for: '${name}'`);

  // Split name into parts for flexible search
  const nameParts = queryNorm.split(' ').filter(part => part.length > 1);

  // Build SQL query with LIKE for each part
  let conditions = [];
  let params = [];

  // Search for each part of the name
  nameParts.forEach(part => {
    conditions.push(`LOWER(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(${NAME_KEY}, 'أ', 'ا'), 'إ', 'ا'), 'آ', 'ا'), 'ى', 'ي'), 'ة', 'ه')) LIKE ?`);
    params.push(`%${part}%`);
  });

  // Query to get results
  const query = `
    SELECT * FROM ${DB_TABLE}
    WHERE ${conditions.join(' AND ')}
    LIMIT ${maxResults * 2}
  `;

  try {
    const startTime = Date.now();
    const stmt = db.prepare(query);
    const results = stmt.all(...params);
    const queryTime = Date.now() - startTime;

    log(`INFO: [${requestId}] Database query executed in ${queryTime}ms | Found ${results.length} records`);

    // Calculate match score for each result
    const scoredResults = results.map((row, index) => {
      const rowName = normalizeText(row[NAME_KEY] || '');
      let score = 0;

      // Calculate score based on matching parts
      nameParts.forEach(part => {
        if (rowName.includes(part)) {
          score += 1 / nameParts.length;
        }
      });

      // Bonus for exact match
      if (rowName === queryNorm) {
        score = 1;
      }

      return {
        __score: Number(score.toFixed(3)),
        __id: index + 1,
        ...row,
        searchName: rowName // For backward compatibility
      };
    });

    // Sort by score and take requested number
    scoredResults.sort((a, b) => b.__score - a.__score);
    const topResults = scoredResults.slice(0, maxResults);

    const data = {
      totalMatches: results.length,
      results: topResults
    };

    // Save to cache
    cache.set(cacheKey, { ts: now, data });
    log(`INFO: [${requestId}] Results cached for future queries`);

    return data;
  } catch (err) {
    log(`ERROR: [${requestId}] Database search failed: ${err.message}`);
    throw err;
  }
}

// ==================== Request Queue ====================
function processQueue() {
  while (activeRequests < MAX_CONCURRENT && requestQueue.length > 0) {
    const task = requestQueue.shift();
    activeRequests++;

    task()
        .catch((err) => {
          log(`ERROR: Queue task failed: ${err.message}`);
        })
        .finally(() => {
          activeRequests--;
          processQueue();
        });
  }
}

function enqueueRequest(task) {
  requestQueue.push(task);
  log(`INFO: Request queued | Queue length: ${requestQueue.length} | Active requests: ${activeRequests}`);
  processQueue();
}

// ==================== Express Server Setup ====================
const app = express();
app.use(bodyParser.json());

// Middleware for API key validation
app.use((req, res, next) => {
  // Skip API key check for health check endpoint
  if (req.path === '/') {
    return next();
  }

  const key = req.headers["x-api-key"];
  if (key !== API_KEY) {
    const ip = req.headers['x-forwarded-for'] || req.connection.remoteAddress;
    log(`WARN: Unauthorized access attempt from IP: ${ip} | Path: ${req.path}`);
    return res.status(403).json({ ok: false, message: "Invalid API key" });
  }
  next();
});

// Main query endpoint
app.post("/query", (req, res) => {
  const requestId = generateRequestId();
  const clientIp = req.headers['x-forwarded-for'] || req.connection.remoteAddress;
  const startTime = Date.now();

  // Log incoming request
  log(`INFO: [${requestId}] Incoming request from ${clientIp} | Body: ${JSON.stringify(req.body)}`);

  const { name, limit } = req.body;

  // Validate input
  if (!name || typeof name !== "string" || name.trim().length < 2) {
    const errorMsg = "Name is required and must be at least 2 characters";
    log(`WARN: [${requestId}] Invalid request: ${errorMsg}`);
    return res
        .status(400)
        .json({ ok: false, message: errorMsg });
  }

  const maxResults =
      typeof limit === "number" && limit > 0 && limit <= 50 ? limit : 5;

  enqueueRequest(async () => {
    try {
      log(`INFO: [${requestId}] Processing query for name: '${name}' | Limit: ${maxResults}`);

      const { totalMatches, results } = searchByName(name, maxResults, requestId);
      const processingTime = Date.now() - startTime;

      log(`INFO: [${requestId}] Query completed successfully | Total matches: ${totalMatches} | Returned: ${results.length} | Processing time: ${processingTime}ms`);

      if (!res.headersSent) {
        res.json({
          ok: true,
          name_column: NAME_KEY,
          total_matches: totalMatches,
          count: results.length,
          results
        });

        log(`INFO: [${requestId}] Response sent successfully | Status: 200`);
      }
    } catch (err) {
      const processingTime = Date.now() - startTime;
      log(`ERROR: [${requestId}] Query failed for name: '${name}' | Error: ${err.message} | Processing time: ${processingTime}ms`);

      if (!res.headersSent) {
        res
            .status(500)
            .json({ ok: false, message: "An error occurred while processing the query" });

        log(`INFO: [${requestId}] Error response sent | Status: 500`);
      }
    }
  });
});

// Health check endpoint
app.get("/", (req, res) => {
  const clientIp = req.headers['x-forwarded-for'] || req.connection.remoteAddress;
  log(`INFO: Health check request from ${clientIp}`);
  res.send(
      "✅ Election DB API v3.2 is running. Use POST /query with x-api-key and name."
  );
});

// ==================== Server Startup ====================
app.listen(PORT, () => {
  log("=====================================");
  log("INFO: Election DB API v3.2 Starting");
  log("=====================================");
  log(`INFO: Server running on http://localhost:${PORT}`);
  log(`INFO: API Key configured: ${API_KEY.substring(0, 4)}****`);
  log(`INFO: Database path: ${DB_PATH}`);
  log(`INFO: Database table: ${DB_TABLE}`);
  log(`INFO: Max concurrent requests: ${MAX_CONCURRENT}`);

  try {
    initDatabase();
    log("INFO: Server startup completed successfully");
    log("=====================================");
  } catch (err) {
    log(`ERROR: Failed to initialize database: ${err.message}`);
    log("ERROR: Server startup failed - exiting");
    process.exit(1);
  }
});

// Graceful shutdown
process.on('SIGINT', () => {
  log('INFO: Received SIGINT signal, initiating graceful shutdown');
  if (db) {
    db.close();
    log('INFO: Database connection closed');
  }
  log('INFO: Server shutdown completed');
  process.exit(0);
});

process.on('SIGTERM', () => {
  log('INFO: Received SIGTERM signal, initiating graceful shutdown');
  if (db) {
    db.close();
    log('INFO: Database connection closed');
  }
  log('INFO: Server shutdown completed');
  process.exit(0);
});