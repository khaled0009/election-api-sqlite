/**
 * Election DB API v3.1 - SQLite + Queue Edition
 *
 * - قراءة الأسماء من ملف SQLite .db بدل Excel
 * - بناء Index في الذاكرة باستخدام Fuse.js (نفس منطق v3)
 * - بحث Fuzzy يدعم الأخطاء الإملائية البسيطة في الأسماء
 * - يرجّع نفس شكل الـ JSON المذكور في README (ok / name_column / total_matches / count / results[])
 * - Cache للطلبات المتكررة
 * - طابور انتظار لتنظيم آلاف الطلبات (MAX_CONCURRENT في نفس الوقت)
 */

const express = require("express");
const bodyParser = require("body-parser");
const fs = require("fs");
const path = require("path");
const Fuse = require("fuse.js");
const Database = require("better-sqlite3");

// ==================== إعدادات عامة ====================
const PORT = process.env.PORT || 3000;
const API_KEY = process.env.API_KEY || "supersecretkey";
const LOG_FILE = "./log.txt";

// مسار ملف SQLite واسم الجدول
// مثال: data/db.sqlite وجدول اسمه voters
const DB_PATH =
  process.env.DB_PATH || path.join(__dirname, "data", "data.db");
const DB_TABLE = process.env.DB_TABLE || "Sheet1"; // غيّره لو اسم الجدول مختلف

// إعدادات الكاش
const CACHE_TTL_MS = 60 * 1000; // 60 ثانية
const cache = new Map(); // key → { ts, data }

// إعدادات الطابور
const MAX_CONCURRENT = Number(process.env.MAX_CONCURRENT || 10); // أقصى عدد طلبات تتنفذ في نفس الوقت
let activeRequests = 0;
const requestQueue = [];

// ==================== دوال مساعدة ====================
function log(message) {
  const line = `[${new Date().toLocaleString()}] ${message}\n`;
  console.log(line.trim());
  try {
    fs.appendFileSync(LOG_FILE, line, "utf8");
  } catch (e) {
    // لو في مشكلة في اللوج، ما نوقفش السيرفر
    console.error("Log write error:", e.message);
  }
}

// تطبيع الحروف العربية وإزالة التشكيل وعلامات الاتجاه
function normalizeArabic(str = "") {
  return String(str)
    // إزالة علامات الاتجاه / التحكم
    .replace(/[\u200e\u200f\u202a-\u202e\u2066-\u2069]/g, "")
    // إزالة التشكيل العربي كله
    .replace(/[\u0610-\u061a\u064b-\u065f\u0670\u06d6-\u06ed]/g, "")
    // توحيد أشكال الألف
    .replace(/[أإآا]/g, "ا")
    // توحيد الياء والألف المقصورة
    .replace(/[ىی]/g, "ي")
    // توحيد الهمزات على واو/ياء
    .replace(/ؤ/g, "و")
    .replace(/ئ/g, "ي");
}

function normalizeText(str = "") {
  return normalizeArabic(str)
    .replace(/[ـ]/g, "") // مدّة
    .replace(/[^\p{Letter}\p{Number}\s]/gu, "") // أي رموز غريبة
    .replace(/\s+/g, " ")
    .trim()
    .toLowerCase();
}

// اكتشاف عمود الاسم من أول صف (نفس فكرة v3)
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

  // fallback: أي عمود اسمه فيه "اسم" بعد التطبيع
  const fallback = keys.find((k) => normalizeText(k).includes("اسم"));
  return fallback || null;
}

// ==================== تحميل الـ DB وبناء الـ Index ====================
let DB = [];
let DB_READY = false;
let NAME_KEY = null;
let FUSE = null;

function loadDatabaseFromSqlite(dbPath, tableName) {
  log(`📊 جاري تحميل قاعدة البيانات من SQLite: ${dbPath} (الجدول: ${tableName})`);

  if (!fs.existsSync(dbPath)) {
    throw new Error(`ملف SQLite غير موجود: ${dbPath}`);
  }

  // فتح قاعدة البيانات للقراءة فقط
  const db = new Database(dbPath, { readonly: true });

  // قراءة كل السجلات من الجدول
  const stmt = db.prepare(`SELECT * FROM ${tableName}`);
  const rows = stmt.all();

  if (!rows.length) {
    db.close();
    throw new Error(`الجدول "${tableName}" لا يحتوي على بيانات`);
  }

  NAME_KEY = detectNameColumn(rows[0]);
  if (!NAME_KEY) {
    db.close();
    throw new Error(
      `تعذر تحديد عمود الاسم في الجدول "${tableName}". تأكد أن هناك عمود باسم 'الأسم' أو 'الاسم' أو 'name'`
    );
  }

  DB = rows.map((row, index) => {
    const name = row[NAME_KEY] || "";
    const norm = normalizeText(name);
    return {
      __id: index + 1, // معرف داخلي
      __name: name, // الاسم الأصلي كما في الـ DB
      searchName: norm, // اسم مُطَبَّع للبحث
      ...row // كل الأعمدة كما هي من الجدول
    };
  });

  log(
    `✅ تم تحميل قاعدة البيانات من SQLite: عدد السجلات = ${DB.length} ، عمود الاسم = "${NAME_KEY}"`
  );

  // بناء Index باستخدام Fuse.js (نفس إعدادات v3 تقريبًا)
  const fuseOptions = {
    includeScore: true,
    keys: ["searchName"],
    threshold: 0.4, // كلما قلّ الرقم كان البحث أدق (0 = تطابق تام)
    ignoreLocation: true, // ما نهتمش بمكان المطابقة في النص
    minMatchCharLength: 2 // أقل طول مقبول للنمط
  };

  FUSE = new Fuse(DB, fuseOptions);

  log("⚙️ تم بناء Index للبحث باستخدام Fuse.js");
  DB_READY = true;

  // خلاص مش محتاجين اتصال مفتوح
  db.close();
}

// ==================== البحث بالاسم مع كاش (نفس المنطق العام) ====================
function searchByName(name, maxResults = 5) {
  if (!DB_READY || !FUSE) {
    throw new Error("قاعدة البيانات غير جاهزة بعد");
  }

  const queryNorm = normalizeText(name);
  if (!queryNorm) {
    return { totalMatches: 0, results: [] };
  }

  const cacheKey = `${queryNorm}::${maxResults}`;
  const now = Date.now();

  // كاش للطلبات المتكررة
  const cached = cache.get(cacheKey);
  if (cached && now - cached.ts < CACHE_TTL_MS) {
    return cached.data;
  }

  // نطلب عدد أكبر داخليًا ثم نقصّه
  const fuseLimit = Math.max(maxResults, 10);
  const fuseResults = FUSE.search(queryNorm, { limit: fuseLimit });

  const mapped = fuseResults.map((item) => {
    const rec = item.item;
    const score = item.score != null ? item.score : 0;
    const simScore = 1 - Math.min(Math.max(score, 0), 1); // تحويل 0=أفضل → 1=أفضل

    return {
      __score: Number(simScore.toFixed(3)),
      ...rec
    };
  });

  const totalMatches = mapped.length;
  const top = mapped.slice(0, maxResults);

  const data = { totalMatches, results: top };
  cache.set(cacheKey, { ts: now, data });

  return data;
}

// ==================== طابور الانتظار ====================
function processQueue() {
  // شغّل لحد ما نوصل للحد الأقصى المتوازي
  while (activeRequests < MAX_CONCURRENT && requestQueue.length > 0) {
    const task = requestQueue.shift();
    activeRequests++;

    task()
      .catch((err) => {
        log(`❌ خطأ غير متوقع داخل مهمة في الطابور: ${err.message}`);
      })
      .finally(() => {
        activeRequests--;
        processQueue(); // شغّل اللي بعده
      });
  }
}

function enqueueRequest(task) {
  requestQueue.push(task);
  log(
    `🕓 تمت إضافة استعلام جديد للطابور (الطول الحالي: ${requestQueue.length} | النشط حاليًا: ${activeRequests})`
  );
  processQueue();
}

// ==================== إعداد السيرفر ====================
const app = express();
app.use(bodyParser.json());

// Middleware للتحقق من الـ API Key (نفس الفكرة)
app.use((req, res, next) => {
  const key = req.headers["x-api-key"];
  if (key !== API_KEY) {
    log(`🚫 محاولة دخول غير مصرح بها من ${req.ip}`);
    return res.status(403).json({ ok: false, message: "Invalid API key" });
  }
  next();
});

// Endpoint رئيسي للاستعلام بالاسم (يدخل الطابور)
app.post("/query", (req, res) => {
  const { name, limit } = req.body;

  if (!name || typeof name !== "string" || name.trim().length < 2) {
    log(`⚠️ طلب غير صالح: ${JSON.stringify(req.body)}`);
    return res
      .status(400)
      .json({ ok: false, message: "الاسم مطلوب ويجب ألا يقل عن حرفين" });
  }

  const maxResults =
    typeof limit === "number" && limit > 0 && limit <= 50 ? limit : 5;

  enqueueRequest(async () => {
    try {
      if (!DB_READY) {
        log("⚠️ طلب وارد قبل جاهزية قاعدة البيانات");
        if (!res.headersSent) {
          res.status(503).json({
            ok: false,
            message:
              "قاعدة البيانات لم تجهز بعد، تأكد من وجود ملف DB الصحيح وإعادة تشغيل السيرفر"
          });
        }
        return;
      }

      const { totalMatches, results } = searchByName(name, maxResults);
      log(
        `🔎 استعلام بالاسم: "${name}" → إجمالي مطابقات تقريبية: ${totalMatches} | المعاد: ${results.length}`
      );

      if (!res.headersSent) {
        // نفس شكل الـ JSON الموجود في README
        res.json({
          ok: true,
          name_column: NAME_KEY,
          total_matches: totalMatches,
          count: results.length,
          results
        });
      }
    } catch (err) {
      log(`❌ خطأ أثناء البحث بالاسم "${name}": ${err.message}`);
      if (!res.headersSent) {
        res
          .status(500)
          .json({ ok: false, message: "حدث خطأ أثناء تنفيذ الاستعلام" });
      }
    }
  });
});

// Endpoint بسيط للفحص
app.get("/", (req, res) =>
  res.send(
    "✅ Election DB API v3.1 جاهز. استخدم POST /query مع x-api-key و name. (مصدر البيانات: SQLite + طابور انتظار)"
  )
);

// ==================== بدء التشغيل ====================
app.listen(PORT, () => {
  log(`🚀 السيرفر شغال على http://localhost:${PORT}`);
  log(`🔑 استخدم API Key: ${API_KEY}`);
  log(`📁 DB_PATH = ${DB_PATH} | DB_TABLE = ${DB_TABLE}`);
  log(`📌 MAX_CONCURRENT = ${MAX_CONCURRENT}`);

  try {
    loadDatabaseFromSqlite(DB_PATH, DB_TABLE);
  } catch (err) {
    log(`❌ فشل تحميل قاعدة البيانات من SQLite: ${err.message}`);
    DB_READY = false;
  }
});
