/**
 * 🌐 CyraxMods WebApp — Локальный HTTP-сервер
 *
 * Раздаёт статические файлы WebApp и предоставляет API
 * для получения данных пользователя из SQLite базы бота.
 *
 * Запуск: node webapp/server.js
 * Порт: 5500 (по умолчанию)
 *
 * API:
 *   GET /api/user/:id — профиль пользователя (баланс, ключи, купоны, статистика)
 */

const express = require('express');
const path = require('path');
const sqlite3 = require('sqlite3').verbose();

const app = express();
const PORT = process.env.WEBAPP_PORT || 5500;

// ==========================================
// 🗄️ ПОДКЛЮЧЕНИЕ К БАЗЕ ДАННЫХ БОТА
// ==========================================
// Используем ту же БД, что и бот (shop.db).
// Путь берём из переменной окружения или ищем в /tmp (Render) или рядом с ботом.
const DB_PATH = process.env.DB_PATH || path.join(__dirname, '..', 'shop.db');

let db;
try {
  db = new sqlite3.Database(DB_PATH, sqlite3.OPEN_READONLY, (err) => {
    if (err) {
      console.warn(`⚠️ Не удалось открыть БД по пути ${DB_PATH}: ${err.message}`);
      console.warn('ℹ️ API /api/user/:id будет возвращать заглушки. Для полной работы запустите бота и укажите DB_PATH.');
      db = null;
    } else {
      console.log(`✅ БД подключена: ${DB_PATH}`);
    }
  });
} catch (e) {
  console.warn('⚠️ Ошибка подключения к БД:', e.message);
  db = null;
}

// ==========================================
// 📂 СТАТИЧЕСКИЕ ФАЙЛЫ
// ==========================================
// Раздаём всё из текущей папки webapp/
app.use(express.static(path.join(__dirname)));

// CORS — разрешаем запросы из WebApp (ngrok и т.д.)
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Headers', 'Content-Type');
  next();
});

// ==========================================
// 📊 API: Данные пользователя
// ==========================================
app.get('/api/user/:id', async (req, res) => {
  const userId = parseInt(req.params.id);
  if (!userId || isNaN(userId)) {
    return res.status(400).json({ error: 'Invalid user ID' });
  }

  // Если БД недоступна — возвращаем заглушку
  if (!db) {
    return res.json({
      user_id: userId,
      balance: 0,
      preferred_currency: 'RUB',
      total_purchases: 0,
      total_spent: 0,
      joined_at: null,
      keys: [],
      coupons: [],
    });
  }

  try {
    // Параллельно запрашиваем все данные
    const [balance, stats, keys, coupons, userRow] = await Promise.all([
      // Баланс
      dbGet(`SELECT * FROM user_balances WHERE user_id = ?`, [userId]),
      // Статистика покупок
      dbGet(
        `SELECT COUNT(*) as total, SUM(amount) as spent
         FROM orders WHERE user_id = ? AND status = 'confirmed'
         AND (balance_topup IS NULL OR balance_topup = 0)`,
        [userId]
      ),
      // Последние ключи
      dbAll(
        `SELECT product, amount, currency, original_currency, original_amount,
                key_issued, created_at, confirmed_at
         FROM orders
         WHERE user_id = ? AND status = 'confirmed' AND (balance_topup IS NULL OR balance_topup = 0)
         ORDER BY confirmed_at DESC LIMIT 5`,
        [userId]
      ),
      // Активные купоны пользователя
      dbAll(
        `SELECT code, discount, expires_at FROM coupons
         WHERE user_id = ? AND (used = 0 OR used IS NULL)
         AND (expires_at IS NULL OR expires_at > datetime('now'))
         ORDER BY created_at DESC LIMIT 10`,
        [userId]
      ),
      // Данные пользователя (дата регистрации)
      dbGet(`SELECT joined_at, language_code FROM users WHERE id = ?`, [userId]),
    ]);

    // Вычисляем expires_at для ключей
    const PERIOD_DAYS = { '1d': 1, '3d': 3, '7d': 7, '30d': 30 };
    const keysWithExpiry = (keys || []).map(k => {
      const days = PERIOD_DAYS[k.product];
      let expires_at = null;
      if (days && (k.confirmed_at || k.created_at)) {
        expires_at = new Date(
          new Date(k.confirmed_at || k.created_at).getTime() + days * 86400000
        ).toISOString();
      }
      return { ...k, expires_at };
    });

    res.json({
      user_id: userId,
      balance: balance?.balance || 0,
      preferred_currency: balance?.preferred_currency || 'RUB',
      total_purchases: stats?.total || 0,
      total_spent: stats?.spent || 0,
      joined_at: userRow?.joined_at || null,
      keys: keysWithExpiry,
      coupons: coupons || [],
    });
  } catch (err) {
    console.error('❌ API error:', err.message);
    res.status(500).json({ error: 'Internal error' });
  }
});

// ==========================================
// 🛠 ХЕЛПЕРЫ ДЛЯ SQLite (Promise-обёртки)
// ==========================================
function dbGet(sql, params) {
  return new Promise((resolve) => {
    db.get(sql, params, (err, row) => {
      if (err) {
        console.error('DB error:', err.message);
        resolve(null);
      } else {
        resolve(row || null);
      }
    });
  });
}

function dbAll(sql, params) {
  return new Promise((resolve) => {
    db.all(sql, params, (err, rows) => {
      if (err) {
        console.error('DB error:', err.message);
        resolve([]);
      } else {
        resolve(rows || []);
      }
    });
  });
}

// ==========================================
// 🚀 ЗАПУСК СЕРВЕРА
// ==========================================
app.listen(PORT, () => {
  console.log(`\n🌐 CyraxMods WebApp сервер запущен!`);
  console.log(`   📍 Локальный:  http://localhost:${PORT}`);
  console.log(`   📱 Для Telegram: используйте ngrok → ngrok http ${PORT}\n`);
});
