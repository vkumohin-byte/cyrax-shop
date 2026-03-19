/**
 * 🤖 CyraxMods WebApp Bot — Отдельный бот для Telegram WebApp
 * 
 * Архитектура:
 *   [index.js — основной бот] → [shop.db] ← [webapp-bot.js] ← [webapp/index.html]
 * 
 * Этот бот:
 *   ✅ Использует ту же БД shop.db что и основной бот
 *   ✅ Обрабатывает /start — отправляет кнопку открытия WebApp
 *   ✅ Обрабатывает web_app_data — покупки, пополнения, навигация
 *   ✅ Раздаёт WebApp через встроенный Express-сервер
 *   ✅ Использует polling (надёжно для локального теста)
 *   ✅ Добавлен API /api/prices для получения цен в WebApp
 */

require('dotenv').config();
const TelegramBot = require('node-telegram-bot-api');
const express = require('express');
const cors = require('cors');
const path = require('path');
const sqlite3 = require('sqlite3').verbose();

// ==========================================
// 🔧 КОНФИГУРАЦИЯ
// ==========================================
const WEBAPP_BOT_TOKEN = process.env.WEBAPP_BOT_TOKEN;
const WEBAPP_URL = process.env.WEBAPP_URL;
const DB_PATH = process.env.DB_PATH || path.join(__dirname, 'shop.db');
const WEBAPP_PORT = parseInt(process.env.WEBAPP_PORT) || 5500;
const ADMIN_ID = parseInt(process.env.ADMIN_ID) || 0;

const PAYPAL_LINK = process.env.PAYPAL_LINK || '';

const PAYMENT_DETAILS = {
  sbp: process.env.PAYMENT_SBP || "",
  card_ua: process.env.PAYMENT_CARD_UA || "0000 0000 0000 0000 (Mono)",
  card_it: process.env.PAYMENT_CARD_IT || "CARD: IT00...",
  binance: process.env.PAYMENT_BINANCE || "ID: 12345678",
  paypal: process.env.PAYMENT_PAYPAL || PAYPAL_LINK,
  cryptobot: process.env.PAYMENT_CRYPTO || "Wallet address"
};

const PAYMENT_METHODS_PER_CURRENCY = {
  RUB: ['sbp', 'cryptobot'],
  USD: ['paypal', 'binance', 'cryptobot'],
  EUR: ['card_it', 'binance', 'cryptobot', 'paypal'],
  UAH: ['card_ua', 'cryptobot']
};

if (!WEBAPP_BOT_TOKEN) {
  console.error('❌ WEBAPP_BOT_TOKEN не задан! Укажите токен нового бота.');
  process.exit(1);
}
if (!WEBAPP_URL) {
  console.error('❌ WEBAPP_URL не задан! Укажите URL WebApp.');
  process.exit(1);
}

console.log('🤖 CyraxMods WebApp Bot');
console.log(`   📍 WebApp URL: ${WEBAPP_URL}`);
console.log(`   🗄️ БД: ${DB_PATH}`);

// ==========================================
// 🗄️ БАЗА ДАННЫХ
// ==========================================
const db = new sqlite3.Database(DB_PATH, (err) => {
  if (err) {
    console.error('❌ Не удалось открыть БД:', err.message);
    process.exit(1);
  }
  console.log('✅ БД подключена');
  db.run('PRAGMA journal_mode = WAL');
  db.run('PRAGMA busy_timeout = 5000');
});

// Promise-обёртки
function dbGet(sql, params = []) {
  return new Promise((resolve, reject) => {
    db.get(sql, params, (err, row) => {
      if (err) reject(err);
      else resolve(row || null);
    });
  });
}

function dbAll(sql, params = []) {
  return new Promise((resolve, reject) => {
    db.all(sql, params, (err, rows) => {
      if (err) reject(err);
      else resolve(rows || []);
    });
  });
}

function dbRun(sql, params = []) {
  return new Promise((resolve, reject) => {
    db.run(sql, params, function(err) {
      if (err) reject(err);
      else resolve({ lastID: this.lastID, changes: this.changes });
    });
  });
}

// ==========================================
// 💵 ЦЕНЫ (зеркало из index.js)
// ==========================================
const PRICES = {
  "1d": { USD: 1.50, EUR: 1.25, RUB: 100, UAH: 65 },
  "3d": { USD: 4.10, EUR: 3.65, RUB: 280, UAH: 180 },
  "7d": { USD: 6.30, EUR: 5.50, RUB: 450, UAH: 285 },
  "30d": { USD: 21.0, EUR: 19.0, RUB: 1500, UAH: 955 },
};

const PERIOD_NAMES = {
  ru: { '1d': '1 день', '3d': '3 дня', '7d': '7 дней', '30d': '30 дней' },
  en: { '1d': '1 day', '3d': '3 days', '7d': '7 days', '30d': '30 days' }
};

const METHOD_NAMES = {
  ru: { sbp: 'СБП', card_ua: 'Карта UA', card_it: 'Карта IT', paypal: 'PayPal', binance: 'Binance', cryptobot: 'CryptoBot' },
  en: { sbp: 'SBP', card_ua: 'UA Card', card_it: 'IT Card', paypal: 'PayPal', binance: 'Binance', cryptobot: 'CryptoBot' }
};

// ==========================================
// 🛠️ ХЕЛПЕРЫ
// ==========================================
function getLang(user) {
  const lc = user?.language_code || '';
  return (lc === 'ru' || lc === 'uk' || lc === 'be') ? 'ru' : 'en';
}

function formatPrice(amount, currency) {
  if (currency === 'RUB') return `${amount} ₽`;
  if (currency === 'UAH') return `${amount} ₴`;
  if (currency === 'USD') return `$${amount}`;
  if (currency === 'EUR') return `€${amount}`;
  return `${amount} ${currency}`;
}

// ==========================================
// 🤖 TELEGRAM BOT — POLLING (НАДЁЖНО)
// ==========================================
const bot = new TelegramBot(WEBAPP_BOT_TOKEN, { polling: true });

bot.on('polling_error', (err) => {
  if (err.code === 'ETELEGRAM' && err.response?.statusCode === 409) {
    console.error('❌ Конфликт: бот уже запущен где-то ещё! Остановите другой экземпляр.');
  } else {
    console.error('❌ Polling error:', err.message);
  }
});

console.log('✅ Polling запущен');

// ==========================================
// 📩 /start — Кнопка открытия WebApp
// ==========================================
bot.onText(/\/start/, async (msg) => {
  const chatId = msg.chat.id;
  const user = msg.from;
  const isRu = getLang(user) === 'ru';

  try {
    await dbRun(
      `INSERT OR IGNORE INTO users (id, username, first_name, last_name, language_code, joined_at)
       VALUES (?, ?, ?, ?, ?, datetime('now'))`,
      [user.id, user.username || null, user.first_name || null, user.last_name || null, user.language_code || null]
    );
  } catch (e) {
    console.error('⚠️ Ошибка регистрации:', e.message);
  }

  const welcomeRu = `⚡ *CyraxMods — Магазин*\n\n🛒 Нажмите кнопку ниже, чтобы открыть витрину.`;
  const welcomeEn = `⚡ *CyraxMods — Shop*\n\n🛒 Press the button below to open the store.`;

  bot.sendMessage(chatId, isRu ? welcomeRu : welcomeEn, {
    parse_mode: 'Markdown',
    reply_markup: {
      inline_keyboard: [
        [{ text: isRu ? '🚀 Открыть магазин' : '🚀 Open Shop', web_app: { url: WEBAPP_URL } }]
      ]
    }
  }).catch(e => console.error('❌ sendMessage error:', e.message));
});

// ==========================================
// 🌐 ОБРАБОТКА web_app_data
// ==========================================
bot.on('message', async (msg) => {
  console.log(`📩 [DEBUG] Получено сообщение: "${msg.text || msg.caption || 'non-text'}" от ${msg.from?.username || msg.from?.id}`);

  if (!msg.web_app_data) return; // Обрабатываем только web_app_data
  const chatId = msg.chat.id;
  const user = msg.from;
  const isRu = getLang(user) === 'ru';

  let waData;
  try {
    waData = JSON.parse(msg.web_app_data.data);
  } catch (e) {
    console.error('❌ [WEBAPP] Ошибка парсинга:', e.message);
    return;
  }

  console.log(`🌐 [WEBAPP] uid=${user.id} action=${waData.action}`);

  try {
    if (waData.action === 'pay') {
      const { product, currency, method } = waData;
      const price = PRICES[product]?.[currency];
      if (!price) return;

      const result = await dbRun(
        `INSERT INTO orders (user_id, product, amount, currency, payment_method, status, created_at)
         VALUES (?, ?, ?, ?, ?, 'pending', datetime('now'))`,
        [user.id, product, price, currency, method]
      );

      const orderId = result.lastID;
      const periodName = PERIOD_NAMES[isRu ? 'ru' : 'en'][product] || product;
      const methodName = METHOD_NAMES[isRu ? 'ru' : 'en'][method] || method;

      bot.sendMessage(chatId,
        `✅ *Заказ #${orderId} создан!*\n\n🔑 ${periodName}\n💰 ${formatPrice(price, currency)}\n💳 ${methodName}\n\n⚠️ После оплаты отправьте скриншот чека.`,
        {
          parse_mode: 'Markdown',
          reply_markup: {
            inline_keyboard: [
              [{ text: isRu ? '❌ Отменить заказ' : '❌ Cancel order', callback_data: `cancel_order_${orderId}` }]
            ]
          }
        }
      );

      if (ADMIN_ID) {
        bot.sendMessage(ADMIN_ID,
          `🛒 *Новый заказ из WebApp!*\n\n👤 ID: ${user.id}\n🔑 ${periodName}\n💰 ${formatPrice(price, currency)}\n💳 ${methodName}\n📦 Заказ #${orderId}`
        ).catch(() => {});
      }
    }

    else if (waData.action === 'profile_topup') {
      const { amount, currency, method } = waData;
      const floatAmount = parseFloat(amount);

      const result = await dbRun(
        `INSERT INTO orders (user_id, product, amount, currency, payment_method, balance_topup, status, created_at)
         VALUES (?, 'balance_topup', ?, ?, ?, 1, 'pending', datetime('now'))`,
        [user.id, floatAmount, currency, method]
      );

      const orderId = result.lastID;

      bot.sendMessage(chatId,
        `💳 *Пополнение баланса #${orderId}*\n\n💰 Сумма: ${formatPrice(floatAmount, currency)}\n\n⚠️ Переведите сумму и отправьте скриншот.`,
        {
          reply_markup: {
            inline_keyboard: [
              [{ text: isRu ? '❌ Отменить' : '❌ Cancel', callback_data: `cancel_order_${orderId}` }]
            ]
          }
        }
      );

      if (ADMIN_ID) {
        bot.sendMessage(ADMIN_ID,
          `💳 *Пополнение баланса!*\n\n👤 ID: ${user.id}\n💰 ${formatPrice(floatAmount, currency)}\n📦 Заказ #${orderId}`
        ).catch(() => {});
      }
    }

    else if (waData.action === 'navigate') {
      bot.sendMessage(chatId,
        isRu
          ? `ℹ️ Эта функция доступна в основном боте:\nhttps://t.me/cyraxxmod_bot`
          : `ℹ️ This feature is available in the main bot:\nhttps://t.me/cyraxxmod_bot`,
        {
          reply_markup: {
            inline_keyboard: [
              [{ text: isRu ? '🚀 Назад в магазин' : '🚀 Back to shop', web_app: { url: WEBAPP_URL } }]
            ]
          }
        }
      );
    }
  } catch (err) {
    console.error('❌ [WEBAPP] Ошибка:', err);
  }
});

// ==========================================
// 🔙 Обработка отмены заказа
// ==========================================
bot.on('callback_query', async (query) => {
  const chatId = query.message?.chat?.id;
  const user = query.from;
  const data = query.data;
  const isRu = getLang(user) === 'ru';

  if (!data) return;
  bot.answerCallbackQuery(query.id).catch(() => {});

  if (data.startsWith('cancel_order_')) {
    const orderId = parseInt(data.replace('cancel_order_', ''));
    if (!orderId) return;

    try {
      const order = await dbGet(
        `SELECT * FROM orders WHERE id = ? AND user_id = ? AND status = 'pending'`,
        [orderId, user.id]
      );

      if (!order) {
        bot.sendMessage(chatId, isRu ? '⚠️ Заказ не найден.' : '⚠️ Order not found.');
        return;
      }

      await dbRun(`UPDATE orders SET status = 'cancelled' WHERE id = ?`, [orderId]);
      bot.sendMessage(chatId, isRu ? `✅ Заказ отменён.` : `✅ Order cancelled.`);
    } catch (e) {
      console.error('❌ Ошибка отмены:', e.message);
    }
  }
});

// ==========================================
// 🌐 EXPRESS — Раздача WebApp + API
// ==========================================
const app = express();

app.use(cors());
app.use(express.json());

app.use(express.static(path.join(__dirname, 'webapp')));
app.use('/site', express.static(path.join(__dirname, 'site')));

// ✅ API: Получить баланс пользователя
app.get('/api/user/:id', async (req, res) => {
  const userId = parseInt(req.params.id);
  if (!userId) return res.status(400).json({ error: 'Invalid ID' });

  try {
    const balance = await dbGet(`SELECT * FROM user_balances WHERE user_id = ?`, [userId]) || { balance: 0, preferred_currency: 'RUB' };
    res.json({ user_id: userId, balance: balance.balance, preferred_currency: balance.preferred_currency });
  } catch (err) {
    res.status(500).json({ error: 'Internal error' });
  }
});

// ✅ API: Получить цены (НОВЫЙ)
app.get('/api/prices', (req, res) => {
  res.json(PRICES);
});

// ✅ API: Доступные методы оплаты (для сайта)
app.get('/api/payment-methods', (req, res) => {
  const currency = req.query.currency;
  if (!currency || !PAYMENT_METHODS_PER_CURRENCY[currency]) {
    return res.json(PAYMENT_METHODS_PER_CURRENCY);
  }
  res.json(PAYMENT_METHODS_PER_CURRENCY[currency]);
});

// ✅ API: Реквизиты для оплаты (для сайта)
app.get('/api/payment-details/:method', (req, res) => {
  const method = req.params.method;
  res.json({ details: PAYMENT_DETAILS[method] || '' });
});

// ✅ API: Статус заказа
app.get('/api/order-status/:orderId', async (req, res) => {
  try {
    const orderId = parseInt(req.params.orderId);
    if (!orderId) return res.status(400).json({ error: 'Invalid ID' });
    const order = await dbGet(`SELECT status FROM orders WHERE id = ?`, [orderId]);
    if (!order) return res.status(404).json({ error: 'Order not found' });
    res.json({ status: order.status });
  } catch (e) {
    res.status(500).json({ error: 'Internal error' });
  }
});

// ✅ API: Создание заказа с сайта
app.post('/api/site/create-order', async (req, res) => {
  try {
    const { telegram_id, product, currency, method } = req.body;
    if (!telegram_id || !product || !currency || !method) {
      return res.status(400).json({ error: 'Missing required fields' });
    }

    const price = PRICES[product]?.[currency];
    if (!price) return res.status(400).json({ error: 'Invalid product or currency' });

    const result = await dbRun(
      `INSERT INTO orders (user_id, product, amount, currency, payment_method, status, created_at)
       VALUES (?, ?, ?, ?, ?, 'pending', datetime('now'))`,
      [parseInt(telegram_id), product, price, currency, method]
    );

    const orderId = result.lastID;
    const periodName = PERIOD_NAMES['ru'][product] || product;
    const methodName = METHOD_NAMES['ru'][method] || method;

    // Send notification to user
    const userMsg = `✅ *Заказ #${orderId} создан!*\n\n🔑 ${periodName}\n💰 ${formatPrice(price, currency)}\n💳 ${methodName}\n\n⚠️ После оплаты отправьте скриншот чека в этот чат.`;
    
    bot.sendMessage(telegram_id, userMsg, {
      parse_mode: 'Markdown',
      reply_markup: {
        inline_keyboard: [
          [{ text: '❌ Отменить заказ', callback_data: `cancel_order_${orderId}` }]
        ]
      }
    }).catch(e => console.error('Site order notify error (user):', e.message));

    // Send notification to admin
    if (ADMIN_ID) {
      bot.sendMessage(ADMIN_ID,
        `🛒 *Новый заказ с САЙТА!*\n\n👤 ID: ${telegram_id}\n🔑 ${periodName}\n💰 ${formatPrice(price, currency)}\n💳 ${methodName}\n📦 Заказ #${orderId}`
      ).catch(() => {});
    }

    res.json({ success: true, orderId });
  } catch (e) {
    console.error('Create order error:', e);
    res.status(500).json({ error: 'Internal error' });
  }
});

// ✅ Health check
app.get('/health', (req, res) => {
  res.json({ status: 'ok' });
});

// ==========================================
// 🚀 ЗАПУСК
// ==========================================
app.listen(WEBAPP_PORT, () => {
  console.log(`\n🚀 Всё запущено!`);
  console.log(`   🌐 WebApp:    http://localhost:${WEBAPP_PORT}`);
  console.log(`   🤖 Бот:       polling активен`);
  console.log(`   📊 API:       http://localhost:${WEBAPP_PORT}/api/user/:id`);
  console.log(`   💰 API Цены:  http://localhost:${WEBAPP_PORT}/api/prices`);
  console.log(`   ❤️ Health:    http://localhost:${WEBAPP_PORT}/health\n`);
});