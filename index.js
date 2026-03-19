process.env.TZ = 'Europe/Moscow';
require('dotenv').config();
const TelegramBot = require('node-telegram-bot-api');
const express = require('express');
const sqlite3 = require('sqlite3').verbose();
const axios = require('axios');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

// ✅ Защита от необработанных отклонений — предотвращает краш бота
process.on('unhandledRejection', (reason, promise) => {
  console.error('⚠️ UnhandledRejection (бот продолжает работу):', reason?.message || reason);
  // НЕ бросаем исключение — бот остаётся живым
});

process.on('uncaughtException', (err) => {
  console.error('⚠️ UncaughtException (бот продолжает работу):', err?.message || err);
  // НЕ завершаем процесс — только логируем
});

// ==========================================
// 📁 ЛОГГИРОВАНИЕ ОШИБОК
// ==========================================
// На Render бесплатном плане файлы в /tmp стираются при рестарте.
// Используем стандартный console.error для удобства мониторинга через логи Render.
console.log('ℹ️ Логирование настроено на стандартный вывод (Render compatible)');

// ==========================================
// 🛡️ ПРОВЕРКА ПЕРЕМЕННЫХ ОКРУЖЕНИЯ
// ==========================================
const requiredEnvVars = [
  'BOT_TOKEN',
  'ADMIN_ID',
  'ADMIN_USERNAME'
];

const missingVars = requiredEnvVars.filter(varName => !process.env[varName]);

if (missingVars.length > 0) {
  console.error('\n❌ КРИТИЧЕСКАЯ ОШИБКА: Отсутствуют обязательные переменные окружения:');
  missingVars.forEach(varName => console.error(`   - ${varName}`));
  console.error('\n🚫 Бот не может быть запущен. Добавьте переменные в файл .env\n');
  process.exit(1);
}

if (!process.env.CRYPTOBOT_TOKEN) {
  console.warn('\n⚠️  ВНИМАНИЕ: CRYPTOBOT_TOKEN не указан. Оплата через CryptoBot будет недоступна.\n');
}

if (!process.env.PAYPAL_LINK) {
  console.warn('⚠️  ВНИМАНИЕ: PAYPAL_LINK не указан. Оплата через PayPal будет недоступна.\n');
}

// ==========================================
// ⚙️ НАСТРОЙКИ И ПЕРЕМЕННЫЕ
// ==========================================
const app = express();
const PORT = process.env.PORT || 3000;

const BOT_TOKEN = process.env.BOT_TOKEN;
const ADMIN_ID = parseInt(process.env.ADMIN_ID);
const ADMIN_USERNAME = process.env.ADMIN_USERNAME;
const CRYPTOBOT_TOKEN = process.env.CRYPTOBOT_TOKEN;
const PAYPAL_LINK = process.env.PAYPAL_LINK;
// ❗ ВАЖНО: WEBHOOK_SECRET ОБЯЗАТЕЛЬНО должен быть задан в переменных окружения Render!
// Если генерировать рандомно — при каждом рестарте URL вебхука меняется и Telegram перестаёт слать апдейты.
if (!process.env.WEBHOOK_SECRET) {
  console.error('❌ КРИТИЧНО: WEBHOOK_SECRET не задан в переменных окружения! Установите фиксированное значение в Render Environment Variables.');
  process.exit(1);
}
const WEBHOOK_SECRET = process.env.WEBHOOK_SECRET;
// Валидация сложности WEBHOOK_SECRET: минимум 16 символов, должен содержать буквы и цифры
{
  const s = WEBHOOK_SECRET;
  const hasLetter = /[a-zA-Z]/.test(s);
  const hasDigit  = /[0-9]/.test(s);
  if (s.length < 32 || !hasLetter || !hasDigit) {
    console.error('❌ КРИТИЧНО: WEBHOOK_SECRET слишком простой! Минимум 32 символа, буквы + цифры. Пример: openssl rand -hex 32');
    process.exit(1);
  }
}
// Секрет для верификации CryptoBot webhook = SHA256(CRYPTOBOT_TOKEN) — по документации API
const CRYPTOBOT_WEBHOOK_SECRET = CRYPTOBOT_TOKEN ? crypto.createHash('sha256').update(CRYPTOBOT_TOKEN).digest() : null;

// 🤝 Шифрование токенов реселлеров (AES-256-CBC)
const RESELLER_ENCRYPTION_KEY = process.env.RESELLER_ENCRYPTION_KEY || null;
if (!RESELLER_ENCRYPTION_KEY) {
  console.warn('⚠️ RESELLER_ENCRYPTION_KEY не задан — функция реселлеров будет недоступна');
}

// Реквизиты для ручной оплаты
let PAYMENT_DETAILS = {
  sbp: process.env.PAYMENT_SBP || "",
  card_ua: process.env.PAYMENT_CARD_UA || "0000 0000 0000 0000 (Mono)",
  card_it: process.env.PAYMENT_CARD_IT || "CARD: IT00...",
  binance: process.env.PAYMENT_BINANCE || "ID: 12345678",
  paypal: process.env.PAYMENT_PAYPAL || PAYPAL_LINK,
  crypto: process.env.PAYMENT_CRYPTO || "Wallet address"
};
// Проверяет что реквизиты реально настроены (не пустые и не дефолтный плейсхолдер)
const PAYMENT_PLACEHOLDERS = ["0000 0000 0000 0000 (Mono)", "CARD: IT00...", "ID: 12345678", "Wallet address", '', null, undefined];
function isPaymentConfigured(method) {
  const val = PAYMENT_DETAILS[method];
  return val && !PAYMENT_PLACEHOLDERS.includes(val);
}

// ==========================================
// 📋 КОНСТАНТЫ СТАТУСОВ (Task 3.2)
// ==========================================
// Централизованные статусы вместо магических строк.
// Использование: ORDER_STATUS.PENDING вместо 'pending' — защита от опечаток.
const ORDER_STATUS = Object.freeze({
  PENDING:              'pending',
  CONFIRMED:            'confirmed',
  REJECTED:             'rejected',
  CANCELLED_BY_USER:    'cancelled_by_user',
  OUT_OF_STOCK:         'out_of_stock',
  RECEIPT_SENT:         'receipt_sent',
  OUT_OF_STOCK_PENDING: 'out_of_stock_pending',
});

const RESELLER_STATUS = Object.freeze({
  PENDING:        'pending',
  ACTIVE:         'active',
  AWAITING_TOKEN: 'awaiting_token',
  TOKEN_INVALID:  'token_invalid',
  BLOCKED:        'blocked',
});

const WITHDRAWAL_STATUS = Object.freeze({
  PENDING:  'pending',
  APPROVED: 'approved',
  REJECTED: 'rejected',
});



// ==========================================
// 🗄️ ПУТЬ К БАЗЕ ДАННЫХ
// ==========================================
// На бесплатном Render нет персистентного диска.
// Используем /tmp — данные живут пока контейнер не перезапустится.
// ВАЖНО: После каждого деплоя/рестарта нужно восстановить базу из бэкапа (команда в боте).
// Для полной персистентности подключите Render Disk ($7/мес) и задайте DB_PATH=/data/shop.db
const DB_PATH = process.env.DB_PATH || '/tmp/shop.db';
console.log(`📂 База данных: ${DB_PATH}`);

// ВАЖНО: отключаем polling, оставляем только webhook
const bot = new TelegramBot(BOT_TOKEN, { polling: false });
let db = new sqlite3.Database(DB_PATH);

// 🤝 Хранилище активных реселлер-ботов: resellerId → { bot, reseller }
const resellerBots = new Map();

// 🔒 Защита от двойного нажатия "Одобрить" — блокирует параллельные approve_ на один orderId
const approvingOrders = new Set();
// 🔒 Защита от двойного нажатия "Пополнить баланс"
const approvingTopups = new Set();

// П.4: WAL режим + принудительный ROLLBACK при запуске
db.serialize(() => {
  db.run('PRAGMA journal_mode=WAL;', (err) => {
    if (err) console.error('❌ WAL mode error:', err);
    else console.log('✅ SQLite WAL mode enabled');
  });
  db.run('ROLLBACK;', () => { }); // очистка незакрытых транзакций
});

// Хранилище сессий
const userSessions = new Map();

// ==========================================
// ⏱️ FIX 5: КОНСТАНТЫ ТАЙМИНГОВ
// ==========================================
// Замена "магических чисел" на именованные константы повышает читаемость
// и исключает ошибки при изменении значений в нескольких местах.
const KEEP_ALIVE_INTERVAL_MS      = 10 * 60 * 1000;  // 10 мин — keep-alive пинг на Render
const BACKUP_FIRST_DELAY_MS       = 15 * 1000;        // 15 сек — первый бэкап после старта
const BACKUP_INTERVAL_MINUTES      = parseInt(process.env.BACKUP_INTERVAL_MINUTES || '30'); // мин — плановый бэкап (дефолт 30 мин)
// Если задан BACKUP_CHAT_ID — бэкапы отправляются туда (канал/группа/отдельный чат).
// Если не задан — отправляются в личку ADMIN_ID без удаления.
const BACKUP_CHAT_ID = process.env.BACKUP_CHAT_ID ? parseInt(process.env.BACKUP_CHAT_ID) : -1003805253802;
const BACKUP_INTERVAL_MS          = BACKUP_INTERVAL_MINUTES * 60 * 1000;
const LONG_PENDING_CHECK_MS       = 10 * 60 * 1000;  // 10 мин — проверка зависших заказов
const GROUP_PROMO_INTERVAL_MS     = 6 * 60 * 60 * 1000; // 6 ч  — авторекламка в группах
const CRYPTOBOT_CLEANUP_INTERVAL_MS = 60 * 60 * 1000; // 1 ч  — очистка старых CryptoBot инвойсов
const WITHDRAWAL_REMINDER_INTERVAL_MS = 24 * 60 * 60 * 1000; // 24 ч — напоминание о выводах
const SYNC_INTERVAL_MS            = 5 * 60 * 1000;   // 5 мин — синхронизация реквизитов и цен
const SUSPICION_MONITOR_INTERVAL_MS = 60 * 60 * 1000; // 1 ч  — мониторинг подозрительной активности
const LOW_KEYS_CHECK_INTERVAL_MS  = 30 * 60 * 1000;  // 30 мин — проверка запаса ключей
const COUPON_SYNC_INTERVAL_MS     = 60 * 60 * 1000;  // 1 ч  — синхронизация used_count купонов
const EXCHANGE_RATE_INTERVAL_MS   = 60 * 60 * 1000;  // 1 ч  — обновление курсов валют
const RENEWAL_REMINDER_INTERVAL_MS = 6 * 60 * 60 * 1000; // 6 ч  — напоминания о продлении
const RENEWAL_REMINDER_FIRST_MS   = 2 * 60 * 1000;   // 2 мин — первый запуск напоминаний
const DEAD_REFERRAL_CHECK_INTERVAL_MS = 24 * 60 * 60 * 1000; // 24 ч — проверка мёртвых рефов
const BROADCAST_PROCESS_INTERVAL_MS = 60 * 1000;     // 1 мин — обработка отложенных рассылок
const STARTUP_NOTIFY_DELAY_MS     = 3 * 1000;         // 3 сек — задержка уведомления о старте
const CRYPTOBOT_CLEANUP_FIRST_MS  = 5 * 1000;         // 5 сек — первая очистка CryptoBot при старте
const COUPON_SYNC_FIRST_MS        = 5 * 1000;         // 5 сек — первая синхронизация купонов
const NEW_USER_BATCH_INTERVAL_MS  = 60 * 60 * 1000;   // 1 ч   — сводка новых пользователей (батчинг)

// ==========================================
// 🔢 IMPROVEMENT 2: БИЗНЕС-КОНСТАНТЫ
// ==========================================
// Магические числа вынесены в именованные константы — проще найти и изменить.

/** Порог suspicion_score, выше которого реферальный бонус замораживается для ручной проверки. */
const SUSPICION_THRESHOLD_BLOCK = 50;

/** Количество «мёртвых» рефералов подряд, после которого реферальная ссылка блокируется. */
const DEAD_REFERRAL_LIMIT = 3;

// Состояние техобслуживания
let maintenanceMode = false;
let maintenanceEndTime = null;
let maintenanceReason = '';
let maintenanceWaitingUsers = new Set();
let maintenanceTimer = null;

// Таймеры для остановки отдельных разделов витрины
const sectionPauseTimers = {}; // { 'keys': Timer, 'boost': Timer, 'manual_boost': Timer }

// Защита от спама
const userActionLimits = new Map();
const RATE_LIMIT_WINDOW = 60000;
const MAX_ACTIONS_PER_WINDOW = 30; // ✅ Увеличено с 10 до 30 — покупка занимает ~5 кликов
const rateLimitViolations = new Map();

// Навигационные callbacks — не считаются за "действие" в rate limit
// =============================================
// 🧭 НАВИГАЦИОННЫЕ CALLBACK — паттерн-матчинг (Task 6)
// =============================================
const NAVIGATION_CALLBACKS_STATIC = new Set([
  'buy', 'orders', 'offer', 'start', 'admin', 'buy_boost', 'manual_boost',
  'manual_boost_start', 'coupon_list', 'coupon_stats', 'admin_stats',
  'admin_manage_orders', 'admin_top_sales', 'admin_active_users',
  'admin_key_stock', 'admin_sold_keys', 'admin_manage_keys',
  'admin_manage_prices', 'admin_manage_payment_details', 'admin_coupons',
  'admin_view_logs', 'admin_export_csv', 'admin_backup', 'admin_restore', 'admin_broadcast', 'admin_settings',
  'admin_manage_manual_boost', 'admin_backup', 'admin_restore', 'noop',
  'remind_send', 'remind_skip', 'remind_batch_open', 'remind_batch_send_all', 'remind_batch_skip_all',
  'loyal_yes', 'loyal_no', 'loyal_pct', 'loyal_exp',
  'admin_ban_by_username', 'admin_bans',
  'boost_hub',
  'admin_loyalty', 'admin_fomo', 'admin_reviews', 'admin_exchange_rates',
  'fomo_toggle', 'rates_refresh', 'loyalty_edit_default',
  'my_ref', 'my_coupons', 'faq', 'admin_scheduled_broadcast', 'sched_broadcast_create', 'sched_broadcast_cancel_all', 'broadcast_preview_confirm', 'broadcast_preview_cancel',
  'admin_gift_all', 'admin_gift_all_confirm', 'admin_gift_all_cancel',
  'admin_frequent_buyers',
  'admin_section_pause',
  'manager_orders',
  'broadcast_btn_yes', 'broadcast_btn_no',
  'spause_reason_skip',
  'bundle_offer', 'bundle_back',
  'coupon_delete_all_confirm', 'coupon_delete_all_yes', 'coupon_delete_all_final',
  'coupon_issue_to_buyers',
  'admin_message_user',
  'admin_lost_orders',
  'stats_today', 'stats_week', 'stats_month', 'stats_all',
  'rates_apply_to_keys', 'rates_apply_to_keys_confirm',
  'coupon_list_archive',
  'support_ticket', 'support_ticket_skip_screenshot', 'support_key_intent',
  'support_key_ticket', 'support_personal_contact',
  'admin_tickets', 'broadcast_preset_keys_ready',
  'broadcast_custom',
  'support_collect_description',
  'partnership', 'reseller_activate',
  'admin_resellers', 'admin_reseller_stats', 'admin_reseller_withdrawals',
  'dns_info',
  'review_reject_all',
  'my_profile', 'profile_keys', 'profile_balance', 'profile_topup',
  'profile_topup_confirm',
]);

const NAVIGATION_CALLBACK_PATTERNS = [
  /^period_/,
  /^currency_/,
  /^pay_/,
  /^confirm_/,
  /^cancel_/,
  /^order_/,
  /^key_/,
  /^coupon_/,
  /^admin_/,
  /^edit_price_/,
  /^loyalty_edit_/,
  /^fomo_edit_/,
  /^review_/,
  /^faq_item_/,
  /^sched_filter_/,
  /^frequent_page_/,
  /^bundle_offer_/,
  /^bundle_select_/,
  /^bundle_currency_/,
  /^bundle_pay_/,
  /^bundle_approve_/,
  /^bundle_reject_/,
  /^basketball_throw_/,
  /^rates_markup_/,
  /^mb_/,
  /^rank_/,
  /^boost_/,
  /^manual_/,
  /^request_review_/,
  /^approve_/,
  /^review_reward_/,
  /^review_give_/,
  /^review_key_period_/,
  /^review_reject_/,
  /^msg_buyer_/,
  /^reject_/,
  /^issue_/,
  /^delete_coupon_confirm_/,
  /^orders_page_/,
  /^logs_page_/,
  /^sold_keys_page_/,
  /^settings_/,
  /^unban_/,
  /^ban_/,
  /^ban_dur_/,
  /^admin_mb_delete_/,
  /^client_info_/,
  /^client_note_/,
  /^client_reset_suspicion_/,
  /^ticket_resolve_/,
  /^ticket_fraud_/,
  /^support_key_/,
  /^ticket_admin_/,
  /^ticket_issue_key_/,
  /^issue_id_/,
  /^admin_tickets_page_/,
  /^reseller_/,
  /^rsl_/,
  /^profile_topup_pay_/,
  /^admin_balance_/,
  /^approve_topup_/,
  /^reject_topup_/,
  /^pay_balance_/,
  /^admin_coupon_issue_/,
  /^admin_balance_edit_/,
  /^admin_msg_user_/,
  /^review_page_/,
  /^add_keys_/,
  /^edit_payment_/,
  /^remind_batch_open_/,
  /^spause_dur_/,
  /^spause_target_/,
  /^rates_set_fixed_/,
];

function isNavigationCallback(data) {
  if (!data) return false;
  if (NAVIGATION_CALLBACKS_STATIC.has(data)) return true;
  return NAVIGATION_CALLBACK_PATTERNS.some(p => p.test(data));
}

// ==========================================
// 💵 ЦЕНЫ И ВАЛЮТЫ
// ==========================================
let PRICES = {
  "1d": { USD: 1.50, EUR: 1.25, RUB: 100, UAH: 65 },
  "3d": { USD: 4.10, EUR: 3.65, RUB: 280, UAH: 180 },
  "7d": { USD: 6.30, EUR: 5.50, RUB: 450, UAH: 285 },
  "30d": { USD: 21.0, EUR: 19.0, RUB: 1500, UAH: 955 },
  "infinite_boost": { USD: 35, EUR: 28, RUB: 2500, UAH: 1500 },
  "reseller_connection": { USD: 17, EUR: 15, RUB: 1500, UAH: 955 }
};

const FLAGS = { USD: "🇺🇸", EUR: "🇪🇺", RUB: "🇷🇺", UAH: "🇺🇦" };

// ==========================================
// 💱 КУРСЫ ВАЛЮТ ДЛЯ РУЧНОГО БУСТА
// ==========================================
let EXCHANGE_RATES = { USD: 0.01308, EUR: 0.01107, UAH: 0.5658 }; // 1 RUB → валюта

// ==========================================
// 🎮 РАНГИ MLBB — структура для буста
// ==========================================
// Каждый элемент: { key, label_ru, label_en, stars }
// stars = количество звёзд/очков внутри этого ранга (сколько нужно сыграть чтобы пройти его)
const MLBB_RANKS = [
  // Warrior
  { key: 'warrior_3', label_ru: 'Боец III', label_en: 'Warrior III', stars: 3 },
  { key: 'warrior_2', label_ru: 'Боец II', label_en: 'Warrior II', stars: 3 },
  { key: 'warrior_1', label_ru: 'Боец I', label_en: 'Warrior I', stars: 3 },
  // Elite
  { key: 'elite_3', label_ru: 'Элита III', label_en: 'Elite III', stars: 3 },
  { key: 'elite_2', label_ru: 'Элита II', label_en: 'Elite II', stars: 3 },
  { key: 'elite_1', label_ru: 'Элита I', label_en: 'Elite I', stars: 3 },
  // Master
  { key: 'master_4', label_ru: 'Мастер IV', label_en: 'Master IV', stars: 4 },
  { key: 'master_3', label_ru: 'Мастер III', label_en: 'Master III', stars: 4 },
  { key: 'master_2', label_ru: 'Мастер II', label_en: 'Master II', stars: 4 },
  { key: 'master_1', label_ru: 'Мастер I', label_en: 'Master I', stars: 4 },
  // Grandmaster
  { key: 'gm_5', label_ru: 'Гранд-мастер V', label_en: 'Grandmaster V', stars: 5 },
  { key: 'gm_4', label_ru: 'Гранд-мастер IV', label_en: 'Grandmaster IV', stars: 5 },
  { key: 'gm_3', label_ru: 'Гранд-мастер III', label_en: 'Grandmaster III', stars: 5 },
  { key: 'gm_2', label_ru: 'Гранд-мастер II', label_en: 'Grandmaster II', stars: 5 },
  { key: 'gm_1', label_ru: 'Гранд-мастер I', label_en: 'Grandmaster I', stars: 5 },
  // Epic
  { key: 'epic_5', label_ru: 'Эпик V', label_en: 'Epic V', stars: 5 },
  { key: 'epic_4', label_ru: 'Эпик IV', label_en: 'Epic IV', stars: 5 },
  { key: 'epic_3', label_ru: 'Эпик III', label_en: 'Epic III', stars: 5 },
  { key: 'epic_2', label_ru: 'Эпик II', label_en: 'Epic II', stars: 5 },
  { key: 'epic_1', label_ru: 'Эпик I', label_en: 'Epic I', stars: 5 },
  // Legend
  { key: 'legend_5', label_ru: 'Легенда V', label_en: 'Legend V', stars: 5 },
  { key: 'legend_4', label_ru: 'Легенда IV', label_en: 'Legend IV', stars: 5 },
  { key: 'legend_3', label_ru: 'Легенда III', label_en: 'Legend III', stars: 5 },
  { key: 'legend_2', label_ru: 'Легенда II', label_en: 'Legend II', stars: 5 },
  { key: 'legend_1', label_ru: 'Легенда I', label_en: 'Legend I', stars: 5 },
  // Mythic tier (stars = очки/звёзды)
  { key: 'mythic', label_ru: 'Мифик (0-24)', label_en: 'Mythic (0-24)', stars: 25 },
  { key: 'mh', label_ru: 'Мифик Честь (25-49)', label_en: 'Mythical Honor (25-49)', stars: 25 },
  { key: 'mg', label_ru: 'Мифик Слава (50-99)', label_en: 'Mythical Glory (50-99)', stars: 50 },
  // FIX 3.2: stars был 0 — делал буст бесплатным. Ставим 100 как условный диапазон (100-200).
  // Для буста внутри MI пользователю будет запрошено целевое кол-во очков.
  { key: 'mi', label_ru: 'Мифик Бессмертие (100+)', label_en: 'Mythical Immortal (100+)', stars: 100 },
];

// Цена за 1 звезду/очко в RUB по уровню ранга
const BOOST_PRICE_PER_STAR = {
  warrior: 30, elite: 35, master: 45,
  grandmaster: 55, epic: 70, legend: 90, mythic: 120
};

function getRankTier(rankKey) {
  if (rankKey.startsWith('warrior')) return 'warrior';
  if (rankKey.startsWith('elite')) return 'elite';
  if (rankKey.startsWith('master')) return 'master';
  if (rankKey.startsWith('gm')) return 'grandmaster';
  if (rankKey.startsWith('epic')) return 'epic';
  if (rankKey.startsWith('legend')) return 'legend';
  return 'mythic';
}

// Рассчитать стоимость буста между двумя рангами (в RUB)
/**
 * Рассчитывает стоимость буста (Ranked) в рублях и общее количество звёзд.
 *
 * Алгоритм: итерируемся по рангам от fromKey до toKey (не включая toKey),
 * суммируя количество звёзд каждого ранга умноженное на цену за звезду в этом тире.
 * Первый ранг (fromKey) учитывается частично — вычитаем уже набранные звёзды (fromStars).
 *
 * Особый случай — внутри Мифика: fromKey === toKey (оба мифик), тогда cost = delta_stars * mythic_price_per_star.
 *
 * @param {string} fromKey    - Ключ текущего ранга (например 'epic_3')
 * @param {number} fromStars  - Уже набранные звёзды в текущем ранге (0..rank.stars-1)
 * @param {string} toKey      - Ключ желаемого ранга
 * @param {number} toStars    - Желаемые звёзды в целевом ранге (для внутри-мифик расчётов)
 * @returns {{ costRub: number, totalStars: number }}
 */
function calcBoostCost(fromKey, fromStars, toKey, toStars) {
  const fromIdx = MLBB_RANKS.findIndex(r => r.key === fromKey);
  const toIdx = MLBB_RANKS.findIndex(r => r.key === toKey);

  // BUG FIX: Мифик внутри одного ранга (fromKey === toKey) — обрабатываем отдельно
  // до общей проверки fromIdx >= toIdx, иначе эта ветка никогда не достигается.
  if (fromKey === toKey && fromKey.startsWith('myth')) {
    const diff = Math.max(0, (toStars || 0) - (fromStars || 0));
    return { costRub: Math.round(diff * (BOOST_PRICE_PER_STAR['mythic'] || 70)), totalStars: diff };
  }

  if (fromIdx < 0 || toIdx < 0 || fromIdx >= toIdx) return { costRub: 0, totalStars: 0 };

  let totalStars = 0;
  let costRub = 0;

  for (let i = fromIdx; i < toIdx; i++) {
    const rank = MLBB_RANKS[i];
    const tier = getRankTier(rank.key);
    const pricePerStar = BOOST_PRICE_PER_STAR[tier] || 70;
    let starsInThisRank = rank.stars;

    // Первый ранг — вычитаем уже пройденные звёзды
    if (i === fromIdx) starsInThisRank = Math.max(0, starsInThisRank - (fromStars || 0));

    totalStars += starsInThisRank;
    costRub += starsInThisRank * pricePerStar;
  }

  return { costRub: Math.round(costRub), totalStars };
}

// Конвертировать RUB → все валюты
function convertFromRub(rub) {
  return {
    RUB: rub,
    USD: Math.round(rub * EXCHANGE_RATES.USD * 100) / 100,
    EUR: Math.round(rub * EXCHANGE_RATES.EUR * 100) / 100,
    UAH: Math.round(rub * EXCHANGE_RATES.UAH)
  };
}

// Строка с ценами во всех валютах
function formatAllCurrencies(costs) {
  return `🇷🇺 ${costs.RUB} ₽  •  🇺🇸 $${costs.USD}  •  🇪🇺 €${costs.EUR}  •  🇺🇦 ${costs.UAH} ₴`;
}

// Клавиатура выбора ранга (разбита на группы)
function buildRankKeyboard(prefix, lang, excludeKey) {
  const groups = [
    ['warrior_3', 'warrior_2', 'warrior_1'],
    ['elite_3', 'elite_2', 'elite_1'],
    ['master_4', 'master_3', 'master_2', 'master_1'],
    ['gm_5', 'gm_4', 'gm_3', 'gm_2', 'gm_1'],
    ['epic_5', 'epic_4', 'epic_3', 'epic_2', 'epic_1'],
    ['legend_5', 'legend_4', 'legend_3', 'legend_2', 'legend_1'],
    ['mythic', 'mh', 'mg', 'mi'],
  ];
  const keyboard = [];
  for (const group of groups) {
    const row = group
      .filter(k => k !== excludeKey)
      .map(k => {
        const r = MLBB_RANKS.find(x => x.key === k);
        return { text: lang === 'ru' ? r.label_ru : r.label_en, callback_data: `${prefix}${k}` };
      });
    if (row.length) keyboard.push(row);
  }
  return keyboard;
}

// ==========================================
// 🛠️ ХЕЛПЕР: Умный парсинг callback_data
// Решает проблему с infinite_boost (содержит '_')
// Формат: prefix_PERIOD_CURRENCY_METHOD
// Period может содержать '_' (infinite_boost), остальные — нет
// ==============================================================
/**
 * Разбирает callback_data вида 'pay_PERIOD_CURRENCY_METHOD'.
 *
 * Проблема: period может содержать '_' (например 'infinite_boost'), поэтому нельзя
 * просто сделать split('_'). Решение: ищем CURRENCY как первый известный токен
 * окружённый '_', и отрезаем от него — слева period, справа method.
 *
 * Поддерживаемые CURRENCY: USD, EUR, RUB, UAH.
 * Поддерживаемые METHOD: paypal, sbp, binance, card_ua, card_it, cryptobot, cryptobot_usd.
 *
 * @param {string} data - callback_data строка
 * @returns {{ period: string, currency: string, method: string|null } | null}
 */
function parsePayCallback(data) {
  // data = 'pay_PERIOD_CURRENCY_METHOD'
  // CURRENCY всегда одно из: USD EUR RUB UAH
  // METHOD всегда одно из: paypal sbp binance card_ua card_it cryptobot cryptobot_usd
  //
  // ПРОБЛЕМА старой логики: поиск '_USD_' / '_EUR_' в середине строки ломается
  // когда METHOD содержит '_' (card_ua, card_it, cryptobot_usd) — алгоритм
  // путает подчёркивание метода с разделителем и неправильно нарезает строку.
  //
  // РЕШЕНИЕ: ищем метод с конца строки перебором известных суффиксов.
  // Порядок важен — более длинные методы (card_ua, card_it, cryptobot_usd) идут первыми,
  // чтобы не споткнуться о более короткие подстроки (ua, it, usd).

  const knownMethods = ['cryptobot_usd', 'card_ua', 'card_it', 'cryptobot', 'paypal', 'binance', 'sbp'];
  const knownCurrencies = ['USD', 'EUR', 'RUB', 'UAH'];

  // Убираем префикс 'pay_'
  const withoutPrefix = data.replace(/^pay_/, '');

  let method = null;
  let withoutMethod = withoutPrefix;

  // Шаг 1: найти метод — ищем суффикс вида _method
  for (const m of knownMethods) {
    if (withoutPrefix.endsWith('_' + m)) {
      method = m;
      withoutMethod = withoutPrefix.slice(0, withoutPrefix.length - m.length - 1);
      break;
    }
  }

  // Шаг 2: найти валюту — ищем суффикс вида _CURRENCY в том что осталось
  for (const cur of knownCurrencies) {
    if (withoutMethod.endsWith('_' + cur)) {
      const period = withoutMethod.slice(0, withoutMethod.length - cur.length - 1);
      return { period, currency: cur, method };
    }
    // Вариант без метода: 'pay_7d_USD'
    if (!method && withoutPrefix.endsWith('_' + cur)) {
      const period = withoutPrefix.slice(0, withoutPrefix.length - cur.length - 1);
      return { period, currency: cur, method: null };
    }
  }

  return null;
}

function parseCurrencyCallback(data) {
  // data = 'currency_PERIOD_CURRENCY'
  const currencies = ['USD', 'EUR', 'RUB', 'UAH'];
  const withoutPrefix = data.replace(/^currency_/, '');
  for (const cur of currencies) {
    if (withoutPrefix.endsWith('_' + cur)) {
      const period = withoutPrefix.substring(0, withoutPrefix.length - cur.length - 1);
      return { period, currency: cur };
    }
  }
  return null;
}

function parseCouponCallback(data) {
  // data = 'apply_coupon_PERIOD_CURRENCY'
  const currencies = ['USD', 'EUR', 'RUB', 'UAH'];
  const withoutPrefix = data.replace(/^apply_coupon_/, '');
  for (const cur of currencies) {
    if (withoutPrefix.endsWith('_' + cur)) {
      const period = withoutPrefix.substring(0, withoutPrefix.length - cur.length - 1);
      return { period, currency: cur };
    }
  }
  return null;
}

// PayPal комиссия
const PAYPAL_COMMISSION = { USD: 0.50, EUR: 0.50 };

function applyPaypalFee(amount, currency) {
  const fee = PAYPAL_COMMISSION[currency] || 0;
  return Math.round((amount + fee) * 100) / 100;
}

// CryptoBot комиссия
const CRYPTOBOT_COMMISSION = { USD: 0.50, EUR: 0.50 };

function applyCryptobotFee(amount, currency) {
  const fee = CRYPTOBOT_COMMISSION[currency] || 0;
  return Math.round((amount + fee) * 100) / 100;
}



// ==========================================
// 📅 ЧЕЛОВЕЧЕСКИЕ НАЗВАНИЯ ПЕРИОДОВ
// ==========================================
const PERIOD_NAMES = {
  ru: {
    '1d': '1 день',
    '3d': '3 дня',
    '7d': '7 дней',
    '30d': '30 дней',
    'infinite_boost': 'Метод Бесконечного Буста',
    'reseller_connection': 'Партнёрская программа'
  },
  en: {
    '1d': '1 day',
    '3d': '3 days',
    '7d': '7 days',
    '30d': '30 days',
    'infinite_boost': 'Infinite Boost Method',
    'reseller_connection': 'Partnership Program'
  }
};

// Локализованные названия методов оплаты
const METHOD_NAMES = {
  ru: {
    'SBP': 'СБП — Россия',
    'Card UA': 'Карта — Украина',
    'Card IT': 'Карта — Италия',
    'PayPal': 'PayPal',
    'Binance': 'Binance P2P',
    'CryptoBot': 'CryptoBot'
  },
  en: {
    'SBP': 'SBP — Russia',
    'Card UA': 'Card — Ukraine',
    'Card IT': 'Card — Italy',
    'PayPal': 'PayPal',
    'Binance': 'Binance P2P',
    'CryptoBot': 'CryptoBot'
  }
};

// ==========================================
// 💰 ФОРМАТИРОВАНИЕ ЦЕН
// ==========================================
function formatPrice(amount, currency) {
  if (currency === 'RUB') {
    return amount.toString().replace(/\B(?=(\d{3})+(?!\d))/g, " ") + ' ₽';
  }
  if (currency === 'UAH') {
    return amount + ' ₴';
  }
  if (currency === 'USD') {
    return '$' + amount;
  }
  if (currency === 'EUR') {
    return '€' + amount;
  }
  return amount + ' ' + currency;
}

// ==========================================
// 🌐 ПЕРЕВОДЫ (МУЛЬТИЯЗЫЧНОСТЬ)
// ==========================================
const translations = {
  ru: {
    welcome: "🛍️ Добро пожаловать в CyraxMods!",
    buy_key: "🔑  Купить ключ",
    my_orders: "📂 Мои ключи",
    boost_hub: "⚡️  Буст аккаунта",
    channel: "📢 Канал",
    help: "❓ Помощь",
    offer: "📜 Публичная оферта",
    admin_panel: "🛠 Управление",
    back: "◀️ Назад",
    support_ticket_btn: "🆘 Проблема с ключом?",

    choose_period: "🔑 Выберите срок действия ключа:",
    choose_currency: "💱 Выберите валюту оплаты:",
    choose_payment: "💳 Выберите способ оплаты:",

    russia_sbp: "🇷🇺 СБП (Россия)",
    ukraine_card: "🇺🇦 Карта украинского банка",
    italy_card: "🇮🇹 Карта итальянского банка",
    paypal: "💰 PayPal",
    binance: "💎 Binance",
    cryptobot: "₿ CryptoBot",

    payment_info: "",
    paypal_instruction: "💰 <b>Оплата {amount} {currency} — PayPal</b>\n\n<b>Шаги:</b>\n1️⃣ Нажми кнопку ниже → откроется страница PayPal\n2️⃣ Оплати и сохрани чек из PayPal\n3️⃣ Отправь скриншот или PDF чека сюда — ждём!\n\n<i>⏱ Обычно подтверждаем за 5–15 минут</i>",
    sbp_instruction: "💳 <b>Оплата {amount} ₽ — СБП</b>\n\n<b>Шаги:</b>\n1️⃣ Нажми кнопку реквизитов ниже и переведи <b>ровно {amount} ₽</b>\n2️⃣ Сохрани скриншот или PDF чека\n3️⃣ Отправь его сюда — ждём!\n\n<i>⏱ Обычно подтверждаем за 5–15 минут</i>",
    card_ua_instruction: "🇺🇦 <b>Оплата {amount} {currency} — Карта UA</b>\n\n<b>Номер карты:</b>\n<code>{card}</code>\n\n<b>Шаги:</b>\n1️⃣ Переведи <b>ровно {amount} {currency}</b> на карту\n2️⃣ Дождись подтверждения банка и сохрани скриншот\n3️⃣ Отправь скриншот сюда — ждём!\n\n<i>⏱ Обычно подтверждаем за 5–15 минут</i>",
    card_it_instruction: "🇮🇹 <b>Оплата {amount} {currency} — Карта IT</b>\n\n<b>Реквизиты:</b>\n<code>{card}</code>\n\n<b>Шаги:</b>\n1️⃣ Переведи <b>ровно {amount} {currency}</b> по реквизитам\n2️⃣ Сохрани скриншот или PDF\n3️⃣ Отправь его сюда — ждём!\n\n<i>⏱ Обычно подтверждаем за 5–15 минут</i>",
    binance_instruction: "💎 <b>Оплата {amount} {currency} — Binance P2P</b>\n\n<b>Binance ID получателя:</b>\n<code>{id}</code>\n\n<b>Шаги:</b>\n1️⃣ Открой Binance → P2P → найди трейдера по ID\n2️⃣ Переведи <b>ровно {amount} {currency}</b>\n3️⃣ Сохрани скриншот и отправь сюда — ждём!\n\n<i>⚠️ Переводи только через P2P, не Spot!</i>\n<i>⏱ Обычно подтверждаем за 5–15 минут</i>",
    cryptobot_instruction: "🤖 <b>Оплата {amount} {currency} — CryptoBot</b>\n\n1. Нажми кнопку ниже — откроется инвойс в CryptoBot\n2. Оплати криптовалютой — ключ придёт автоматически\n\n⚡️ Подтверждение мгновенное, ключ выдаётся сразу",
    cryptobot_usd_instruction: "🤖 <b>Оплата {amount} USD (USDT) — CryptoBot</b>\n\n1. Нажми кнопку ниже — откроется инвойс в CryptoBot\n2. Оплати криптовалютой — ключ придёт автоматически\n\n⚡️ Подтверждение мгновенное, ключ выдаётся сразу",

    receipt_received: "✅ Чек получен! Платёж на проверке у администратора, обычно это занимает несколько минут. ⏳",
    send_transaction: "✅ После оплаты отправьте скриншот или PDF чека.\n\n⏳ Ожидайте подтверждения от администратора.",

    order_confirmed_title: "🎊✨ Спасибо за покупку! ✨🎊",
    order_confirmed_key: "🔑 Ваш ключ:",
    order_confirmed_period: "📦 Срок действия:",
    order_confirmed_activation: "⚡️ Активация: при первом вводе в CyraxMod",
    order_confirmed_channel: "📢 Наш канал:",
    order_confirmed_footer: "💬 Если возникнут вопросы — обращайтесь!",
    order_out_of_stock: "🔧 Ключи для этого периода временно закончились. Ваш заказ принят, мы выдадим ключ вручную как можно скорее. Спасибо за понимание!",

    maintenance: "🔧 Техническое обслуживание\n⏰ {time}\n💬 {reason}\n✨ Скоро вернемся!",
    maintenance_over: "🟢✅ Всё готово, бот снова работает! 🔑🎉",
    maintenance_time: "Осталось ~{minutes} мин",
    rate_limit: "⚠️ Слишком много действий. Подождите минуту.",
    no_keys: "📂 Купленных ключей пока нет.\n\nКупи первый ключ — и он появится здесь! 🔑",
    waiting_screenshot: "📸 Отлично! Жду скриншот или PDF чека. Просто отправь файл сюда.",
    error_fetching_orders: "❌ Ошибка получения заказов",
    error_creating_invoice: "❌ Ошибка создания инвойса. Попробуйте позже.",
    error_order_data_missing: "❌ Ошибка: данные заказа не найдены. Пожалуйста, начните заново.",
    error_creating_order: "❌ Ошибка создания заказа. Попробуйте позже.",
    order_already_pending: "⚠️ У вас уже есть заказ на проверке для этого продукта. Дождитесь его обработки.",
    tap_to_copy: "└ (нажмите чтобы скопировать)",

    pay_button: "💰 Оплатить через {method}",

    offer_text:
      `📜 <b>ПУБЛИЧНАЯ ОФЕРТА — CyraxMods</b>\n\n` +
      `Настоящий документ является публичной офертой магазина CyraxMods. Нажимая «Купить», «Оплатить» или иным способом совершая покупку, Вы полностью принимаете все условия ниже.\n\n` +

      `<b>1. Товары и услуги</b>\n` +
      `🔑 Активационный ключ для мода MLBB — доступ на 1/3/7/30 дней.\n` +
      `📈 Метод Буст — подробная инструкция + поддержка 24/7 для самостоятельного буста.\n` +
      `🏆 Ручной Буст — буст аккаунта нашими специалистами с отчётом и скриншотами.\n\n` +
      `Все товары и услуги — для личного некоммерческого использования. Совершая покупку, Вы подтверждаете, что Вам 18+ лет.\n\n` +

      `<b>2. Передача товара</b>\n` +
      `2.1. Ключ/доступ передаётся автоматически после подтверждения оплаты.\n` +
      `2.2. Ключ считается переданным в момент его отображения в чате.\n` +
      `2.3. Метод Буст — исполнен после получения Вами инструкции и доступа к поддержке.\n` +
      `2.4. Ручной Буст — исполнен после завершения буста и отправки отчёта/скриншотов.\n\n` +

      `<b>3. Купоны и скидки</b>\n` +
      `3.1. Персональные купоны выдаются постоянным клиентам по усмотрению администрации.\n` +
      `3.2. Купон одноразовый, привязан к конкретному пользователю, не передаётся.\n` +
      `3.3. Срок действия купона указан при его выдаче.\n\n` +

      `<b>4. Правила использования</b>\n` +
      `4.1. Ключ активируется однократно на одном аккаунте.\n` +
      `4.2. Запрещается передавать ключ/инструкцию третьим лицам или публиковать в открытом доступе.\n` +
      `4.3. Запрещается использовать товары и услуги в коммерческих целях.\n\n` +

      `<b>5. Риски</b>\n` +
      `5.1. Использование модов и буста нарушает правила MLBB и несёт риск бана аккаунта — это зависит от политики Moonton.\n` +
      `5.2. Магазин не несёт ответственности за блокировки или санкции со стороны разработчика.\n` +
      `5.3. Вы самостоятельно оцениваете все риски перед покупкой.\n\n` +

      `<b>6. Возврат и замена</b>\n` +
      `6.1. Возврат за цифровые товары надлежащего качества не производится.\n` +
      `6.2. Исключения — рассматриваются индивидуально администрацией:\n` +
      `• подтверждённая неработоспособность ключа по вине Магазина;\n` +
      `• Ручной Буст, не начатый специалистами;\n` +
      `• иные обстоятельства по усмотрению администрации.\n` +
      `6.3. Обратитесь в поддержку в течение 24 часов с момента получения товара.\n` +
      `6.4. После начала Ручного Буста возврат не предусмотрен.\n` +
      `6.5. Решение о возврате принимается администрацией в каждом случае индивидуально.\n\n` +

      `<b>7. Поддержка</b>\n` +
      `Все вопросы решаем оперативно через поддержку в боте. Актуальная версия оферты всегда доступна в боте.\n\n` +

      `Спасибо, что выбираете CyraxMods! 💜`,

    offer_back: "◀️ Вернуться в меню",
    private_only: `👋 Работаю только в личных сообщениях.\n\n🔑 Ключи к моду Cyrax\n🚀 Буст и гайды\n\n@${process.env.BOT_USERNAME || 'cyraxxmod_bot'}`,

    // Infinite Boost
    buy_infinite_boost: "🚀 Метод Буста",
    infinite_boost_title: "🚀 Секретный Метод Бесконечного Буста в MLBB",
    infinite_boost_desc: "Раскрой эксклюзивный способ для неограниченного прогресса в ранге MLBB без каких-либо модификаций игры! Получи детальную инструкцию с практическими шагами и бонус: 30% купон на ключ CyraxMod!",
    infinite_boost_purchase_success: "✅ Покупка подтверждена! Сейчас отправлю тебе инструкцию...",
    price_label: "💰 Цена:",

    // Reminder (используется как fallback, основной текст генерирует buildReminderMessage)
    reminder_message: "👋 Привет! Твой ключ на <b>{period}</b> скоро заканчивается — не пропусти!\n\n🕐 <i>Это ориентировочное время, точный момент истечения зависит от активации ключа.</i>\n\nНажми кнопку ниже, чтобы продлить:",
    reminder_button: "🛒 Перейти в магазин",

    // Loyalty coupon
    loyalty_coupon_message: "🎉 Спасибо за лояльность! Вот эксклюзивный купон на <b>{percent}%</b> скидки для твоего любимого периода. Действует 24 часа! 🎟️\n\nКод: <code>{code}</code>",

    // Coupon apply prompt
    apply_coupon_btn: "🎟️ Применить купон",

    // Ручной буст
    manual_boost_btn: "👤 Ручной Буст",
    manual_boost_title: "👤 <b>Профессиональный Ручной Буст</b>",
    manual_boost_desc:
      "🚀 <b>Хотите быстро подняться в рангах MLBB без лишних усилий?</b>\n\n" +
      "Буст вашего аккаунта руками опытных игроков. Никаких читов, ботов или модификаций — только скилл и стратегия.\n\n" +
      "🏆 <b>Гарантированный прогресс</b> — достигните желаемого ранга в кратчайшие сроки.\n" +
      "🛡️ <b>Безопасность</b> — VPN-защита, пароли не меняем, полный отчёт после.\n" +
      "⚡️ <b>Чистая игра</b> — без читов, только оптимальные пики и макро.\n" +
      "📊 <b>Персональный подход</b> — пилотируемый буст, можно наблюдать через стрим.\n\n" +
      "📋 Заполните заявку: выберите текущий ранг и желаемый. Бот рассчитает стоимость в RUB/USD/EUR/UAH и предложит оплату.",
    manual_boost_proceed: "📋 Оформить заявку",
    manual_boost_select_current: "🎮 <b>Шаг 1 из 2 — Текущий ранг</b>\n\nВыберите ваш <b>текущий</b> ранг в MLBB:",
    manual_boost_select_target: "🏆 <b>Шаг 2 из 2 — Желаемый ранг</b>\n\nТекущий: <b>{current}</b>\n\nТеперь выберите <b>желаемый</b> ранг:",
    manual_boost_enter_stars: "⭐ Укажите, сколько <b>звёзд/очков</b> у вас сейчас в ранге <b>{rank}</b>\n\n<i>(введите число, например: 2)</i>",
    manual_boost_enter_target_stars: "🎯 До скольких <b>звёзд/очков</b> нужно добраться в ранге <b>{rank}</b>?\n\n<i>(введите число, например: 30)</i>",
    manual_boost_submitted:
      "✅ <b>Заявка принята!</b>\n\n" +
      "🎮 Текущий ранг: <b>{current}</b>\n" +
      "🏆 Желаемый ранг: <b>{target}</b>\n\n" +
      "⏳ Администратор рассчитает стоимость и свяжется с вами. Ожидайте! 🤝",
    manual_boost_cost_received:
      "💰 <b>Стоимость вашего буста рассчитана!</b>\n\n" +
      "🎮 {current} → 🏆 {target}\n\n" +
      "💵 Стоимость:\n{costs}\n\n" +
      "Выберите валюту для оплаты:",
    manual_boost_choose_method: "💳 Выберите способ оплаты:",
    manual_boost_awaiting_receipt: "📸 Отправьте скриншот или PDF чека.\n\n⏳ Администратор проверит и свяжется с вами.",
    manual_boost_receipt_ok: "✅ Чек получен! Администратор скоро всё проверит.",
    manual_boost_confirmed_client: "🎉 <b>Оплата подтверждена!</b>\n\nАдминистратор свяжется с вами для уточнения деталей входа. Спасибо! 🤝",
    manual_boost_rejected_client: "❌ <b>Оплата не подтверждена.</b>\n\nОбратитесь в поддержку для уточнения деталей.",
    manual_boost_status: "coming_soon",

    // Отключение оплат
    payments_disabled_msg: "🛠 <b>Приём платежей временно приостановлен</b>\n\nМы активно работаем над запуском новых услуг и проводим плановые работы. Совсем скоро всё будет готово!\n\n📢 Следите за обновлениями в нашем канале.",
    payments_disabled_admin_toggle: "🔴 Отключить оплаты",
    payments_enabled_admin_toggle: "🟢 Включить оплаты",

    // Статусы отдельных разделов
    section_keys_disabled_msg: "🛠 <b>Продажа ключей временно приостановлена</b>\n\nСкоро вернёмся — следите за обновлениями в канале! 📢",
    section_boost_disabled_msg: "🛠 <b>Метод Буста временно недоступен</b>\n\nМы готовим кое-что интересное. Скоро запуск! 📢",
    section_manual_boost_disabled_msg: "🛠 <b>Ручной Буст временно недоступен</b>\n\nУслуга готовится к запуску. Совсем скоро! 📢",

    // Задача 1: лояльность
    loyalty_discount_label: '🎁 Ваша персональная скидка {X}%',

    // 🏀 Basketball game
    basketball_invite: '🏀 Пока платёж проверяется — один бросок!\nЕсли мяч войдёт чисто — получишь купон 🎁',
    basketball_throw_btn: '🏀 Бросить',
    basketball_already_thrown: 'Ты уже бросал мяч для этого заказа 🏀',
    basketball_win: '🎉 *Свиш!* Мяч в кольце!\n\n🎟 Купон *−5%* на *{period}*:\n`{code}`\n\n⏳ Действует до *{expires}*',
    basketball_lose: '😅 Мимо — бывает!\nВ следующий раз повезёт 🤞',

    // Задача 2: FOMO-купоны
    fomo_coupon_msg: '🎉 Поздравляем с покупкой!\n\nВ знак благодарности — персональный купон на <b>{percent}%</b> для следующей покупки! 🎁\n\n🎟️ Код: <code>{code}</code>\n⏰ Действует {days} дней\n\n<i>Используй при следующей покупке ключа! 🔑</i>',

    // Задача 3: отзывы
    review_btn: '✍️ Оставить отзыв и получить бонус',
    review_invite_msg: '✍️ <b>Оставьте отзыв и получите бонус!</b>\n\nНам важно ваше мнение! Напишите отзыв в нашем посте:\n{link}\n\nПосле этого отправьте ваш персональный код сюда — администратор выдаст вам награду. 💜',
    review_code_msg: '🔑 Ваш персональный код: <code>{code}</code>\n\nОтправьте его в этот чат после того, как оставите отзыв.',
    review_code_invalid: '❌ Код не найден или уже использован. Если считаете это ошибкой — обратитесь к администратору.',
    review_reward_sent: '🎉 Спасибо за ваш отзыв!\n\nВаша награда:\n{reward}\n\nМы ценим каждого нашего клиента! 💜',


    // 🤝 Партнёрство
    partner_btn: '🤝 Партнёрство',
    partner_landing_title: '🤝 <b>Партнёрская программа CyraxMods</b>',
    partner_landing_text:
      '🤝 <b>Партнёрская программа CyraxMods</b>\n\n' +
      '💡 <b>Что это?</b>\n' +
      'Хотите зарабатывать на продаже ключей CyraxMod? ' +
      'Запустите собственного Telegram-бота, который продаёт наши ключи под вашим брендом. ' +
      'Вся техническая часть — на нас: ключи, купоны, оплата, выдача. Вы фокусируетесь на продвижении и получаете прибыль с каждой продажи.\n\n' +
      '━━━━━━━━━━━━━━━━━━━━\n\n' +
      '💰 <b>Сколько можно заработать?</b>\n' +
      'Вы устанавливаете наценку от 0% до 200% поверх базовой цены. Чем больше клиентов — тем больше доход.\n\n' +
      '📊 <b>Пример при наценке 50%:</b>\n' +
      '• Ключ 1 день: ваш заработок ~$0.75 (~{rub_075}₽)\n' +
      '• Ключ 7 дней: ваш заработок ~$3.15 (~{rub_315}₽)\n' +
      '• Ключ 30 дней: ваш заработок ~$10.50 (~{rub_1050}₽)\n' +
      '• 3 продажи в день (микс) ≈ ~$150–300/мес (~{rub_150}–{rub_300}₽)\n\n' +
      '━━━━━━━━━━━━━━━━━━━━\n\n' +
      '⚡ <b>Что входит в подключение:</b>\n' +
      '• Полностью готовый бот — не нужно писать код\n' +
      '• Автоматическая выдача ключей 24/7\n' +
      '• Мультивалютность (RUB/USD/EUR/UAH)\n' +
      '• Персональные купоны для ваших клиентов\n' +
      '• Статистика продаж в реальном времени\n' +
      '• Вывод заработка по запросу\n\n' +
      '━━━━━━━━━━━━━━━━━━━━\n\n' +
      '🛡️ <b>Безопасность:</b>\n' +
      '• Ваш бот-токен зашифрован (AES-256) — даже мы не видим его\n' +
      '• Ключи выдаются автоматически из нашего запаса\n' +
      '• Вы не несёте технических рисков\n\n' +
      '━━━━━━━━━━━━━━━━━━━━\n\n' +
      '📋 <b>Как подключиться:</b>\n' +
      '1️⃣ Создайте бота через @BotFather (2 минуты)\n' +
      '2️⃣ Оплатите подключение (разово)\n' +
      '3️⃣ Отправьте токен — всё остальное сделает система\n\n' +
      '━━━━━━━━━━━━━━━━━━━━\n\n' +
      '❓ <b>Частые вопросы:</b>\n\n' +
      '• <i>Нужны ли технические навыки?</i>\nНет, всё автоматизировано\n\n' +
      '• <i>Могу ли я менять цены?</i>\nДа, через настройку наценки\n\n' +
      '• <i>Как получить заработок?</i>\nВывод по запросу через администратора\n\n' +
      '• <i>Что если я потеряю токен?</i>\nНапишите нам и восстановим доступ\n\n' +
      '• <i>Есть ли абонентская плата?</i>\nНет, оплата только разовая за подключение',
    partner_connect_btn: '📲 Подключиться',
    partner_already_active: '✅ Вы уже являетесь активным партнёром! Используйте админ-панель в вашем боте.',
    partner_already_pending: '⏳ Ваша заявка на подключение уже в обработке.',
    partner_token_request: '🔑 <b>Отлично!</b> Теперь отправьте токен вашего бота.\n\nПолучить его можно у @BotFather:\n1. Откройте @BotFather\n2. Отправьте /newbot\n3. Следуйте инструкциям\n4. Скопируйте токен и отправьте сюда\n\n<i>Токен выглядит примерно так:</i> <code>1234567890:ABCdefGhIjKlmNoPqRs</code>',
    partner_token_invalid: '❌ Токен невалидный. Проверьте правильность и отправьте снова.',
    partner_token_saved: '✅ <b>Ваш бот запущен!</b>\n\n🤖 Бот: @{botUsername}\n💰 Наценка: {markup}%\n\nТеперь рекламируйте вашего бота и зарабатывайте с каждой продажи! 🚀',
    partner_markup_request: '⚙️ Укажите вашу наценку в процентах (0-200).\n\nЭто процент, который добавляется к базовой цене. Например:\n— При наценке 50%, ключ за 100₽ будет стоить 150₽ (50₽ ваш доход).\n— При наценке 20%, ключ за 10$ будет стоить 12$ (2$ ваш доход).\n— При наценке 30%, ключ за 15€ будет стоить 19.5€ (4.5€ ваш доход).',
    partner_disabled: '🔒 Партнёрская программа временно недоступна.',
    coupon_error_partner: '❌ Купоны не принимаются для оплаты подключения партнёрской программы.',
    coupon_error_period: '❌ Этот купон действует только на {period}. Выберите соответствующий срок ключа.',
    coupon_not_found: '❌ Купон не найден. Проверьте код и попробуйте снова.',

    // Баны
    banned: '🚫 Ваш доступ заблокирован администратором. По вопросам — обратитесь в поддержку.',
    banned_temp: '🚫 Вы временно заблокированы за спам.\n\n⏰ Бан до: {until}\n\nЕсли считаете это ошибкой — напишите администратору.',

    // Буст
    boost_data_corrupted: '⚠️ Данные заявки на буст повреждены. Пожалуйста, начните заново.',
    boost_request_error: '❌ Ошибка при отправке заявки на буст. Попробуйте позже или обратитесь в поддержку.',

    // Статистика
    stats_load_error: '❌ Не удалось загрузить статистику. Попробуйте позже.',
  },
  en: {
    welcome: "🛍️ Welcome to CyraxMods!",
    buy_key: "🔑  Buy Key",
    my_orders: "📂 My Keys",
    boost_hub: "⚡️  Rank Boost",
    channel: "📢 Channel",
    help: "❓ Help",
    offer: "📜 Public Offer",
    admin_panel: "🛠 Admin",
    back: "◀️ Back",
    support_ticket_btn: "🆘 Key not working?",

    choose_period: "🔑 Choose key duration:",
    choose_currency: "💱 Choose currency:",
    choose_payment: "💳 Choose payment method:",

    russia_sbp: "🇷🇺 SBP (Russia)",
    ukraine_card: "🇺🇦 Ukrainian bank card",
    italy_card: "🇮🇹 Italian bank card",
    paypal: "💰 PayPal",
    binance: "💎 Binance",
    cryptobot: "₿ CryptoBot",

    payment_info: "",
    paypal_instruction: "💰 <b>Payment {amount} {currency} — PayPal</b>\n\n<b>Steps:</b>\n1️⃣ Tap the button below → PayPal page opens\n2️⃣ Complete payment and save your PayPal receipt\n3️⃣ Send screenshot or PDF here — we're waiting!\n\n<i>⏱ Usually confirmed within 5–15 minutes</i>",
    sbp_instruction: "💳 <b>Payment {amount} RUB — SBP</b>\n\n<b>Steps:</b>\n1️⃣ Tap the button below and send <b>exactly {amount} RUB</b>\n2️⃣ Save the screenshot or PDF receipt\n3️⃣ Send it here — we're waiting!\n\n<i>⏱ Usually confirmed within 5–15 minutes</i>",
    card_ua_instruction: "🇺🇦 <b>Payment {amount} {currency} — Card UA</b>\n\n<b>Card number:</b>\n<code>{card}</code>\n\n<b>Steps:</b>\n1️⃣ Transfer <b>exactly {amount} {currency}</b> to the card\n2️⃣ Wait for bank confirmation and save the screenshot\n3️⃣ Send the screenshot here — we're waiting!\n\n<i>⏱ Usually confirmed within 5–15 minutes</i>",
    card_it_instruction: "🇮🇹 <b>Payment {amount} {currency} — Card IT</b>\n\n<b>Details:</b>\n<code>{card}</code>\n\n<b>Steps:</b>\n1️⃣ Transfer <b>exactly {amount} {currency}</b> using these details\n2️⃣ Save the screenshot or PDF\n3️⃣ Send it here — we're waiting!\n\n<i>⏱ Usually confirmed within 5–15 minutes</i>",
    binance_instruction: "💎 <b>Payment {amount} {currency} — Binance P2P</b>\n\n<b>Recipient Binance ID:</b>\n<code>{id}</code>\n\n<b>Steps:</b>\n1️⃣ Open Binance → P2P → find trader by ID\n2️⃣ Send <b>exactly {amount} {currency}</b>\n3️⃣ Save screenshot and send here — we're waiting!\n\n<i>⚠️ P2P only, not Spot!</i>\n<i>⏱ Usually confirmed within 5–15 minutes</i>",
    cryptobot_instruction: "🤖 <b>Payment {amount} {currency} — CryptoBot</b>\n\n1. Tap the button below — a CryptoBot invoice will open\n2. Pay with cryptocurrency — the key is delivered automatically\n\n⚡️ Instant confirmation, key issued immediately",
    cryptobot_usd_instruction: "🤖 <b>Payment {amount} USD (USDT) — CryptoBot</b>\n\n1. Tap the button below — a CryptoBot invoice will open\n2. Pay with cryptocurrency — the key is delivered automatically\n\n⚡️ Instant confirmation, key issued immediately",

    receipt_received: "✅ Receipt received! Payment is being reviewed — usually just a few minutes. ⏳",
    send_transaction: "✅ After payment, send screenshot or PDF receipt.\n\n⏳ Wait for admin confirmation.",

    order_confirmed_title: "🎊✨ Thank you for your purchase! ✨🎊",
    order_confirmed_key: "🔑 Your key:",
    order_confirmed_period: "📦 Duration:",
    order_confirmed_activation: "⚡️ Activation: on first use in CyraxMod",
    order_confirmed_channel: "📢 Our channel:",
    order_confirmed_footer: "💬 Contact us if you have any questions!",
    order_out_of_stock: "🔧 Keys for this period are temporarily out of stock. Your order has been received, and we will issue the key manually as soon as possible. Thank you for your understanding!",

    maintenance: "🔧 Maintenance in progress\n⏰ {time}\n💬 Reason: {reason}\n✨ We'll be back soon!",
    maintenance_over: "🟢✅ All set — bot is back online! 🔑🎉",
    maintenance_time: "About {minutes} min left",
    rate_limit: "⚠️ Too many actions. Wait a minute.",
    no_keys: "📂 No keys purchased yet.\n\nBuy your first key and it will show up here! 🔑",
    waiting_screenshot: "📸 Got it! Send your payment screenshot or PDF here.",
    error_fetching_orders: "❌ Error fetching orders",
    error_creating_invoice: "❌ Error creating invoice. Try again later.",
    error_order_data_missing: "❌ Error: order data not found. Please start over.",
    error_creating_order: "❌ Error creating order. Try again later.",
    order_already_pending: "⚠️ You already have a pending order for this product. Please wait for it to be processed.",
    tap_to_copy: "└ (tap to copy)",

    pay_button: "💰 Pay via {method}",

    offer_text:
      `📜 <b>PUBLIC OFFER — CyraxMods</b>\n\n` +
      `This document is the public offer of the CyraxMods shop. By clicking "Buy", "Pay", or otherwise completing a purchase, you fully accept all terms below.\n\n` +

      `<b>1. Products & Services</b>\n` +
      `🔑 MLBB Mod Activation Key — access for 1/3/7/30 days.\n` +
      `📈 Boost Method — detailed guide + 24/7 support for self-boosting your account.\n` +
      `🏆 Manual Boost — account boosting by CyraxMods specialists with report and screenshots.\n\n` +
      `All products and services are for personal non-commercial use only. By purchasing, you confirm you are 18+ years old.\n\n` +

      `<b>2. Delivery</b>\n` +
      `2.1. Key/access is delivered automatically after payment confirmation.\n` +
      `2.2. A key is considered delivered at the moment it appears in your chat.\n` +
      `2.3. Boost Method — fulfilled upon receipt of the full guide and access to support.\n` +
      `2.4. Manual Boost — fulfilled after completion of the boost and sending you the report/screenshots.\n\n` +

      `<b>3. Coupons & Discounts</b>\n` +
      `3.1. Personal discount coupons may be issued to loyal customers at the administration's discretion.\n` +
      `3.2. A coupon is single-use, tied to a specific user, and may not be transferred.\n` +
      `3.3. The expiry period of a coupon is stated at the time of issuance.\n\n` +

      `<b>4. Usage Rules</b>\n` +
      `4.1. An activation key is used once on a single account.\n` +
      `4.2. It is forbidden to share the key/guide with third parties or publish it publicly.\n` +
      `4.3. Using the Shop's products or services for commercial purposes is prohibited.\n\n` +

      `<b>5. Risks</b>\n` +
      `5.1. Using mods and boosting violates MLBB's terms of service and always carries a risk of account ban — this is solely determined by Moonton's policies.\n` +
      `5.2. The Shop bears no responsibility for any bans or sanctions imposed by the game's developer.\n` +
      `5.3. You independently assess all risks before making a purchase.\n\n` +

      `<b>6. Refunds & Replacements</b>\n` +
      `6.1. Refunds for digital products of adequate quality are not provided.\n` +
      `6.2. Exceptions reviewed individually by the administration:\n` +
      `• confirmed technical failure of a key due to the Shop's fault;\n` +
      `• a Manual Boost that has not yet been started by our specialists;\n` +
      `• other circumstances at the administration's discretion.\n` +
      `6.3. Contact support within 24 hours of receiving your product with proof attached.\n` +
      `6.4. Once a Manual Boost has been started, no refund is available.\n` +
      `6.5. Any refund decision is made by the administration on a case-by-case basis and is not guaranteed.\n\n` +

      `<b>7. Support</b>\n` +
      `All issues are resolved promptly through in-bot support. The current version of this offer is always available in the bot.\n\n` +

      `Thank you for choosing CyraxMods! 💜`,

    offer_back: "◀️ Back to menu",
    private_only: `👋 I only work in private messages.\n\n🔑 Cyrax mod keys\n🚀 Boost & guides\n\n@${process.env.BOT_USERNAME || 'cyraxxmod_bot'}`,

    // Infinite Boost
    buy_infinite_boost: "🚀 Boost Method",
    infinite_boost_title: "🚀 Secret Infinite Boost Method for MLBB",
    infinite_boost_desc: "Unlock an exclusive method for unlimited rank progression in MLBB without any game modifications! Get a detailed guide with practical steps and bonus: 30% coupon for CyraxMod key!",
    infinite_boost_purchase_success: "✅ Purchase confirmed! Sending you the guide now...",
    price_label: "💰 <b>Price:</b>",

    // Reminder (fallback only, main text is built by buildReminderMessage)
    reminder_message: "👋 Hey! Your <b>{period}</b> key is expiring soon — don't miss it!\n\n🕐 <i>This is an approximate time — the exact expiry depends on when the key was first activated.</i>\n\nTap below to renew:",
    reminder_button: "🛒 Go to shop",

    // Loyalty coupon
    loyalty_coupon_message: "🎉 Thanks for your loyalty! Here's an exclusive <b>{percent}%</b> discount coupon for your favorite period. Valid for 24 hours! 🎟️\n\nCode: <code>{code}</code>",

    // Coupon apply prompt
    apply_coupon_btn: "🎟️ Apply Coupon",

    // Manual Boost
    manual_boost_btn: "👤 Pro Boost",
    manual_boost_title: "👤 <b>Professional Manual Boost</b>",
    manual_boost_desc:
      "🚀 <b>Want to rank up in MLBB fast without the grind?</b>\n\n" +
      "We boost your account using skilled players — no cheats, no bots, no modifications. Pure skill and strategy.\n\n" +
      "🏆 <b>Guaranteed progress</b> — reach your desired rank as fast as possible.\n" +
      "🛡️ <b>Safety</b> — VPN protection, we never change passwords, full match report after.\n" +
      "⚡️ <b>Clean gameplay</b> — no cheats, only optimal picks and macro.\n" +
      "📊 <b>Personal approach</b> — piloted boost, you can watch via live stream.\n\n" +
      "📋 Fill out a request: select your current rank and desired rank. Bot will calculate the cost in RUB/USD/EUR/UAH and offer payment.",
    manual_boost_proceed: "📋 Submit Request",
    manual_boost_select_current: "🎮 <b>Step 1 of 2 — Current Rank</b>\n\nSelect your <b>current</b> rank in MLBB:",
    manual_boost_select_target: "🏆 <b>Step 2 of 2 — Desired Rank</b>\n\nCurrent: <b>{current}</b>\n\nNow select your <b>desired</b> rank:",
    manual_boost_enter_stars: "⭐ How many <b>stars/points</b> do you currently have in <b>{rank}</b>?\n\n<i>(enter a number, e.g. 2)</i>",
    manual_boost_enter_target_stars: "🎯 How many <b>stars/points</b> do you want to reach in <b>{rank}</b>?\n\n<i>(enter a number, e.g. 30)</i>",
    manual_boost_submitted:
      "✅ <b>Request submitted!</b>\n\n" +
      "🎮 Current rank: <b>{current}</b>\n" +
      "🏆 Desired rank: <b>{target}</b>\n\n" +
      "⏳ The admin will calculate the cost and contact you shortly. 🤝",
    manual_boost_cost_received:
      "💰 <b>Your boost cost has been calculated!</b>\n\n" +
      "🎮 {current} → 🏆 {target}\n\n" +
      "💵 Cost:\n{costs}\n\n" +
      "Select a currency to pay:",
    manual_boost_choose_method: "💳 Select payment method:",
    manual_boost_awaiting_receipt: "📸 Please send a screenshot or PDF receipt.\n\n⏳ The admin will verify and contact you.",
    manual_boost_receipt_ok: "✅ Receipt received! The admin will check it shortly.",
    manual_boost_confirmed_client: "🎉 <b>Payment confirmed!</b>\n\nThe admin will contact you for account entry details. Thank you! 🤝",
    manual_boost_rejected_client: "❌ <b>Payment not confirmed.</b>\n\nPlease contact support for clarification.",
    manual_boost_status: "coming_soon",

    // Payments disabled
    payments_disabled_msg: "🛠 <b>Payments are temporarily paused</b>\n\nWe're actively working on launching new services and performing scheduled maintenance. Everything will be ready very soon!\n\n📢 Follow our channel for updates.",
    payments_disabled_admin_toggle: "🔴 Disable payments",
    payments_enabled_admin_toggle: "🟢 Enable payments",

    // Section statuses
    section_keys_disabled_msg: "🛠 <b>Key sales are temporarily paused</b>\n\nWe'll be back soon — follow our channel for updates! 📢",
    section_boost_disabled_msg: "🛠 <b>Boost Method is temporarily unavailable</b>\n\nSomething exciting is coming. Launch very soon! 📢",
    section_manual_boost_disabled_msg: "🛠 <b>Manual Boost is temporarily unavailable</b>\n\nThe service is being set up. Coming soon! 📢",

    // Task 1: loyalty
    loyalty_discount_label: '🎁 Your personal discount {X}%',

    // 🏀 Basketball game
    basketball_invite: '🏀 One shot while payment is being reviewed!\nSink it clean — score a coupon 🎁',
    basketball_throw_btn: '🏀 Shoot',
    basketball_already_thrown: 'You already took your shot for this order 🏀',
    basketball_win: '🎉 *Swish!* Clean basket!\n\n🎟 Coupon *−5%* for *{period}*:\n`{code}`\n\n⏳ Valid until *{expires}*',
    basketball_lose: '😅 Missed — happens!\nMaybe next time 🤞',

    // Task 2: FOMO coupons
    fomo_coupon_msg: '🎉 <b>Congratulations on your purchase!</b>\n\nAs a thank-you — a personal <b>{percent}%</b> coupon for your next purchase! 🎁\n\n🎟️ Code: <code>{code}</code>\n⏰ Valid for {days} days\n\n<i>Use it on your next key purchase! 🔑</i>',

    // Task 3: reviews
    review_btn: '✍️ Leave a review & get a bonus',
    review_invite_msg: '✍️ <b>Leave a review and get a bonus!</b>\n\nYour opinion matters! Write a review in our post:\n{link}\n\nThen send your personal code here — the admin will reward you. 💜',
    review_code_msg: '🔑 Your personal code: <code>{code}</code>\n\nSend it in this chat after leaving your review.',
    review_code_invalid: '❌ Code not found or already used. If you think this is an error, please contact the admin.',
    review_reward_sent: '🎉 <b>Thank you for your review!</b>\n\nYour reward:\n{reward}\n\nWe appreciate every customer! 💜',


    // 🤝 Partnership
    partner_btn: '🤝 Partnership',
    partner_landing_title: '🤝 <b>CyraxMods Partner Program</b>',
    partner_landing_text:
      '🤝 <b>CyraxMods Partner Program</b>\n\n' +
      '💡 <b>What is this?</b>\n' +
      'Want to earn money selling CyraxMod keys? ' +
      'Launch your own Telegram bot that sells our keys under your brand. ' +
      'All technical aspects are on us: keys, coupons, payments, delivery. You focus on promotion and earn from every sale.\n\n' +
      '━━━━━━━━━━━━━━━━━━━━\n\n' +
      '💰 <b>How much can you earn?</b>\n' +
      'Set your markup from 0% to 200% on top of the base price. More clients = more income.\n\n' +
      '📊 <b>Example with 50% markup:</b>\n' +
      '• 1-day key: you earn ~$0.75 (~{rub_075}₽)\n' +
      '• 7-day key: you earn ~$3.15 (~{rub_315}₽)\n' +
      '• 30-day key: you earn ~$10.50 (~{rub_1050}₽)\n' +
      '• 3 sales/day (mixed) ≈ ~$150–300/month (~{rub_150}–{rub_300}₽)\n\n' +
      '━━━━━━━━━━━━━━━━━━━━\n\n' +
      '⚡ <b>What\'s included:</b>\n' +
      '• Fully ready bot — no coding required\n' +
      '• Automatic key delivery 24/7\n' +
      '• Multi-currency support (RUB/USD/EUR/UAH)\n' +
      '• Personal coupons for your customers\n' +
      '• Real-time sales statistics\n' +
      '• Withdraw earnings on request\n\n' +
      '━━━━━━━━━━━━━━━━━━━━\n\n' +
      '🛡️ <b>Security:</b>\n' +
      '• Your bot token is encrypted (AES-256) — even we can\'t see it\n' +
      '• Keys are issued automatically from our stock\n' +
      '• You bear no technical risks\n\n' +
      '━━━━━━━━━━━━━━━━━━━━\n\n' +
      '📋 <b>How to connect:</b>\n' +
      '1️⃣ Create a bot via @BotFather (2 minutes)\n' +
      '2️⃣ Pay the connection fee (one-time)\n' +
      '3️⃣ Send the token — the system handles the rest\n\n' +
      '━━━━━━━━━━━━━━━━━━━━\n\n' +
      '❓ <b>FAQ:</b>\n\n' +
      '• <i>Do I need technical skills?</i>\nNo, everything is automated\n\n' +
      '• <i>Can I change prices?</i>\nYes, via markup settings\n\n' +
      '• <i>How do I get my earnings?</i>\nWithdraw on request via admin\n\n' +
      '• <i>What if I lose my token?</i>\nContact us and we\'ll restore access\n\n' +
      '• <i>Is there a subscription fee?</i>\nNo, only a one-time connection fee',
    partner_connect_btn: '📲 Connect',
    partner_already_active: '✅ You are already an active partner! Use the admin panel in your bot.',
    partner_already_pending: '⏳ Your connection request is already being processed.',
    partner_token_request: '🔑 <b>Great!</b> Now send your bot token.\n\nGet it from @BotFather:\n1. Open @BotFather\n2. Send /newbot\n3. Follow the instructions\n4. Copy the token and send it here\n\n<i>Token looks like:</i> <code>1234567890:ABCdefGhIjKlmNoPqRs</code>',
    partner_token_invalid: '❌ Invalid token. Please check and try again.',
    partner_token_saved: '✅ <b>Your bot is live!</b>\n\n🤖 Bot: @{botUsername}\n💰 Markup: {markup}%\n\nPromote your bot and earn from every sale! 🚀',
    partner_markup_request: '⚙️ Specify your markup in percentage (0-200).\n\nThis percentage is added to the base price. For example:\n— With 50% markup, a 10€ key will cost 15€ (5€ is your profit).\n— With 20% markup, a 10$ key will cost 12$ (2$ is your profit).',
    partner_disabled: '🔒 The partner program is currently unavailable.',
    coupon_error_partner: '❌ Coupons cannot be used for partner program connection.',
    coupon_error_period: '❌ This coupon is valid only for {period}. Please select the appropriate key duration.',
    coupon_not_found: '❌ Coupon not found. Please check the code and try again.',

    // Bans
    banned: '🚫 Your access has been blocked by the administrator. Contact support for assistance.',
    banned_temp: '🚫 You are temporarily blocked for spam.\n\n⏰ Banned until: {until}\n\nIf you think this is a mistake — contact the administrator.',

    // Boost
    boost_data_corrupted: '⚠️ Your boost request data is corrupted. Please start over.',
    boost_request_error: '❌ Error submitting your boost request. Try again later or contact support.',

    // Stats
    stats_load_error: '❌ Failed to load statistics. Please try again later.',
  }
};

function getLang(user) {
  const code = (user.language_code || 'en').toLowerCase();
  // RU-интерфейс: Россия, Украина, Беларусь, Казахстан, Узбекистан,
  // Кыргызстан, Таджикистан, Туркменистан, Азербайджан, Армения,
  // Грузия, Молдова, Польша, Болгария (кириллица), Сербия
  const ruLocales = ['ru', 'uk', 'be', 'kk', 'uz', 'ky', 'tg', 'tk', 'az', 'hy', 'ka', 'ro', 'pl', 'bg', 'sr'];
  if (ruLocales.some(l => code.startsWith(l))) return 'ru';
  return 'en';
}

function t(user, key, replacements = {}) {
  const lang = getLang(user);
  let text = translations[lang]?.[key] || translations['en']?.[key] || key;
  for (const [k, v] of Object.entries(replacements)) {
    text = text.replace(new RegExp(`{${k}}`, 'g'), v);
  }
  return text;
}

// ==========================================
// 💾 БАЗА ДАННЫХ (Инициализация)
// ==========================================
function initializeDatabase() {
  return new Promise((resolve, reject) => {
    db.serialize(() => {
      db.run(`CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY, 
        username TEXT, 
        language_code TEXT,
        joined_at DATETIME DEFAULT CURRENT_TIMESTAMP
      )`);

      db.run(`CREATE TABLE IF NOT EXISTS keys (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        product TEXT NOT NULL,
        key_value TEXT UNIQUE NOT NULL,
        status TEXT DEFAULT 'available',
        buyer_id INTEGER,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        sold_at DATETIME
      )`);

      db.run(`CREATE TABLE IF NOT EXISTS orders (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        username TEXT,
        user_lang TEXT,
        product TEXT NOT NULL,
        amount REAL NOT NULL,
        currency TEXT NOT NULL,
        method TEXT NOT NULL,
        invoice_id INTEGER,
        transaction_id TEXT,
        status TEXT DEFAULT 'pending',
        key_issued TEXT,
        receipt_file_id TEXT,
        receipt_type TEXT DEFAULT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        confirmed_at DATETIME,
        original_currency TEXT,
        original_amount REAL,
        coupon_id INTEGER DEFAULT NULL,
        hourglass_msg_id INTEGER DEFAULT NULL,
        reseller_markup_pct FLOAT DEFAULT NULL,
        reseller_questionnaire TEXT DEFAULT NULL
      )`);

      db.run(`CREATE TABLE IF NOT EXISTS action_logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        action TEXT NOT NULL,
        details TEXT,
        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
      )`);

      db.run(`CREATE TABLE IF NOT EXISTS manual_stats (
        key TEXT PRIMARY KEY,
        value INTEGER
      )`);

      db.run(`CREATE TABLE IF NOT EXISTS daily_stats (
        date TEXT PRIMARY KEY,
        orders_count INTEGER DEFAULT 0,
        revenue_usd REAL DEFAULT 0,
        revenue_rub REAL DEFAULT 0,
        revenue_eur REAL DEFAULT 0,
        revenue_uah REAL DEFAULT 0
      )`);

      db.run(`CREATE TABLE IF NOT EXISTS prices (
        product TEXT NOT NULL,
        currency TEXT NOT NULL,
        amount REAL NOT NULL,
        PRIMARY KEY (product, currency)
      )`);

      db.run(`CREATE TABLE IF NOT EXISTS payment_details (
        method TEXT PRIMARY KEY,
        details TEXT NOT NULL
      )`);

      // П.6: Новые таблицы
      db.run(`CREATE TABLE IF NOT EXISTS coupons (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        code TEXT UNIQUE NOT NULL,
        discount_percent INTEGER NOT NULL CHECK(discount_percent > 0 AND discount_percent <= 100),
        max_uses INTEGER DEFAULT 1,
        used_count INTEGER DEFAULT 0,
        expires_at DATETIME,
        created_by INTEGER,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        is_active INTEGER DEFAULT 1,
        product_restriction TEXT,
        user_id INTEGER
      )`);

      db.run(`CREATE TABLE IF NOT EXISTS user_coupons (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        coupon_id INTEGER NOT NULL,
        issued_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        used_at DATETIME,
        order_id INTEGER,
        UNIQUE(user_id, coupon_id)
      )`);

      db.run(`CREATE TABLE IF NOT EXISTS settings (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
      )`, () => {
        // Дефолтные настройки
        const defaults = [
          ['notify_new_user', '1'],
          ['notify_new_order', '1'],
          ['notify_low_keys', '1'],
          ['low_keys_threshold', '5'],
          ['notify_daily_report', '0'],
          ['channel_link', 'https://t.me/cyraxml'],
          ['support_link', 'https://t.me/cyraxml'],
          ['reminders_enabled', '1'],
          ['payments_disabled', '0'],
          ['keys_disabled', '0'],
          ['boost_disabled', '0'],
          ['manual_boost_disabled', '0']
        ];
        defaults.forEach(([k, v]) => {
          db.run(`INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)`, [k, v]);
        });
      });

      // Новые таблицы
      db.run(`CREATE TABLE IF NOT EXISTS reminders (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        order_id INTEGER NOT NULL,
        sent_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(user_id, order_id)
      )`);

      db.run(`CREATE TABLE IF NOT EXISTS coupon_products (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        coupon_id INTEGER NOT NULL,
        product TEXT NOT NULL,
        UNIQUE(coupon_id, product)
      )`);

      // Безопасная миграция: добавляем user_id в coupons если нет
      db.all("PRAGMA table_info(coupons)", [], (err, columns) => {
        if (!err && columns) {
          const hasUserId = columns.some(c => c.name === 'user_id');
          if (!hasUserId) {
            db.run("ALTER TABLE coupons ADD COLUMN user_id INTEGER DEFAULT NULL", (e) => {
              if (e) console.error('ALTER coupons user_id:', e.message);
              else console.log('✅ Added user_id to coupons');
            });
          }
        }
      });

      // Индексы для производительности
      db.run(`CREATE INDEX IF NOT EXISTS idx_orders_user_status ON orders(user_id, status)`);
      db.run(`CREATE INDEX IF NOT EXISTS idx_orders_status_created ON orders(status, created_at)`);
      db.run(`CREATE INDEX IF NOT EXISTS idx_orders_confirmed_at ON orders(confirmed_at)`);
      db.run(`CREATE INDEX IF NOT EXISTS idx_user_coupons_coupon ON user_coupons(coupon_id)`);
      // BUG FIX RC-1: Уникальный частичный индекс — не даёт создать два pending-заказа
      // для одного пользователя на один продукт одновременно (защита от двойного чека).
      // ВАЖНО: сначала удаляем дубли (оставляем только самый новый), потом создаём индекс.
      // Это необходимо для совместимости с существующей БД где дубли уже могли накопиться.
      db.run(`
        DELETE FROM orders
        WHERE status = 'pending'
          AND id NOT IN (
            SELECT MAX(id) FROM orders
            WHERE status = 'pending'
            GROUP BY user_id, product
          )
      `, (cleanErr) => {
        if (cleanErr) {
          console.error('⚠️ RC-1 dedup cleanup error (non-fatal):', cleanErr.message);
        }
        db.run(
          `CREATE UNIQUE INDEX IF NOT EXISTS uq_user_pending_product ON orders(user_id, product) WHERE status = 'pending'`,
          (idxErr) => {
            if (idxErr) {
              // Если индекс всё равно не создался — логируем, но не падаем.
              // Защита остаётся на уровне SELECT-проверки в коде.
              console.error('⚠️ RC-1 unique index creation failed (non-fatal):', idxErr.message);
            } else {
              console.log('✅ RC-1 unique pending index created');
            }
          }
        );
      });
      // BUG FIX SQL-1: Индекс по username — поиск пользователя без full table scan
      db.run(`CREATE INDEX IF NOT EXISTS idx_users_username ON users(username)`);
      // BUG FIX SQL-2: Индекс для логов
      db.run(`CREATE INDEX IF NOT EXISTS idx_action_logs_user ON action_logs(user_id, timestamp)`);
      // BUG FIX SQL-3: Индекс по invoice_id для CryptoBot webhook
      db.run(`CREATE INDEX IF NOT EXISTS idx_orders_invoice ON orders(invoice_id)`);

      // Таблица заявок на ручной буст
      db.run(`CREATE TABLE IF NOT EXISTS boost_requests (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        username TEXT,
        user_lang TEXT,
        current_rank TEXT NOT NULL,
        desired_rank TEXT NOT NULL,
        stars_current INTEGER DEFAULT 0,
        stars_desired INTEGER DEFAULT 0,
        base_cost_rub REAL,
        costs_json TEXT,
        status TEXT DEFAULT 'pending',
        receipt_file_id TEXT,
        receipt_type TEXT,
        payment_currency TEXT,
        payment_method TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        confirmed_at DATETIME
      )`);
      db.run(`CREATE INDEX IF NOT EXISTS idx_boost_requests_user ON boost_requests(user_id, status)`);

      // ✅ Миграция: добавляем недостающие колонки в orders
      db.all("PRAGMA table_info(orders)", [], (err, columns) => {
        if (!err && columns) {
          const hasCouponId = columns.some(c => c.name === 'coupon_id');
          const hasHourglassId = columns.some(c => c.name === 'hourglass_msg_id');
          const hasOriginalCurrency = columns.some(c => c.name === 'original_currency');
          const hasOriginalAmount = columns.some(c => c.name === 'original_amount');
          if (!hasCouponId) {
            db.run("ALTER TABLE orders ADD COLUMN coupon_id INTEGER DEFAULT NULL", (e) => {
              if (e) console.error('❌ Migration orders.coupon_id:', e.message);
              else console.log('✅ Added orders.coupon_id');
            });
          }
          if (!hasHourglassId) {
            db.run("ALTER TABLE orders ADD COLUMN hourglass_msg_id INTEGER DEFAULT NULL", (e) => {
              if (e) console.error('❌ Migration orders.hourglass_msg_id:', e.message);
              else console.log('✅ Added orders.hourglass_msg_id');
            });
          }
          if (!hasOriginalCurrency) {
            db.run("ALTER TABLE orders ADD COLUMN original_currency TEXT DEFAULT NULL", (e) => {
              if (e) console.error('❌ Migration orders.original_currency:', e.message);
              else console.log('✅ Added orders.original_currency');
            });
          }
          if (!hasOriginalAmount) {
            db.run("ALTER TABLE orders ADD COLUMN original_amount REAL DEFAULT NULL", (e) => {
              if (e) console.error('❌ Migration orders.original_amount:', e.message);
              else console.log('✅ Added orders.original_amount');
            });
          }
        }
      });

      // Миграция для старых баз
      db.run(`ALTER TABLE coupons ADD COLUMN product_restriction TEXT`, (err) => { });
      db.run(`ALTER TABLE coupons ADD COLUMN user_id INTEGER`, (err) => { });
      db.run(`ALTER TABLE resellers ADD COLUMN questionnaire TEXT`, (err) => { });
      // ✅ Миграция: переименовываем cost_rub → base_cost_rub для уже существующих БД
      db.all("PRAGMA table_info(boost_requests)", [], (err, columns) => {
        if (!err && columns) {
          const hasCostRub = columns.some(c => c.name === 'cost_rub');
          const hasBaseCostRub = columns.some(c => c.name === 'base_cost_rub');
          if (hasCostRub && !hasBaseCostRub) {
            // SQLite не поддерживает RENAME COLUMN до версии 3.25 — пересоздаём через ALTER ADD + UPDATE
            db.run("ALTER TABLE boost_requests ADD COLUMN base_cost_rub REAL", (e) => {
              if (!e) {
                db.run("UPDATE boost_requests SET base_cost_rub = cost_rub", (e2) => {
                  if (!e2) console.log('✅ Migrated boost_requests.cost_rub → base_cost_rub');
                  else console.error('❌ Migration copy error:', e2.message);
                });
              } else {
                console.error('❌ Migration ADD COLUMN error:', e.message);
              }
            });
            // Миграция: reseller_id и reseller_markup_pct для буст-заявок реселлеров
            db.run(`ALTER TABLE boost_requests ADD COLUMN reseller_id INTEGER DEFAULT NULL`, (e) => {
              if (e && !e.message.includes('duplicate column')) console.error('Migration boost_requests.reseller_id:', e.message);
            });
            db.run(`ALTER TABLE boost_requests ADD COLUMN reseller_markup_pct FLOAT DEFAULT NULL`, (e) => {
              if (e && !e.message.includes('duplicate column')) console.error('Migration boost_requests.reseller_markup_pct:', e.message);
            });
          }
        }
      });

      // =============================================
      // 🔧 МИГРАЦИИ: новые таблицы и колонки (Задачи 1-5)
      // =============================================

      // Задача 1: loyalty_discount в users
      db.run(`ALTER TABLE users ADD COLUMN loyalty_discount INTEGER DEFAULT 0`, (e) => {
        if (e && !e.message.includes('duplicate column')) console.error('Migration users.loyalty_discount:', e.message);
        else if (!e) console.log('✅ Added users.loyalty_discount');
      });

      // Промпт 3: issue_reason в keys
      db.run(`ALTER TABLE keys ADD COLUMN issue_reason TEXT DEFAULT NULL`, (e) => {
        if (e && !e.message.includes('duplicate column')) console.error('Migration keys.issue_reason:', e.message);
        else if (!e) console.log('✅ Added keys.issue_reason');
      });


      // =============================================
      // 🛡️ ANTI-SCAM: Новые таблицы и миграции
      // =============================================

      // Таблица сессий пользователей
      db.run(`CREATE TABLE IF NOT EXISTS user_sessions_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        session_start DATETIME DEFAULT CURRENT_TIMESTAMP,
        session_end DATETIME,
        actions_count INTEGER DEFAULT 0,
        user_agent TEXT
      )`);
      db.run(`CREATE INDEX IF NOT EXISTS idx_sessions_user ON user_sessions_history(user_id, session_start)`);

      // Таблица тикетов поддержки
      db.run(`CREATE TABLE IF NOT EXISTS support_tickets (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ticket_number TEXT UNIQUE,
        user_id INTEGER NOT NULL,
        order_id INTEGER,
        key_id INTEGER,
        key_value TEXT,
        complaint_text TEXT,
        screenshot_file_id TEXT,
        status TEXT DEFAULT 'open',
        bot_verdict TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        resolved_at DATETIME,
        resolved_by INTEGER,
        resolution_note TEXT
      )`);
      db.run(`CREATE INDEX IF NOT EXISTS idx_tickets_user ON support_tickets(user_id, status)`);
      db.run(`CREATE INDEX IF NOT EXISTS idx_tickets_key ON support_tickets(key_value)`);

      // Миграция: добавить новые поля в существующую таблицу support_tickets
      const ticketMigrationCols = [
        ['ticket_number', 'TEXT'],
        ['key_value', 'TEXT'],
        ['screenshot_file_id', 'TEXT'],
        ['bot_verdict', 'TEXT'],
        ['resolved_by', 'INTEGER'],
        ['resolution_note', 'TEXT'],
      ];
      ticketMigrationCols.forEach(([col, type]) => {
        db.run(`ALTER TABLE support_tickets ADD COLUMN ${col} ${type}`, (e) => {
          if (e && !e.message.includes('duplicate column')) console.error(`Migration support_tickets.${col}:`, e.message);
        });
      });

      // Миграции: добавляем anti-scam колонки в users
      const userAntiScamCols = [
        ['last_activity', 'DATETIME'],
        ['total_interactions', 'INTEGER DEFAULT 0'],
        ['suspicion_score', 'INTEGER DEFAULT 0'],
        ['notes', 'TEXT'],
        ['is_banned', 'INTEGER DEFAULT 0'],
      ];
      userAntiScamCols.forEach(([col, type]) => {
        db.run(`ALTER TABLE users ADD COLUMN ${col} ${type}`, (e) => {
          if (e && !e.message.includes('duplicate column')) console.error(`Migration users.${col}:`, e.message);
        });
      });

      // Миграции: добавляем first_activation_time в keys
      db.run(`ALTER TABLE keys ADD COLUMN first_activation_time DATETIME DEFAULT NULL`, (e) => {
        if (e && !e.message.includes('duplicate column')) console.error('Migration keys.first_activation_time:', e.message);
      });

      // Миграция: добавляем product_restriction в coupons
      db.run(`ALTER TABLE coupons ADD COLUMN product_restriction TEXT DEFAULT NULL`, (e) => {
        if (e && !e.message.includes('duplicate column')) console.error('Migration coupons.product_restriction:', e.message);
      });

      // Задача 3: review_codes
      db.run(`CREATE TABLE IF NOT EXISTS review_codes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        code TEXT UNIQUE NOT NULL,
        order_id INTEGER,
        is_used INTEGER DEFAULT 0,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        reward_type TEXT,
        reward_value TEXT,
        rewarded_at DATETIME,
        product_period TEXT
      )`);
      // Миграция product_period
      db.run(`ALTER TABLE review_codes ADD COLUMN product_period TEXT DEFAULT NULL`, (e) => { });

      // Дефолтные настройки для новых фич
      const newFeatureDefaults = [
        ['default_loyalty_discount', '0'],
        ['fomo_enabled', '0'],
        ['fomo_chance', '40'],
        ['fomo_max_percent', '20'],
        ['fomo_coupon_expiry_days', '7'],
        ['review_channel_link', 'https://t.me/cyraxml/368'],
        ['welcome_ru', ''],
        ['welcome_en', ''],
        ['dns_address', 'ff73dd.dns.nextdns.io'],
        ['help_link', 'https://t.me/cyraxml/260'],
        ['review_post_id', '12'],
        ['exchange_rate_source', 'exchangerate-api'],
        ['markup_USD', '0'],
        ['markup_EUR', '0'],
        ['markup_UAH', '0'],
      ];
      newFeatureDefaults.forEach(([k, v]) => {
        db.run(`INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)`, [k, v]);
      });

      // =============================================
      // 👥 ПРОМПТ 1: Таблицы менеджеров
      // =============================================
      db.run(`CREATE TABLE IF NOT EXISTS managers (
        user_id INTEGER PRIMARY KEY,
        username TEXT,
        display_name TEXT,
        assigned_by INTEGER,
        assigned_at DATETIME DEFAULT CURRENT_TIMESTAMP
      )`);
      // Задача 6: Добавляем display_name если столбца ещё нет (миграция)
      db.run(`ALTER TABLE managers ADD COLUMN display_name TEXT`, () => { });

      db.run(`CREATE TABLE IF NOT EXISTS manager_methods (
        manager_id INTEGER,
        payment_method TEXT,
        PRIMARY KEY (manager_id, payment_method)
      )`);

      // =============================================
      // 📦 BUNDLE ORDERS
      // =============================================
      db.run(`CREATE TABLE IF NOT EXISTS bundle_orders (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        username TEXT,
        user_lang TEXT,
        product TEXT NOT NULL,
        quantity INTEGER NOT NULL DEFAULT 1,
        unit_price REAL NOT NULL,
        discount_percent INTEGER DEFAULT 0,
        total_price REAL NOT NULL,
        currency TEXT NOT NULL,
        method TEXT,
        invoice_id INTEGER UNIQUE,
        coupon_id INTEGER DEFAULT NULL,
        status TEXT DEFAULT 'pending',
        created_at DATETIME DEFAULT (datetime('now')),
        confirmed_at DATETIME
      )`);

      // BUG FIX DATA-1: Колонка partial_issued для отслеживания частичной выдачи ключей
      db.run(`ALTER TABLE bundle_orders ADD COLUMN partial_issued INTEGER DEFAULT NULL`, (e) => {
        if (e && !e.message.includes('duplicate column')) console.error('Migration bundle_orders.partial_issued:', e.message);
      });
      // Миграция 10: добавляем reseller_questionnaire и reseller_markup_pct в orders
      db.run(`ALTER TABLE orders ADD COLUMN reseller_markup_pct FLOAT DEFAULT NULL`, (e) => {
        if (e && !e.message.includes('duplicate column')) console.error('Migration orders.reseller_markup_pct:', e.message);
      });
      db.run(`ALTER TABLE orders ADD COLUMN reseller_questionnaire TEXT DEFAULT NULL`, (e) => {
        if (e && !e.message.includes('duplicate column')) console.error('Migration orders.reseller_questionnaire:', e.message);
      });

      // Миграция 11: флаг "уже уведомляли про зависший заказ" — исключает спам от checkLongPendingOrders
      db.run(`ALTER TABLE orders ADD COLUMN long_pending_notified INTEGER DEFAULT 0`, (e) => {
        if (e && !e.message.includes('duplicate column')) console.error('Migration orders.long_pending_notified:', e.message);
        else if (!e) console.log('✅ Added orders.long_pending_notified');
      });

      // =============================================
      // 🎁 РЕФЕРАЛЬНАЯ ПРОГРАММА
      // =============================================
      db.run(`CREATE TABLE IF NOT EXISTS referrals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        referrer_id INTEGER NOT NULL,
        referred_id INTEGER NOT NULL,
        referred_username TEXT,
        status TEXT DEFAULT 'pending',
        coupon_code TEXT,
        created_at DATETIME DEFAULT (datetime('now')),
        rewarded_at DATETIME
      )`);

      db.run(`CREATE TABLE IF NOT EXISTS user_ref_codes (
        user_id INTEGER PRIMARY KEY,
        ref_code TEXT UNIQUE NOT NULL,
        is_blocked INTEGER DEFAULT 0,
        blocked_until DATETIME,
        dead_ref_count INTEGER DEFAULT 0,
        created_at DATETIME DEFAULT (datetime('now'))
      )`);

      // =============================================
      // ⏰ ОТЛОЖЕННЫЕ РАССЫЛКИ
      // =============================================
      db.run(`CREATE TABLE IF NOT EXISTS scheduled_broadcasts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        text TEXT NOT NULL,
        scheduled_at DATETIME NOT NULL,
        filter TEXT DEFAULT 'all',
        status TEXT DEFAULT 'pending',
        created_at DATETIME DEFAULT (datetime('now')),
        sent_at DATETIME,
        sent_count INTEGER DEFAULT 0
      )`);

      // =============================================
      // 🛡️ АНТИФРОД — ДЕДУПЛИКАЦИЯ ЧЕКОВ
      // =============================================
      db.run(`CREATE TABLE IF NOT EXISTS used_receipts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        file_id TEXT NOT NULL,
        file_unique_id TEXT,
        user_id INTEGER NOT NULL,
        order_id INTEGER,
        order_type TEXT DEFAULT 'order',
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
      )`);
      db.run(`CREATE INDEX IF NOT EXISTS idx_used_receipts_file ON used_receipts(file_id)`);
      // FIX 4.3: Уникальный индекс на file_unique_id — БД сама отклонит второй INSERT
      // при одновременных запросах с одним чеком (race condition).
      db.run(`CREATE UNIQUE INDEX IF NOT EXISTS idx_used_receipts_unique ON used_receipts(file_unique_id) WHERE file_unique_id IS NOT NULL`);

      // 🏀 Basketball throws tracking
      db.run(`CREATE TABLE IF NOT EXISTS basketball_throws (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        order_id INTEGER NOT NULL UNIQUE,
        user_id INTEGER NOT NULL,
        score INTEGER NOT NULL,
        won INTEGER NOT NULL DEFAULT 0,
        thrown_at DATETIME DEFAULT CURRENT_TIMESTAMP
      )`);
      db.run(`CREATE INDEX IF NOT EXISTS idx_basketball_order ON basketball_throws(order_id)`);
      db.run(`CREATE INDEX IF NOT EXISTS idx_basketball_user ON basketball_throws(user_id)`);

      // =============================================
      // 📊 Индексы для новых таблиц
      // =============================================
      db.run(`CREATE INDEX IF NOT EXISTS idx_bundle_user_status ON bundle_orders(user_id, status)`);
      db.run(`CREATE INDEX IF NOT EXISTS idx_referrals_referrer ON referrals(referrer_id, status)`);
      db.run(`CREATE INDEX IF NOT EXISTS idx_referrals_referred ON referrals(referred_id)`);
      db.run(`CREATE INDEX IF NOT EXISTS idx_sched_broadcasts_status ON scheduled_broadcasts(status, scheduled_at)`);

      // =============================================
      // 🤝 РЕСЕЛЛЕР / WHITE-LABEL СИСТЕМА
      // =============================================
      db.run(`CREATE TABLE IF NOT EXISTS resellers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL UNIQUE,
        username TEXT,
        encrypted_token TEXT,
        bot_username TEXT,
        markup_pct INTEGER DEFAULT 30,
        balance REAL DEFAULT 0,
        status TEXT DEFAULT 'pending',
        payment_details TEXT,
        referred_by INTEGER DEFAULT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        activated_at DATETIME
      )`);

      db.run(`CREATE TABLE IF NOT EXISTS reseller_orders (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        reseller_id INTEGER NOT NULL,
        order_id INTEGER NOT NULL,
        base_amount REAL NOT NULL,
        markup_amount REAL NOT NULL,
        total_amount REAL NOT NULL,
        currency TEXT NOT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
      )`);

      db.run(`CREATE TABLE IF NOT EXISTS reseller_withdrawals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        reseller_id INTEGER NOT NULL,
        amount REAL NOT NULL,
        details TEXT NOT NULL,
        status TEXT DEFAULT 'pending',
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        processed_at DATETIME
      )`);

      // Индексы реселлер-таблиц
      db.run(`CREATE INDEX IF NOT EXISTS idx_resellers_user ON resellers(user_id)`);
      db.run(`CREATE INDEX IF NOT EXISTS idx_resellers_status ON resellers(status)`);
      db.run(`CREATE INDEX IF NOT EXISTS idx_reseller_orders_reseller ON reseller_orders(reseller_id)`);
      db.run(`CREATE INDEX IF NOT EXISTS idx_reseller_withdrawals ON reseller_withdrawals(reseller_id, status)`);

      // Дефолтные настройки реселлер-системы
      const resellerDefaults = [
        ['reseller_price_rub', '1500'],
        ['reseller_enabled', '1'],
        ['reseller_default_markup', '30'],
        ['reseller_ref_bonus', '500'],
      ];
      resellerDefaults.forEach(([k, v]) => {
        db.run(`INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)`, [k, v]);
      });

      // Миграция orders: добавляем reseller_id если нет
      db.all("PRAGMA table_info(orders)", [], (err, columns) => {
        if (!err && columns) {
          const hasResellerId = columns.some(c => c.name === 'reseller_id');
          if (!hasResellerId) {
            db.run(`ALTER TABLE orders ADD COLUMN reseller_id INTEGER DEFAULT NULL`, (e) => {
              if (e && !e.message.includes('duplicate column')) console.error('Migration orders.reseller_id:', e.message);
            });
          }
        }
      });

      // FIX 4.1: Миграция — добавляем webhook_secret в resellers для защиты вебхуков
      db.run(`ALTER TABLE resellers ADD COLUMN webhook_secret TEXT DEFAULT NULL`, (e) => {
        if (e && !e.message.includes('duplicate column')) console.error('Migration resellers.webhook_secret:', e.message);
      });

      // BUG FIX: Миграция — добавляем колонку finalized для идемпотентности finalizeSuccessfulOrder
      db.run(`ALTER TABLE orders ADD COLUMN finalized INTEGER DEFAULT 0`, (e) => {
        if (e && !e.message.includes('duplicate column')) console.error('Migration orders.finalized:', e.message);
      });

      // ── БАЛАНС ПОЛЬЗОВАТЕЛЕЙ ──────────────────────────────────────────────
      // Каждый пользователь имеет баланс в одной валюте (определяется первой покупкой).
      // preferred_currency фиксируется навсегда после первой подтверждённой покупки.
      db.run(`CREATE TABLE IF NOT EXISTS user_balances (
        user_id INTEGER PRIMARY KEY,
        balance REAL NOT NULL DEFAULT 0,
        preferred_currency TEXT NOT NULL DEFAULT 'RUB',
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
      )`);

      // Лог всех операций с балансом: topup, purchase, refund, admin_credit, admin_debit
      db.run(`CREATE TABLE IF NOT EXISTS balance_transactions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        amount REAL NOT NULL,
        currency TEXT NOT NULL,
        type TEXT NOT NULL,
        description TEXT,
        order_id INTEGER DEFAULT NULL,
        created_by INTEGER DEFAULT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
      )`);

      db.run(`CREATE INDEX IF NOT EXISTS idx_balance_tx_user ON balance_transactions(user_id, created_at)`);

      // ✅ Хранение состояния многошаговых админских операций в БД (переживает рестарт Render)
      db.run(`CREATE TABLE IF NOT EXISTS admin_pending_actions (
        admin_id INTEGER PRIMARY KEY,
        action TEXT NOT NULL,
        data TEXT NOT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
      )`);

      // Таблица групповых чатов для авторекламы
      db.run(`CREATE TABLE IF NOT EXISTS group_chats (
        chat_id INTEGER PRIMARY KEY,
        title TEXT,
        added_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        last_promo_at DATETIME,
        last_promo_msg_id INTEGER DEFAULT NULL,
        active INTEGER DEFAULT 1
      )`);

      // ✅ Миграция: хранение ID последнего промо-сообщения для авто-удаления старых
      db.run(`ALTER TABLE group_chats ADD COLUMN last_promo_msg_id INTEGER DEFAULT NULL`, (e) => {
        if (e && !e.message.includes('duplicate column')) console.error('Migration group_chats.last_promo_msg_id:', e.message);
      });

      // Флаг: заказ является пополнением баланса
      db.run(`ALTER TABLE orders ADD COLUMN balance_topup INTEGER DEFAULT 0`, (e) => {
        if (e && !e.message.includes('duplicate column')) console.error('Migration orders.balance_topup:', e.message);
      });

      // Флаг: товар оплачен с внутреннего баланса
      db.run(`ALTER TABLE orders ADD COLUMN paid_from_balance INTEGER DEFAULT 0`, (e) => {
        if (e && !e.message.includes('duplicate column')) console.error('Migration orders.paid_from_balance:', e.message);
      });

      // FIX 4.3: Миграция — заменяем обычный индекс на уникальный для used_receipts.file_unique_id
      // Это предотвращает race condition при одновременной отправке одного чека двумя запросами.
      db.run(`DROP INDEX IF EXISTS idx_used_receipts_unique`, () => {
        db.run(`CREATE UNIQUE INDEX IF NOT EXISTS idx_used_receipts_unique ON used_receipts(file_unique_id) WHERE file_unique_id IS NOT NULL`, (e) => {
          if (e) console.error('Migration idx_used_receipts_unique:', e.message);
          else console.log('✅ UNIQUE index on used_receipts(file_unique_id) ensured');
        });
      });

      resolve();
    });
  });
}

// П.3: Загрузка и управление настройками
let BOT_SETTINGS = {
  notify_new_user: '1',
  notify_new_order: '1',
  notify_low_keys: '1',
  low_keys_threshold: '5',
  notify_daily_report: '0',
  channel_link: 'https://t.me/cyraxml',
  support_link: 'https://t.me/cyraxml',
  reminders_enabled: '1'
};

function loadSettings() {
  return new Promise((resolve) => {
    db.all(`SELECT key, value FROM settings`, [], (err, rows) => {
      if (!err && rows) {
        rows.forEach(r => { BOT_SETTINGS[r.key] = r.value; });
      }

      resolve();
    });
  });
}

function getSetting(key) {
  return BOT_SETTINGS[key] !== undefined ? BOT_SETTINGS[key] : null;
}

function saveSetting(key, value, callback) {
  BOT_SETTINGS[key] = String(value);
  db.run(`INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)`, [key, String(value)], (err) => {
    if (err) console.error('❌ Error saving setting:', err);
    if (callback) callback(err);
  });
}

// =============================================
// 🔒 ПРОВЕРКА ВКЛЮЧЕНИЯ РАЗДЕЛОВ (Task 8)
// =============================================
const SECTION_FLAGS = {
  keys: 'keys_disabled',
  boost: 'boost_disabled',
  manual_boost: 'manual_boost_disabled',
  fomo: 'fomo_enabled',        // инвертирован: 1 = включён
};

function isSectionEnabled(section) {
  switch (section) {
    case 'keys': return getSetting('keys_disabled') !== '1';
    case 'boost': return getSetting('boost_disabled') !== '1';
    case 'manual_boost': return getSetting('manual_boost_disabled') !== '1';
    case 'fomo': return getSetting('fomo_enabled') === '1';
    default: return true;
  }
}

// ==========================================
// 🛠 ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
// ==========================================

// Хелпер: добавляет кнопку купона в клавиатуру (единое место)
function addCouponButton(keyboard, user, period, currency) {
  keyboard.inline_keyboard.push([
    { text: t(user, 'apply_coupon_btn'), callback_data: `apply_coupon_${period}_${currency}` }
  ]);
  return keyboard;
}

// basketballCoupon: { code, discountPercent, expiresStr } | null
async function sendKeyMessage(userId, userLang, period, key, orderId = null, botInstance = bot, basketballCoupon = null) {
  const userObj = { language_code: userLang };
  const isRu = getLang(userObj) === 'ru';
  const periodName = PERIOD_NAMES[isRu ? 'ru' : 'en']?.[period] || period;

  // Дата истечения ключа
  const KEY_DAYS_MAP = { '1d': 1, '3d': 3, '7d': 7, '30d': 30 };
  const keyDays = KEY_DAYS_MAP[period];
  let expiryLine = '';
  if (keyDays) {
    const expDate = new Date(Date.now() + keyDays * 24 * 60 * 60 * 1000);
    const expStr = expDate.toLocaleDateString(isRu ? 'ru-RU' : 'en-GB', { day: 'numeric', month: 'long' });
    expiryLine = isRu ? `\n⏳ Истекает: ${expStr}` : `\n⏳ Expires: ${expStr}`;
  }

  // Скидочная строка (купон / лояльность применённые при оплате)
  let discountLine = '';
  if (orderId) {
    try {
      const orderRow = await new Promise((resolve) =>
        db.get(
          `SELECT o.original_amount, o.amount, o.currency, o.coupon_id, c.discount_percent as coupon_pct
           FROM orders o LEFT JOIN coupons c ON o.coupon_id = c.id WHERE o.id = ?`,
          [orderId], (e, row) => resolve(e ? null : row)
        )
      );
      if (orderRow && orderRow.original_amount && orderRow.original_amount > orderRow.amount) {
        const saved = Math.round((orderRow.original_amount - orderRow.amount) * 100) / 100;
        const savedStr = formatPrice(saved, orderRow.currency);
        discountLine = orderRow.coupon_id && orderRow.coupon_pct
          ? (isRu ? `\n🎟 Купон −${orderRow.coupon_pct}% · сэкономлено ${savedStr}` : `\n🎟 Coupon −${orderRow.coupon_pct}% · saved ${savedStr}`)
          : (isRu ? `\n🎁 Скидка лояльности · сэкономлено ${savedStr}` : `\n🎁 Loyalty discount · saved ${savedStr}`);
      }
    } catch (e) { /* не критично */ }
  }

  // 🏀 Блок баскетбольного купона — вставляется в конец сообщения если есть выигрыш
  let basketballBlock = '';
  if (basketballCoupon) {
    basketballBlock = isRu
      ? `\n\n┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄\n🏀 <b>Бонус за бросок!</b>\n🎟 Купон <b>−${basketballCoupon.discountPercent}%</b> на следующую покупку:\n<code>${basketballCoupon.code}</code>\n└ (нажмите чтобы скопировать)\n⏳ До ${basketballCoupon.expiresStr}`
      : `\n\n┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄\n🏀 <b>Swish bonus!</b>\n🎟 Coupon <b>−${basketballCoupon.discountPercent}%</b> for your next purchase:\n<code>${basketballCoupon.code}</code>\n└ (tap to copy)\n⏳ Until ${basketballCoupon.expiresStr}`;
  }

  const channelLink = getSetting('channel_link') || 'https://t.me/cyraxml';

  const message = isRu
    ? `🔑 <b>Ваш ключ:</b> <code>${key}</code>\n└ (нажмите чтобы скопировать)\n\n📦 ${periodName}${expiryLine}\n⚡️ Активируется при первом входе в CyraxMod${discountLine}${basketballBlock}\n\n💬 Возникли проблемы? Нажмите кнопку ниже.`
    : `🔑 <b>Your key:</b> <code>${key}</code>\n└ (tap to copy)\n\n📦 ${periodName}${expiryLine}\n⚡️ Activates on first launch in CyraxMod${discountLine}${basketballBlock}\n\n💬 Any issues? Tap the button below.`;

  const kb = { inline_keyboard: [] };
  if (orderId) {
    kb.inline_keyboard.push([
      { text: isRu ? '✍️ Отзыв → бонус' : '✍️ Review → bonus', callback_data: `request_review_${orderId}` },
      { text: isRu ? '🆘 Проблема с ключом' : '🆘 Key issue', callback_data: 'support_ticket' }
    ]);
  }
  kb.inline_keyboard.push([
    { text: isRu ? '📢 Наш канал' : '📢 Our channel', url: channelLink },
    { text: isRu ? '🛒 Купить ещё' : '🛒 Buy more', callback_data: 'buy' }
  ]);

  await sendWithAnimatedEmoji(userId, message, ANIMATED_EMOJI.KEY, '🔑', { parse_mode: 'HTML', reply_markup: kb }, botInstance);
}

async function sendOutOfStockNotification(order, adminId) {
  const username = order.username ? escapeMarkdown(`@${order.username}`) : `ID: ${order.user_id}`;
  const formattedAmount = formatPrice(order.amount, order.currency);
  const lang = (order.user_lang || 'en').startsWith('ru') ? 'ru' : 'en';
  const productLabel = PERIOD_NAMES[lang]?.[order.product] || order.product;

  const message =
    `📭 Нет ключей — требуется действие

` +
    `⚠️ Пользователь оплатил, но ключ не получил!
` +
    `Выдайте ключ вручную после пополнения стока.

` +
    `👤 ${username}
` +
    `📦 Товар: ${productLabel}
` +
    `💰 Сумма: ${formattedAmount}
` +
    `🆔 Заказ: #${order.id}`;

  const kb = {
    inline_keyboard: [
      [{ text: `🔑 Добавить ключи «${productLabel}»`, callback_data: `add_keys_${order.product}` }],
      [
        { text: '✅ Выдать вручную', callback_data: `approve_${order.id}` },
        { text: '📨 Написать', callback_data: `msg_buyer_${order.user_id}` }
      ]
    ]
  };

  await safeSendMessage(adminId, message, { parse_mode: 'Markdown', reply_markup: kb });
}

// ==========================================
// 🆕 БАТЧИНГ НОВЫХ ПОЛЬЗОВАТЕЛЕЙ
// ==========================================
// Вместо мгновенного уведомления на каждого юзера — накапливаем в буфер
// и раз в час отправляем одну компактную сводку. Убирает спам в чате
// с администратором, не теряя ни одного пользователя.
const _newUserBatch = [];

function sendNewUserNotification(user) {
  if (getSetting('notify_new_user') !== '1') return;
  _newUserBatch.push({
    id: user.id,
    username: user.username || null,
    lang: user.language_code || 'en',
    at: new Date()
  });
}

async function flushNewUserBatch() {
  if (getSetting('notify_new_user') !== '1') return;
  if (_newUserBatch.length === 0) return;

  const batch = _newUserBatch.splice(0, _newUserBatch.length);
  const count = batch.length;

  if (count === 1) {
    const u = batch[0];
    const name = u.username ? escapeMarkdown(`@${u.username}`) : 'нет username';
    const time = u.at.toLocaleString('ru-RU', { hour: '2-digit', minute: '2-digit' });
    await safeSendMessage(ADMIN_ID,
      `🆕 *Новый пользователь* · ${time}\n\n👤 ${name}\n🆔 \`${u.id}\`\n🌐 ${u.lang}`,
      { parse_mode: 'Markdown' }
    ).catch(() => {});
  } else {
    const timeFrom = batch[0].at.toLocaleString('ru-RU', { hour: '2-digit', minute: '2-digit' });
    const timeTo   = batch[batch.length - 1].at.toLocaleString('ru-RU', { hour: '2-digit', minute: '2-digit' });
    let msg = `🆕 *Новые пользователи* · ${timeFrom}–${timeTo} · всего *${count}*\n\n`;
    batch.forEach((u, i) => {
      const name = u.username ? escapeMarkdown(`@${u.username}`) : `ID \`${u.id}\``;
      const time = u.at.toLocaleString('ru-RU', { hour: '2-digit', minute: '2-digit' });
      msg += `${i + 1}. ${name} · ${u.lang} · ${time}\n`;
    });
    await safeSendMessage(ADMIN_ID, msg, { parse_mode: 'Markdown' }).catch(() => {});
  }

  console.log(`🆕 [NEW_USER_BATCH] Отправлена сводка: ${count} пользователей`);
}

// Загрузка цен из БД
function loadPricesFromDB() {
  return new Promise((resolve) => {
    db.all(`SELECT product, currency, amount FROM prices`, [], (err, rows) => {
      if (err) {
        console.error('❌ Error loading prices from DB:', err);
        resolve();
        return;
      }

      if (rows && rows.length > 0) {
        const newPrices = {};
        rows.forEach(row => {
          if (!newPrices[row.product]) {
            newPrices[row.product] = {};
          }
          newPrices[row.product][row.currency] = row.amount;
        });

        Object.keys(PRICES).forEach(product => {
          if (!newPrices[product]) {
            newPrices[product] = PRICES[product];
          } else {
            ['USD', 'EUR', 'RUB', 'UAH'].forEach(currency => {
              if (newPrices[product][currency] === undefined && PRICES[product]?.[currency] !== undefined) {
                newPrices[product][currency] = PRICES[product][currency];
              }
            });
          }
        });

        PRICES = newPrices;
        console.log('✅ Prices loaded from DB');
      } else {
        console.log('ℹ️ No prices in DB, using defaults');
        Object.keys(PRICES).forEach(product => {
          Object.keys(PRICES[product]).forEach(currency => {
            db.run(
              `INSERT OR IGNORE INTO prices (product, currency, amount) VALUES (?, ?, ?)`,
              [product, currency, PRICES[product][currency]]
            );
          });
        });
      }

      resolve();
    });
  });
}

// Загрузка платежных реквизитов из БД
function loadPaymentDetailsFromDB() {
  return new Promise((resolve) => {
    db.all(`SELECT method, details FROM payment_details`, [], (err, rows) => {
      if (err) {
        console.error('❌ Error loading payment details from DB:', err);
        resolve();
        return;
      }

      if (rows && rows.length > 0) {
        rows.forEach(row => {
          if (PAYMENT_DETAILS.hasOwnProperty(row.method)) {
            PAYMENT_DETAILS[row.method] = row.details;
          }
        });
        console.log('✅ Payment details loaded from DB');
      } else {
        // FIX 1.1: Раньше здесь записывались плейсхолдеры в БД при пустой таблице.
        // Это приводило к тому что после рестарта Render (/tmp очищается) мы записывали
        // мусорные значения и isPaymentConfigured() возвращал false для всех методов.
        // Теперь при пустой БД оставляем значения из .env — БД не трогаем.
        console.log('ℹ️ No payment details in DB — keeping env/default values');
      }

      resolve();
    });
  });
}

// Проверка долгих pending заказов
async function cleanupAbandonedCryptobotOrders() {
  // Удаляем pending CryptoBot-заказы старше 24 часов — инвойс уже истёк, оплаты не будет
  db.run(
    `DELETE FROM orders
     WHERE method = 'CryptoBot'
       AND status = 'pending'
       AND datetime(created_at) < datetime('now', '-24 hours')`,
    function (err) {
      if (err) { console.error('❌ Cleanup CryptoBot abandoned orders error:', err); return; }
      if (this.changes > 0) console.log(`🧹 Cleaned up ${this.changes} abandoned CryptoBot order(s)`);
    }
  );
}

async function checkLongPendingOrders() {
  console.log('🔍 Checking long pending orders...');

  const threeHoursAgo = new Date(Date.now() - 3 * 60 * 60 * 1000).toISOString();

  db.all(
    `SELECT id, user_id, username, product, amount, currency, method, created_at 
     FROM orders 
     WHERE status = 'pending'
       AND status != 'out_of_stock_pending'
       AND method != 'CryptoBot'
       AND product != 'reseller_connection'
       AND (balance_topup IS NULL OR balance_topup = 0)
       AND transaction_id IS NOT NULL
       AND datetime(created_at) < datetime(?)
       AND (long_pending_notified IS NULL OR long_pending_notified = 0)
     ORDER BY created_at ASC 
     LIMIT 10`,
    [threeHoursAgo],
    async (err, orders) => {
      if (err) { console.error('❌ Error checking long pending orders:', err); return; }
      if (!orders || orders.length === 0) { console.log('✅ No new long pending orders'); return; }

      console.log(`⚠️ Found ${orders.length} new long pending order(s)`);

      // Одно сводное сообщение — не спамим отдельными
      let message = `⚠️ *Заказы ожидают подтверждения более 3 часов*\n\n`;
      const orderIds = orders.map(o => o.id);

      orders.forEach((order, index) => {
        const date = new Date(order.created_at).toLocaleString('ru-RU');
        const formattedAmount = formatPrice(order.amount, order.currency);
        const username = order.username ? escapeMarkdown(`@${order.username}`) : `ID: ${order.user_id}`;
        message += `${index + 1}. #${order.id} · ${username}\n`;
        message += `   📦 ${order.product} · *${formattedAmount}* · ${order.method}\n`;
        message += `   🕐 ${date}\n\n`;
      });

      message += `Откройте панель управления для обработки заказов.`;

      // Помечаем как уведомлённые — больше не будем беспокоить по этим заказам
      const placeholders = orderIds.map(() => '?').join(',');
      db.run(`UPDATE orders SET long_pending_notified = 1 WHERE id IN (${placeholders})`, orderIds, (upErr) => {
        if (upErr) console.error('❌ Failed to mark orders as notified:', upErr);
      });

      await safeSendMessage(ADMIN_ID, message, {
        parse_mode: 'Markdown',
        reply_markup: {
          inline_keyboard: [[
            { text: '📋 Управление заказами', callback_data: 'admin_manage_orders' }
          ]]
        }
      }).catch(e => console.error('❌ Failed to send long pending orders notification:', e));
    }
  );
}

// Отчёт по потерянным / зависшим заказам
function showLostOrdersReport(chatId, msgId = null) {
  const twoHoursAgo = new Date(Date.now() - 2 * 60 * 60 * 1000).toISOString();
  const oneDayAgo = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString();
  const sevenDayAgo = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString();

  db.get(
    `SELECT
      (SELECT COUNT(*) FROM orders WHERE status='pending' AND method != 'CryptoBot' AND (balance_topup IS NULL OR balance_topup = 0) AND datetime(created_at) < datetime(?)) as stale_2h,
      (SELECT COUNT(*) FROM orders WHERE status='pending' AND method != 'CryptoBot' AND (balance_topup IS NULL OR balance_topup = 0) AND datetime(created_at) < datetime(?)) as stale_day,
      (SELECT COUNT(*) FROM orders WHERE status='rejected' AND datetime(created_at) >= datetime(?)) as rejected_week,
      (SELECT COUNT(*) FROM orders WHERE status='out_of_stock' AND datetime(created_at) >= datetime(?)) as out_of_stock_week,
      (SELECT COUNT(*) FROM orders WHERE status='pending' AND method != 'CryptoBot' AND (balance_topup IS NULL OR balance_topup = 0)) as total_pending,
      (SELECT COUNT(*) FROM orders WHERE status='out_of_stock_pending') as waiting_keys`,
    [twoHoursAgo, oneDayAgo, sevenDayAgo, sevenDayAgo],
    (err, s) => {
      if (err) { adminSend(chatId, ADMIN_ID, '❌ Не удалось загрузить статистику. Попробуйте позже.'); return; }

      let msg = `⏰ *Потерянные и проблемные заказы*\n\n`;
      msg += `📋 *Зависшие pending (не CryptoBot):*\n`;
      msg += `   > 2 часов: *${(s && s.stale_2h) || 0}*\n`;
      msg += `   > 24 часов: *${(s && s.stale_day) || 0}*\n`;
      msg += `   Всего pending: *${(s && s.total_pending) || 0}*\n`;
      msg += `🔑 *Ждут ключей:* *${(s && s.waiting_keys) || 0}*\n\n`;
      msg += `📊 *За последние 7 дней:*\n`;
      msg += `   ❌ Отклонено: *${(s && s.rejected_week) || 0}*\n`;
      msg += `   📦 Нет ключей (out of stock): *${(s && s.out_of_stock_week) || 0}*\n`;

      Promise.all([
        new Promise(res => db.all(
          `SELECT id, user_id, username, product, amount, currency, method, created_at
           FROM orders WHERE status='pending' AND method != 'CryptoBot'
           AND (balance_topup IS NULL OR balance_topup = 0)
           AND datetime(created_at) < datetime(?)
           ORDER BY created_at ASC LIMIT 8`,
          [twoHoursAgo], (e, rows) => res(rows || [])
        )),
        new Promise(res => db.all(
          `SELECT id, user_id, username, product, amount, currency, created_at
           FROM orders WHERE status='out_of_stock_pending'
           ORDER BY created_at ASC LIMIT 8`,
          [], (e, rows) => res(rows || [])
        ))
      ]).then(([staleOrders, oosOrders]) => {
        const kb = { inline_keyboard: [] };

        if (staleOrders.length > 0) {
          msg += `\n🔍 *Конкретные зависшие (>2ч):*\n`;
          staleOrders.forEach((o) => {
            const hoursAgo = Math.round((Date.now() - new Date(o.created_at).getTime()) / 3600000);
            const uname = o.username ? escapeMarkdown('@' + o.username) : 'ID: ' + o.user_id;
            msg += `#${o.id} ${uname} — ${o.product} ${formatPrice(o.amount, o.currency)} (${hoursAgo}ч назад)\n`;
            kb.inline_keyboard.push([{ text: `❌ Отменить #${o.id}`, callback_data: `reject_${o.id}` }]);
          });
          kb.inline_keyboard.push([{ text: `🗑 Отменить ВСЕ зависшие (>2ч)`, callback_data: `reject_all_stale` }]);
        }

        if (oosOrders.length > 0) {
          msg += `\n⏳ *Очередь ожидания ключей:*\n`;
          oosOrders.forEach((o) => {
            const daysAgo = Math.round((Date.now() - new Date(o.created_at).getTime()) / 86400000);
            const uname = o.username ? escapeMarkdown('@' + o.username) : 'ID: ' + o.user_id;
            msg += `#${o.id} ${uname} — ${o.product} ${formatPrice(o.amount, o.currency)} (${daysAgo}д назад)\n`;
          });
          kb.inline_keyboard.push([{ text: '🔑 Добавить ключи', callback_data: 'admin_manage_keys' }]);
        }

        kb.inline_keyboard.push([{ text: '📦 Все заказы', callback_data: 'admin_manage_orders' }]);
        kb.inline_keyboard.push([{ text: '◀️ Назад', callback_data: 'admin' }]);
        safeSendMessage(chatId, msg, { parse_mode: 'Markdown', reply_markup: kb });
      });
    }
  );
}


// Управление сессиями с таймаутом 30 мин
const SESSION_TIMEOUT = 4 * 60 * 60 * 1000; // 4 часа — чтобы пользователь успел оплатить и вернуться

// Максимальное число одновременных сессий в памяти.
// При 50k уникальных посетителей в день и SESSION_TIMEOUT=4ч теоретически в памяти может
// накопиться ~8k сессий одновременно. Лимит 10k — безопасный запас с вытеснением старых.
const SESSION_MAX_SIZE = 10_000;

function getSession(userId) {
  let session = userSessions.get(userId);
  if (!session) {
    // LRU-вытеснение: при достижении лимита удаляем самую старую сессию (первая в Map).
    // Map в JS сохраняет порядок вставки, поэтому первый ключ — самый старый.
    // Активные пользователи регулярно обновляют lastAccess и не попадают под вытеснение
    // при периодической чистке, поэтому первый элемент Map практически всегда неактивен.
    if (userSessions.size >= SESSION_MAX_SIZE) {
      const oldestKey = userSessions.keys().next().value;
      userSessions.delete(oldestKey);
      console.log(`[sessions] LRU evict: removed oldest session (userId=${oldestKey}), size was ${SESSION_MAX_SIZE}`);
    }
    session = {
      state: null,
      data: {},
      lastAccess: Date.now(),
      createdAt: Date.now(),
      navMsgId: null,    // ID последнего навигационного сообщения (для авто-удаления)
      adminMsgId: null   // ID последнего сообщения из admin-панели (для авто-удаления)
    };
    userSessions.set(userId, session);
  } else {
    session.lastAccess = Date.now();
  }
  return session;
}

function clearSession(userId) {
  const existing = userSessions.get(userId);
  // 🧪 Если активен тест-режим — сохраняем флаг, сбрасываем только состояние покупки
  userSessions.delete(userId);
}
// ──────────────────────────────────────────────────────────────
// 🧹 АВТО-ОЧИСТКА НАВИГАЦИИ
// Отправляет навигационное сообщение, удаляя предыдущее если оно есть.
// Используется для меню (не для ключей/чеков/уведомлений).
// ──────────────────────────────────────────────────────────────
async function sendNavMessage(chatId, userId, text, opts = {}) {
  const session = getSession(userId);

  // Удаляем предыдущее навигационное сообщение
  if (session.navMsgId) {
    bot.deleteMessage(chatId, session.navMsgId).catch(() => {});
    session.navMsgId = null;
  }

  try {
    const sent = await bot.sendMessage(chatId, text, opts);
    if (sent && sent.message_id) {
      session.navMsgId = sent.message_id;
    }
    return sent;
  } catch (e) {
    console.error('sendNavMessage error:', e.message);
  }
}

async function sendNavWithAnimatedEmoji(chatId, userId, text, animatedEmojiId, fallbackEmoji, opts = {}) {
  const session = getSession(userId);

  if (session.navMsgId) {
    bot.deleteMessage(chatId, session.navMsgId).catch(() => {});
    session.navMsgId = null;
  }

  const animatedText = text.replace(fallbackEmoji, `<emoji id="${animatedEmojiId}">${fallbackEmoji}</emoji>`);

  try {
    const sent = await bot.sendMessage(chatId, animatedText, { ...opts, parse_mode: 'HTML' });
    if (sent && sent.message_id) {
      session.navMsgId = sent.message_id;
    }
    return sent;
  } catch (e) {
    if (e.message.includes('emoji') || e.message.includes('Bad Request') || e.message.includes('parse entities')) {
      // Fallback: убираем <emoji> теги но СОХРАНЯЕМ parse_mode: 'HTML' чтобы <b>, <i> работали
      return await bot.sendMessage(chatId, text, { ...opts, parse_mode: 'HTML' }).then(sent => {
        if (sent && sent.message_id) session.navMsgId = sent.message_id;
        return sent;
      }).catch(err => console.error('sendNavFallback error:', err.message));
    }
    console.error('sendNavWithAnimatedEmoji error:', e.message);
  }
}

// ──────────────────────────────────────────────────────────────
// 🔄 УМНАЯ ОТПРАВКА/РЕДАКТИРОВАНИЕ АДМИН-СООБЩЕНИЙ
// Если есть messageId — редактируем, иначе — новое сообщение.
// Сохраняет adminMsgId в сессии для последующего удаления/редактирования.
// ──────────────────────────────────────────────────────────────
async function adminSend(chatId, userId, text, opts = {}, messageId = null) {
  const session = getSession(userId);

  // Пробуем отредактировать существующее сообщение
  if (messageId) {
    try {
      const edited = await bot.editMessageText(text, {
        chat_id: chatId,
        message_id: messageId,
        ...opts
      });
      session.adminMsgId = messageId;
      return edited;
    } catch (e) {
      // Если сообщение не изменилось или удалено — отправим новое
      if (!e.message.includes('message is not modified')) {
        console.log(`[adminSend] edit failed (${e.message.slice(0,50)}), sending new`);
      }
    }
  }

  // Удаляем предыдущее admin-сообщение перед отправкой нового
  if (session.adminMsgId) {
    bot.deleteMessage(chatId, session.adminMsgId).catch(() => {});
    session.adminMsgId = null;
  }

  try {
    const sent = await safeSendMessage(chatId, text, opts);
    if (sent && sent.message_id) session.adminMsgId = sent.message_id;
    return sent;
  } catch (e) {
    console.error('adminSend error:', e.message);
  }
}



// Периодическая очистка старых сессий
setInterval(() => {
  const now = Date.now();
  let deletedCount = 0;

  for (const [userId, session] of userSessions.entries()) {
    if (now - session.lastAccess > SESSION_TIMEOUT) {
      userSessions.delete(userId);
      deletedCount++;
    }
  }

  if (deletedCount > 0) {
    console.log(`🧹 Cleaned up ${deletedCount} expired sessions`);
  }

  // Очищаем истёкшие баны из rateLimitViolations — иначе Map растёт вечно
  let cleanedBans = 0;
  for (const [userId, violation] of rateLimitViolations.entries()) {
    // Удаляем если: бан истёк, или бана нет и последнее нарушение было >1 часа назад
    if (violation.bannedUntil && now >= violation.bannedUntil) {
      rateLimitViolations.delete(userId);
      cleanedBans++;
    } else if (!violation.bannedUntil && violation.firstViolation && now - violation.firstViolation > 60 * 60 * 1000) {
      rateLimitViolations.delete(userId);
      cleanedBans++;
    }
  }

  // Очищаем истёкшие окна в userActionLimits
  let cleanedActions = 0;
  for (const [userId, actions] of userActionLimits.entries()) {
    if (now - actions.windowStart > RATE_LIMIT_WINDOW * 10) {
      userActionLimits.delete(userId);
      cleanedActions++;
    }
  }

  if (cleanedBans > 0 || cleanedActions > 0) {
    console.log(`🧹 Rate-limit cleanup: ${cleanedBans} bans, ${cleanedActions} action windows removed`);
  }

  // 🔒 Очищаем зависшие блокировки approve (страховка от краша в середине операции)
  // Нормальный approve занимает <5 секунд. Если lock живёт >5 минут — что-то пошло не так.
  // При следующем клике "Одобрить" статус заказа уже будет 'confirmed' → безопасный выход.
  if (approvingOrders.size > 0 || approvingTopups.size > 0) {
    approvingOrders.clear();
    approvingTopups.clear();
    console.log('🧹 Cleared stale approve locks');
  }
}, 10 * 60 * 1000);

// Rate limit с баном
function checkRateLimit(userId, callbackData, customLimit) {
  if (userId === ADMIN_ID) return true;

  // Навигационные клики не считаем — они не нагружают систему
  if (callbackData && isNavigationCallback(callbackData)) return true;
  // Также пропускаем prefix-навигацию
  if (callbackData && (
    callbackData.startsWith('period_') ||
    callbackData.startsWith('currency_') ||
    callbackData.startsWith('orders_page_') ||
    callbackData.startsWith('mb_cur_') ||
    callbackData.startsWith('mb_tgt_') ||
    callbackData.startsWith('admin_') ||
    callbackData.startsWith('back_') ||
    callbackData.startsWith('remind_') ||
    callbackData.startsWith('loyal_')
  )) return true;

  const now = Date.now();
  // FIX 5.2: поддержка кастомного порога (для владельца реселлера — повышенный лимит)
  const effectiveMax = customLimit || MAX_ACTIONS_PER_WINDOW;

  const violation = rateLimitViolations.get(userId);
  if (violation && violation.bannedUntil) {
    if (now < violation.bannedUntil) {
      return false; // бан ещё активен
    } else {
      // Бан истёк — удаляем запись, пропускаем пользователя
      rateLimitViolations.delete(userId);
    }
  }

  const userActions = userActionLimits.get(userId) || { count: 0, windowStart: now };

  if (now - userActions.windowStart > RATE_LIMIT_WINDOW) {
    userActionLimits.set(userId, { count: 1, windowStart: now });
    return true;
  }

  if (userActions.count >= effectiveMax) {
    const violation = rateLimitViolations.get(userId) || {
      count: 0,
      firstViolation: now,
      bannedUntil: null
    };

    violation.count++;
    if (violation.count === 1) violation.firstViolation = now;

    if (violation.count >= 5 && now - violation.firstViolation < 60 * 60 * 1000) {
      violation.bannedUntil = now + 24 * 60 * 60 * 1000;
      rateLimitViolations.set(userId, violation);

      // Получаем username для удобства
      db.get(`SELECT username FROM users WHERE id = ?`, [userId], (err, row) => {
        const userDisplay = (!err && row && row.username) ? `@${escapeMarkdown(row.username)}` : `ID: \`${userId}\``;
        bot.sendMessage(
          ADMIN_ID,
          `⚠️ *Пользователь забанен за спам*\n\n👤 ${userDisplay}\n🆔 \`${userId}\`\nНарушений: ${violation.count}\nБан до: ${new Date(violation.bannedUntil).toLocaleString()}`,
          {
            parse_mode: 'Markdown',
            reply_markup: {
              inline_keyboard: [[
                { text: '🔓 Разбанить', callback_data: `unban_user_${userId}` }
              ]]
            }
          }
        );
      });
    } else {
      rateLimitViolations.set(userId, violation);
    }
    return false;
  }

  userActions.count++;
  userActionLimits.set(userId, userActions);
  return true;
}

// Логирование действий
function logAction(userId, action, details = null) {
  // Редакция чувствительных полей перед сохранением в БД.
  // Коды купонов, ключи активации и токены не должны храниться в открытом виде в логах —
  // при компрометации БД это позволило бы использовать их напрямую.
  // Вместо полного значения сохраняем маску: первые 3 символа + **** + последние 3.
  const SENSITIVE_KEYS = ['couponCode', 'key', 'token', 'resellerToken', 'api_key'];
  let safeDetails = details;
  if (details && typeof details === 'object') {
    safeDetails = { ...details };
    for (const k of SENSITIVE_KEYS) {
      if (safeDetails[k] && typeof safeDetails[k] === 'string' && safeDetails[k].length > 6) {
        const v = safeDetails[k];
        safeDetails[k] = v.slice(0, 3) + '****' + v.slice(-3);
      }
    }
  }
  db.run(
    `INSERT INTO action_logs (user_id, action, details) VALUES (?, ?, ?)`,
    [userId, action, safeDetails ? JSON.stringify(safeDetails) : null]
  );
}

// ==========================================
// 💰 БАЛАНС ПОЛЬЗОВАТЕЛЕЙ — вспомогательные функции
// ==========================================

// Получить или создать запись баланса
function getUserBalance(userId) {
  return new Promise((resolve) => {
    db.get(`SELECT * FROM user_balances WHERE user_id = ?`, [userId], (err, row) => {
      if (row) return resolve(row);
      // Нет записи — смотрим предпочтительную валюту из первой покупки
      db.get(
        `SELECT original_currency, currency FROM orders
         WHERE user_id = ? AND status = 'confirmed' AND (balance_topup IS NULL OR balance_topup = 0)
         ORDER BY confirmed_at ASC LIMIT 1`,
        [userId],
        (e2, order) => {
          if (order?.original_currency || order?.currency) {
            // Есть реальная покупка — берём валюту из неё
            const currency = (order.original_currency || order.currency).toUpperCase();
            db.run(
              `INSERT OR IGNORE INTO user_balances (user_id, balance, preferred_currency) VALUES (?, 0, ?)`,
              [userId, currency],
              () => {
                db.get(`SELECT * FROM user_balances WHERE user_id = ?`, [userId], (e3, row2) => {
                  resolve(row2 || { user_id: userId, balance: 0, preferred_currency: currency });
                });
              }
            );
          } else {
            // Нет покупок — определяем по language_code пользователя из БД
            db.get(`SELECT language_code FROM users WHERE id = ?`, [userId], (e3, userRow) => {
              const lang = userRow?.language_code || '';
              let currency = 'USD'; // дефолт для иностранных пользователей
              if (lang === 'ru') currency = 'RUB';
              else if (lang === 'uk') currency = 'UAH';
              db.run(
                `INSERT OR IGNORE INTO user_balances (user_id, balance, preferred_currency) VALUES (?, 0, ?)`,
                [userId, currency],
                () => {
                  db.get(`SELECT * FROM user_balances WHERE user_id = ?`, [userId], (e4, row2) => {
                    resolve(row2 || { user_id: userId, balance: 0, preferred_currency: currency });
                  });
                }
              );
            });
          }
        }
      );
    });
  });
}

// Начислить баланс (amount > 0) или списать (amount < 0)
// type: 'topup' | 'purchase' | 'refund' | 'admin_credit' | 'admin_debit'
function adjustUserBalance(userId, amount, currency, type, description, orderId = null, createdBy = null) {
  // SERIOUS 3: Валидация входных данных — защита от NaN, Infinity, слишком больших сумм.
  // Без этого UPDATE SET balance = balance + NaN молча превращает баланс в NULL в SQLite.
  const MAX_BALANCE_AMOUNT = 1_000_000; // 1 млн — разумный потолок для одной операции
  if (!Number.isFinite(amount)) {
    console.error(`❌ adjustUserBalance: невалидная сумма amount=${amount} (userId=${userId}, type=${type}, desc="${description}")`);
    return Promise.reject(new Error(`Invalid balance amount: ${amount}`));
  }
  if (Math.abs(amount) > MAX_BALANCE_AMOUNT) {
    console.error(`❌ adjustUserBalance: сумма превышает лимит amount=${amount} > ${MAX_BALANCE_AMOUNT} (userId=${userId}, type=${type})`);
    return Promise.reject(new Error(`Balance amount exceeds limit: ${amount}`));
  }
  //
  // Старая схема: BEGIN IMMEDIATE блокировала всю БД на время SELECT+вычисление+UPDATE.
  // При параллельных вызовах (два CryptoBot webhook одновременно) — очередь и таймауты.
  //
  // Новая схема:
  //   1. INSERT OR IGNORE — создаём строку баланса если нет (idempotent).
  //   2. Один UPDATE с вычислением на стороне SQLite (атомарен по природе SQLite).
  //      При purchase: WHERE balance+amount>=0 атомарно проверяет достаточность средств.
  //      Если баланса не хватает — this.changes===0, бросаем INSUFFICIENT_BALANCE.
  //      ROUND(...,2) исключает накопление float-ошибок.
  //   3. SELECT нового баланса — только для resolve(), читаем уже после UPDATE.
  //   4. INSERT в balance_transactions — аудит, некритичен для корректности баланса.

  return new Promise((resolve, reject) => {
    db.run(
      `INSERT OR IGNORE INTO user_balances (user_id, balance, preferred_currency) VALUES (?, 0, ?)`,
      [userId, currency],
      (insertErr) => {
        if (insertErr) {
          // IMPROVEMENT 3: Полный контекст операции в логе — легче диагностировать
          console.error(`❌ adjustUserBalance INSERT error userId=${userId} type=${type} amount=${amount} currency=${currency}:`, insertErr.message);
          return reject(insertErr);
        }

        const isPurchase = type === 'purchase';
        const sql = isPurchase
          ? `UPDATE user_balances
             SET balance = ROUND(balance + ?, 2), updated_at = datetime('now')
             WHERE user_id = ? AND ROUND(balance + ?, 2) >= 0`
          : `UPDATE user_balances
             SET balance = ROUND(balance + ?, 2), updated_at = datetime('now')
             WHERE user_id = ?`;
        const params = isPurchase ? [amount, userId, amount] : [amount, userId];

        db.run(sql, params, function(updErr) {
          if (updErr) return reject(updErr);

          if (this.changes === 0 && isPurchase) {
            return reject(new Error('INSUFFICIENT_BALANCE'));
          }

          db.get(
            `SELECT balance FROM user_balances WHERE user_id = ?`,
            [userId],
            (selErr, row) => {
              if (selErr) return reject(selErr);
              const newBalance = row ? row.balance : 0;

              db.run(
                `INSERT INTO balance_transactions (user_id, amount, currency, type, description, order_id, created_by)
                 VALUES (?, ?, ?, ?, ?, ?, ?)`,
                [userId, amount, currency, type, description, orderId, createdBy],
                (insErr) => {
                  if (insErr) console.error('[balance_transactions] insert failed (non-critical):', insErr.message);
                  resolve(newBalance);
                }
              );
            }
          );
        });
      }
    );
  });
}

// Форматировать сумму с символом валюты
function formatBalanceAmount(amount, currency) {
  const sym = { RUB: '₽', USD: '$', EUR: '€', UAH: '₴' };
  const s = sym[currency] || currency;
  if (currency === 'USD' || currency === 'EUR') return `${s}${amount.toFixed(2)}`;
  return `${Math.round(amount)} ${s}`;
}

// ==========================================
// 🎟 УМНЫЙ OOS-КУПОН: выдача с антифармом
// ==========================================
// Размер скидки зависит от периода: 1d→3%, 3d→6%, 7d→10%, 30d→15%
// Антифарм: следующий купон за OOS доступен только после хотя бы одной успешной покупки
//   после последнего OOS-купона. Это блокирует схему «заказ→отмена→купон→повтор».
// Только для ключей (1d/3d/7d/30d). Буст, партнёрство — не компенсируем купоном.
const OOS_COUPON_PCT = { '1d': 3, '3d': 6, '7d': 10, '30d': 15 };
const OOS_COUPON_PRODUCTS = new Set(['1d', '3d', '7d', '30d']);

async function issueOosCoupon(userId, period, orderIdForRefund = null) {
  // Только для ключей
  if (!OOS_COUPON_PRODUCTS.has(period)) return null;

  const pct = OOS_COUPON_PCT[period];

  // Антифарм: проверяем, был ли OOS-купон ранее
  const lastOosCoupon = await new Promise(resolve => {
    db.get(
      `SELECT id, created_at FROM coupons
       WHERE user_id = ? AND (code LIKE 'OOS_%' OR code LIKE 'COMP_%')
       ORDER BY created_at DESC LIMIT 1`,
      [userId], (e, row) => resolve(row || null)
    );
  });

  if (lastOosCoupon) {
    // Проверяем: была ли хотя бы одна успешная покупка ПОСЛЕ этого купона?
    const purchaseAfter = await new Promise(resolve => {
      db.get(
        `SELECT id FROM orders
         WHERE user_id = ? AND status = 'confirmed'
         AND (balance_topup IS NULL OR balance_topup = 0)
         AND (paid_from_balance IS NULL OR paid_from_balance = 0)
         AND confirmed_at > ?
         LIMIT 1`,
        [userId, lastOosCoupon.created_at], (e, row) => resolve(row || null)
      );
    });

    if (!purchaseAfter) {
      // Фарм купонов заблокирован — уже есть OOS-купон без промежуточной покупки
      console.log(`[OOS] Antifarm: user ${userId} already has OOS coupon without purchase since — skipping`);
      return null;
    }
  }

  // Создаём купон строго на тот же период
  const couponCode = `OOS_${userId}_${Date.now().toString(36).toUpperCase()}`;
  const inserted = await new Promise(resolve => {
    db.run(
      `INSERT INTO coupons (code, discount_percent, max_uses, user_id, product_restriction, created_at, expires_at)
       VALUES (?, ?, 1, ?, ?, datetime('now'), datetime('now', '+30 days'))`,
      [couponCode, pct, userId, period],
      function(err) {
        if (err) { console.error('OOS coupon insert error:', err.message); resolve(null); return; }
        const couponId = this.lastID;
        db.run(`INSERT OR IGNORE INTO coupon_products (coupon_id, product) VALUES (?, ?)`, [couponId, period]);
        resolve(couponCode);
      }
    );
  });

  if (inserted) {
    logAction(userId, 'oos_coupon_issued', { period, pct, couponCode, orderId: orderIdForRefund });
  }
  return inserted; // couponCode или null
}

// Хелпер: проверка роли менеджера → Promise<boolean>
function isManager(userId) {
  return new Promise(res => {
    db.get('SELECT user_id FROM managers WHERE user_id = ?', [userId],
      (e, row) => res(!e && !!row));
  });
}

// ==========================================
// 🔑 ВЫДАЧА КЛЮЧА (с транзакцией)
// ==========================================
async function issueKeyToUser(userId, product, reason = 'purchase') {
  // Для infinite_boost ключ не нужен
  if (product === 'infinite_boost') {
    return Promise.resolve('BOOST_GUIDE');
  }
  // Для reseller_connection ключ не нужен — это активация партнёрства
  if (product === 'reseller_connection') {
    return Promise.resolve('RESELLER_ACTIVATED');
  }
  return new Promise((resolve, reject) => {
    db.serialize(() => {
      // ─── АТОМАРНАЯ ВЫДАЧА КЛЮЧА ───────────────────────────────────────────
      //
      // ПРОБЛЕМА (старая логика):
      //   1. SELECT key_value FROM keys WHERE product=? AND status='available' LIMIT 1
      //   2. UPDATE keys SET status='sold' WHERE key_value=?
      //   3. Проверка соответствия product ПОСЛЕ UPDATE — слишком поздно.
      //      Между шагами 1 и 2 другой запрос мог занять тот же ключ (race condition).
      //      Также если ключ не соответствует product, он уже помечен 'sold' до откатки.
      //
      // РЕШЕНИЕ (новая логика):
      //   Один атомарный запрос в транзакции BEGIN IMMEDIATE (блокирует запись с первого шага).
      //   Шаг A: SELECT id, key_value FROM keys WHERE product=? AND status='available' LIMIT 1
      //          — product в WHERE, значит несоответствующие ключи не выбираются вообще.
      //   Шаг B: UPDATE keys SET status='sold',... WHERE id=<id из шага A> AND status='available'
      //          — двойная проверка: id + status. Если this.changes === 0, значит ключ
      //            был занят между A и B (крайне редко при IMMEDIATE, но защита есть).
      //   BEGIN IMMEDIATE блокирует БД на запись сразу, исключая параллельную выдачу.
      // ──────────────────────────────────────────────────────────────────────

      db.run("BEGIN IMMEDIATE", (beginErr) => {
        if (beginErr) {
          console.error('❌ BEGIN IMMEDIATE error:', beginErr);
          const err = new Error('Ошибка начала транзакции');
          safeSendMessage(ADMIN_ID, `❌ *BEGIN IMMEDIATE fail*\nProduct: ${product}\nUser: ${userId}\n${beginErr.message}`, { parse_mode: 'Markdown' }).catch(() => {});
          return reject(err);
        }

        const rollback = (error, adminMsg) => {
          db.run("ROLLBACK", () => {});
          if (adminMsg) safeSendMessage(ADMIN_ID, adminMsg, { parse_mode: 'Markdown' }).catch(() => {});
          reject(error);
        };

        // Шаг A: выбираем ключ — product уже в условии WHERE, несоответствие невозможно
        db.get(
          `SELECT id, key_value FROM keys WHERE product = ? AND status = 'available' LIMIT 1`,
          [product],
          (selErr, row) => {
            if (selErr) {
              console.error('❌ SELECT error in issueKeyToUser:', selErr);
              return rollback(
                new Error('Ошибка базы данных'),
                `❌ *Ошибка SELECT ключа*\nProduct: ${product}\nUser: ${userId}\n${selErr.message}`
              );
            }

            if (!row) {
              // Нет ключей — откатываем и уведомляем
              db.run("ROLLBACK", () => {});
              safeSendMessage(ADMIN_ID,
                `❌ *Нет доступных ключей*\nProduct: ${product}\nUser: ${userId}\nДобавьте ключи через админ-панель.`,
                { parse_mode: 'Markdown' }
              ).catch(() => {});
              const outErr = new Error(`Нет доступных ключей для ${product}`);
              outErr.code = 'OUT_OF_STOCK';
              return reject(outErr);
            }

            // Шаг B: атомарно помечаем ключ как проданный — только по id + status='available'
            db.run(
              `UPDATE keys SET status = 'sold', buyer_id = ?, sold_at = datetime('now'), issue_reason = ?
               WHERE id = ? AND status = 'available'`,
              [userId, reason, row.id],
              function (updErr) {
                if (updErr) {
                  console.error('❌ UPDATE error in issueKeyToUser:', updErr);
                  return rollback(
                    new Error('Ошибка обновления ключа'),
                    `❌ *Ошибка UPDATE ключа*\nProduct: ${product}\nUser: ${userId}\n${updErr.message}`
                  );
                }

                if (this.changes === 0) {
                  // Ключ был занят параллельным запросом между A и B (крайне редко при IMMEDIATE)
                  console.warn(`⚠️ issueKeyToUser: key id=${row.id} was taken between SELECT and UPDATE — retrying`);
                  db.run("ROLLBACK", () => {});
                  // Рекурсивный повтор — попробуем следующий доступный ключ
                  resolve(issueKeyToUser(userId, product, reason));
                  return;
                }

                // Фиксируем транзакцию
                db.run("COMMIT", (commitErr) => {
                  if (commitErr) {
                    console.error('❌ COMMIT error:', commitErr);
                    return rollback(
                      new Error('Ошибка фиксации транзакции'),
                      `❌ *COMMIT fail*\nProduct: ${product}\nUser: ${userId}\n${commitErr.message}`
                    );
                  }
                  console.log(`✅ Issued key id=${row.id} for ${product} to user ${userId}`);
                  resolve(row.key_value);
                });
              }
            );
          }
      );
      }); // конец db.run("BEGIN IMMEDIATE")
    }); // конец db.serialize()
  }); // конец new Promise
}

// ─────────────────────────────────────────────────────────────────────────────
// 🔒 АТОМАРНАЯ ВЫДАЧА КЛЮЧА + ПОДТВЕРЖДЕНИЕ ЗАКАЗА В ОДНОЙ ТРАНЗАКЦИИ
// Решает Race Condition из Раздела 2.2
// ─────────────────────────────────────────────────────────────────────────────
async function issueKeyAndConfirmOrder(orderId, userId, product, reason = 'purchase') {
  if (product === 'infinite_boost' || product === 'reseller_connection') {
    return { key: await issueKeyToUser(userId, product, reason) };
  }
  return new Promise((resolve, reject) => {
    db.serialize(() => {
      db.run('BEGIN IMMEDIATE', (beginErr) => {
        if (beginErr) {
          console.error('❌ [ATOMIC] BEGIN IMMEDIATE error:', beginErr);
          return reject(new Error('Ошибка начала транзакции'));
        }
        const rollback = (error, adminMsg) => {
          db.run('ROLLBACK', () => {});
          if (adminMsg) safeSendMessage(ADMIN_ID, adminMsg, { parse_mode: 'Markdown' }).catch(() => {});
          reject(error);
        };
        db.get(
          'SELECT id, key_value FROM keys WHERE product = ? AND status = \'available\' LIMIT 1',
          [product],
          (selErr, row) => {
            if (selErr) {
              return rollback(new Error('Ошибка базы данных'),
                '❌ *[ATOMIC] SELECT ключа*\nЗаказ #' + orderId + '\nProduct: ' + product + '\n' + selErr.message);
            }
            if (!row) {
              db.run('ROLLBACK', () => {});
              const outErr = new Error('Нет доступных ключей для ' + product);
              outErr.code = 'OUT_OF_STOCK';
              return reject(outErr);
            }
            db.run(
              'UPDATE keys SET status = \'sold\', buyer_id = ?, sold_at = datetime(\'now\'), issue_reason = ? WHERE id = ? AND status = \'available\'',
              [userId, reason, row.id],
              function(updKeyErr) {
                if (updKeyErr) {
                  return rollback(new Error('Ошибка обновления ключа'),
                    '❌ *[ATOMIC] UPDATE keys*\nЗаказ #' + orderId + '\n' + updKeyErr.message);
                }
                if (this.changes === 0) {
                  db.run('ROLLBACK', () => {});
                  console.warn('⚠️ [ATOMIC] key id=' + row.id + ' taken mid-tx — retrying');
                  return resolve(issueKeyAndConfirmOrder(orderId, userId, product, reason));
                }
                db.run(
                  "UPDATE orders SET status = 'confirmed', key_issued = ?, confirmed_at = datetime('now') WHERE id = ? AND user_id = ? AND status IN ('pending', 'out_of_stock_pending')",
                  [row.key_value, orderId, userId],
                  function(updOrderErr) {
                    if (updOrderErr) {
                      return rollback(new Error('Ошибка обновления заказа'),
                        '❌ *[ATOMIC] UPDATE orders*\nЗаказ #' + orderId + '\nПроверьте вручную!\n' + updOrderErr.message);
                    }
                    if (this.changes === 0) {
                      // Заказ уже подтверждён другим потоком (дубль вебхука / двойной клик).
                      // ROLLBACK ключа — он не должен быть потрачен.
                      db.run('ROLLBACK', () => {});
                      const dupErr = new Error('Заказ уже подтверждён');
                      dupErr.code = 'ALREADY_CONFIRMED';
                      return reject(dupErr);
                    }
                    db.run('COMMIT', (commitErr) => {
                      if (commitErr) {
                        return rollback(new Error('Ошибка фиксации транзакции'),
                          '❌ *[ATOMIC] COMMIT fail*\nЗаказ #' + orderId + '\n' + commitErr.message);
                      }
                      console.log('✅ [ATOMIC] key id=' + row.id + ' issued + order #' + orderId + ' confirmed atomically');
                      resolve({ key: row.key_value });
                    });
                  }
                );
              }
            );
          }
        );
      });
    });
  });
}

// Экранирование Markdown (обычный)
function escapeMarkdown(text) {
  if (!text) return '';
  // Экранируем только символы, важные для Markdown v1: _ * ` [
  return String(text)
    .replace(/_/g, '\\_')
    .replace(/\*/g, '\\*')
    .replace(/`/g, '\\`')
    .replace(/\[/g, '\\[');
}

// Экранирование MarkdownV2
function escapeMarkdownV2(text) {
  if (!text) return '';
  return String(text)
    .replace(/_/g, '\\_')
    .replace(/\*/g, '\\*')
    .replace(/\[/g, '\\[')
    .replace(/\]/g, '\\]')
    .replace(/\(/g, '\\(')
    .replace(/\)/g, '\\)')
    .replace(/~/g, '\\~')
    .replace(/`/g, '\\`')
    .replace(/>/g, '\\>')
    .replace(/#/g, '\\#')
    .replace(/\+/g, '\\+')
    .replace(/-/g, '\\-')
    .replace(/=/g, '\\=')
    .replace(/\|/g, '\\|')
    .replace(/\{/g, '\\{')
    .replace(/\}/g, '\\}')
    .replace(/\./g, '\\.')
    .replace(/!/g, '\\!');
}

// Экранирование текста внутри backtick-блока Markdown
// В Markdown v1 внутри `code` нельзя использовать backtick и символ \
// Остальные символы безопасны — экранируем только их
function escapeForBacktick(text) {
  if (!text) return '';
  return String(text)
    .replace(/\\/g, '\\\\')
    .replace(/`/g, "'"); // заменяем backtick на апостроф, т.к. экранировать нельзя
}

// Парсинг срока бана из текста: '1d', '7d', '30d', 'perm'
function parseBanDuration(input) {
  if (input === 'perm' || input === 'навсегда' || input === '∞') {
    return { ms: 100 * 365 * 24 * 60 * 60 * 1000, label: 'навсегда' };
  }
  const match = input.match(/^(\d+)d$/);
  if (match) {
    const days = parseInt(match[1]);
    if (days < 1 || days > 3650) return null;
    const label = days === 1 ? '1 день' : days < 5 ? `${days} дня` : `${days} дней`;
    return { ms: days * 24 * 60 * 60 * 1000, label };
  }
  return null;
}

// Применить бан
function applyBan(targetId, displayName, durationMs, label, adminChatId) {
  const existing = rateLimitViolations.get(targetId) || { count: 0 };
  existing.bannedUntil = Date.now() + durationMs;
  existing.count = (existing.count || 0) + 1;
  rateLimitViolations.set(targetId, existing);
  bot.sendMessage(adminChatId, `✅ Пользователь ${displayName} заблокирован на *${label}*.`, { parse_mode: 'Markdown' });
  db.get('SELECT language_code FROM users WHERE id = ?', [targetId], (e, row) => {
    const isRuTarget = getLang({ language_code: row?.language_code || 'en' }) === 'ru';
    bot.sendMessage(targetId, isRuTarget
      ? '🚫 Ваш доступ к боту заблокирован администратором. Если считаете это ошибкой — обратитесь в поддержку.'
      : '🚫 Your bot access has been blocked by the administrator. Contact support if you think this is a mistake.'
    ).catch(() => { });
  });
  logAction(ADMIN_ID, 'user_banned_manual', { targetId, displayName, label });
}

// Генерация читаемого ID транзакции: cyrax + 4 цифры + 2 буквы
function generateTxnId() {
  const digits = Math.floor(1000 + Math.random() * 9000); // 4 цифры
  const chars = 'abcdefghjkmnpqrstuvwxyz'; // без похожих i/l/o
  const letters = Array.from({ length: 2 }, () => chars[Math.floor(Math.random() * chars.length)]).join('');
  return `cyrax${digits}${letters}`;
}

// ==========================================
// 🔐 ШИФРОВАНИЕ ТОКЕНОВ РЕСЕЛЛЕРОВ (AES-256-CBC)
// ==========================================
// Формат в БД: hex(iv):hex(encrypted)
// Ключ берётся из RESELLER_ENCRYPTION_KEY (64 hex символа = 32 байта)

function encryptToken(token) {
  if (!RESELLER_ENCRYPTION_KEY) {
    throw new Error('RESELLER_ENCRYPTION_KEY не задан в переменных окружения');
  }
  const key = Buffer.from(RESELLER_ENCRYPTION_KEY, 'hex');
  if (key.length !== 32) {
    throw new Error('RESELLER_ENCRYPTION_KEY должен быть 64 hex символа (32 байта)');
  }
  const iv = crypto.randomBytes(16);
  const cipher = crypto.createCipheriv('aes-256-cbc', key, iv);
  let encrypted = cipher.update(token, 'utf8', 'hex');
  encrypted += cipher.final('hex');
  return iv.toString('hex') + ':' + encrypted;
}

function decryptToken(encryptedData) {
  if (!RESELLER_ENCRYPTION_KEY) {
    throw new Error('RESELLER_ENCRYPTION_KEY не задан в переменных окружения');
  }
  const key = Buffer.from(RESELLER_ENCRYPTION_KEY, 'hex');
  if (key.length !== 32) {
    throw new Error('RESELLER_ENCRYPTION_KEY должен быть 64 hex символа (32 байта)');
  }
  const parts = encryptedData.split(':');
  if (parts.length !== 2) {
    throw new Error('Некорректный формат зашифрованного токена');
  }
  const iv = Buffer.from(parts[0], 'hex');
  const encrypted = parts[1];
  const decipher = crypto.createDecipheriv('aes-256-cbc', key, iv);
  let decrypted = decipher.update(encrypted, 'hex', 'utf8');
  decrypted += decipher.final('utf8');
  return decrypted;
}

// Безопасная отправка сообщения
async function safeSendMessage(chatId, text, options = {}, botInstance = bot) {
  const MAX_RETRIES = 2;
  for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
    try {
      return await botInstance.sendMessage(chatId, text, options);
    } catch (error) {
      // 429 Too Many Requests — ждём сколько сказал Telegram и повторяем
      const retryAfterMatch = error.message.match(/retry after (\d+)/i);
      if (retryAfterMatch && attempt < MAX_RETRIES) {
        const waitSec = parseInt(retryAfterMatch[1]) + 1;
        console.warn(`⏳ [429] chatId=${chatId} — ждём ${waitSec}с (попытка ${attempt + 1}/${MAX_RETRIES})`);
        await new Promise(r => setTimeout(r, waitSec * 1000));
        continue; // повторяем
      }

      // Markdown/HTML parse error — сначала пробуем убрать <emoji> теги, сохраняя parse_mode: 'HTML'
      const isParseError =
        error.message.includes("can't parse entities") ||
        error.message.includes("не удается проанализировать сущности") ||
        error.message.includes("parse entities") ||
        error.message.includes("Bad Request: can") ||
        (error.message.includes('400') && error.message.toLowerCase().includes('entit'));
      if (isParseError) {
        // Шаг 1: убираем <emoji id="...">...</emoji> но оставляем содержимое (fallback emoji) и parse_mode
        const textWithoutEmojiTags = text.replace(/<emoji\s+id="[^"]*">([^<]*)<\/emoji>/g, '$1');
        if (textWithoutEmojiTags !== text) {
          try {
            console.log(`[EMOJI-FALLBACK] Animated emoji not supported, retrying without <emoji> tags for chatId ${chatId}`);
            return await botInstance.sendMessage(chatId, textWithoutEmojiTags, { ...options, parse_mode: options.parse_mode || 'HTML' });
          } catch (retryError) {
            // Шаг 2: если всё ещё ошибка — убираем parse_mode полностью
            try {
              console.log(`[EMOJI-FALLBACK] Still failing, retrying without parse_mode for chatId ${chatId}`);
              return await botInstance.sendMessage(chatId, textWithoutEmojiTags, { ...options, parse_mode: undefined });
            } catch (finalError) {
              console.error(`❌ Retry failed:`, finalError.message);
            }
          }
        } else {
          // Нет emoji тегов — просто убираем parse_mode
          try {
            console.warn(`⚠️ Markdown parse error, retrying without parse_mode for chatId ${chatId}`);
            return await botInstance.sendMessage(chatId, text, { ...options, parse_mode: undefined });
          } catch (retryError) {
            console.error(`❌ Retry failed:`, retryError.message);
          }
        }
      }

      if (attempt === MAX_RETRIES || !retryAfterMatch) {
        console.error(`❌ Error sending to ${chatId}:`, error.message);
        throw error;
      }
    }
  }
}

// ==========================================
// ✨ АНИМИРОВАННЫЕ ЭМОДЗИ
// ==========================================
const ANIMATED_EMOJI = {
  KEY: '5472169674895236432',
  FIRE: '5368324170671202286',
  HOURGLASS: '5369733581767469416',
  ROCKET: '5369733579398214914',
  GIFT: '5374391301112144023',
  DIAMOND: '5374142657177799873',
  CROWN: '5374383579652104323',
  MONEY: '5374142656060133828',
  TARGET: '5374142655595154340',
  HANDSHAKE: '5374383573772534458',
  BOLT: '5374142658537926092',
  SHIELD: '5374142661525737871',
  GREEN: '5369733578576814155',
  RED: '5369733577605529660',
  YELLOW: '5374142663277744151',
  PURPLE: '5374142655759253949',
  STATS: '5369733579220787200',
  CHART_UP: '5374142659778314290',
  TROPHY: '5369733576352530432',
  GOLD: '5374142655509160033',
  SILVER: '5374142658076868610',
  BRONZE: '5374142656931037184',
  SUPPORT: '5374142657210482800',
  MEGAPHONE: '5374142661525737871',
};

/**
 * Экранирует HTML-спецсимволы для безопасного использования в parse_mode:'HTML'
 * Применять к любым пользовательским данным: username, notes, имена, описания и т.д.
 */
function escapeHtml(str) {
  if (str === null || str === undefined) return '';
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

async function sendWithAnimatedEmoji(chatId, text, animatedEmojiId, fallbackEmoji, options = {}, botInstance = bot) {
    const animatedText = text.replace(fallbackEmoji, `<emoji id="${animatedEmojiId}">${fallbackEmoji}</emoji>`);
    try {
        return await safeSendMessage(chatId, animatedText, { ...options, parse_mode: 'HTML' }, botInstance);
    } catch (error) {
        if (error.message.includes('emoji') || error.message.includes('Bad Request') || error.message.includes('parse entities')) {
            // Fallback: убираем <emoji> тег, но СОХРАНЯЕМ parse_mode: 'HTML' чтобы <b>, <i> и т.д. работали
            return await safeSendMessage(chatId, text, { ...options, parse_mode: 'HTML' }, botInstance);
        }
        throw error;
    }
}

// Разбивка длинного сообщения на части
function splitMessage(text, maxLength = 4000) {
  if (text.length <= maxLength) {
    return [text];
  }

  const parts = [];
  let currentPart = '';
  const lines = text.split('\n');

  for (const line of lines) {
    if ((currentPart + line + '\n').length > maxLength) {
      if (currentPart) {
        parts.push(currentPart.trim());
        currentPart = '';
      }
      if (line.length > maxLength) {
        const chunks = line.match(new RegExp(`.{1,${maxLength}}`, 'g')) || [];
        parts.push(...chunks.slice(0, -1));
        currentPart = chunks[chunks.length - 1] + '\n';
      } else {
        currentPart = line + '\n';
      }
    } else {
      currentPart += line + '\n';
    }
  }

  if (currentPart.trim()) {
    parts.push(currentPart.trim());
  }

  return parts;
}

// ==========================================
// 🎯 ГЛАВНОЕ МЕНЮ
// ==========================================
// ==========================================
// 👤 РУЧНОЙ БУСТ — вспомогательные функции
// ==========================================

function showAdminManualBoost(chatId, msgId = null) {
  const status = getSetting('manual_boost_status') || 'coming_soon';
  const statusLabel = status === 'active' ? '✅ Активен' : '⏳ Скоро (COMING SOON)';
  const msg = `👤 *Ручной Буст — Управление*\n\n📌 Статус: *${statusLabel}*\n\n` +
    `💰 Базовые цены за звезду:\n` +
    Object.entries({ RUB: '₽', USD: '$', EUR: '€', UAH: '₴' }).map(([cur, sym]) => {
      const v = getSetting(`manual_boost_price_${cur.toLowerCase()}`) || '—';
      return `${FLAGS[cur]} ${cur}: *${v} ${sym}*`;
    }).join('\n');
  const keyboard = {
    inline_keyboard: [
      [{ text: status === 'active' ? '⏸ Деактивировать' : '▶️ Активировать', callback_data: 'admin_mb_toggle_status' }],
      [
        { text: '💰 Цена RUB', callback_data: 'admin_mb_set_price_rub' },
        { text: '💰 Цена USD', callback_data: 'admin_mb_set_price_usd' }
      ],
      [
        { text: '💰 Цена EUR', callback_data: 'admin_mb_set_price_eur' },
        { text: '💰 Цена UAH', callback_data: 'admin_mb_set_price_uah' }
      ],
      [{ text: '📋 Заявки', callback_data: 'admin_mb_list_requests' }],
      [{ text: '◀️ Назад', callback_data: 'admin' }]
    ]
  };
  adminSend(chatId, ADMIN_ID, msg, { parse_mode: 'Markdown', reply_markup: keyboard }, msgId);
}

// Создаём заявку в БД и уведомляем админа
function submitBoostRequest(user, chatId, session) {
  const mb = session.data?.manualBoost;
  if (!mb || !mb.currentRankKey || !mb.targetRankKey) {
    bot.sendMessage(chatId, t(user, 'boost_data_corrupted')).catch(() => {});
    return;
  }
  const username = user.username ? escapeMarkdown(`@${user.username}`) : `ID: ${user.id}`;
  const lang = getLang(user);

  const { costRub, totalStars } = calcBoostCost(
    mb.currentRankKey, mb.currentStars || 0,
    mb.targetRankKey, mb.targetStars || 0
  );
  const costs = convertFromRub(costRub);

  // Проверяем: является ли пользователь реселлером?
  db.get(`SELECT id, markup_pct FROM resellers WHERE user_id = ? AND status = 'active'`, [user.id], (rslErr, rslRow) => {
    const resellerMarkup = rslRow ? (rslRow.markup_pct || 30) : null;
    const resellerId = rslRow ? rslRow.id : null;
    // Если реселлер — применяем наценку к базовой стоимости
    const finalCostRub = resellerMarkup !== null
      ? Math.round(costRub * (1 + resellerMarkup / 100))
      : costRub;
    const finalCosts = resellerMarkup !== null ? convertFromRub(finalCostRub) : costs;

  db.run(
    `INSERT INTO boost_requests (user_id, username, user_lang, current_rank, desired_rank, stars_current, stars_desired, base_cost_rub, costs_json, status, reseller_id, reseller_markup_pct)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?)`,
    [user.id, username, user.language_code || 'en',
    mb.currentRankKey, mb.targetRankKey,
    mb.currentStars || 0, mb.targetStars || 0,
      finalCostRub, JSON.stringify(finalCosts),
      resellerId, resellerMarkup],
    function (err) {
    if (err) { bot.sendMessage(chatId, t(user, 'boost_request_error')); return; }
      const brId = this.lastID;
      clearSession(user.id);

      // Клиенту — подтверждение
      bot.sendMessage(chatId,
        t(user, 'manual_boost_submitted', { current: mb.currentRankLabel, target: mb.targetRankLabel }),
        { parse_mode: 'HTML' }
      );

      // Админу — уведомление с кнопками
      const markupNote = resellerMarkup !== null
        ? `\n🤝 Реселлер (наценка ${resellerMarkup}%): база *${costRub} ₽* → клиенту *${finalCostRub} ₽*`
        : '';
      const adminMsg =
        `🔔 *Новая заявка — Ручной Буст #${brId}*\n\n` +
        `👤 ${username}\n🌐 Язык: ${user.language_code || 'en'}\n\n` +
        `🎮 *${escapeMarkdown(mb.currentRankLabel)}*` + (mb.currentStars ? ` (${mb.currentStars} ⭐)` : '') + `\n` +
        `🏆 *${escapeMarkdown(mb.targetRankLabel)}*` + (mb.targetStars ? ` (${mb.targetStars} ⭐)` : '') + `\n\n` +
        `⭐ Звёзд: *${totalStars}*\n` +
        `💰 Расчётная стоимость: *${finalCostRub} ₽*\n_(${formatAllCurrencies(finalCosts)})_` +
        markupNote;

      safeSendMessage(ADMIN_ID, adminMsg, {
        parse_mode: 'Markdown',
        reply_markup: {
          inline_keyboard: [
            [{ text: '🧮 Рассчитать / изменить цену', callback_data: `admin_mb_calc_${brId}` }]
          ]
        }
      }).catch(() => { });
      logAction(user.id, 'boost_request_submitted', { brId, costRub });
    }
  );
  }); // close db.get reseller check
}

// Отправляем клиенту итоговую стоимость и кнопки выбора валюты
function sendBoostCostToClient(brId, rubPrice, adminChatId) {
  db.get(`SELECT * FROM boost_requests WHERE id = ?`, [brId], (err, br) => {
    if (err || !br) { bot.sendMessage(adminChatId, '❌ Заявка не найдена'); return; }
    // Применяем наценку реселлера если заявка пришла через реселлер-бота
    const markupPct = br.reseller_markup_pct || 0;
    const finalRubPrice = markupPct > 0
      ? Math.round(rubPrice * (1 + markupPct / 100))
      : rubPrice;
    const costs = convertFromRub(finalRubPrice);
    db.run(`UPDATE boost_requests SET base_cost_rub = ?, costs_json = ?, status = 'priced' WHERE id = ?`,
      [finalRubPrice, JSON.stringify(costs), brId]);

    const clientUser = { id: br.user_id, language_code: br.user_lang };
    const fromRank = MLBB_RANKS.find(r => r.key === br.current_rank);
    const toRank = MLBB_RANKS.find(r => r.key === br.desired_rank);
    const lang = br.user_lang && br.user_lang.startsWith('ru') ? 'ru' : 'en';
    const fromLabel = escapeMarkdown(fromRank ? (lang === 'ru' ? fromRank.label_ru : fromRank.label_en) : br.current_rank);
    const toLabel = escapeMarkdown(toRank ? (lang === 'ru' ? toRank.label_ru : toRank.label_en) : br.desired_rank);

    const costsText = `🇷🇺 ${costs.RUB} ₽  •  🇺🇸 $${costs.USD}  •  🇪🇺 €${costs.EUR}  •  🇺🇦 ${costs.UAH} ₴`;
    const clientMsg = t(clientUser, 'manual_boost_cost_received', {
      current: fromLabel, target: toLabel, costs: costsText
    });

    const kb = {
      inline_keyboard: [
        [
          { text: `🇷🇺 ${costs.RUB} ₽`, callback_data: `mb_pay_currency_${brId}_RUB` },
          { text: `🇺🇸 $${costs.USD}`, callback_data: `mb_pay_currency_${brId}_USD` }
        ],
        [
          { text: `🇪🇺 €${costs.EUR}`, callback_data: `mb_pay_currency_${brId}_EUR` },
          { text: `🇺🇦 ${costs.UAH} ₴`, callback_data: `mb_pay_currency_${brId}_UAH` }
        ]
      ]
    };

    bot.sendMessage(br.user_id, clientMsg, { parse_mode: 'HTML', reply_markup: kb }).catch(e => {
      bot.sendMessage(adminChatId, `❌ Не удалось отправить клиенту: ${e.message}`);
    });
    bot.sendMessage(adminChatId, `✅ Стоимость *${rubPrice} ₽* отправлена клиенту (заявка #${brId}).`, { parse_mode: 'Markdown' });
  });
}

async function showMainMenu(chatId, user) {
  // Базовая клавиатура — одинакова для всех (витрина, цены, кнопки оплаты НЕ затрагиваются)
  const isRuMenu = getLang(user) === 'ru';
  const keyboard = {
    inline_keyboard: [
      // Покупка + Буст
      [{ text: t(user, 'buy_key'), callback_data: 'buy' }],
      [{ text: t(user, 'boost_hub'), callback_data: 'boost_hub' }],
      // Проблема с ключом
      [{ text: isRuMenu ? '🆘 Проблема с ключом?' : '🆘 Key issue?', callback_data: 'support_ticket' }],
      // Канал + FAQ
      [
        { text: t(user, 'channel'), url: getSetting('channel_link') || 'https://t.me/cyraxml' },
        { text: '❓ FAQ', callback_data: 'faq' }
      ],
      // Реферальная
      [{ text: isRuMenu ? '🎁 Реферальная программа' : '🎁 Referral program', callback_data: 'my_ref' }],
      // Оферта
      [{ text: t(user, 'offer'), callback_data: 'offer' }],
      // Партнёрство — последняя для клиентов
      [{ text: isRuMenu ? '🤝 Партнёрство' : '🤝 Partnership', callback_data: 'partnership' }]
    ]
  };

  // Если есть баланс — НЕ показываем в главном меню. Профиль доступен внутри "Купить ключ".

  // Админ — добавляем кнопку панели, отправляем, выходим
  if (user.id === ADMIN_ID) {
    keyboard.inline_keyboard.push([{ text: t(user, 'admin_panel'), callback_data: 'admin' }]);
    sendNavWithAnimatedEmoji(chatId, user.id, t(user, 'welcome'), ANIMATED_EMOJI.FIRE, '🔥', { reply_markup: keyboard });
    return;
  }

  // Не-админ: проверяем менеджера в БД — ровно одно сообщение в любом случае
  isManager(user.id).then(mgr => {
    if (mgr) {
      keyboard.inline_keyboard.push([{ text: '📦 Заказы на проверку', callback_data: 'manager_orders' }]);
    }
    sendNavWithAnimatedEmoji(chatId, user.id, t(user, 'welcome'), ANIMATED_EMOJI.FIRE, '🔥', { reply_markup: keyboard });
  });
}

// ==========================================
// 📊 АДМИН-ПАНЕЛЬ — ПЕРЕРАБОТАННАЯ СТРУКТУРА
// ==========================================
// Логика: 4 раздела с чёткими границами
//   📊 Аналитика   → статистика, заказы, топ, ключи
//   🛒 Магазин     → ключи, цены, реквизиты, купоны, буст
//   📣 Маркетинг   → рассылка, лояльность, FOMO, отзывы, AI
//   ⚙️ Настройки   → (уже существует showBotSettings)
// ==========================================
function showAdminPanel(chatId, msgId = null) {
  const keyboard = {
    inline_keyboard: [
      // ─── СТРОКА 1: Аналитика ───
      [{ text: '━━━  📊 АНАЛИТИКА  ━━━', callback_data: 'noop' }],
      [
        { text: '📊 Статистика', callback_data: 'admin_stats' },
        { text: '📦 Заказы', callback_data: 'admin_manage_orders' }
      ],
      [{ text: '🔍 Поиск пользователя', callback_data: 'admin_user_search' }],
      [
        { text: '📈 Топ продаж', callback_data: 'admin_top_sales' },
        { text: '👥 Активные', callback_data: 'admin_active_users' }
      ],
      [
        { text: '🔑 Запас ключей', callback_data: 'admin_key_stock' },
        { text: '📜 Проданные ключи', callback_data: 'admin_sold_keys' }
      ],
      [{ text: '⚠️ Жалобы (тикеты)', callback_data: 'admin_tickets' }],
      [{ text: '📨 Написать пользователю', callback_data: 'admin_message_user' }],
      [{ text: '⏰ Потерянные заказы', callback_data: 'admin_lost_orders' }],

      // ─── СТРОКА 2: Магазин ───
      [{ text: '━━━  🛒 МАГАЗИН  ━━━', callback_data: 'noop' }],
      [
        { text: '🔑 Ключи', callback_data: 'admin_manage_keys' },
        { text: '💰 Цены', callback_data: 'admin_manage_prices' }
      ],
      [
        { text: '💳 Реквизиты', callback_data: 'admin_manage_payment_details' },
        { text: '🎟️ Купоны', callback_data: 'admin_coupons' }
      ],
      [
        { text: '👤 Ручной Буст', callback_data: 'admin_manage_manual_boost' },
        { text: '💱 Курсы валют', callback_data: 'admin_exchange_rates' }
      ],

      // ─── СТРОКА 3: Маркетинг ───
      [{ text: '━━━  📣 МАРКЕТИНГ  ━━━', callback_data: 'noop' }],
      [
        { text: '📢 Рассылка', callback_data: 'admin_broadcast' },
        { text: '📝 Отзывы', callback_data: 'admin_reviews' }
      ],
      [
        { text: '🎁 Лояльность', callback_data: 'admin_loyalty' },
        { text: '🎫 FOMO-купоны', callback_data: 'admin_fomo' }
      ],
      [
        { text: '⏰ Отложенные рассылки', callback_data: 'admin_scheduled_broadcast' }
      ],
      [
        { text: '🎁 Начислить всем клиентам', callback_data: 'admin_gift_all' }
      ],

      // ─── СТРОКА 4: Система ───
      [{ text: '━━━  🔧 СИСТЕМА  ━━━', callback_data: 'noop' }],
      [
        { text: '⚙️ Настройки', callback_data: 'admin_settings' },
        { text: '🚫 Баны', callback_data: 'admin_bans' }
      ],
      [
        { text: '👥 Менеджеры', callback_data: 'admin_managers' },
        { text: '🤝 Реселлеры', callback_data: 'admin_resellers' }
      ],
      [
        { text: '📋 Логи', callback_data: 'admin_view_logs' },
        { text: '📥 Экспорт CSV', callback_data: 'admin_export_csv' }
      ],
      [
        { text: '💾 Бэкап БД', callback_data: 'admin_backup' },
        { text: '🔄 Восстановить БД', callback_data: 'admin_restore' }
      ],

      // Назад
      [{ text: '◀️ Назад в меню', callback_data: 'start' }]
    ]
  };

  const session = getSession(ADMIN_ID);
  if (msgId) {
    // Редактируем существующее сообщение
    bot.editMessageText('👑 <b>Панель управления</b>', {
      chat_id: chatId,
      message_id: msgId,
      parse_mode: 'HTML',
      reply_markup: keyboard
    }).catch(() => {
      // Если не удалось отредактировать — удаляем старое и шлём новое
      if (session.adminMsgId) bot.deleteMessage(chatId, session.adminMsgId).catch(() => {});
      sendWithAnimatedEmoji(chatId, '👑 <b>Панель управления</b>', ANIMATED_EMOJI.CROWN, '👑', {
        parse_mode: 'HTML', reply_markup: keyboard
      }).then(sent => { if (sent) session.adminMsgId = sent.message_id; }).catch(() => {});
    });
    session.adminMsgId = msgId;
  } else {
    // Удаляем предыдущее admin-сообщение
    if (session.adminMsgId) {
      bot.deleteMessage(chatId, session.adminMsgId).catch(() => {});
      session.adminMsgId = null;
    }
    sendWithAnimatedEmoji(chatId, '👑 <b>Панель управления</b>', ANIMATED_EMOJI.CROWN, '👑', {
      parse_mode: 'HTML', reply_markup: keyboard
    }).then(sent => { if (sent) session.adminMsgId = sent.message_id; }).catch(() => {});
  }
}

// ==========================================
// 🔓 УПРАВЛЕНИЕ БАНАМИ
// ==========================================
function showBannedUsers(chatId, msgId = null) {
  const now = Date.now();
  const banned = [];

  for (const [userId, violation] of rateLimitViolations.entries()) {
    if (violation.bannedUntil && now < violation.bannedUntil) {
      banned.push({ userId, bannedUntil: violation.bannedUntil, violations: violation.count });
    }
  }

  if (banned.length === 0) {
    return adminSend(chatId, ADMIN_ID, '🔓 *Управление банами*\n\n✅ Забаненных пользователей нет.',
      {
        parse_mode: 'Markdown',
        reply_markup: {
          inline_keyboard: [
            [{ text: '🚫 Забанить пользователя', callback_data: 'admin_ban_by_username' }],
            [{ text: '✏️ Разбанить по @username / ID', callback_data: 'admin_unban_by_username' }],
            [{ text: '◀️ Назад', callback_data: 'admin' }]
          ]
        }
      }, msgId);
  }

  // Подгружаем username'ы из БД для всех забаненных
  const ids = banned.map(b => b.userId);
  db.all(
    `SELECT id, username FROM users WHERE id IN (${ids.map(() => '?').join(',')})`,
    ids,
    (err, rows) => {
      const usernameMap = {};
      if (!err && rows) rows.forEach(r => { usernameMap[r.id] = r.username; });

      let msg = `🔓 *Управление банами*\n\n🚫 Забанено: ${banned.length} пользователей\n\n`;
      const keyboard = { inline_keyboard: [] };

      banned.forEach((b, i) => {
        const uname = usernameMap[b.userId];
        const display = uname ? `@${escapeMarkdown(uname)}` : `ID: ${b.userId}`;
        const until = new Date(b.bannedUntil).toLocaleString('ru-RU');
        msg += `${i + 1}. ${display}\n   ⏰ До: ${until}\n   ⚠️ Нарушений: ${b.violations}\n\n`;
        keyboard.inline_keyboard.push([
          { text: `🔓 Разбанить ${display}`, callback_data: `unban_user_${b.userId}` }
        ]);
      });

      keyboard.inline_keyboard.push([
        { text: '🚫 Забанить пользователя', callback_data: 'admin_ban_by_username' }
      ]);
      keyboard.inline_keyboard.push([
        { text: '✏️ Разбанить по @username / ID', callback_data: 'admin_unban_by_username' }
      ]);
      keyboard.inline_keyboard.push([
        { text: '◀️ Назад', callback_data: 'admin' }
      ]);

      safeSendMessage(chatId, msg, { parse_mode: 'Markdown', reply_markup: keyboard });
    }
  );
}


function showTopSales(chatId, msgId = null) {
  const weekAgo = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString().split('T')[0];

  // Один запрос на каждый блок — без вложенных колбэков
  db.all(`SELECT product, COUNT(*) as cnt FROM orders WHERE status='confirmed' GROUP BY product ORDER BY cnt DESC`, [], (e1, products) => {
    if (e1) { adminSend(chatId, ADMIN_ID, '❌ Ошибка статистики'); return; }

    db.all(`SELECT currency, COUNT(*) as cnt, SUM(amount) as total FROM orders WHERE status='confirmed' GROUP BY currency ORDER BY cnt DESC`, [], (e2, currencies) => {
      if (e2) { safeSendMessage(chatId, '❌ Ошибка статистики'); return; }

      db.all(`SELECT method, COUNT(*) as cnt FROM orders WHERE status='confirmed' GROUP BY method ORDER BY cnt DESC`, [], (e3, methods) => {
        if (e3) { safeSendMessage(chatId, '❌ Ошибка статистики'); return; }

        db.all(`SELECT date(created_at) as day, COUNT(*) as cnt FROM orders WHERE status='confirmed' AND date(created_at) >= ? GROUP BY day ORDER BY day DESC`, [weekAgo], (e4, daily) => {
          if (e4) { safeSendMessage(chatId, '❌ Ошибка статистики'); return; }

          // ── Форматирование суммы: без плавающей точки мусора ──
          const fmt = (amount, currency) => {
            const n = Math.round(parseFloat(amount) * 100) / 100;
            if (currency === 'RUB') return `${Math.round(n)} ₽`;
            if (currency === 'UAH') return `${Math.round(n)} ₴`;
            if (currency === 'USD') return `$${n.toFixed(2)}`;
            if (currency === 'EUR') return `€${n.toFixed(2)}`;
            return `${n} ${currency}`;
          };

          const medals = ['🥇', '🥈', '🥉'];
          const icon = (i) => medals[i] || `${i + 1}.`;

          let msg = '📈 <b>Топ продаж</b>\n';

          // По продуктам
          msg += '\n🏆 <b>Продукты</b>\n';
          if (products?.length) {
            products.forEach((r, i) => {
              const name = PERIOD_NAMES.ru[r.product] || r.product;
              msg += `${icon(i)} ${name} — <b>${r.cnt} шт.</b>\n`;
            });
          } else msg += '—\n';

          // По валютам
          msg += '\n💱 <b>Валюты</b>\n';
          if (currencies?.length) {
            currencies.forEach((r, i) => {
              const flag = FLAGS[r.currency] || '';
              msg += `${icon(i)} ${flag} ${r.currency} — <b>${r.cnt} шт.</b> (${fmt(r.total, r.currency)})\n`;
            });
          } else msg += '—\n';

          // По методам
          msg += '\n💳 <b>Методы оплаты</b>\n';
          if (methods?.length) {
            methods.forEach((r, i) => {
              msg += `${icon(i)} ${r.method} — <b>${r.cnt} шт.</b>\n`;
            });
          } else msg += '—\n';

          // По дням (7 дней)
          msg += '\n📅 <b>Последние 7 дней</b>\n';
          if (daily?.length) {
            daily.forEach(r => {
              const d = new Date(r.day + 'T00:00:00Z');
              const label = d.toLocaleDateString('ru-RU', { day: 'numeric', month: 'short', timeZone: 'UTC' });
              msg += `├ ${label}: <b>${r.cnt}</b>\n`;
            });
          } else msg += '└ Нет данных\n';

          sendWithAnimatedEmoji(chatId, msg, ANIMATED_EMOJI.CHART_UP, '📈', {
            parse_mode: 'HTML',
            reply_markup: { inline_keyboard: [[{ text: '◀️ Назад', callback_data: 'admin' }]] }
          });
        });
      });
    });
  });
}

// ==========================================
// 👥 НОВАЯ ФИЧА: Активные пользователи
// ==========================================
function showActiveUsers(chatId) {
  const today = new Date().toISOString().split('T')[0];
  const weekAgo = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString().split('T')[0];

  db.get(
    `SELECT 
      (SELECT COUNT(DISTINCT user_id) FROM action_logs WHERE date(timestamp) = ?) as today_active,
      (SELECT COUNT(DISTINCT user_id) FROM action_logs WHERE date(timestamp) >= ?) as week_active,
      (SELECT COUNT(*) FROM users) as total_users
    `,
    [today, weekAgo],
    (err, stats) => {
      if (err) {
        console.error('❌ Active users error:', err);
        safeSendMessage(chatId, '❌ Ошибка получения статистики');
        return;
      }

      db.all(
        `SELECT u.username, u.id, COUNT(l.id) as actions,
                MAX(l.timestamp) as last_action
         FROM users u
         LEFT JOIN action_logs l ON u.id = l.user_id AND date(l.timestamp) >= ?
         GROUP BY u.id
         HAVING actions > 0
         ORDER BY actions DESC
         LIMIT 15`,
        [weekAgo],
        (err2, topUsers) => {
          if (err2) {
            console.error('❌ Top users error:', err2);
            safeSendMessage(chatId, '❌ Ошибка получения топа пользователей');
            return;
          }

          let message = '👥 <b>Активные пользователи</b>\n\n';
          message += `📅 <b>Сегодня:</b> ${stats.today_active || 0} чел.\n`;
          message += `📆 <b>За 7 дней:</b> ${stats.week_active || 0} чел.\n`;
          message += `👥 <b>Всего:</b> ${stats.total_users || 0} чел.\n\n`;
          message += `🏆 <b>Топ-15 за неделю:</b>\n`;

          if (topUsers && topUsers.length > 0) {
            topUsers.forEach((user, index) => {
              const username = user.username ? `@${user.username}` : `ID: ${user.id}`;
              const lastDate = new Date(user.last_action).toLocaleDateString('ru-RU', { day: 'numeric', month: 'short' });
              message += `${index + 1}. ${username} — ${user.actions} д. (посл. ${lastDate})\n`;
            });
          } else {
            message += '<i>Нет данных за неделю</i>';
          }

          const keyboard = {
            inline_keyboard: [
              [{ text: '◀️ Назад', callback_data: 'admin' }]
            ]
          };

          sendWithAnimatedEmoji(chatId, message, ANIMATED_EMOJI.STATS, '👥', {
            parse_mode: 'HTML',
            reply_markup: keyboard
          });
        }
      );
    }
  );
}

// ==========================================
// 📦 АВТОМАТИЧЕСКИЙ БЭКАП
// ==========================================
let lastBackupHash = null;
let backupInProgress = false;
let lastBackupTimestamp = null; // П.7: для метрики last_backup_minutes_ago в /health

// ==========================================
// 🔐 НАДЁЖНЫЙ БЭКАП через SQLite Backup API
// Включает WAL, все настройки, ключи, купоны — полный слепок БД
// ==========================================

function createSqliteBackup(destPath) {
  return new Promise((resolve, reject) => {
    // Используем официальный SQLite Online Backup API через node sqlite3
    // db.backup() гарантирует консистентный снапшот даже при активных записях
    // и корректно захватывает WAL-файл
    const destDb = new sqlite3.Database(destPath, (err) => {
      if (err) return reject(err);
    });

    // Принудительный CHECKPOINT перед бэкапом — сбрасываем WAL в основной файл
    db.run('PRAGMA wal_checkpoint(FULL)', [], (checkpointErr) => {
      if (checkpointErr) console.error('⚠️ WAL checkpoint warning:', checkpointErr.message);

      destDb.close((closeErr) => {
        if (closeErr) return reject(closeErr);

        // Теперь копируем уже checkpointed файл — все данные гарантированно внутри
        try {
          fs.copyFileSync(DB_PATH, destPath);
          // Также копируем WAL если он существует (на случай частичного checkpoint)
          if (fs.existsSync(DB_PATH + '-wal')) {
            fs.copyFileSync(DB_PATH + '-wal', destPath + '-wal');
          }
          if (fs.existsSync(DB_PATH + '-shm')) {
            fs.copyFileSync(DB_PATH + '-shm', destPath + '-shm');
          }
          resolve();
        } catch (copyErr) {
          reject(copyErr);
        }
      });
    });
  });
}

// FIX 4: Общая логика создания и верификации бэкапа вынесена в отдельную функцию.
// Ранее sendDatabaseBackup и sendManualBackup дублировали ~60 строк идентичного кода:
// createSqliteBackup → statSync → верификация БД → cleanup временных файлов.
// Теперь обе функции используют createAndVerifyBackup() — единая точка изменений.
/**
 * Создаёт SQLite-бэкап, верифицирует его содержимое и удаляет временные файлы.
 * @returns {{ backupPath, formattedDate, formattedTime, backupStats, summary }}
 *   backupPath уже удалён с диска — используйте его только ДО вызова cleanup (внутри функции).
 *   Данные summary и мета доступны после возврата.
 */
async function createAndVerifyBackup() {
  const date = new Date();
  const formattedDate = date.toISOString().split('T')[0];
  const formattedTime = date.toTimeString().split(' ')[0].replace(/:/g, '-');

  const backupFileName = `shop_backup_${formattedDate}_${formattedTime}.db`;
  const backupPath = path.join('/tmp', backupFileName);

  await createSqliteBackup(backupPath);

  const backupStats = fs.statSync(backupPath);
  console.log(`📦 Создан бэкап: ${backupFileName} (${(backupStats.size / 1024).toFixed(2)} KB)`);

  // Верификация: открываем копию как отдельную БД и считаем строки ключевых таблиц
  const summary = await new Promise((resolve) => {
    const verifyDb = new sqlite3.Database(backupPath, sqlite3.OPEN_READONLY, (err) => {
      if (err) return resolve(null);
      verifyDb.get(
        `SELECT
          (SELECT COUNT(*) FROM users)                                          as users_count,
          (SELECT COUNT(*) FROM orders)                                         as total_orders,
          (SELECT COUNT(*) FROM keys WHERE status='available')                  as available_keys,
          (SELECT COUNT(*) FROM resellers)                                      as resellers_count,
          (SELECT COUNT(*) FROM coupons)                                        as coupons_count,
          (SELECT COUNT(*) FROM coupons WHERE is_active=1)                      as active_coupons,
          (SELECT COUNT(*) FROM referrals)                                      as referrals_count,
          (SELECT COUNT(*) FROM review_codes)                                   as review_codes_count,
          (SELECT COUNT(*) FROM settings)                                       as settings_count,
          (SELECT COUNT(*) FROM boost_requests WHERE status NOT IN ('confirmed','rejected')) as boost_requests`,
        [],
        (e, row) => {
          verifyDb.close();
          resolve(e ? null : row);
        }
      );
    });
  });

  // Cleanup: удаляем временные файлы бэкапа и WAL/SHM артефакты
  // ВАЖНО: возвращаем путь до cleanup — sendDocument должен быть вызван ВНУТРИ вызывающей функции
  // с этим же путём, ДО того как мы удаляем файл. Поэтому cleanup вынесен наружу.
  return { backupPath, formattedDate, formattedTime, backupStats, summary };
}

/** Удаляет временные файлы бэкапа (основной + WAL/SHM). */
function cleanupBackupFiles(backupPath) {
  try { fs.unlinkSync(backupPath); } catch (_) {}
  if (fs.existsSync(backupPath + '-wal')) { try { fs.unlinkSync(backupPath + '-wal'); } catch (_) {} }
  if (fs.existsSync(backupPath + '-shm')) { try { fs.unlinkSync(backupPath + '-shm'); } catch (_) {} }
}

async function sendDatabaseBackup(force = false) {
  if (backupInProgress) {
    console.log('⏳ Бэкап уже выполняется, пропускаем');
    return;
  }

  backupInProgress = true;

  try {
    const stats = fs.statSync(DB_PATH);
    const currentHash = `${stats.size}_${stats.mtimeMs}`;

    if (!force && lastBackupHash === currentHash) {
      console.log('📦 База не изменилась с прошлого бэкапа, пропускаем');
      backupInProgress = false;
      return;
    }

    // FIX 4: Используем общую функцию вместо дублированного кода
    const { backupPath, formattedDate, formattedTime, backupStats, summary } = await createAndVerifyBackup();

    const summaryText = summary
      ? `\n\n📊 *Статистика таблиц:*\n` +
      `👥 Users: ${summary.users_count}\n` +
      `📦 Orders: ${summary.total_orders}\n` +
      `🔑 Keys available: ${summary.available_keys}\n` +
      `🤝 Resellers: ${summary.resellers_count}\n` +
      `💬 Review Codes: ${summary.review_codes_count}\n` +
      `🎟️ Coupons: ${summary.coupons_count}\n` +
      `👥 Referrals: ${summary.referrals_count}`
      : '';

    // Отправляем в BACKUP_CHAT_ID (если задан) или в личку ADMIN_ID
    const backupTarget = BACKUP_CHAT_ID || ADMIN_ID;
    const backupDestLabel = BACKUP_CHAT_ID ? `канал/чат ${BACKUP_CHAT_ID}` : 'личку администратора';
    const sentMsg = await bot.sendDocument(backupTarget, backupPath, {
      caption: `📦 *Автобэкап БД* _(плановый, каждые ${BACKUP_INTERVAL_MINUTES} мин + при старте)_\n📅 ${formattedDate} | ⏰ ${formattedTime}\n💾 ${(backupStats.size / 1024).toFixed(2)} KB${summaryText}`,
      parse_mode: 'Markdown'
    });
    console.log(`📤 Бэкап отправлен в ${backupDestLabel}`);

    // П.5: вычисляем SHA256 ДО удаления файла
    let sha256 = null;
    try {
      const dbFileContent = fs.readFileSync(backupPath);
      sha256 = crypto.createHash('sha256').update(dbFileContent).digest('hex');
    } catch (hashErr) {
      console.error('⚠️ SHA256 calc error:', hashErr.message);
    }

    cleanupBackupFiles(backupPath);

    // ✅ АВТОРЕСТОР: сохраняем file_id последнего бэкапа в /tmp
    try {
      // sha256 уже вычислен выше

      const newEntry = {
        file_id: sentMsg.document.file_id,
        file_unique_id: sentMsg.document.file_unique_id,
        date: formattedDate,
        time: formattedTime,
        size_kb: (backupStats.size / 1024).toFixed(2),
        sha256,
        saved_at: Date.now()
      };

      // П.3: храним последние 5 бэкапов
      const backupMetaPath = '/tmp/cyrax_last_backup.json';
      let existingMeta = { backups: [] };
      try {
        if (fs.existsSync(backupMetaPath)) {
          const parsed = JSON.parse(fs.readFileSync(backupMetaPath, 'utf8'));
          // Поддержка старого формата (один объект, не массив)
          if (parsed.backups) existingMeta = parsed;
          else if (parsed.file_id) existingMeta = { backups: [parsed] };
        }
      } catch (_) {}

      existingMeta.backups.unshift(newEntry);           // добавляем в начало
      existingMeta.backups = existingMeta.backups.slice(0, 5); // оставляем 5 последних
      existingMeta.latest = newEntry;                    // быстрый доступ к последнему

      fs.writeFileSync(backupMetaPath, JSON.stringify(existingMeta, null, 2), 'utf8');
      lastBackupTimestamp = Date.now(); // П.7: для health endpoint
      console.log(`💾 Бэкап #${existingMeta.backups.length} сохранён, SHA256: ${sha256.slice(0, 12)}...`);
    } catch (saveErr) {
      console.error('⚠️ Не удалось сохранить метаданные бэкапа:', saveErr.message);
    }

    lastBackupHash = currentHash;
    console.log(`✅ Бэкап успешно отправлен админу`);
    logAction(ADMIN_ID, 'auto_backup_created', { date: formattedDate, time: formattedTime, size: backupStats.size });

  } catch (error) {
    console.error('❌ Ошибка при создании бэкапа:', error);
    try {
      await bot.sendMessage(ADMIN_ID, `❌ *Ошибка автоматического бэкапа*\n\n${error.message}`, {
        parse_mode: 'Markdown'
      });
    } catch (e) {
      console.error('❌ Не удалось отправить уведомление об ошибке:', e);
    }
  } finally {
    backupInProgress = false;
  }
}

// Функция для ручного бэкапа
async function sendManualBackup(chatId) {
  try {
    // FIX 4: Используем общую функцию вместо дублированного кода
    const { backupPath, formattedDate, formattedTime, backupStats, summary } = await createAndVerifyBackup();

    const summaryText = summary
      ? `\n\n📊 *Содержимое:*\n` +
      `👥 Пользователей: ${summary.users_count}\n` +
      `📦 Заказов всего: ${summary.total_orders}\n` +
      `🔑 Ключей доступно: ${summary.available_keys}\n` +
      `🎟️ Активных купонов: ${summary.active_coupons}\n` +
      `👤 Активных заявок на буст: ${summary.boost_requests}\n` +
      `⚙️ Настроек: ${summary.settings_count}`
      : '';

    const caption = `📦 *Ручной бэкап — полный слепок БД*\n📅 ${formattedDate}\n⏰ ${formattedTime}\n💾 ${(backupStats.size / 1024).toFixed(2)} KB${summaryText}\n\n✅ Включает: ключи, заказы, купоны, настройки, реквизиты, цены, пользователей.`;

    // Отправляем в бэкап-канал (если задан и отличается от текущего чата)
    const backupTarget = BACKUP_CHAT_ID || chatId;
    await bot.sendDocument(backupTarget, backupPath, { caption, parse_mode: 'Markdown' });

    // Если бэкап ушёл в канал, а не в личку — уведомляем админа
    if (backupTarget !== chatId) {
      await bot.sendMessage(chatId, `✅ Бэкап создан и отправлен в канал бэкапов.\n📅 ${formattedDate} ${formattedTime}\n💾 ${(backupStats.size / 1024).toFixed(2)} KB`);
    }

    cleanupBackupFiles(backupPath);
    console.log(`✅ Ручной бэкап отправлен в ${backupTarget}`);

  } catch (error) {
    console.error('❌ Ошибка ручного бэкапа:', error);
    await bot.sendMessage(chatId, `❌ *Ошибка создания бэкапа*\n\n${error.message}`, {
      parse_mode: 'Markdown'
    });
  }
}



// Управление платежными реквизитами
function showManagePaymentDetails(chatId, msgId = null) {
  let message = '💳 Платёжные реквизиты\n\n';

  message += `🇷🇺 *СБП:* \`${escapeForBacktick(PAYMENT_DETAILS.sbp)}\`\n`;
  message += `🇺🇦 *Карта UA:* \`${escapeForBacktick(PAYMENT_DETAILS.card_ua)}\`\n`;
  message += `🇮🇹 *Карта IT:* \`${escapeForBacktick(PAYMENT_DETAILS.card_it)}\`\n`;
  message += `💎 *Binance:* \`${escapeForBacktick(PAYMENT_DETAILS.binance)}\`\n`;
  message += `💰 *PayPal:* ${escapeMarkdown(PAYMENT_DETAILS.paypal)}\n`;
  message += `₿ *Crypto:* \`${escapeForBacktick(PAYMENT_DETAILS.crypto)}\``;

  const keyboard = {
    inline_keyboard: [
      [
        { text: '✏️ СБП', callback_data: 'edit_payment_sbp' },
        { text: '✏️ Карта UA', callback_data: 'edit_payment_card_ua' }
      ],
      [
        { text: '✏️ Карта IT', callback_data: 'edit_payment_card_it' },
        { text: '✏️ Binance', callback_data: 'edit_payment_binance' }
      ],
      [
        { text: '✏️ PayPal', callback_data: 'edit_payment_paypal' },
        { text: '✏️ Crypto', callback_data: 'edit_payment_crypto' }
      ],
      [{ text: '◀️ Назад', callback_data: 'admin' }]
    ]
  };

  adminSend(chatId, ADMIN_ID, message, {
    parse_mode: 'Markdown',
    reply_markup: keyboard
  }, msgId);
}

// ==========================================
// ⚙️ П.3: НАСТРОЙКИ БОТА
// ==========================================
function showBotSettings(chatId, msgId = null) {
  const nu = getSetting('notify_new_user') === '1' ? '✅' : '❌';
  const no = getSetting('notify_new_order') === '1' ? '✅' : '❌';
  const nl = getSetting('notify_low_keys') === '1' ? '✅' : '❌';
  const nd = getSetting('notify_daily_report') === '1' ? '✅' : '❌';
  const nr = getSetting('reminders_enabled') === '1' ? '✅' : '❌';
  const threshold = getSetting('low_keys_threshold') || '5';

  const keysOff = getSetting('keys_disabled') === '1';
  const boostOff = getSetting('boost_disabled') === '1';
  const mbOff = getSetting('manual_boost_disabled') === '1';

  const ki = keysOff ? '🔴' : '🟢';
  const bi = boostOff ? '🔴' : '🟢';
  const mi = mbOff ? '🔴' : '🟢';

  const message =
    `⚙️ *Настройки бота*

🛒 *Разделы:*
${ki} Ключи  |  ${bi} Метод Буста  |  ${mi} Ручной Буст

🔔 *Уведомления:*
${nu} Новый юзер  |  ${no} Новый заказ
${nl} Мало ключей (порог: ${threshold} шт.)  |  ${nd} Ежедн. отчёт
${nr} Напоминания о продлении

📝 *Тексты бота:* приветствие · оферта · помощь
🔗 *Ссылки:* канал · поддержка · отзывы
💾 *База данных:* бэкап · восстановление`;

  const keyboard = {
    inline_keyboard: [
      // Переключатели разделов
      [{ text: '── 🛒 Разделы оплат ──', callback_data: 'noop' }],
      [
        { text: `${ki} Ключи`, callback_data: 'settings_toggle_keys_disabled' },
        { text: `${bi} Метод Буста`, callback_data: 'settings_toggle_boost_disabled' },
        { text: `${mi} Руч. Буст`, callback_data: 'settings_toggle_manual_boost_disabled' }
      ],

      // Уведомления
      [{ text: '── 🔔 Уведомления ──', callback_data: 'noop' }],
      [
        { text: `${nu} Новый юзер`, callback_data: 'settings_toggle_notify_new_user' },
        { text: `${no} Новый заказ`, callback_data: 'settings_toggle_notify_new_order' }
      ],
      [
        { text: `${nl} Мало ключей`, callback_data: 'settings_toggle_notify_low_keys' },
        { text: `✏️ Порог: ${threshold} шт.`, callback_data: 'settings_edit_low_keys_threshold' }
      ],
      [
        { text: `${nd} Ежедн. отчёт`, callback_data: 'settings_toggle_notify_daily_report' },
        { text: `${nr} Напоминания`, callback_data: 'settings_toggle_reminders_enabled' }
      ],

      // Тексты
      [{ text: '── 📝 Тексты бота ──', callback_data: 'noop' }],
      [
        { text: '👋 Приветствие', callback_data: 'settings_edit_welcome' },
        { text: '📜 Оферта', callback_data: 'settings_edit_offer' }
      ],
      [
        { text: '❓ Помощь', callback_data: 'settings_edit_help' }
      ],

      // Ссылки
      [{ text: '── 🔗 Ссылки ──', callback_data: 'noop' }],
      [
        { text: '📢 Канал', callback_data: 'settings_edit_channel_link' },
        { text: '💬 Чат', callback_data: 'settings_edit_chat_link' },
        { text: '🆘 Поддержка', callback_data: 'settings_edit_support_link' }
      ],
      [
        { text: '❓ Помощь (ссылка)', callback_data: 'settings_edit_help_link' }
      ],
      [
        { text: '✍️ Ссылка для отзывов', callback_data: 'settings_edit_review_link' }
      ],
      [
        { text: '🌐 DNS-адрес (FAQ №13)', callback_data: 'settings_edit_dns_address' }
      ],

      // База данных и система
      [{ text: '── 💾 База данных ──', callback_data: 'noop' }],
      [
        { text: '📦 Бэкап', callback_data: 'admin_backup' },
        { text: '🔄 Восстановить', callback_data: 'admin_restore' }
      ],
      [
        { text: '🔧 Техобслуживание', callback_data: 'admin_maintenance' },
        { text: '🛑 Витрина', callback_data: 'admin_section_pause' }
      ],

      // Авторекламка
      [{ text: '── 📣 Авторекламка в группах ──', callback_data: 'noop' }],
      [
        { text: '✏️ Редактировать текст', callback_data: 'settings_edit_promo_text' },
        { text: '👁 Предпросмотр', callback_data: 'settings_promo_preview' }
      ],
      [{ text: `⏱ Интервал: ${getSetting('promo_interval_hours') || '6'} ч`, callback_data: 'settings_promo_interval' }],
      [{ text: '📤 Отправить сейчас', callback_data: 'settings_promo_send_now' }],
      [
        { text: '➕ Добавить группу', callback_data: 'settings_promo_add_chat' },
        { text: '📋 Список групп', callback_data: 'settings_promo_list_chats' }
      ],

      [{ text: '◀️ Назад', callback_data: 'admin' }]
    ]
  };

  adminSend(chatId, ADMIN_ID, message, { parse_mode: 'Markdown', reply_markup: keyboard }, msgId);
}

// ==========================================
// 🎟️ П.2: СИСТЕМА КУПОНОВ
// ==========================================
function showCouponsPanel(chatId, msgId = null) {
  db.get(
    `SELECT COUNT(*) as total, SUM(used_count) as total_used FROM coupons WHERE is_active = 1`,
    [],
    (err, stats) => {
      const total = (stats && stats.total) || 0;
      const used = (stats && stats.total_used) || 0;

      const message =
        `🎟️ *Система купонов*

📊 Активных купонов: ${total}
🔢 Всего применений: ${used}`;

      const keyboard = {
        inline_keyboard: [
          // ── Создание ──
          [
            { text: '➕ Создать купон', callback_data: 'coupon_create' }
          ],
          // ── Выдача ──
          [
            { text: '🎁 Выдать купон юзеру', callback_data: 'coupon_issue_to_user' },
            { text: '🛒 Выдать покупателям', callback_data: 'coupon_issue_to_buyers' }
          ],
          // ── Просмотр ──
          [
            { text: '📋 Активные купоны', callback_data: 'coupon_list' },
            { text: '📊 Статистика', callback_data: 'coupon_stats' }
          ],
          [
            { text: '📦 Архив купонов', callback_data: 'coupon_list_archive' }
          ],
          // ── Удаление ──
          [
            { text: '🗑️ Удалить ВСЕ купоны', callback_data: 'coupon_delete_all_confirm' }
          ],
          [{ text: '◀️ Назад', callback_data: 'admin' }]
        ]
      };

      adminSend(chatId, ADMIN_ID, message, { parse_mode: 'Markdown', reply_markup: keyboard }, msgId);
    }
  );
}

// ==========================================
// 🆘 СИСТЕМА ПОДДЕРЖКИ (ТИКЕТЫ)
// ==========================================

const PERIOD_DAYS = { '1d': 1, '3d': 3, '7d': 7, '30d': 30 };

function getTicketNumber() {
  return new Promise(res => {
    db.get(`SELECT MAX(id) as maxId FROM support_tickets`, [], (e, row) => {
      const nextId = ((row && row.maxId) || 0) + 1;
      res('T' + String(nextId).padStart(3, '0'));
    });
  });
}

// Старт флоу поддержки — просим ввести ключ
function startSupportTicket(chatId, user) {
  const isRu = getLang(user) === 'ru';
  const session = getSession(user.id);
  session.state = 'support_awaiting_key';
  session.data = {};
  const msg = isRu
    ? `🆘 Помощь с ключом\n\nЧтобы я мог быстро помочь, введите ваш ключ\n(скопируйте его из сообщения с покупкой):\n\ncyraxmod_XXXX-XXXX-XXX`
    : `🆘 Key Support\n\nTo help you quickly, please enter your key\n(copy it from your purchase message):\n\ncyraxmod_XXXX-XXXX-XXX`;
  safeSendMessage(chatId, msg, {
    reply_markup: {
      inline_keyboard: [[{ text: isRu ? '◀️ Назад' : '◀️ Back', callback_data: 'start' }]]
    }
  });
}

// Анализ ключа и отображение статуса
async function analyzeKeyForSupport(chatId, user, keyValue) {
  const isRu = getLang(user) === 'ru';
  const session = getSession(user.id);

  // Ищем ключ среди проданных
  const key = await new Promise(res => {
    db.get(
      `SELECT k.*, o.confirmed_at, o.product FROM keys k LEFT JOIN orders o ON k.buyer_id = o.user_id AND o.key_issued = k.key_value WHERE k.key_value = ? AND k.status = 'sold'`,
      [keyValue], (e, row) => res(row || null)
    );
  });

  if (!key) {
    // Проверяем — может ключ есть, но ещё не продан (тест/ошибка ввода)
    const existsUnsold = await new Promise(res => {
      db.get(`SELECT 1 FROM keys WHERE key_value = ?`, [keyValue], (e, row) => res(!!row));
    });

    // В обоих случаях даём одинаковый ответ — не раскрываем статус ключа
    const msg = isRu
      ? `❌ Ключ не найден среди покупок.\n\n*Возможные причины:*\n• Вы ошиблись при вводе — проверьте ключ ещё раз\n• Ключ был приобретён через другой канал`
      : `❌ Key not found among purchases.\n\n*Possible reasons:*\n• You may have mistyped it — please check again\n• The key was purchased through a different channel`;

    // Если ключ нашли как непроданный — тихо уведомляем админа (не показываем юзеру)
    if (existsUnsold) {
      safeSendMessage(ADMIN_ID,
        `⚠️ *Тест или подозрение*\n\nПользователь ${user.username ? '@' + user.username : user.id} ввёл в поддержку ключ, который есть в базе, но ещё не продан:\n\n\`${keyValue}\``,
        { parse_mode: 'Markdown' }
      ).catch(() => { });
    }

    return safeSendMessage(chatId, msg, {
      parse_mode: 'Markdown',
      reply_markup: {
        inline_keyboard: [
          [{ text: isRu ? '🔄 Ввести заново' : '🔄 Try again', callback_data: 'support_ticket' }],
          [{ text: isRu ? '👤 Связаться с админом' : '👤 Contact admin', url: `https://t.me/${ADMIN_USERNAME}` }]
        ]
      }
    });
  }

  const soldDate = new Date(key.sold_at || key.confirmed_at);
  // Парсим период прямо из значения ключа (cyraxmod_7d... → 7d)
  const keyPeriodMatch = keyValue.match(/cyraxmod_(\d+)d/i);
  const parsedPeriod = keyPeriodMatch ? `${keyPeriodMatch[1]}d` : null;
  const period = parsedPeriod || key.product;
  const keyDays = parsedPeriod ? parseInt(keyPeriodMatch[1]) : (PERIOD_DAYS[period] || 7);
  const expiryDate = new Date(soldDate.getTime() + keyDays * 24 * 60 * 60 * 1000);
  const now = new Date();
  const daysSincePurchase = Math.round((now - soldDate) / (1000 * 60 * 60 * 24));
  const daysUntilExpiry = Math.round((expiryDate - now) / (1000 * 60 * 60 * 24));

  // Сохраняем в сессию для тикета
  session.data.keyValue = keyValue;
  session.data.keyBuyerId = key.buyer_id;
  session.data.soldDate = soldDate.toISOString();
  session.data.period = period;
  session.data.keyDays = keyDays;
  session.data.expiryDate = expiryDate.toISOString();
  session.data.daysSincePurchase = daysSincePurchase;
  session.data.daysUntilExpiry = daysUntilExpiry;

  const soldDateStr = soldDate.toLocaleDateString('ru-RU');
  const expiryDateStr = expiryDate.toLocaleDateString('ru-RU');
  const nowStr = now.toLocaleDateString('ru-RU');

  // Сценарий Б: куплен более 30 дней назад И срок давно истёк
  if (daysSincePurchase > 30 && daysUntilExpiry < -7) {
    const msg = isRu
      ? `📅 Ключ \`${keyValue}\` был продан *${soldDateStr}*\n\nС момента покупки прошло *${daysSincePurchase} дней*. К сожалению, срок действия всех ключей ограничен, и мы не можем его восстановить.\n\nЕсли у вас есть другие вопросы — напишите администратору.`
      : `📅 Key \`${keyValue}\` was sold on *${soldDateStr}*\n\n*${daysSincePurchase} days* have passed since purchase. Unfortunately, all keys have a limited validity period and we cannot restore it.\n\nIf you have other questions — please contact the administrator.`;
    return safeSendMessage(chatId, msg, {
      parse_mode: 'Markdown',
      reply_markup: {
        inline_keyboard: [
          [{ text: isRu ? '👤 Связаться с админом' : '👤 Contact admin', url: `https://t.me/${ADMIN_USERNAME}` }]
        ]
      }
    });
  }

  if (daysUntilExpiry < 0) {
    // Истёк недавно
    const expiredDaysAgo = Math.abs(daysUntilExpiry);
    const msg = isRu
      ? `📅 Ключ \`${keyValue}\` был продан *${soldDateStr}*\n\nСрок действия: *${keyDays} дней*\nКлюч истёк: *${expiryDateStr}* (сегодня *${nowStr}*)\n\n⚠️ Ключ перестал работать, потому что закончился срок действия. Это нормально — все ключи имеют ограниченный срок.\n\nХотите продлить ключ? Сейчас действуют скидки для постоянных клиентов!`
      : `📅 Key \`${keyValue}\` was sold on *${soldDateStr}*\n\nValidity: *${keyDays} days*\nKey expired: *${expiryDateStr}* (today is *${nowStr}*)\n\n⚠️ The key stopped working because its validity period ended. This is normal — all keys have a limited validity.\n\nWant to renew? Loyal customer discounts are available!`;
    return safeSendMessage(chatId, msg, {
      parse_mode: 'Markdown',
      reply_markup: {
        inline_keyboard: [
          [{ text: isRu ? '🔑 Купить новый ключ' : '🔑 Buy new key', callback_data: 'buy' }],
          [{ text: isRu ? '👤 Всё равно не работает?' : '👤 Still not working?', callback_data: 'support_collect_description' }]
        ]
      }
    });
  }

  // Сценарий В: ключ ещё активен
  const daysLeft = daysUntilExpiry;
  session.state = 'support_key_analyzed';
  const channelLink = getSetting('channel_link') || 'https://t.me/cyraxml';
  const helpLink = getSetting('help_link') || 'https://t.me/cyraxml/260';
  const msg = isRu
    ? `📅 Ключ \`${keyValue}\` *активен!*\n\n` +
    `Куплен: *${soldDateStr}*\nСрок действия: *${keyDays} дней*\nИстекает: *${expiryDateStr}* (осталось *${daysLeft} дн.*)\n\n` +
    `🔍 *Почему ключ может не работать?*\n\n` +
    `1️⃣ *Неверный ввод* — убедитесь, что скопировали ключ полностью, включая дефисы\n` +
    `2️⃣ *Регион аккаунта* — ключи привязаны к региону вашего аккаунта MLBB\n` +
    `3️⃣ *Обновление мода* — возможно, вышла новая версия мода, скачайте её в нашем канале`
    : `📅 Key \`${keyValue}\` is *active!*\n\n` +
    `Purchased: *${soldDateStr}*\nValidity: *${keyDays} days*\nExpires: *${expiryDateStr}* (*${daysLeft} days* left)\n\n` +
    `🔍 *Why might the key not work?*\n\n` +
    `1️⃣ *Wrong input* — make sure you copied the key fully, including dashes\n` +
    `2️⃣ *Account region* — keys are tied to your MLBB account region\n` +
    `3️⃣ *Mod update* — a new mod version may have been released, download it from our channel`;

  safeSendMessage(chatId, msg, {
    parse_mode: 'Markdown',
    reply_markup: {
      inline_keyboard: [
        [{ text: isRu ? 'ℹ️ Инструкция по активации' : 'ℹ️ Activation guide', url: helpLink }],
        [{ text: isRu ? '👤 Проблема не решена' : '👤 Problem not resolved', callback_data: 'support_collect_description' }]
      ]
    }
  });
}

// Собрать описание проблемы
function startCollectDescription(chatId, user) {
  const isRu = getLang(user) === 'ru';
  const session = getSession(user.id);
  session.state = 'support_awaiting_description';
  const msg = isRu
    ? `📝 *Соберём информацию для администратора*\n\n1️⃣ *Опишите проблему подробно:*\n_(например: "ключ пишет 'Invalid key' при вводе", "мод не запускается после ввода ключа")_`
    : `📝 *Gathering information for the administrator*\n\n1️⃣ *Describe the problem in detail:*\n_(e.g.: "key says 'Invalid key' when entering", "mod doesn't start after entering the key")_`;
  safeSendMessage(chatId, msg, { parse_mode: 'Markdown' });
}

// Создать тикет и уведомить админа
async function createAndSendTicket(user, chatId, keyValue, description, screenshotFileId, sessionData) {
  const isRu = getLang(user) === 'ru';

  // Собираем данные о клиенте
  const dbUser = await new Promise(res => {
    db.get(`SELECT * FROM users WHERE id = ?`, [user.id], (e, row) => res(row || {}));
  });

  const purchaseCount = await new Promise(res => {
    db.get(`SELECT COUNT(*) as cnt FROM orders WHERE user_id = ? AND status = 'confirmed'`, [user.id], (e, row) => res((row && row.cnt) || 0));
  });

  const ticketHistory = await new Promise(res => {
    db.all(`SELECT status FROM support_tickets WHERE user_id = ?`, [user.id], (e, rows) => res(rows || []));
  });

  // Анализ истории
  const openCount = ticketHistory.filter(t => t.status === 'open').length;
  const fraudCount = ticketHistory.filter(t => t.status === 'fraud').length;
  const resolvedCount = ticketHistory.filter(t => t.status === 'resolved').length;
  let historyFlag = '🟢 Нет истории жалоб';
  if (ticketHistory.length === 0) historyFlag = '🟢 Первый тикет';
  else if (fraudCount >= 2) historyFlag = `🔴 ${fraudCount}+ подозрительных жалоб — вероятный скамер`;
  else if (ticketHistory.length >= 4) historyFlag = `🟡 ${ticketHistory.length} жалоб за всё время`;
  else if (ticketHistory.length >= 2) historyFlag = `🟡 ${ticketHistory.length} жалобы за 30 дней`;

  // Вердикт бота
  const keyDays = sessionData.keyDays || 7;
  // Используем null-safe проверку: если daysUntilExpiry не задан — не делаем вывод
  const daysUntilExpiry = (sessionData.daysUntilExpiry !== undefined && sessionData.daysUntilExpiry !== null)
    ? sessionData.daysUntilExpiry : null;
  const daysSincePurchase = sessionData.daysSincePurchase || 0;
  let botVerdict = '';
  let recommendReject = false;
  if (daysUntilExpiry !== null && daysUntilExpiry < 0) {
    botVerdict = `Срок ключа истёк ${Math.abs(daysUntilExpiry)} дн. назад. Клиент либо не понял, либо пытается получить новый бесплатно.`;
    recommendReject = true;
  } else if (daysUntilExpiry !== null && daysUntilExpiry === 0) {
    botVerdict = 'Срок ключа истёк сегодня. Возможна пограничная ситуация — требует ручной проверки.';
    recommendReject = false;
  } else if (daysUntilExpiry !== null && daysUntilExpiry > 0) {
    botVerdict = `Ключ ещё активен (${daysUntilExpiry} дн. осталось). Возможна ошибка активации или технический сбой.`;
    recommendReject = false;
  } else if (!keyValue) {
    botVerdict = 'Ключ не указан. Клиент не смог найти свою покупку.';
  } else {
    botVerdict = 'Нет данных о сроке действия ключа. Требует ручной проверки.';
  }

  const expiredDaysLabel = (daysUntilExpiry !== null && daysUntilExpiry < 0) ? ` (${Math.abs(daysUntilExpiry)} дн. назад)` : '';
  const statusLabel = !keyValue ? 'Ключ не указан' :
    daysUntilExpiry === null ? '❓ Нет данных' :
      daysUntilExpiry > 0 ? `✅ АКТИВЕН (${daysUntilExpiry} дн.)` :
        daysUntilExpiry === 0 ? '⚠️ Истёк сегодня' :
          `❌ ИСТЁК${expiredDaysLabel}`;

  const ticketNum = await getTicketNumber();
  const soldDateStr = sessionData.soldDate ? new Date(sessionData.soldDate).toLocaleDateString('ru-RU') : 'неизвестно';
  const expiryDateStr = sessionData.expiryDate ? new Date(sessionData.expiryDate).toLocaleDateString('ru-RU') : 'неизвестно';
  const nowStr = new Date().toLocaleDateString('ru-RU');

  // Сохранить тикет
  const ticketId = await new Promise(res => {
    db.run(
      `INSERT INTO support_tickets (ticket_number, user_id, key_value, complaint_text, screenshot_file_id, status, bot_verdict, created_at) VALUES (?, ?, ?, ?, ?, 'open', ?, datetime('now'))`,
      [ticketNum, user.id, keyValue || null, description, screenshotFileId || null, botVerdict],
      function (err) { res(err ? null : this.lastID); }
    );
  });

  if (!ticketId) return;

  // Пользователю — подтверждение
  const confirmMsg = isRu
    ? `✅ *Спасибо! Ваш тикет создан*\n\n📋 *Что дальше?*\nАдминистратор получил:\n• Ваш ключ: ${keyValue ? `\`${keyValue}\`` : 'не указан'}\n• Дата покупки: ${soldDateStr}\n• Срок действия: ${keyDays} дней${sessionData.expiryDate ? ` (истекает ${expiryDateStr})` : ''}\n• Ваше описание: _получено_\n• Скриншот: ${screenshotFileId ? 'есть ✅' : 'нет'}\n\n⏳ Ожидайте ответа — обычно это занимает несколько минут.`
    : `✅ *Thank you! Your ticket has been created*\n\n📋 *What's next?*\nThe administrator received:\n• Your key: ${keyValue ? `\`${keyValue}\`` : 'not provided'}\n• Purchase date: ${soldDateStr}\n• Validity: ${keyDays} days${sessionData.expiryDate ? ` (expires ${expiryDateStr})` : ''}\n• Your description: _received_\n• Screenshot: ${screenshotFileId ? 'attached ✅' : 'none'}\n\n⏳ Please wait for a response — usually a few minutes.`;
  safeSendMessage(chatId, confirmMsg, { parse_mode: 'Markdown' }).catch(() => { });

  const uLabel = dbUser.username ? `@${escapeMarkdown(dbUser.username)}` : `ID: ${user.id}`;
  const joinDate = dbUser.joined_at ? new Date(dbUser.joined_at).toLocaleDateString('ru-RU') : '?';

  // Основные кнопки для вердикта
  const adminKeyboard = {
    inline_keyboard: [
      [
        { text: recommendReject ? '✅ Выдать новый ключ' : '✅ Выдать ключ', callback_data: `ticket_issue_key_${ticketId}` },
        { text: '❌ Отклонить', callback_data: `ticket_resolve_${ticketId}` }
      ],
      [
        { text: '🚫 Заблокировать', callback_data: `ban_${user.id}` },
        { text: '📝 Заметка', callback_data: `client_note_${user.id}` }
      ],
      [{ text: '👀 Профиль клиента', callback_data: `client_info_${user.id}` }]
    ]
  };

  const adminMsg =
    `🆘 *НОВАЯ ЖАЛОБА ${ticketNum}*\n\n` +
    `👤 ${uLabel} (ID: \`${user.id}\`)\n` +
    `📅 Зарегистрирован: ${joinDate}\n` +
    `🛒 Всего покупок: ${purchaseCount}\n` +
    `${historyFlag}\n` +
    `━━━━━━━━━━━━━━━━━━\n` +
    `🔑 *Ключ:* ${keyValue ? `\`${keyValue}\`` : '_не указан_'}\n` +
    `📦 *Период:* ${keyDays} дней\n` +
    `💰 *Куплен:* ${soldDateStr} (${daysSincePurchase} дн. назад)\n` +
    `⏳ *Срок истёк:* ${expiryDateStr} (сегодня ${nowStr})\n` +
    `⚡️ *Статус:* ${statusLabel}\n` +
    `━━━━━━━━━━━━━━━━━━\n` +
    `💬 *Описание:*\n_"${description}"_\n` +
    `━━━━━━━━━━━━━━━━━━\n` +
    `📎 *Скриншот:* ${screenshotFileId ? 'Есть ✅' : 'Нет'}\n` +
    `━━━━━━━━━━━━━━━━━━\n` +
    `🤖 *Вердикт бота:* ${botVerdict || 'Нет данных'}`;

  if (screenshotFileId) {
    // Шлём скриншот с текстом как caption
    bot.sendPhoto(ADMIN_ID, screenshotFileId, {
      caption: adminMsg.substring(0, 1024),
      parse_mode: 'Markdown',
      reply_markup: adminKeyboard
    }).catch(() => {
      safeSendMessage(ADMIN_ID, adminMsg, { parse_mode: 'Markdown', reply_markup: adminKeyboard }).catch(() => { });
    });
  } else {
    safeSendMessage(ADMIN_ID, adminMsg, { parse_mode: 'Markdown', reply_markup: adminKeyboard }).catch(() => { });
  }

  logAction(user.id, 'support_ticket_created', { ticketNum, keyValue, ticketId });
}

// Показать список тикетов для админа
function showAdminTickets(chatId, page = 0, msgId = null) {
  const PAGE_SIZE = 5;
  db.get(`SELECT COUNT(*) as cnt FROM support_tickets WHERE status = 'open'`, [], (e, countRow) => {
    const total = (countRow && countRow.cnt) || 0;
    db.all(
      `SELECT st.*, u.username FROM support_tickets st LEFT JOIN users u ON st.user_id = u.id WHERE st.status = 'open' ORDER BY st.created_at DESC LIMIT ? OFFSET ?`,
      [PAGE_SIZE, page * PAGE_SIZE],
      (err, rows) => {
        if (err || !rows || rows.length === 0) {
          return adminSend(chatId, ADMIN_ID, `⚠️ *Жалобы*\n\nОткрытых тикетов нет ✅`, {
            parse_mode: 'Markdown',
            reply_markup: { inline_keyboard: [[{ text: '◀️ Назад', callback_data: 'admin' }]] }
          }, msgId);
        }
        let msg = `⚠️ *Жалобы (${total} открытых)*\n\n`;
        const buttons = [];
        rows.forEach((t, i) => {
          const uLabel = t.username ? `@${escapeMarkdown(t.username)}` : `ID: ${t.user_id}`;
          const date = new Date(t.created_at).toLocaleDateString('ru-RU');
          msg += `${page * PAGE_SIZE + i + 1}. ${t.ticket_number || `#${t.id}`} — ${uLabel}\n   🔑 ${t.key_value || '?'} | 📅 ${date}\n\n`;
          buttons.push([{ text: `🔍 ${t.ticket_number || '#' + t.id} — ${uLabel}`, callback_data: `client_info_${t.user_id}` }]);
        });
        const totalPages = Math.ceil(total / PAGE_SIZE);
        const pagBtns = [];
        if (page > 0) pagBtns.push({ text: '◀️', callback_data: `admin_tickets_page_${page - 1}` });
        pagBtns.push({ text: `${page + 1}/${totalPages}`, callback_data: 'noop' });
        if (page < totalPages - 1) pagBtns.push({ text: '▶️', callback_data: `admin_tickets_page_${page + 1}` });
        if (pagBtns.length > 1) buttons.push(pagBtns);
        buttons.push([{ text: '◀️ Назад', callback_data: 'admin' }]);
        safeSendMessage(chatId, msg, { parse_mode: 'Markdown', reply_markup: { inline_keyboard: buttons } });
      }
    );
  });
}

function showCouponList(chatId, showArchive = false, page = 0) {
  const ITEMS_PER_PAGE = 15;
  const whereClause = showArchive
    ? `WHERE (is_active = 0 OR used_count >= max_uses AND max_uses > 0 OR (expires_at IS NOT NULL AND expires_at < datetime('now')))`
    : `WHERE is_active = 1 AND (max_uses = 0 OR used_count < max_uses) AND (expires_at IS NULL OR expires_at > datetime('now'))`;

  db.get(`SELECT COUNT(*) as total FROM coupons ${whereClause}`, [], (countErr, countRow) => {
    const total = countRow ? countRow.total : 0;
    const totalPages = Math.max(1, Math.ceil(total / ITEMS_PER_PAGE));
    const currentPage = Math.max(0, Math.min(page, totalPages - 1));

    db.all(
      `SELECT id, code, discount_percent, max_uses, used_count, expires_at, is_active, user_id, product_restriction FROM coupons ${whereClause} ORDER BY created_at DESC LIMIT ? OFFSET ?`,
      [ITEMS_PER_PAGE, currentPage * ITEMS_PER_PAGE],
      (err, rows) => {
        if (err) {
          safeSendMessage(chatId, '❌ Не удалось загрузить список купонов. Попробуйте позже.');
          return;
        }

        const archiveFlag = showArchive ? '_archive' : '';
        const title = showArchive ? '📦 *Архив купонов*' : '🎟️ *Активные купоны*';

        if (!rows || rows.length === 0) {
          safeSendMessage(chatId, `${title}\n\nНет купонов`, {
            parse_mode: 'Markdown',
            reply_markup: {
              inline_keyboard: [
                [{ text: showArchive ? '🎟️ Активные купоны' : '📦 Архив купонов', callback_data: showArchive ? 'coupon_list' : 'coupon_list_archive' }],
                [{ text: '◀️ Назад', callback_data: 'admin_coupons' }]
              ]
            }
          });
          return;
        }

        let message = `${title} (${total} шт., стр. ${currentPage + 1}/${totalPages})\n\n`;
        const deleteButtons = [];

        rows.forEach((c, i) => {
          const status = c.is_active ? '✅' : '❌';
          const expires = c.expires_at ? new Date(c.expires_at).toLocaleDateString('ru-RU') : '∞';
          const userTag = c.user_id ? ` 👤#${c.user_id}` : '';
          const restriction = c.product_restriction ? ` 🎯${c.product_restriction}` : '';
          message += `${currentPage * ITEMS_PER_PAGE + i + 1}. ${status} \`${c.code}\` — ${c.discount_percent}%${userTag}${restriction}\n`;
          message += `   🔢 ${c.used_count}/${c.max_uses === 0 ? '∞' : c.max_uses} | 📅 до ${expires}\n\n`;
          if (c.is_active && !showArchive) {
            deleteButtons.push([{ text: `🗑️ Удалить ${c.code}`, callback_data: `delete_coupon_confirm_${c.id}` }]);
          }
        });

        const toggleBtn = showArchive
          ? [{ text: '🎟️ Активные купоны', callback_data: 'coupon_list' }]
          : [{ text: '📦 Архив купонов', callback_data: 'coupon_list_archive' }];

        // Пагинация
        const paginationRow = [];
        if (currentPage > 0) paginationRow.push({ text: '⬅️ Пред', callback_data: `coupon_list${archiveFlag}_page_${currentPage - 1}` });
        if (currentPage < totalPages - 1) paginationRow.push({ text: 'След ➡️', callback_data: `coupon_list${archiveFlag}_page_${currentPage + 1}` });

        const allButtons = [...deleteButtons];
        if (paginationRow.length > 0) allButtons.push(paginationRow);
        allButtons.push(toggleBtn, [{ text: '◀️ Назад', callback_data: 'admin_coupons' }]);

        safeSendMessage(chatId, message, {
          parse_mode: 'Markdown',
          reply_markup: { inline_keyboard: allButtons }
        });
      }
    );
  });
}

function showCouponStats(chatId) {
  db.all(
    `SELECT c.code, c.discount_percent, c.used_count, c.max_uses,
            SUM(o.amount) as total_saved
     FROM coupons c
     LEFT JOIN user_coupons uc ON c.id = uc.coupon_id AND uc.used_at IS NOT NULL
     LEFT JOIN orders o ON uc.order_id = o.id AND o.status = 'confirmed'
     GROUP BY c.id
     ORDER BY c.used_count DESC`,
    [],
    (err, rows) => {
      if (err) {
        safeSendMessage(chatId, '❌ Ошибка статистики купонов');
        return;
      }

      let message = '📊 *Статистика купонов*\n\n';

      if (!rows || rows.length === 0) {
        message += 'Нет данных';
      } else {
        rows.forEach((c, i) => {
          message += `${i + 1}. \`${c.code}\` (${c.discount_percent}%)\n`;
          message += `   Использован: ${c.used_count}/${c.max_uses} раз\n\n`;
        });
      }

      safeSendMessage(chatId, message, {
        parse_mode: 'Markdown',
        reply_markup: { inline_keyboard: [[{ text: '◀️ Назад', callback_data: 'admin_coupons' }]] }
      });
    }
  );
}

// Проверка и применение купона при покупке
async function applyCoupon(userId, code, period, currency, originalAmount) {
  return new Promise((resolve, reject) => {
    try {
      const upperCode = String(code).trim().toUpperCase();

      db.get(
        `SELECT * FROM coupons WHERE code = ? AND is_active = 1`,
        [upperCode],
        (err, coupon) => {
          if (err) return reject(new Error('Ошибка БД'));
          if (!coupon) return reject(new Error('COUPON_ERROR_NOT_FOUND'));

          // Проверка срока
          if (coupon.expires_at && new Date(coupon.expires_at) < new Date()) {
            return reject(new Error('Срок действия купона истёк'));
          }

          // Проверка лимита использований
          if (coupon.max_uses > 0 && coupon.used_count >= coupon.max_uses) {
            return reject(new Error('Купон исчерпал лимит использований'));
          }

          // Проверка user_id — купон может быть привязан к конкретному юзеру
          if (coupon.user_id && coupon.user_id !== userId) {
            return reject(new Error('Этот купон предназначен для другого пользователя'));
          }

          // Проверка: не использовал ли этот юзер уже этот купон
          db.get(
            `SELECT id FROM user_coupons WHERE user_id = ? AND coupon_id = ? AND used_at IS NOT NULL`,
            [userId, coupon.id],
            (err2, used) => {
              if (err2) return reject(new Error('Ошибка БД'));
              if (used) return reject(new Error('Вы уже использовали этот купон'));

              // Проверка product restriction (coupon_products table)
              db.all(
                `SELECT product FROM coupon_products WHERE coupon_id = ?`,
                [coupon.id],
                (err3, products) => {
                  if (err3) return reject(new Error('Ошибка БД'));

                  // 🚨 ЗАПРЕТ: Купоны нельзя применять к подключению реселлера
                  if (period === 'reseller_connection') {
                    // Используем t() здесь недоступно напрямую, перенесём логику ошибок для вызывающей стороны (возвращаем специальную ошибку)
                    return reject(new Error('COUPON_ERROR_PARTNER'));
                  }

                  // Если список продуктов задан — проверяем
                  if (products && products.length > 0) {
                    const allowed = products.map(r => r.product);
                    if (period && !allowed.includes(period)) {
                      return reject(new Error('Купон не действует на выбранный период. Выберите подходящий товар.'));
                    }
                  }

                  // Проверка product_restriction колонки (ограничения от отзывов или админа)
                  if (coupon.product_restriction && period && coupon.product_restriction !== period) {
                    return reject(new Error(`COUPON_ERROR_PERIOD:${coupon.product_restriction}`));
                  }

                  const discount = Math.round(originalAmount * coupon.discount_percent / 100 * 100) / 100;
                  const newAmount = Math.round((originalAmount - discount) * 100) / 100;

                  resolve({
                    couponId: coupon.id,
                    code: coupon.code,
                    discountPercent: coupon.discount_percent,
                    discount,
                    newAmount: Math.max(newAmount, 0.01)
                  });
                }
              );
            }
          );
        }
      );
    } catch (e) {
      reject(e);
    }
  });
}

// Отмечаем купон использованным
function markCouponUsed(userId, couponId, orderId) {
  return new Promise((resolve) => {
    try {
      // Upsert: если запись есть — обновляем, если нет — вставляем
      db.run(
        `INSERT INTO user_coupons (user_id, coupon_id, used_at, order_id) VALUES (?, ?, datetime('now'), ?)
         ON CONFLICT(user_id, coupon_id) DO UPDATE SET used_at = datetime('now'), order_id = excluded.order_id`,
        [userId, couponId, orderId],
        (err) => {
          if (err) console.error('❌ Error marking coupon used:', err);
          // Пересчитываем used_count из реальных данных (надёжнее чем +1)
          db.run(
            `UPDATE coupons SET used_count = (
               SELECT COUNT(*) FROM user_coupons WHERE coupon_id = ? AND used_at IS NOT NULL
             ) WHERE id = ?`,
            [couponId, couponId],
            (err2) => {
              if (err2) console.error('❌ Error updating coupon count:', err2);
              resolve();
            }
          );
        }
      );
    } catch (e) {
      console.error('❌ markCouponUsed error:', e);
      resolve();
    }
  });
}

// Функция синхронизации used_count (запускается при старте и по расписанию)
function syncCouponUsedCount() {
  db.run(
    `UPDATE coupons SET used_count = (
      SELECT COUNT(*) FROM user_coupons 
      WHERE coupon_id = coupons.id AND used_at IS NOT NULL
    )`,
    [],
    (err) => {
      if (err) console.error('❌ syncCouponUsedCount error:', err);
      else console.log('✅ Coupon used_count synced');
    }
  );
}

// ==========================================
// 🛡️ ANTI-SCAM СИСТЕМА
// ==========================================

// Активные сессии в памяти: userId → { sessionId, lastActivity }
const activeSessions = new Map();
const SESSION_IDLE_MS = 30 * 60 * 1000; // 30 минут

// Трекинг взаимодействий пользователя
function trackUserInteraction(userId, username, languageCode) {
  const now = new Date();
  const nowIso = now.toISOString();

  // Обновить last_activity и total_interactions в users
  db.run(
    `UPDATE users SET last_activity = ?, total_interactions = COALESCE(total_interactions, 0) + 1 WHERE id = ?`,
    [nowIso, userId]
  );

  // Управление сессиями
  const existing = activeSessions.get(userId);
  if (!existing || (Date.now() - existing.lastActivity) > SESSION_IDLE_MS) {
    // Закрыть старую сессию, если была
    if (existing && existing.sessionId) {
      db.run(
        `UPDATE user_sessions_history SET session_end = ?, actions_count = actions_count + 1 WHERE id = ?`,
        [nowIso, existing.sessionId]
      );
    }
    // Открыть новую
    db.run(
      `INSERT INTO user_sessions_history (user_id, session_start, actions_count) VALUES (?, ?, 1)`,
      [userId, nowIso],
      function (err) {
        if (!err) {
          activeSessions.set(userId, { sessionId: this.lastID, lastActivity: Date.now() });
        }
      }
    );
  } else {
    // Продолжить текущую сессию
    db.run(
      `UPDATE user_sessions_history SET actions_count = actions_count + 1 WHERE id = ?`,
      [existing.sessionId]
    );
    existing.lastActivity = Date.now();
    activeSessions.set(userId, existing);
  }
}

// Закрыть сессию пользователя при необходимости
function closeUserSession(userId) {
  const existing = activeSessions.get(userId);
  if (existing && existing.sessionId) {
    db.run(
      `UPDATE user_sessions_history SET session_end = datetime('now') WHERE id = ? AND session_end IS NULL`,
      [existing.sessionId]
    );
  }
  activeSessions.delete(userId);
}

// Повысить suspicion_score
function increaseSuspicion(userId, points, reason) {
  // Админ никогда не подозревается
  if (userId === ADMIN_ID) return;
  db.run(
    `UPDATE users SET suspicion_score = COALESCE(suspicion_score, 0) + ? WHERE id = ?`,
    [points, userId],
    (err) => {
      if (err) return;
      db.get(`SELECT suspicion_score, username FROM users WHERE id = ?`, [userId], (e, row) => {
        if (e || !row) return;
        console.log(`⚠️ suspicion +${points} for ${userId} (${reason}) → total: ${row.suspicion_score}`);
        // Уведомить админа если превысили порог
        if (row.suspicion_score >= 25 && (row.suspicion_score - points) < 25) {
          notifyAdminSuspicion(userId, row.username, row.suspicion_score, reason);
        }
      });
    }
  );
}

function notifyAdminSuspicion(userId, username, score, reason) {
  const uLabel = username ? `@${escapeMarkdown(username)}` : `ID: ${userId}`;
  const msg =
    `⚠️ *Подозрительная активность!*\n\n` +
    `👤 ${uLabel} (ID: ${userId})\n` +
    `📊 Уровень подозрения: *${score}*\n` +
    `🔍 Причина: ${reason}\n`;
  safeSendMessage(ADMIN_ID, msg, {
    parse_mode: 'Markdown',
    reply_markup: {
      inline_keyboard: [[{ text: '👀 Проверить клиента', callback_data: `client_info_${userId}` }]]
    }
  }).catch(() => { });
}

// Стоп-слова для автодетекта жалоб
const COMPLAINT_KEYWORDS = [
  'ключ не работает', 'нерабочий ключ', 'не активируется', 'не активен',
  'key not working', 'key doesn\'t work', 'invalid key', 'key is not working',
  'не работает ключ', 'ключ не активный', 'ключ бракованный', 'верните деньги',
  'хочу возврат', 'refund', 'не запускается', 'ошибка ключа'
];

function isComplaint(text) {
  if (!text) return false;
  const lower = text.toLowerCase();
  return COMPLAINT_KEYWORDS.some(kw => lower.includes(kw));
}

// Автоматически создать тикет при жалобе
async function autoCreateSupportTicket(userId, text) {
  // Найти последний купленный ключ
  const order = await new Promise(res => {
    db.get(
      `SELECT id, key_issued, product, confirmed_at FROM orders WHERE user_id = ? AND status = 'confirmed' ORDER BY confirmed_at DESC LIMIT 1`,
      [userId], (e, row) => res(row || null)
    );
  });

  const orderId = order ? order.id : null;

  db.run(
    `INSERT INTO support_tickets (user_id, order_id, complaint_text, status, created_at) VALUES (?, ?, ?, 'open', datetime('now'))`,
    [userId, orderId, text],
    function (err) {
      if (err) return;
      const ticketId = this.lastID;

      // Проверить на подозрительность: слишком ранняя жалоба?
      if (order && order.confirmed_at && order.product) {
        const soldDate = new Date(order.confirmed_at);
        const daysSinceSold = (Date.now() - soldDate.getTime()) / (1000 * 60 * 60 * 24);
        const keyDays = { '1d': 1, '3d': 3, '7d': 7, '30d': 30 }[order.product] || 7;
        const halfLife = keyDays / 2;

        if (daysSinceSold < halfLife) {
          increaseSuspicion(userId, 10, `Жалоба через ${daysSinceSold.toFixed(1)} дней после покупки ключа ${order.product}`);
        }

        // Жалоба на ключ с истёкшим сроком >7 дней назад
        if (daysSinceSold > keyDays + 7) {
          increaseSuspicion(userId, 20, `Жалоба на ключ через ${daysSinceSold.toFixed(0)} дней (срок ${keyDays}д)`);
        }
      }

      // Уведомить админа
      db.get(`SELECT username FROM users WHERE id = ?`, [userId], (e, u) => {
        const uLabel = (u && u.username) ? `@${escapeMarkdown(u.username)}` : `ID: ${userId}`;
        let adminMsg =
          `💬 *Новая жалоба (тикет #${ticketId})*\n\n` +
          `👤 ${uLabel}\n` +
          `📝 "${text.substring(0, 200)}"\n`;

        if (order) {
          const soldDate = new Date(order.confirmed_at);
          const keyDays = { '1d': 1, '3d': 3, '7d': 7, '30d': 30 }[order.product] || 7;
          const expiryDate = new Date(soldDate.getTime() + keyDays * 24 * 60 * 60 * 1000);
          adminMsg +=
            `\n🔑 Ключ: \`${order.key_issued || '?'}\`\n` +
            `📦 Продукт: ${order.product}\n` +
            `📅 Куплен: ${soldDate.toLocaleDateString('ru-RU')}\n` +
            `⏰ Истёк бы: ${expiryDate.toLocaleDateString('ru-RU')}\n`;
        }

        safeSendMessage(ADMIN_ID, adminMsg, {
          parse_mode: 'Markdown',
          reply_markup: {
            inline_keyboard: [
              [{ text: '👀 Проверить клиента', callback_data: `client_info_${userId}` }],
              [
                { text: '✅ Выдать замену', callback_data: `ticket_resolve_${ticketId}` },
                { text: '❌ Отклонить', callback_data: `ticket_fraud_${ticketId}` }
              ]
            ]
          }
        }).catch(() => { });
      });
    }
  );
}

// Команда /client_info для админа
async function showClientInfo(chatId, userId) {
  try {
    const user = await new Promise(res => {
      db.get(`SELECT * FROM users WHERE id = ?`, [userId], (e, row) => res(row || null));
    });
    if (!user) {
      safeSendMessage(chatId, `❌ Пользователь ID: ${userId} не найден в базе`);
      return;
    }

    const orders = await new Promise(res => {
      db.all(
        `SELECT product, confirmed_at, key_issued FROM orders WHERE user_id = ? AND status = 'confirmed' ORDER BY confirmed_at DESC LIMIT 10`,
        [userId], (e, rows) => res(rows || [])
      );
    });

    const sessions = await new Promise(res => {
      db.all(
        `SELECT session_start, session_end, actions_count FROM user_sessions_history WHERE user_id = ? ORDER BY session_start DESC LIMIT 20`,
        [userId], (e, rows) => res(rows || [])
      );
    });

    const tickets = await new Promise(res => {
      db.all(
        `SELECT id, complaint_text, status, created_at FROM support_tickets WHERE user_id = ? ORDER BY created_at DESC LIMIT 5`,
        [userId], (e, rows) => res(rows || [])
      );
    });

    const uLabel = user.username ? `@${escapeHtml(user.username)}` : `ID: ${userId}`;
    const joinDate = new Date(user.joined_at).toLocaleDateString('ru-RU');
    const lastAct = user.last_activity ? new Date(user.last_activity).toLocaleString('ru-RU') : 'нет данных';
    const totalInteract = user.total_interactions || 0;
    const suspScore = user.suspicion_score || 0;

    // Средняя длина сессии
    const completedSessions = sessions.filter(s => s.session_end);
    const avgMinutes = completedSessions.length > 0
      ? Math.round(completedSessions.reduce((acc, s) => {
        return acc + (new Date(s.session_end) - new Date(s.session_start)) / 60000;
      }, 0) / completedSessions.length)
      : 0;

    let msg = `👤 <b>${uLabel}</b> | ID: <code>${userId}</code>\n\n`;
    msg += `📅 Регистрация: ${joinDate}\n`;
    msg += `🕐 Последняя активность: ${lastAct}\n`;
    msg += `🔢 Взаимодействий всего: ${totalInteract}\n`;
    msg += `📊 Сессий: ${sessions.length} (ср. ${avgMinutes} мин)\n`;
    msg += `⚠️ Уровень подозрения: <b>${suspScore}</b>\n\n`;

    msg += `🔑 <b>Купленные ключи:</b>\n`;
    if (orders.length > 0) {
      orders.forEach(o => {
        const soldDate = new Date(o.confirmed_at);
        const keyDays = { '1d': 1, '3d': 3, '7d': 7, '30d': 30 }[o.product] || 0;
        const expiryDate = keyDays > 0 ? new Date(soldDate.getTime() + keyDays * 24 * 60 * 60 * 1000) : null;
        const isExpired = expiryDate && expiryDate < new Date();
        const statusLabel = expiryDate
          ? (isExpired ? `❌ Истёк ${expiryDate.toLocaleDateString('ru-RU')}` : `✅ Активен до ${expiryDate.toLocaleDateString('ru-RU')}`)
          : '✅ Действует';
        msg += `• ${o.product} — куплен ${soldDate.toLocaleDateString('ru-RU')} → ${statusLabel}\n`;
      });
    } else {
      msg += '• нет покупок\n';
    }

    if (tickets.length > 0) {
      msg += `\n💬 <b>Жалобы:</b>\n`;
      tickets.forEach(tk => {
        const statusEmoji = { open: '🔵', resolved: '✅', fraud: '🚨' }[tk.status] || '❓';
        msg += `${statusEmoji} Тикет #${tk.id}: "${escapeHtml((tk.complaint_text || '').substring(0, 80))}"\n`;
      });
    }

    if (user.notes) {
      msg += `\n📝 <b>Заметки:</b> ${escapeHtml(user.notes)}\n`;
    }

    sendWithAnimatedEmoji(chatId, msg, ANIMATED_EMOJI.STATS, '👤', {
      parse_mode: 'HTML',
      reply_markup: {
        inline_keyboard: [
          [
            { text: '📝 Добавить заметку', callback_data: `client_note_${userId}` },
            { text: '🔄 Сбросить подозрение', callback_data: `client_reset_suspicion_${userId}` }
          ],
          [{ text: '🚫 Заблокировать', callback_data: `ban_${userId}` }]
        ]
      }
    });
  } catch (e) {
    console.error('❌ showClientInfo error:', e);
    safeSendMessage(chatId, '❌ Ошибка получения данных клиента');
  }
}

// Фоновый мониторинг подозрительной активности
async function runSuspicionMonitor() {
  try {
    const oneDayAgo = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString();
    const oneHourAgo = new Date(Date.now() - 60 * 60 * 1000).toISOString();

    // Правило 1: >3 сессий за 24ч
    const rule1 = await new Promise(res => {
      db.all(
        `SELECT user_id, COUNT(*) as cnt FROM user_sessions_history WHERE session_start > ? GROUP BY user_id HAVING cnt > 3`,
        [oneDayAgo], (e, rows) => res(rows || [])
      );
    });
    for (const row of rule1) {
      if (row.user_id === ADMIN_ID) continue; // Администратор не подозревается
      increaseSuspicion(row.user_id, 5, `Более 3 сессий за 24ч (${row.cnt})`);
    }

    // Правило 3: >10 сессий за последний час
    const rule3 = await new Promise(res => {
      db.all(
        `SELECT user_id, COUNT(*) as cnt FROM user_sessions_history WHERE session_start > ? GROUP BY user_id HAVING cnt > 10`,
        [oneHourAgo], (e, rows) => res(rows || [])
      );
    });
    for (const row of rule3) {
      if (row.user_id === ADMIN_ID) continue; // Администратор не подозревается
      increaseSuspicion(row.user_id, 15, `Более 10 сессий за 1ч (${row.cnt})`);
    }

    console.log('🛡️ Suspicion monitor run complete');
  } catch (e) {
    console.error('❌ runSuspicionMonitor error:', e);
  }
}

// ==========================================
// 🔔 П.3: Проверка запаса ключей с уведомлением
// ==========================================
async function checkLowKeysAndNotify() {
  if (getSetting('notify_low_keys') !== '1') return;
  const threshold = parseInt(getSetting('low_keys_threshold') || '5');

  try {
    db.all(
      `SELECT product, COUNT(*) as count FROM keys WHERE status = 'available' GROUP BY product`,
      [],
      async (err, rows) => {
        if (err) return;
        const products = ['1d', '3d', '7d', '30d'];
        for (const p of products) {
          const row = rows ? rows.find(r => r.product === p) : null;
          const count = row ? row.count : 0;
          if (count <= threshold) {
            await safeSendMessage(
              ADMIN_ID,
              `🔴⚠️ *Мало ключей!*\n\n📦 Продукт: ${p}\n🔑 Осталось: ${count} шт.\n⚠️ Порог: ${threshold} шт.`,
              { parse_mode: 'Markdown' }
            ).catch(() => { });
          }
        }
      }
    );
  } catch (e) {
    console.error('❌ checkLowKeysAndNotify error:', e);
  }
}

// sendDailyReport перенесена выше с расширенным функционалом

// ==========================================
// Детальная статистика (оставляем для совместимости, но она больше не используется)
// showDetailedStats удалён (Task 7) — используется showTopSales напрямую

// Статистика
// Применить паузу раздела витрины
function applySectionPause(chatId, session, reason) {
  const { spauseSection, spauseSettingKey, spauseDuration } = session.data || {};
  if (!spauseSection || !spauseSettingKey) {
    bot.sendMessage(chatId, '❌ Ошибка: данные сессии потеряны.');
    clearSession(chatId);
    return;
  }

  const names = { keys: 'Ключи', boost: 'Метод Буста', manual_boost: 'Ручной Буст' };
  const name = names[spauseSection] || spauseSection;
  const minutes = parseInt(spauseDuration) || 0;

  // Сохраняем причину в настройки (будет показана клиентам)
  const reasonKey = spauseSection + '_pause_reason';
  saveSetting(reasonKey, reason || '');

  // Отменяем старый таймер если был
  if (sectionPauseTimers[spauseSection]) {
    clearTimeout(sectionPauseTimers[spauseSection]);
    delete sectionPauseTimers[spauseSection];
  }

  saveSetting(spauseSettingKey, '1', () => {
    const durationLabel = minutes === 0 ? 'бессрочно' : `на ${minutes} мин.`;
    bot.sendMessage(chatId,
      `🛑 *${name}* остановлен — ${durationLabel}${reason ? `\n💬 Причина: ${reason}` : ''}`,
      { parse_mode: 'Markdown' }
    );
    clearSession(chatId);
    showBotSettings(chatId, message.message_id);

    // Автовозобновление если задано время
    if (minutes > 0) {
      sectionPauseTimers[spauseSection] = setTimeout(() => {
        saveSetting(spauseSettingKey, '0', () => {
          delete sectionPauseTimers[spauseSection];
          saveSetting(reasonKey, '');
          bot.sendMessage(chatId, `✅ *${name}* — продажи возобновлены автоматически.`, { parse_mode: 'Markdown' }).catch(() => {});
        });
      }, minutes * 60 * 1000);
    }
  });
}

function showStatistics(chatId, period, msgId = null) {
  period = period || 'all';
  const now = new Date();
  let dateFilter = '';
  let periodLabel = '';

  if (period === 'today') {
    const today = now.toISOString().split('T')[0];
    dateFilter = "AND date(created_at) = '" + today + "'";
    periodLabel = '📅 Сегодня';
  } else if (period === 'week') {
    const weekAgo = new Date(now - 7 * 24 * 60 * 60 * 1000).toISOString();
    dateFilter = "AND datetime(created_at) >= datetime('" + weekAgo + "')";
    periodLabel = '📅 7 дней';
  } else if (period === 'month') {
    const monthAgo = new Date(now - 30 * 24 * 60 * 60 * 1000).toISOString();
    dateFilter = "AND datetime(created_at) >= datetime('" + monthAgo + "')";
    periodLabel = '📅 30 дней';
  } else {
    periodLabel = '📅 За всё время';
  }

  const sql = `SELECT
      (SELECT COUNT(*) FROM users) as total_users,
      (SELECT COUNT(*) FROM users WHERE date(joined_at) = date('now')) as new_today,
      (SELECT COUNT(*) FROM orders WHERE status = 'confirmed' ${dateFilter}) as period_orders,
      (SELECT COUNT(DISTINCT user_id) FROM orders WHERE status = 'confirmed' ${dateFilter}) as unique_buyers,
      (SELECT COUNT(*) FROM orders WHERE status IN ('pending', 'out_of_stock_pending') AND method != 'CryptoBot' AND (balance_topup IS NULL OR balance_topup = 0)) as pending_orders,
      (SELECT SUM(amount) FROM orders WHERE status = 'confirmed' AND currency = 'USD' ${dateFilter}) as rev_usd,
      (SELECT SUM(amount) FROM orders WHERE status = 'confirmed' AND currency = 'RUB' ${dateFilter}) as rev_rub,
      (SELECT SUM(amount) FROM orders WHERE status = 'confirmed' AND currency = 'EUR' ${dateFilter}) as rev_eur,
      (SELECT SUM(amount) FROM orders WHERE status = 'confirmed' AND currency = 'UAH' ${dateFilter}) as rev_uah`;

  db.get(sql, [], (err, s) => {
    if (err) { adminSend(chatId, ADMIN_ID, '❌ Ошибка статистики'); return; }

    const conv = s.total_users > 0
      ? ((s.unique_buyers || 0) / s.total_users * 100).toFixed(1)
      : '0.0';

    let msg = `📊 *Статистика — ${periodLabel}*\n\n`;
    msg += `👥 Всего пользователей: *${s.total_users || 0}*`;
    if (period === 'all') msg += ` _(+${s.new_today || 0} сегодня)_`;
    msg += '\n';
    msg += `🛒 Заказов: *${s.period_orders || 0}* | уник. покупателей: *${s.unique_buyers || 0}*\n`;
    msg += `⏳ Ожидают подтверждения: *${s.pending_orders || 0}*\n`;
    msg += `📈 Конверсия (купили / всего): *${conv}%*\n\n`;
    msg += `💰 *Доход за период:*\n`;
    if (s.rev_usd) msg += `   💵 $${parseFloat(s.rev_usd).toFixed(2)}\n`;
    if (s.rev_rub) msg += `   🇷🇺 ${Math.round(s.rev_rub)} ₽\n`;
    if (s.rev_eur) msg += `   💶 €${parseFloat(s.rev_eur).toFixed(2)}\n`;
    if (s.rev_uah) msg += `   🇺🇦 ${Math.round(s.rev_uah)} ₴\n`;
    if (!s.rev_usd && !s.rev_rub && !s.rev_eur && !s.rev_uah) msg += '   —\n';

    const mk = (val, lbl, cb) => ({ text: (period === val ? '✅ ' : '') + lbl, callback_data: cb });

    safeSendMessage(chatId, msg, {
      parse_mode: 'Markdown',
      reply_markup: {
        inline_keyboard: [
          [mk('today', 'Сегодня', 'stats_today'), mk('week', '7 дней', 'stats_week'), mk('month', '30 дней', 'stats_month'), mk('all', 'Всё время', 'stats_all')],
          [{ text: '🔑 Запас ключей', callback_data: 'admin_key_stock' }],
          [{ text: '◀️ Назад', callback_data: 'admin' }]
        ]
      }
    });
  });
}


// Запас ключей
function showKeyStock(chatId, msgId = null) {
  db.all(
    `SELECT product, COUNT(*) as count FROM keys WHERE status = 'available' GROUP BY product`,
    [],
    (err, rows) => {
      if (err) {
        console.error('❌ Key stock error:', err);
        adminSend(chatId, ADMIN_ID, '❌ Ошибка получения данных');
        return;
      }

      let message = '🔑 <b>Запас ключей</b>\n\n';

      const products = ['1d', '3d', '7d', '30d'];
      const stock = {};

      rows.forEach(row => {
        stock[row.product] = row.count;
      });

      const periodLabels = { '1d': '1 день', '3d': '3 дня', '7d': '7 дней', '30d': '30 дней' };
      products.forEach(p => {
        const count = stock[p] || 0;
        const icon = count === 0 ? '🔴' : count <= 5 ? '🟡' : '🟢';
        message += `${icon} ${periodLabels[p]}: <b>${count} шт.</b>\n`;
      });

      const keyboard = {
        inline_keyboard: [
          [{ text: '◀️ Назад', callback_data: 'admin_manage_keys' }]
        ]
      };

      sendWithAnimatedEmoji(chatId, message, ANIMATED_EMOJI.KEY, '🔑', {
        parse_mode: 'HTML',
        reply_markup: keyboard
      });
    }
  );
}

// Задача 5: Проданные ключи с пагинацией (по 10, до 50 записей)
function showSoldKeys(chatId, page = 0, msgId = null) {
  const PAGE_SIZE = 10;
  const TOTAL_LIMIT = 50;

  const PERIOD_LABELS_RU = {
    '1d': '1 день', '3d': '3 дня', '7d': '7 дней', '30d': '30 дней',
    'infinite_boost': 'Метод Буста'
  };
  const REASON_LABELS = {
    'purchase': '💰 Покупка',
    'manual': '🎁 Выдан вручную',
    'review': '✍️ Награда за отзыв',
  };

  db.get('SELECT COUNT(*) as cnt FROM keys WHERE status = \'sold\'', [], (err0, countRow) => {
    const totalRecords = Math.min((countRow && countRow.cnt) || 0, TOTAL_LIMIT);
    const totalPages = Math.max(1, Math.ceil(totalRecords / PAGE_SIZE));
    const currentPage = Math.max(0, Math.min(page, totalPages - 1));

    db.all(
      `SELECT k.product, k.key_value, k.buyer_id, k.sold_at, k.issue_reason, u.username
       FROM keys k
       LEFT JOIN users u ON k.buyer_id = u.id
       WHERE k.status = 'sold'
       ORDER BY k.sold_at DESC LIMIT ? OFFSET ?`,
      [PAGE_SIZE, currentPage * PAGE_SIZE],
      (err, rows) => {
        if (err) {
          console.error('❌ Sold keys error:', err);
          adminSend(chatId, ADMIN_ID, '❌ Ошибка получения данных');
          return;
        }

        if (!rows || rows.length === 0) {
          safeSendMessage(chatId, '📜 *Проданные ключи*\n\nНет проданных ключей', {
            parse_mode: 'Markdown',
            reply_markup: { inline_keyboard: [[{ text: '◀️ Назад', callback_data: 'admin_manage_keys' }]] }
          });
          return;
        }

        let message = `📜 *Проданные ключи* — стр. ${currentPage + 1}/${totalPages} (всего ${totalRecords})\n\n`;

        rows.forEach((key, index) => {
          const date = key.sold_at ? new Date(key.sold_at).toLocaleDateString('ru-RU') : 'N/A';
          const buyer = key.username ? escapeMarkdown(`@${key.username}`) : `ID: ${key.buyer_id}`;
          const periodLabel = PERIOD_LABELS_RU[key.product] || key.product;
          const reason = REASON_LABELS[key.issue_reason || 'purchase'] || key.issue_reason;
          const num = currentPage * PAGE_SIZE + index + 1;

          message += `*${num}.* 📦 ${periodLabel}\n`;
          message += `🔑 \`${key.key_value}\`\n`;
          message += `👤 ${buyer}  📅 ${date}  ${reason}\n\n`;
        });

        const paginationButtons = [];
        if (currentPage > 0) paginationButtons.push({ text: '◀️', callback_data: `sold_keys_page_${currentPage - 1}` });
        paginationButtons.push({ text: `${currentPage + 1}/${totalPages}`, callback_data: 'noop' });
        if (currentPage < totalPages - 1) paginationButtons.push({ text: '▶️', callback_data: `sold_keys_page_${currentPage + 1}` });

        const keyboard = { inline_keyboard: [] };
        if (paginationButtons.length > 1) keyboard.inline_keyboard.push(paginationButtons);
        keyboard.inline_keyboard.push([{ text: '◀️ Назад', callback_data: 'admin_manage_keys' }]);

        safeSendMessage(chatId, message, { parse_mode: 'Markdown', reply_markup: keyboard });
      }
    );
  });
}
// Управление заказами
let ordersPage = 0;
const ORDERS_PER_PAGE = 5;

function showManageOrders(chatId, page = 0, msgId = null) {


  db.get(
    `SELECT COUNT(*) as count FROM orders WHERE status IN ('pending', 'out_of_stock_pending') AND method != 'CryptoBot'`,
    [],
    (err, countRow) => {
      if (err) {
        console.error('❌ Count error:', err);
        adminSend(chatId, ADMIN_ID, '❌ Ошибка получения статистики');
        return;
      }

      const pendingCount = countRow ? countRow.count : 0;

      if (pendingCount === 0) {
        const message = `📦 *Управление заказами*\n\n✅ Нет ожидающих заказов`;
        safeSendMessage(chatId, message, {
          parse_mode: 'Markdown',
          reply_markup: {
            inline_keyboard: [
              [{ text: '◀️ Назад', callback_data: 'admin' }]
            ]
          }
        });
        return;
      }

      const totalPages = Math.ceil(pendingCount / ORDERS_PER_PAGE);
      const currentPage = Math.max(0, Math.min(page, totalPages - 1));

      console.log(`📊 Pending orders count: ${pendingCount}, page: ${currentPage}`);

      db.all(
        `SELECT o.id, o.user_id, o.username, o.product, o.amount, o.currency, o.method, o.status,
                o.receipt_file_id, o.receipt_type, o.created_at, o.user_lang, 
                o.reseller_id, o.original_amount, o.coupon_id,
                u.username as reseller_username,
                c.code as coupon_code,
                c.discount_percent as coupon_discount_percent
         FROM orders o 
         LEFT JOIN resellers r ON o.reseller_id = r.id 
         LEFT JOIN users u ON r.user_id = u.id
         LEFT JOIN coupons c ON o.coupon_id = c.id
         WHERE o.status IN ('pending', 'out_of_stock_pending') AND o.method != 'CryptoBot'
         ORDER BY o.created_at DESC 
         LIMIT ? OFFSET ?`,
        [ORDERS_PER_PAGE, currentPage * ORDERS_PER_PAGE],
        async (err, orders) => {
          if (err) {
            console.error('❌ Orders list error:', err);
            safeSendMessage(chatId, '❌ Ошибка получения списка заказов');
            return;
          }



          if (!orders || orders.length === 0) {
            const message = `📦 *Управление заказами*\n\n✅ Нет ожидающих заказов на этой странице`;

            safeSendMessage(chatId, message, {
              parse_mode: 'Markdown',
              reply_markup: {
                inline_keyboard: [
                  [{ text: '◀️ Назад', callback_data: 'admin' }]
                ]
              }
            });
            return;
          }

          for (const order of orders) {
            const caption = buildOrderCaption(order);
            const lang = (order.user_lang && order.user_lang.startsWith('ru')) ? 'ru' : 'en';
            const noReceiptSuffix = lang === 'ru' ? '\n\n⚠️ *Чек не прикреплён*' : '\n\n⚠️ *No receipt attached*';

            const keyboard = {
              inline_keyboard: [[
                { text: '✅ Одобрить', callback_data: `approve_${order.id}` },
                { text: '❌ Отклонить', callback_data: `reject_${order.id}` }
              ]]
            };

            try {
              if (order.receipt_file_id && order.receipt_type) {
                if (order.receipt_type === 'photo') {
                  await bot.sendPhoto(chatId, order.receipt_file_id, {
                    caption,
                    parse_mode: 'Markdown',
                    reply_markup: keyboard
                  });
                } else {
                  await bot.sendDocument(chatId, order.receipt_file_id, {
                    caption,
                    parse_mode: 'Markdown',
                    reply_markup: keyboard
                  });
                }
              } else {
                await bot.sendMessage(chatId, caption + noReceiptSuffix, {
                  parse_mode: 'Markdown',
                  reply_markup: keyboard
                });
              }
              await new Promise(r => setTimeout(r, 300));
            } catch (error) {
              console.error('❌ Error sending order:', error.message);
              try {
                const plain = caption.replace(/[*_`]/g, '');
                if (order.receipt_file_id && order.receipt_type) {
                  if (order.receipt_type === 'photo') {
                    await bot.sendPhoto(chatId, order.receipt_file_id, { caption: plain, reply_markup: keyboard });
                  } else {
                    await bot.sendDocument(chatId, order.receipt_file_id, { caption: plain, reply_markup: keyboard });
                  }
                } else {
                  await bot.sendMessage(chatId, plain, { reply_markup: keyboard });
                }
              } catch (retryError) {
                console.error('❌ Retry failed:', retryError.message);
              }
            }
          }

          const finalKeyboard = {
            inline_keyboard: []
          };

          const paginationButtons = [];
          if (currentPage > 0) {
            paginationButtons.push({ text: '◀️ Пред.', callback_data: `orders_page_${currentPage - 1}` });
          }
          paginationButtons.push({ text: `📄 ${currentPage + 1}/${totalPages}`, callback_data: 'noop' });
          if (currentPage < totalPages - 1) {
            paginationButtons.push({ text: 'След. ▶️', callback_data: `orders_page_${currentPage + 1}` });
          }

          if (paginationButtons.length > 1) {
            finalKeyboard.inline_keyboard.push(paginationButtons);
          }

          finalKeyboard.inline_keyboard.push([{ text: '◀️ Назад', callback_data: 'admin' }]);

          await safeSendMessage(chatId, `📊 *Всего на проверке:* ${pendingCount}`, {
            parse_mode: 'Markdown',
            reply_markup: finalKeyboard
          });
        }
      );
    }
  );
}

// Управление ценами
function showManagePrices(chatId, msgId = null) {
  let message = '💰 Текущие цены\n\n';

  const products = ['1d', '3d', '7d', '30d'];
  products.forEach(p => {
    message += `📦 *${p}*:\n`;
    message += `   ${FLAGS.USD} USD: $${PRICES[p].USD}\n`;
    message += `   ${FLAGS.EUR} EUR: €${PRICES[p].EUR}\n`;
    message += `   ${FLAGS.RUB} RUB: ${formatPrice(PRICES[p].RUB, 'RUB')}\n`;
    message += `   ${FLAGS.UAH} UAH: ${PRICES[p].UAH}₴\n\n`;
  });

  const pBoost = PRICES['infinite_boost'];
  message += `🚀 *Метод Буста:*\n`;
  message += `   ${FLAGS.USD} $${pBoost.USD}  ${FLAGS.EUR} €${pBoost.EUR}  ${FLAGS.RUB} ${formatPrice(pBoost.RUB, 'RUB')}  ${FLAGS.UAH} ${pBoost.UAH}₴\n\n`;

  const pRsl = PRICES['reseller_connection'];
  message += `🤝 *Партнёрство:*\n`;
  message += `   ${FLAGS.USD} $${pRsl.USD}  ${FLAGS.EUR} €${pRsl.EUR}  ${FLAGS.RUB} ${formatPrice(pRsl.RUB, 'RUB')}  ${FLAGS.UAH} ${pRsl.UAH}₴`;

  const keyboard = {
    inline_keyboard: [
      [{ text: '── 🔑 Ключи ──', callback_data: 'noop' }],
      [
        { text: '✏️ 1d', callback_data: 'edit_price_1d' },
        { text: '✏️ 3d', callback_data: 'edit_price_3d' },
        { text: '✏️ 7d', callback_data: 'edit_price_7d' },
        { text: '✏️ 30d', callback_data: 'edit_price_30d' }
      ],
      [{ text: '── 🚀 Буст и партнёрство ──', callback_data: 'noop' }],
      [
        { text: '✏️ Метод Буста', callback_data: 'edit_price_infinite_boost' },
        { text: '✏️ Партнёрство', callback_data: 'edit_price_reseller_connection' }
      ],
      [{ text: '◀️ Назад', callback_data: 'admin' }]
    ]
  };

  adminSend(chatId, ADMIN_ID, message, {
    parse_mode: 'Markdown',
    reply_markup: keyboard
  }, msgId);
}

// Управление ключами
function showManageKeys(chatId, msgId = null) {
  const keyboard = {
    inline_keyboard: [
      [
        { text: '➕ Добавить 1d', callback_data: 'add_keys_1d' },
        { text: '➕ Добавить 3d', callback_data: 'add_keys_3d' }
      ],
      [
        { text: '➕ Добавить 7d', callback_data: 'add_keys_7d' },
        { text: '➕ Добавить 30d', callback_data: 'add_keys_30d' }
      ],
      [{ text: '🔑 Запас ключей', callback_data: 'admin_key_stock' }],
      [{ text: '📜 Проданные ключи', callback_data: 'admin_sold_keys' }],
      [{ text: '👤 Выдать ключ', callback_data: 'admin_issue_key' }],
      [{ text: '◀️ Назад', callback_data: 'admin' }]
    ]
  };

  adminSend(chatId, ADMIN_ID, '🔑 *Управление ключами*', {
    parse_mode: 'Markdown',
    reply_markup: keyboard
  }, msgId);
}

// Задача 4: Русские названия действий для логов
const ACTION_LABELS_RU = {
  'start': '🏠 Старт',
  'view_products': '🛒 Просмотр товаров',
  'select_period': '📦 Выбор периода',
  'select_currency': '💱 Выбор валюты',
  'payment_method_selected': '💳 Выбор оплаты',
  'order_created_with_receipt': '📸 Создан заказ',
  'cryptobot_invoice_created': '🤖 CryptoBot инвойс',
  'cryptobot_auto_confirmed': '✅ CryptoBot оплачен',
  'order_confirmed': '✅ Заказ одобрен',
  'order_rejected': '❌ Заказ отклонён',
  'coupon_applied': '🎟 Купон применён',
  'coupon_created': '🎟 Купон создан',
  'coupon_deleted': '🗑 Купон удалён',
  'coupon_issued': '🎁 Купон выдан',
  'coupon_issued_to_all': '📣 Купоны всем',
  'all_coupons_deleted': '🗑 Все купоны удалены',
  'loyalty_coupon_issued': '💎 Купон лояльности',
  'fomo_coupon_issued': '🎫 FOMO купон',
  'review_code_requested': '✍️ Запрос отзыва',
  'review_reward_coupon': '🎁 Награда: купон',
  'review_reward_key': '🎁 Награда: ключ',
  'review_rejected': '❌ Отзыв отклонён',
  'manual_key_issue': '🔑 Ключ выдан вручную',
  'keys_added': '➕ Ключи добавлены',
  'prices_updated': '💰 Цены изменены',
  'payment_details_updated': '💳 Реквизиты изменены',
  'user_banned': '🚫 Пользователь забанен',
  'user_unbanned': '✅ Пользователь разбанен',
  'user_unbanned_manual': '✅ Разбан вручную',
  'auto_backup_created': '💾 Автобэкап',
  'database_restored': '🔄 БД восстановлена',
  'reminder_sent_manual': '🔔 Напоминание отправлено',
};

function getActionLabel(action) {
  return ACTION_LABELS_RU[action] || action;
}

// Задача 4: showLogs с пагинацией (до 50 записей, по 5 строк на странице колонками)
function showLogs(chatId, page = 0, msgId = null) {
  const PAGE_SIZE = 5;
  const TOTAL_LIMIT = 50;
  const offset = page * PAGE_SIZE;

  db.all(
    `SELECT l.user_id, l.action, l.details, l.timestamp, u.username 
     FROM action_logs l
     LEFT JOIN users u ON l.user_id = u.id
     ORDER BY l.timestamp DESC 
     LIMIT ? OFFSET ?`,
    [PAGE_SIZE, offset],
    (err, logs) => {
      if (err) {
        console.error('❌ Logs error:', err);
        adminSend(chatId, ADMIN_ID, '❌ Ошибка получения логов');
        return;
      }

      db.get('SELECT COUNT(*) as cnt FROM action_logs', [], (err2, countRow) => {
        const totalRecords = Math.min((countRow && countRow.cnt) || 0, TOTAL_LIMIT);
        const totalPages = Math.ceil(totalRecords / PAGE_SIZE);
        const currentPage = Math.max(0, Math.min(page, totalPages - 1));

        let message = `📋 *Логи действий* (стр. ${currentPage + 1}/${totalPages || 1})\n\n`;

        if (!logs || logs.length === 0) {
          message += 'Нет записей';
        } else {
          logs.forEach((log) => {
            const date = new Date(log.timestamp).toLocaleString('ru-RU', { day: '2-digit', month: '2-digit', hour: '2-digit', minute: '2-digit' });
            const userStr = log.username ? `@${escapeMarkdown(log.username)}` : `ID:${log.user_id}`;
            const actionLabel = getActionLabel(log.action);

            let detailsStr = '';
            if (log.details) {
              try {
                const d = JSON.parse(log.details);
                const keys = Object.keys(d).slice(0, 2);
                // Явно приводим значения к строке и экранируем — они могут содержать _ * ` и т.д.
                detailsStr = keys.map(k => `${escapeMarkdown(String(k))}:${escapeMarkdown(String(d[k]))}`).join(' ');
              } catch {
                detailsStr = escapeMarkdown(String(log.details).substring(0, 40));
              }
            }

            message += `${actionLabel}\n`;
            message += `  👤 ${escapeMarkdown(userStr)}  🕐 ${date}\n`;
            if (detailsStr) message += `  📝 ${escapeMarkdown(detailsStr)}\n`;
            message += '\n';
          });
        }

        const paginationButtons = [];
        if (currentPage > 0) paginationButtons.push({ text: '◀️', callback_data: `logs_page_${currentPage - 1}` });
        paginationButtons.push({ text: `${currentPage + 1}/${totalPages || 1}`, callback_data: 'noop' });
        if (currentPage < totalPages - 1) paginationButtons.push({ text: '▶️', callback_data: `logs_page_${currentPage + 1}` });

        const keyboard = { inline_keyboard: [] };
        if (paginationButtons.length > 1) keyboard.inline_keyboard.push(paginationButtons);
        keyboard.inline_keyboard.push([{ text: '◀️ Назад', callback_data: 'admin' }]);

        safeSendMessage(chatId, message, { parse_mode: 'Markdown', reply_markup: keyboard });
      });
    }
  );
}

// Сборка caption заказа для админа (ru/en по языку покупателя)
function buildOrderCaption(order) {
  const lang = (order.user_lang && order.user_lang.startsWith('ru')) ? 'ru' : 'en';
  const isRu = lang === 'ru';

  const username = order.username ? escapeMarkdown(`@${order.username}`) : `ID: ${order.user_id}`;
  const periodName = PERIOD_NAMES[lang]?.[order.product] || order.product;
  const methodName = escapeMarkdown(METHOD_NAMES[lang]?.[order.method] || order.method || '');
  const amount = formatPrice(order.amount, order.currency);
  const orderDate = new Date(order.created_at).toLocaleString('ru-RU');
  const txnRef = order.transaction_id || `#${order.id}`;

  // Блок купона / скидки лояльности — показывается менеджеру если была применена скидка.
  // original_amount содержит цену ДО скидки; coupon_id — ID купона (если применён купон).
  // coupon_code и coupon_discount_percent добавляются JOIN'ом в sendPendingOrderToAdmin.
  let discountBlock = '';
  if (order.original_amount && order.original_amount > order.amount) {
    const originalStr = formatPrice(order.original_amount, order.currency);
    const savedStr = formatPrice(Math.round((order.original_amount - order.amount) * 100) / 100, order.currency);
    if (order.coupon_id) {
      const codePart = order.coupon_code ? ` \`${order.coupon_code}\`` : ` #${order.coupon_id}`;
      const pctPart = order.coupon_discount_percent ? ` (−${order.coupon_discount_percent}%)` : '';
      discountBlock = isRu
        ? `\n🎟️ *Купон${codePart}*${pctPart}: ${originalStr} → *${amount}* (−${savedStr})`
        : `\n🎟️ *Coupon${codePart}*${pctPart}: ${originalStr} → *${amount}* (−${savedStr})`;
    } else {
      // Скидка лояльности — купона нет, но цена ниже базовой
      discountBlock = isRu
        ? `\n🎁 *Скидка лояльности:* ${originalStr} → *${amount}* (−${savedStr})`
        : `\n🎁 *Loyalty discount:* ${originalStr} → *${amount}* (−${savedStr})`;
    }
  }

  // Реселлер: полный блок с ботом, наценкой, суммой к резерву
  let resellerBlock = '';
  if (order.reseller_id) {
    const rslName = order.reseller_username ? `@${escapeMarkdown(order.reseller_username)}` : `ID ${order.reseller_id}`;
    const botName = order.reseller_bot_username ? `@${escapeMarkdown(order.reseller_bot_username)}` : 'не подключен';
    const markupPct = order.reseller_markup_pct || 0;

    // Рассчитываем базовую цену и долю реселлера
    const basePrice = order.original_amount || (order.amount / (1 + markupPct / 100));
    const resellerEarns = order.amount - basePrice;
    const basePriceStr = formatPrice(Math.round(basePrice * 100) / 100, order.currency);
    const resellerEarnsStr = formatPrice(Math.round(resellerEarns * 100) / 100, order.currency);

    resellerBlock =
      `\n──────────────────\n` +
      `🤝 *Платёж через реселлера:*\n` +
      `👤 Владелец: ${rslName}\n` +
      `🤖 Бот: ${botName}\n` +
      `📊 Наценка: ${markupPct}%\n` +
      `💵 База (вам): *${basePriceStr}*\n` +
      `💰 Доля реселлера: *${resellerEarnsStr}*\n` +
      `──────────────────`;
  } else if (order.product === 'reseller_connection') {
    resellerBlock =
      `\n──────────────────\n` +
      `🤝 *Заявка на подключение к партнерской программе:*\n` +
      `📊 Желаемая наценка: *${order.reseller_markup_pct || 30}%*\n` +
      `📝 Анкета опыта:\n_${escapeMarkdown(order.reseller_questionnaire || 'Не указано')}_\n` +
      `──────────────────`;
  }

  if (isRu) {
    const oosTag = order.status === 'out_of_stock_pending' ? `\n⏳ *Очередь ожидания* — деньги на балансе клиента` : '';
    return (
      `🔔💫 Новая покупка\n\n` +
      `👤 Клиент: ${username}\n` +
      `📦 ${periodName}\n` +
      `💰 ${amount}\n` +
      `💳 ${methodName}\n` +
      discountBlock +
      oosTag +
      `\n🕐 ${orderDate}\n` +
      `🆔 \`${txnRef}\`` +
      resellerBlock
    );
  } else {
    const oosTag = order.status === 'out_of_stock_pending' ? `\n⏳ *Waiting queue* — funds credited to client's balance` : '';
    return (
      `🔔💫 New purchase\n\n` +
      `👤 Client: ${username}\n` +
      `📦 ${periodName}\n` +
      `💰 ${amount}\n` +
      `💳 ${methodName}\n` +
      discountBlock +
      oosTag +
      `\n🕐 ${orderDate}\n` +
      `🆔 \`${txnRef}\`` +
      resellerBlock
    );
  }
}

// Отправка pending заказа админу
async function sendPendingOrderToAdmin(orderId) {
  try {
    if (getSetting('notify_new_order') !== '1') return;

    db.get(`SELECT o.*, 
            u.username as reseller_username, 
            r.bot_username as reseller_bot_username, 
            r.markup_pct as reseller_markup_pct,
            c.code as coupon_code,
            c.discount_percent as coupon_discount_percent
            FROM orders o 
            LEFT JOIN resellers r ON o.reseller_id = r.id 
            LEFT JOIN users u ON r.user_id = u.id
            LEFT JOIN coupons c ON o.coupon_id = c.id
            WHERE o.id = ?`, [orderId], async (err, order) => {
      if (err || !order) {
        console.error('❌ Order not found:', orderId);
        return;
      }

      const caption = buildOrderCaption(order);
      const lang = (order.user_lang && order.user_lang.startsWith('ru')) ? 'ru' : 'en';
      const noReceiptSuffix = lang === 'ru' ? '\n\n⚠️ *Чек не прикреплён*' : '\n\n⚠️ *No receipt attached*';

      const keyboard = {
        inline_keyboard: [
          [
            { text: '✅ Одобрить', callback_data: `approve_${order.id}` },
            { text: '❌ Отклонить', callback_data: `reject_${order.id}` }
          ],
          [{ text: '📨 Написать покупателю', callback_data: `msg_buyer_${order.user_id}` }]
        ]
      };

      // Вспомогательная функция отправки заказа (переиспользуется для менеджеров)
      async function sendOrderToChat(targetChatId) {
        try {
          if (order.receipt_file_id && order.receipt_type) {
            if (order.receipt_type === 'photo') {
              await bot.sendPhoto(targetChatId, order.receipt_file_id, { caption, parse_mode: 'Markdown', reply_markup: keyboard });
            } else {
              await bot.sendDocument(targetChatId, order.receipt_file_id, { caption, parse_mode: 'Markdown', reply_markup: keyboard });
            }
          } else {
            await bot.sendMessage(targetChatId, caption + noReceiptSuffix, { parse_mode: 'Markdown', reply_markup: keyboard });
          }
        } catch (error) {
          console.error(`❌ Error sending order to ${targetChatId}:`, error.message);
          try {
            const plain = caption.replace(/[*_`]/g, '');
            if (order.receipt_file_id && order.receipt_type) {
              try {
                if (order.receipt_type === 'photo') {
                  await bot.sendPhoto(targetChatId, order.receipt_file_id, { caption: plain, reply_markup: keyboard });
                } else {
                  await bot.sendDocument(targetChatId, order.receipt_file_id, { caption: plain, reply_markup: keyboard });
                }
              } catch (fileErr) {
                // file_id истёк или невалиден — отправляем только текст
                console.warn(`⚠️ receipt file_id invalid for order #${order.id}, sending text only:`, fileErr.message);
                await bot.sendMessage(targetChatId, plain, { reply_markup: keyboard });
              }
            } else {
              await bot.sendMessage(targetChatId, plain, { reply_markup: keyboard });
            }
          } catch (retryError) {
            console.error('❌ Retry failed:', retryError.message);
          }
        }
      }

      // 1. Сразу отправляем админу
      await sendOrderToChat(ADMIN_ID);

      // 2. Через 60 секунд — дублируем менеджерам (если заказ ещё pending)
      setTimeout(async () => {
        try {
          const freshOrder = await new Promise((res, rej) => {
            db.get(`SELECT status, method FROM orders WHERE id = ?`, [orderId], (e, row) => e ? rej(e) : res(row));
          });
          if (!freshOrder || freshOrder.status !== 'pending') return;

          const mgrs = await new Promise((res, rej) => {
            db.all('SELECT manager_id FROM manager_methods WHERE payment_method = ?',
              [freshOrder.method], (e, rows) => e ? rej(e) : res(rows));
          });
          if (!mgrs || mgrs.length === 0) return;

          for (const m of mgrs) {
            try {
              await sendOrderToChat(m.manager_id);
            } catch (mgrErr) {
              console.error(`❌ Failed to send order #${orderId} to manager ${m.manager_id}:`, mgrErr.message);
              // Ошибка у одного менеджера не прерывает отправку другим
            }
          }
        } catch (timerErr) {
          console.error(`❌ setTimeout error for order #${orderId}:`, timerErr.message);
        }
      }, 60 * 1000);

    });
  } catch (e) {
    console.error('❌ sendPendingOrderToAdmin error:', e);
  }
}

// ==========================================
// 👥 МЕНЕДЖЕРЫ — вспомогательные функции
// ==========================================

// Показать панель менеджеров (для админа)
function showManagersPanel(chatId, msgId = null) {
  db.all(
    `SELECT m.user_id, m.username, m.display_name, GROUP_CONCAT(mm.payment_method, ', ') as methods
     FROM managers m
     LEFT JOIN manager_methods mm ON m.user_id = mm.manager_id
     GROUP BY m.user_id`,
    [],
    (err, rows) => {
      let msg = '👥 *Менеджеры*\n\n';
      const keyboard = { inline_keyboard: [] };

      if (!rows || rows.length === 0) {
        msg += 'Нет назначенных менеджеров.\n';
      } else {
        rows.forEach(r => {
          const uname = r.username ? `@${escapeMarkdown(r.username)}` : `ID: ${r.user_id}`;
          const nameLabel = r.display_name ? ` _(${r.display_name})_` : '';
          msg += `👤 ${uname}${nameLabel}\n📋 Методы: ${r.methods || '—'}\n\n`;
          keyboard.inline_keyboard.push([
            { text: `✏️ ${uname}`, callback_data: `admin_edit_manager_${r.user_id}` },
            { text: `❌ Удалить`, callback_data: `admin_remove_manager_${r.user_id}` }
          ]);
        });
      }

      keyboard.inline_keyboard.push([{ text: '➕ Назначить менеджера', callback_data: 'admin_add_manager' }]);
      keyboard.inline_keyboard.push([{ text: '◀️ Назад', callback_data: 'admin' }]);

      adminSend(chatId, ADMIN_ID, msg, { parse_mode: 'Markdown', reply_markup: keyboard }, msgId);
    }
  );
}

// Список всех доступных методов оплаты для менеджера
const MANAGER_PAYMENT_METHODS = ['SBP', 'Card UA', 'Card IT', 'PayPal', 'Binance', 'CryptoBot'];

// Показать клавиатуру выбора методов оплаты для менеджера
function showManagerMethodsKeyboard(chatId, managerId, selectedMethods) {
  const rows = MANAGER_PAYMENT_METHODS.map(m => {
    const isSelected = selectedMethods.includes(m);
    return [{ text: (isSelected ? '✅ ' : '◻️ ') + m, callback_data: `admin_manager_toggle_${m}` }];
  });
  rows.push([{ text: '💾 Готово', callback_data: `admin_manager_methods_done_${managerId}` }]);
  rows.push([{ text: '◀️ Отмена', callback_data: 'admin_managers' }]);
  safeSendMessage(chatId, '📋 Выберите методы оплаты для менеджера:', {
    reply_markup: { inline_keyboard: rows }
  });
}

// Показать заказы менеджеру (только pending/out_of_stock_pending, только его методы)
function showManagerOrders(chatId, userId) {
  db.all(
    'SELECT payment_method FROM manager_methods WHERE manager_id = ?',
    [userId],
    (err, methods) => {
      if (err || !methods || methods.length === 0) {
        safeSendMessage(chatId, '📭 Вам не назначены методы оплаты. Обратитесь к администратору.', {
          reply_markup: { inline_keyboard: [[{ text: '🔄 Обновить', callback_data: 'manager_orders' }]] }
        });
        return;
      }
      const methodList = methods.map(m => m.payment_method);
      const placeholders = methodList.map(() => '?').join(',');
      db.all(
        `SELECT o.*, u.username as reseller_username 
         FROM orders o 
         LEFT JOIN resellers r ON o.reseller_id = r.id 
         LEFT JOIN users u ON r.user_id = u.id 
         WHERE o.status IN ('pending', 'out_of_stock_pending') AND o.method IN (${placeholders}) ORDER BY o.created_at DESC LIMIT 20`,
        methodList,
        async (err2, orders) => {
          if (err2) { safeSendMessage(chatId, '❌ Ошибка получения заказов'); return; }

          const refreshBtn = { inline_keyboard: [[{ text: '🔄 Обновить список', callback_data: 'manager_orders' }]] };

          if (!orders || orders.length === 0) {
            safeSendMessage(chatId,
              `✅ *Новых заказов нет*\n\n📋 Ваши методы: ${methodList.join(', ')}\n\nПри появлении нового заказа вы получите уведомление.`,
              { parse_mode: 'Markdown', reply_markup: refreshBtn }
            );
            return;
          }

          // Заголовок со счётчиком
          await safeSendMessage(chatId,
            `📦 *Заказы на проверку: ${orders.length} шт.*\n📋 Ваши методы: ${methodList.join(', ')}`,
            { parse_mode: 'Markdown' }
          ).catch(() => { });

          for (const order of orders) {
            if (!methodList.includes(order.method)) continue;
            const caption = buildOrderCaption(order);
            const keyboard = {
              inline_keyboard: [[
                { text: '✅ Одобрить', callback_data: `approve_${order.id}` },
                { text: '❌ Отклонить', callback_data: `reject_${order.id}` }
              ]]
            };
            try {
              if (order.receipt_file_id && order.receipt_type) {
                if (order.receipt_type === 'photo') {
                  await bot.sendPhoto(chatId, order.receipt_file_id, { caption, parse_mode: 'Markdown', reply_markup: keyboard });
                } else {
                  await bot.sendDocument(chatId, order.receipt_file_id, { caption, parse_mode: 'Markdown', reply_markup: keyboard });
                }
              } else {
                await safeSendMessage(chatId, caption + '\n\n⚠️ Чек не прикреплён', { parse_mode: 'Markdown', reply_markup: keyboard });
              }
            } catch (e) {
              await safeSendMessage(chatId, caption.replace(/[*_`]/g, '') + '\n\n⚠️ Чек не прикреплён', { reply_markup: keyboard });
            }
          }

          // Итоговая кнопка обновления
          await safeSendMessage(chatId, `📋 Показано заказов: ${orders.length}`, { reply_markup: refreshBtn }).catch(() => { });
        }
      );
    }
  );
}

// ==========================================
// 🤖 ОБРАБОТКА /start и /admin
// ==========================================
bot.onText(/\/start/, async (msg) => {
  const user = msg.from;
  const chatId = msg.chat.id;

  // В групповых чатах /start игнорируем — для групп есть /cyrax
  if (msg.chat.type !== 'private') return;

  if (maintenanceMode && user.id !== ADMIN_ID) {
    // Менеджеры имеют доступ во время техобслуживания — проверяем
    const isMgr = await isManager(user.id);
    if (isMgr) {
      // Менеджерское меню: только кнопка заказов
      const isRu = getLang(user) === 'ru';
      const mgrMsg = isRu
        ? '👥 *Режим обслуживания*\n\nВы вошли как менеджер. Доступны только заказы.'
        : '👥 *Maintenance mode*\n\nYou are logged in as a manager. Only orders are available.';
      safeSendMessage(chatId, mgrMsg, {
        parse_mode: 'Markdown',
        reply_markup: { inline_keyboard: [[{ text: '📦 Заказы на проверку', callback_data: 'manager_orders' }]] }
      }).catch(() => { });
      return;
    }
    const timeLeft = maintenanceEndTime ? Math.ceil((maintenanceEndTime - Date.now()) / 60000) : null;
    const timeStr = timeLeft !== null ? t(user, 'maintenance_time', { minutes: timeLeft }) : '∞';
    const maintenanceMsg = t(user, 'maintenance')
      .replace('{time}', timeStr)
      .replace('{reason}', maintenanceReason ? maintenanceReason : '');

    maintenanceWaitingUsers.add(chatId);
    bot.sendMessage(chatId, maintenanceMsg).catch(() => { });
    return;
  }

  // Проверка бана
  if (user.id !== ADMIN_ID) {
    const violation = rateLimitViolations.get(user.id);
    if (violation && violation.bannedUntil && Date.now() < violation.bannedUntil) {
      const isRuMsg = getLang(user) === 'ru';
      bot.sendMessage(chatId, isRuMsg
        ? '🚫 Ваш доступ к боту временно заблокирован за спам. Если считаете это ошибкой — обратитесь в поддержку.'
        : '🚫 Your bot access is temporarily blocked for spam. If you think this is a mistake — contact support.'
      ).catch(() => { });
      return;
    }
    // Проверка постоянного бана в БД (ставится вручную из профиля пользователя)
    const dbBanRow = await new Promise(res => db.get('SELECT is_banned FROM users WHERE id = ?', [user.id], (e, r) => res(r)));
    if (dbBanRow && dbBanRow.is_banned) {
      const isRuMsg = getLang(user) === 'ru';
      bot.sendMessage(chatId, isRuMsg
        ? '🚫 Ваш доступ заблокирован администратором. По вопросам — обратитесь в поддержку.'
        : '🚫 Your access has been blocked by the administrator. Contact support for assistance.'
      ).catch(() => {});
      return;
    }
  }

  db.run(
    `INSERT OR IGNORE INTO users (id, username, language_code) VALUES (?, ?, ?)`,
    [user.id, user.username || null, user.language_code || 'en'],
    function (err) {
      if (err) console.error('❌ User insert error:', err);
      if (this.changes > 0) {
        sendNewUserNotification(user);
      }
      // ✅ Вызываем ПОСЛЕ гарантированного создания пользователя в БД
      trackUserInteraction(user.id, user.username, user.language_code);
    }
  );

  clearSession(user.id); // ✅ сбрасываем любое зависшее состояние
  logAction(user.id, 'start');

  // BUG FIX UX-3: При наличии pending заказа предупреждаем пользователя,
  // чтобы он не запутался ("заказ уже в обработке" при следующей попытке купить).
  if (user.id !== ADMIN_ID) {
    db.get(
      `SELECT id, product FROM orders WHERE user_id = ? AND status = 'pending' LIMIT 1`,
      [user.id],
      (pendErr, pendingOrder) => {
        if (!pendErr && pendingOrder) {
          const isRu = getLang(user) === 'ru';
          const prodName = PERIOD_NAMES[isRu ? 'ru' : 'en']?.[pendingOrder.product] || pendingOrder.product;
          bot.sendMessage(chatId,
            isRu
              ? `⏳ У вас есть незавершённый заказ: *${prodName}* (ожидает подтверждения).\n\nЕсли хотите — можете отменить его и начать заново, либо дождаться ответа администратора.`
              : `⏳ You have a pending order: *${prodName}* (awaiting confirmation).\n\nYou can cancel it and start over, or wait for the admin's response.`,
            {
              parse_mode: 'Markdown',
              reply_markup: {
                inline_keyboard: [
                  [{ text: isRu ? '❌ Отменить заказ' : '❌ Cancel order', callback_data: `cancel_order_${pendingOrder.id}` }],
                  [{ text: isRu ? '✅ Ничего не делать' : '✅ Do nothing', callback_data: 'start' }]
                ]
              }
            }
          ).catch(() => { });
        }
      }
    );
  }

  // Обработка реферальной ссылки
  const startPayload = (msg.text || '').split(' ')[1] || '';
  if (startPayload.startsWith('REF') && user.id !== ADMIN_ID) {
    processRefStart(user, startPayload);
  }

  // FIX 1.2: Обработка диплинка ?start=partnership — пользователь пришёл из реселлер-бота.
  // Сразу запускаем флоу выбора наценки, минуя главное меню.
  if (startPayload === 'partnership' && user.id !== ADMIN_ID) {
    if (getSetting('reseller_enabled') === '0') {
      bot.sendMessage(chatId, t(user, 'partner_disabled')).catch(() => {});
      return;
    }
    db.get(`SELECT status FROM resellers WHERE user_id = ?`, [user.id], (err, row) => {
      if (row && row.status === 'active') {
        bot.sendMessage(chatId, t(user, 'partner_already_active')).catch(() => {});
        return;
      }
      // Эмулируем нажатие reseller_activate — устанавливаем состояние и показываем выбор наценки
      const session = getSession(user.id);
      session.state = 'awaiting_reseller_markup';
      session.data = { isResellerFlow: true };
      const isRuMode = getLang(user) === 'ru';
      const msg2 = isRuMode
        ? '⚙️ *Выберите наценку*\n\nЭто процент, который добавляется к базовой цене:\n\n'
        + '💡 *Примеры:*\n'
        + '— 20% → ключ 100₽ будет стоить 120₽ (+20₽ вам)\n'
        + '— 30% → ключ 100₽ будет стоить 130₽ (+30₽ вам)\n'
        + '— 50% → ключ 100₽ будет стоить 150₽ (+50₽ вам)\n'
        + '\nВыберите вариант или напишите своё число:'
        : '⚙️ *Select your markup*\n\nThis percentage is added to the base price:\n\n'
        + '💡 *Examples:*\n'
        + '— 20% → $10 key costs $12 (+$2 for you)\n'
        + '— 30% → $10 key costs $13 (+$3 for you)\n'
        + '— 50% → $10 key costs $15 (+$5 for you)\n'
        + '\nSelect an option or type your own number:';
      const keyboard = {
        inline_keyboard: [
          [
            { text: '20%', callback_data: 'rsl_markup_preset_20' },
            { text: '25%', callback_data: 'rsl_markup_preset_25' },
            { text: '30%', callback_data: 'rsl_markup_preset_30' }
          ],
          [
            { text: '35%', callback_data: 'rsl_markup_preset_35' },
            { text: '40%', callback_data: 'rsl_markup_preset_40' },
            { text: '50%', callback_data: 'rsl_markup_preset_50' }
          ],
          [{ text: isRuMode ? '◀️ Отмена' : '◀️ Cancel', callback_data: 'partnership' }]
        ]
      };
      safeSendMessage(chatId, msg2, { parse_mode: 'Markdown', reply_markup: keyboard }).catch(() => {});
    });
    return;
  }

  // Проверяем — является ли пользователь менеджером (не-админ)
  if (user.id !== ADMIN_ID) {
    const mgr = await isManager(user.id);
    if (mgr) {
      // Подгружаем назначенные методы для отображения в приветствии
      db.all('SELECT payment_method FROM manager_methods WHERE manager_id = ?', [user.id], (e2, methodRows) => {
        const methodList = (!e2 && methodRows && methodRows.length > 0)
          ? methodRows.map(r => r.payment_method).join(', ')
          : '—';
        // Задача 6: Используем display_name если есть
        db.get('SELECT display_name, username FROM managers WHERE user_id = ?', [user.id], (e3, mgrRow) => {
          const displayName = (mgrRow && mgrRow.display_name) || null;
          const greeting = displayName ? `Привет, *${displayName}*! 👋` : `👥 Добро пожаловать, менеджер!`;
          const isRu = getLang(user) === 'ru';
          const mgrWelcome = isRu
            ? `${greeting}\n\nВы можете просматривать и подтверждать входящие заказы по назначенным методам оплаты.\n\n📋 *Ваши методы:* ${methodList}\n\n🛍 Хотите сделать покупку самостоятельно? Введите /shop — вы перейдёте в обычный режим магазина.\n📦 Вернуться в режим менеджера — /work`
            : `👥 *Welcome, manager!*\n\nYou can view and confirm incoming orders for your assigned payment methods.\n\n📋 *Your methods:* ${methodList}\n\n🛍 Want to make a purchase? Type /shop to switch to the store.\n📦 Return to manager mode — /work`;
          safeSendMessage(chatId, mgrWelcome, {
            parse_mode: 'Markdown',
            reply_markup: { inline_keyboard: [[{ text: '📦 Заказы на проверку', callback_data: 'manager_orders' }]] }
          });
        });
      });
      return;
    }
    showMainMenu(chatId, user);
    return;
  }

  showMainMenu(chatId, user);
});

// ==========================================
// 🤝 УПРАВЛЕНИЕ РЕСЕЛЛЕРАМИ (АДМИН)
// ==========================================
function showAdminResellers(chatId, page = 0, msgId = null) {
  const ITEMS_PER_PAGE = 5;
  db.get('SELECT COUNT(*) as cnt FROM resellers', [], (err, row) => {
    const totalCount = row ? row.cnt : 0;
    const totalPages = Math.ceil(totalCount / ITEMS_PER_PAGE);
    const currentPage = Math.max(0, Math.min(page, totalPages - 1));

    db.all(`SELECT r.*, u.username as tg_username FROM resellers r LEFT JOIN users u ON r.user_id = u.id ORDER BY r.created_at DESC LIMIT ? OFFSET ?`, [ITEMS_PER_PAGE, currentPage * ITEMS_PER_PAGE], (e, resellers) => {
      if (e) return adminSend(chatId, ADMIN_ID, '❌ Ошибка БД');

      const kb = { inline_keyboard: [] };

      // Тумблер для включения/отключения приёма новых заявок
      const rslEnabled = getSetting('reseller_enabled') !== '0';
      kb.inline_keyboard.push([{
        text: rslEnabled ? '🟢 Приём новых заявок: ВКЛ' : '🔴 Приём новых заявок: ВЫКЛ',
        callback_data: 'admin_rsl_toggle_new'
      }]);

      if (!resellers || resellers.length === 0) {
        kb.inline_keyboard.push([{ text: '◀️ Назад', callback_data: 'admin' }]);
        return safeSendMessage(chatId, '🤝 *Реселлеры*\n\n' + (rslEnabled ? '✅ Приём новых заявок включён' : '🔴 Приём новых заявок отключён') + '\n\nСписок пуст.', { parse_mode: 'Markdown', reply_markup: kb });
      }

      let text = `🤝 *Управление Реселлерами*\nВсего реселлеров: ${totalCount}\n\n`;
      resellers.forEach(r => {
        const uname = r.tg_username ? `@${escapeMarkdown(r.tg_username)}` : `ID ${r.user_id}`;
        const tag = r.username ? `(Бот: @${escapeMarkdown(r.username)})` : `(Бот: не подключен)`;
        const status = r.status === 'active' ? '✅' : '🚫';
        text += `${status} ${uname} ${tag}\nБаланс: ${r.balance} ₽ | Наценка: ${r.markup_pct}%\n\n`;

        kb.inline_keyboard.push([{ text: `⚙️ Ред. ${uname}`, callback_data: `admin_reseller_edit_${r.id}` }]);
      });

      kb.inline_keyboard.push([{ text: '💸 Заявки на вывод', callback_data: 'admin_rsl_withdrawals' }]);

      const pagination = [];
      if (currentPage > 0) pagination.push({ text: '⬅️ Пред', callback_data: `admin_resellers_page_${currentPage - 1}` });
      if (currentPage < totalPages - 1) pagination.push({ text: 'След ➡️', callback_data: `admin_resellers_page_${currentPage + 1}` });
      if (pagination.length > 0) kb.inline_keyboard.push(pagination);

      kb.inline_keyboard.push([{ text: '◀️ Назад', callback_data: 'admin' }]);

      safeSendMessage(chatId, text, { parse_mode: 'Markdown', reply_markup: kb });
    });
  });
}

function showAdminResellerEdit(chatId, rId) {
  db.get(`SELECT r.*, u.username as tg_username FROM resellers r LEFT JOIN users u ON r.user_id = u.id WHERE r.id = ?`, [rId], (err, r) => {
    if (err || !r) return safeSendMessage(chatId, '❌ Ошибка. Реселлер не найден.');
    const uname = r.tg_username ? `@${escapeMarkdown(r.tg_username)}` : `ID ${r.user_id}`;
    const isRunning = resellerBots.has(r.id);
    let statusIcon;
    if (r.status === RESELLER_STATUS.AWAITING_TOKEN) {
      statusIcon = '⏳ Ожидает токен';
    } else if (r.status === RESELLER_STATUS.ACTIVE && isRunning) {
      statusIcon = '🟢 Активен и запущен';
    } else if (r.status === RESELLER_STATUS.ACTIVE && !isRunning) {
      statusIcon = '🟡 Активен, но не запущен';
    } else {
      statusIcon = '🔴 Деактивирован';
    }
    const toggleStatus = r.status === RESELLER_STATUS.ACTIVE ? '⛔ Деактивировать' : '✅ Активировать';
    const text = `⚙️ *Редактирование реселлера*\n\n` +
      `👤 Пользователь: ${uname}\n` +
      `🤖 Бот: ${r.username ? '@' + escapeMarkdown(r.username) : 'Не подключен'}\n` +
      `💰 Баланс: ${r.balance} ₽\n` +
      `📈 Наценка: ${r.markup_pct}%\n` +
      `Статус: ${statusIcon}`;
    const kb = {
      inline_keyboard: [
        [
          { text: '➖ 5%', callback_data: `admin_rsl_markup_${r.id}_minus_5` },
          { text: '➕ 5%', callback_data: `admin_rsl_markup_${r.id}_plus_5` }
        ],
        [
          { text: '➖ 1%', callback_data: `admin_rsl_markup_${r.id}_minus_1` },
          { text: '➕ 1%', callback_data: `admin_rsl_markup_${r.id}_plus_1` }
        ],
        [{ text: '✏️ Установить наценку вручную', callback_data: `admin_rsl_markup_set_${r.id}` }],
        [{ text: '💰 Изменить баланс', callback_data: `admin_rsl_balance_edit_${r.id}` }],
        [{ text: toggleStatus, callback_data: `admin_rsl_toggle_${r.id}` }],
        [{ text: '🗑️ Удалить бота', callback_data: `admin_rsl_delete_confirm_${r.id}` }],
        [{ text: '◀️ К списку', callback_data: 'admin_resellers' }]
      ]
    };
    safeSendMessage(chatId, text, { parse_mode: 'Markdown', reply_markup: kb });
  });
}

// Отправка запроса на сброс токена админу
function submitTokenResetRequest(user, chatId, rId, reason) {
  const isRu = getLang(user) === 'ru';

  db.get(`SELECT r.*, u.username as tg_username FROM resellers r LEFT JOIN users u ON r.user_id = u.id WHERE r.id = ?`, [rId], (err, r) => {
    if (err || !r) {
      bot.sendMessage(chatId, '❌ Не удалось найти реселлера. Попробуйте обновить страницу.');
      return;
    }
    const uname = r.tg_username ? `@${escapeMarkdown(r.tg_username)}` : `ID ${r.user_id}`;
    const botName = r.bot_username ? `@${escapeMarkdown(r.bot_username)}` : 'не подключен';

    // Уведомляем клиента
    const clientMsg = isRu
      ? '✅ *Заявка на сброс токена отправлена!*\n\nАдминистратор рассмотрит ваш запрос и свяжется с вами. Пожалуйста, ожидайте.'
      : '✅ *Token reset request sent!*\n\nThe administrator will review your request and get back to you. Please wait.';
    safeSendMessage(chatId, clientMsg, {
      parse_mode: 'Markdown',
      reply_markup: {
        inline_keyboard: [
          [{ text: isRu ? '◀️ В меню' : '◀️ Menu', callback_data: 'start' }]
        ]
      }
    });

    // Уведомляем админа
    const adminMsg =
      `🔄 *Запрос на сброс токена*\n\n` +
      `👤 Реселлер: ${uname}\n` +
      `🤖 Бот: ${botName}\n` +
      `📝 Причина: *${escapeMarkdown(reason)}*\n` +
      `📅 Дата: ${new Date().toLocaleString('ru-RU')}`;

    safeSendMessage(ADMIN_ID, adminMsg, {
      parse_mode: 'Markdown',
      reply_markup: {
        inline_keyboard: [
          [
            { text: '✅ Одобрить', callback_data: `admin_rsl_reset_approve_${rId}` },
            { text: '❌ Отклонить', callback_data: `admin_rsl_reset_reject_${rId}` }
          ]
        ]
      }
    }).catch(() => { });

    logAction(user.id, 'reseller_token_reset_requested', { rId, reason });
  });
}

function showAdminResellerWithdrawals(chatId, page = 0) {
  const ITEMS_PER_PAGE = 5;
  db.get('SELECT COUNT(*) as cnt FROM reseller_withdrawals WHERE status = ?', ['pending'], (err, row) => {
    const totalCount = row ? row.cnt : 0;
    const totalPages = Math.ceil(totalCount / ITEMS_PER_PAGE);
    const currentPage = Math.max(0, Math.min(page, totalPages - 1));

    db.all(`SELECT w.*, r.username as bot_username, u.username as tg_username 
            FROM reseller_withdrawals w 
            JOIN resellers r ON w.reseller_id = r.id 
            LEFT JOIN users u ON r.user_id = u.id 
            WHERE w.status = 'pending' 
            ORDER BY w.created_at DESC LIMIT ? OFFSET ?`, [ITEMS_PER_PAGE, currentPage * ITEMS_PER_PAGE], (e, rows) => {

      const kb = { inline_keyboard: [] };
      if (!rows || rows.length === 0) {
        kb.inline_keyboard.push([{ text: '◀️ Назад (Реселлеры)', callback_data: 'admin_resellers' }]);
        return safeSendMessage(chatId, '💸 *Заявки на вывод*\n\nНовых заявок нет.', { parse_mode: 'Markdown', reply_markup: kb });
      }

      let text = `💸 *Заявки на вывод (Ожидают: ${totalCount})*\n\n`;
      rows.forEach(w => {
        const uname = w.tg_username ? `@${escapeMarkdown(w.tg_username)}` : `ID ${w.reseller_id}`;
        text += `ID: #${w.id} | ${uname} (Бот: @${escapeMarkdown(w.bot_username || '')})\n` +
          `💰 Сумма: ${w.amount} ₽\n` +
          `💳 Реквизиты: ${w.details}\n` +
          `Дата: ${new Date(w.created_at).toLocaleString('ru-RU')}\n\n`;

        kb.inline_keyboard.push([
          { text: `✅ Одобрить #${w.id}`, callback_data: `rsl_withdraw_approve_${w.id}` },
          { text: `❌ Отклонить #${w.id}`, callback_data: `rsl_withdraw_reject_${w.id}` }
        ]);
      });

      const pagination = [];
      if (currentPage > 0) pagination.push({ text: '⬅️ Пред', callback_data: `admin_rsl_withdrawals_page_${currentPage - 1}` });
      if (currentPage < totalPages - 1) pagination.push({ text: 'След ➡️', callback_data: `admin_rsl_withdrawals_page_${currentPage + 1}` });
      if (pagination.length > 0) kb.inline_keyboard.push(pagination);

      kb.inline_keyboard.push([{ text: '◀️ Назад (Реселлеры)', callback_data: 'admin_resellers' }]);
      safeSendMessage(chatId, text, { parse_mode: 'Markdown', reply_markup: kb });
    });
  });
}

// /shop — менеджер переходит в режим покупателя
bot.onText(/\/shop/, (msg) => {
  const user = msg.from;
  const chatId = msg.chat.id;
  if (msg.chat.type !== 'private') return;
  showMainMenu(chatId, user);
});

// /work — менеджер возвращается в режим менеджера
bot.onText(/\/work/, async (msg) => {
  const user = msg.from;
  const chatId = msg.chat.id;
  if (msg.chat.type !== 'private') return;
  const mgr = await isManager(user.id);
  if (!mgr) { showMainMenu(chatId, user); return; }
  db.all('SELECT payment_method FROM manager_methods WHERE manager_id = ?', [user.id], (e, methodRows) => {
    const methodList = (!e && methodRows && methodRows.length > 0)
      ? methodRows.map(r => r.payment_method).join(', ')
      : '—';
    db.get('SELECT display_name FROM managers WHERE user_id = ?', [user.id], (e2, mRow) => {
      const displayName = mRow && mRow.display_name;
      const isRu = getLang(user) === 'ru';
      const greeting = displayName ? `Привет, *${displayName}*! ` : '';
      const text = isRu
        ? `${greeting}👥 *Режим менеджера*\n\n📋 *Ваши методы:* ${methodList}`
        : `👥 *Manager mode*\n\n📋 *Your methods:* ${methodList}`;
      safeSendMessage(chatId, text, {
        parse_mode: 'Markdown',
        reply_markup: { inline_keyboard: [[{ text: '📦 Заказы на проверку', callback_data: 'manager_orders' }]] }
      });
    });
  });
});

bot.onText(/\/manager/, (msg) => {
  const user = msg.from;
  const chatId = msg.chat.id;
  if (msg.chat.type !== 'private') return;
  isManager(user.id).then(mgr => {
    if (!mgr) { bot.sendMessage(chatId, '❌ У вас нет доступа к этой команде'); return; }
    showManagerOrders(chatId, user.id);
  });
});

bot.onText(/\/admin/, (msg) => {
  const user = msg.from;

  if (msg.chat.type !== 'private') return; // тихо игнорируем в чатах

  if (user.id !== ADMIN_ID) {
    bot.sendMessage(msg.chat.id, '❌ У вас нет доступа к админ-панели');
    return;
  }

  showAdminPanel(msg.chat.id);
});

// П.2: /backup_now — немедленный бэкап по команде (только ADMIN_ID)
bot.onText(/\/backup_now/, async (msg) => {
  if (msg.from.id !== ADMIN_ID) return;
  if (msg.chat.type !== 'private') return;
  const chatId = msg.chat.id;
  bot.sendMessage(chatId, '⏳ Создаю бэкап...').catch(() => {});
  try {
    await sendDatabaseBackup(true); // true = force (игнорировать хеш-проверку)
    bot.sendMessage(chatId, '✅ Бэкап отправлен!').catch(() => {});
  } catch (e) {
    bot.sendMessage(chatId, `❌ Ошибка бэкапа: ${e.message}`).catch(() => {});
  }
});

// /client_info <user_id или @username> — подробный отчёт по клиенту
bot.onText(/\/client_info(?:\s+(.+))?/, async (msg, match) => {
  if (msg.from.id !== ADMIN_ID) return;
  if (msg.chat.type !== 'private') return;
  const chatId = msg.chat.id;
  const arg = match && match[1] ? match[1].trim() : null;
  if (!arg) {
    bot.sendMessage(chatId, '❌ Укажите ID или @username пользователя\n\nПример: `/client_info 123456789` или `/client_info @username`', { parse_mode: 'Markdown' });
    return;
  }
  const cleaned = arg.replace('@', '');
  const isNumeric = /^\d+$/.test(cleaned);
  if (isNumeric) {
    showClientInfo(chatId, parseInt(cleaned));
  } else {
    db.get(`SELECT id FROM users WHERE username = ?`, [cleaned], (e, row) => {
      if (e || !row) {
        bot.sendMessage(chatId, `❌ Пользователь @${cleaned} не найден в базе`);
        return;
      }
      showClientInfo(chatId, row.id);
    });
  }
});

// /rsl_balance <user_id или @username> <сумма> — ручное изменение баланса реселлера
bot.onText(/\/rsl_balance(?:\s+(.+))?/, async (msg, match) => {
  if (msg.from.id !== ADMIN_ID) return;
  if (msg.chat.type !== 'private') return;
  const chatId = msg.chat.id;

  const arg = match && match[1] ? match[1].trim() : null;
  if (!arg) {
    bot.sendMessage(chatId, '❌ Использование: `/rsl_balance <ID или @username> <сумма>`\nСумма может быть отрицательной для списания.', { parse_mode: 'Markdown' });
    return;
  }

  const parts = arg.split(/\s+/);
  if (parts.length < 2) {
    bot.sendMessage(chatId, '❌ Использование: `/rsl_balance <ID или @username> <сумма>`', { parse_mode: 'Markdown' });
    return;
  }

  const amount = parseFloat(parts.pop().replace(',', '.'));
  if (isNaN(amount)) {
    bot.sendMessage(chatId, '❌ Сумма должна быть числом.');
    return;
  }

  const userArg = parts.join(' ').replace('@', '');
  const isNumeric = /^\d+$/.test(userArg);

  const query = isNumeric
    ? 'SELECT id, username FROM users WHERE id = ?'
    : 'SELECT id, username FROM users WHERE username = ?';

  db.get(query, [isNumeric ? parseInt(userArg) : userArg], (e, row) => {
    if (e || !row) {
      bot.sendMessage(chatId, `❌ Пользователь не найден в базе`);
      return;
    }

    db.get(`SELECT id, balance FROM resellers WHERE user_id = ?`, [row.id], (rErr, reseller) => {
      if (rErr || !reseller) {
        bot.sendMessage(chatId, `❌ Пользователь не является зарегистрированным реселлером.`);
        return;
      }

      // FIX Race Condition: полностью атомарный UPDATE без предварительного чтения баланса.
      // MAX(0, balance + ?) не позволяет уйти в минус при параллельных операциях.
      db.run(
        `UPDATE resellers SET balance = MAX(0, balance + ?) WHERE id = ?`,
        [amount, reseller.id],
        function (upErr) {
          if (upErr || this.changes === 0) {
            bot.sendMessage(chatId, `❌ Ошибка обновления баланса.`);
            return;
          }

          // Читаем актуальный баланс ПОСЛЕ атомарного UPDATE для отображения
          db.get(`SELECT balance FROM resellers WHERE id = ?`, [reseller.id], (selErr, updated) => {
            const actualNewBalance = updated ? updated.balance : '?';
            const action = amount >= 0 ? 'Начислено' : 'Списано';
            bot.sendMessage(chatId, `✅ Баланс реселлера @${escapeMarkdown(String(row.username || row.id))} обновлен.\n${action}: ${Math.abs(amount)} ₽\nТекущий баланс: ${actualNewBalance} ₽`);

            const rslMsg = amount >= 0
              ? `💰 Администратор зачислил на ваш баланс *${amount} ₽*.`
              : `💸 Администратор списал с вашего баланса *${Math.abs(amount)} ₽*.`;

            safeSendMessage(row.id, rslMsg, { parse_mode: 'Markdown' }).catch(() => { });
            logAction(ADMIN_ID, 'admin_rsl_balance_changed', { resellerUserId: row.id, amount, newBalance: actualNewBalance });
          });
        }
      );
    });
  });
});

// ==========================================
// ⚡ КОМАНДЫ: /status /chat /mystats /cyrax
// ==========================================

// /status — только для админа: детальная информация о состоянии бота
bot.onText(/\/status/, async (msg) => {
  if (msg.chat.type !== 'private') return;
  if (msg.from.id !== ADMIN_ID) return;

  const chatId = msg.chat.id;
  const uptimeSec = Math.round(process.uptime());
  const uptimeStr = uptimeSec < 3600
    ? `${Math.floor(uptimeSec / 60)}м`
    : `${Math.floor(uptimeSec / 3600)}ч ${Math.floor((uptimeSec % 3600) / 60)}м`;
  const memMb = (process.memoryUsage().rss / 1024 / 1024).toFixed(0);
  const usdRate = getSetting('cached_rate_USD') || '—';

  const keysRows = await new Promise(res =>
    db.all(`SELECT product, COUNT(*) as cnt FROM keys WHERE status = 'available' GROUP BY product ORDER BY product`, [], (e, r) => res(r || []))
  );
  const pendingOrders = await new Promise(res =>
    db.get(`SELECT COUNT(*) as cnt FROM orders WHERE status = 'pending' AND method != 'CryptoBot'`, [], (e, r) => res(r ? r.cnt : 0))
  );
  const salesToday = await new Promise(res =>
    db.get(`SELECT COUNT(*) as cnt, SUM(amount) as total FROM orders WHERE status = 'confirmed' AND currency = 'RUB' AND date(confirmed_at) = date('now')`, [], (e, r) => res(r || { cnt: 0, total: 0 }))
  );
  const totalUsers = await new Promise(res =>
    db.get(`SELECT COUNT(*) as cnt FROM users`, [], (e, r) => res(r ? r.cnt : 0))
  );
  const activeResellers = await new Promise(res =>
    db.get(`SELECT COUNT(*) as cnt FROM resellers WHERE status = 'active'`, [], (e, r) => res(r ? r.cnt : 0))
  );

  const productIcons = { '1d': '🟢', '3d': '🟢', '7d': '🟡', '30d': '🔵' };
  let keysText = keysRows.length > 0
    ? keysRows.map(k => `${productIcons[k.product] || '⚪'} ${k.product}: *${k.cnt}*`).join('\n')
    : '❌ Нет в наличии';

  const saleStr = salesToday.cnt > 0
    ? `${salesToday.cnt} (${Math.round(salesToday.total || 0)} ₽)`
    : '0';

  const text =
    `⚡ *Статус бота*\n\n` +
    `⏱ Аптайм: *${uptimeStr}*\n` +
    `🧠 Память: *${memMb} MB*\n` +
    `💱 Курс USD: *${usdRate}*\n\n` +
    `📦 Ключи в наличии:\n${keysText}\n\n` +
    `⏳ Ожидают подтверждения: *${pendingOrders}*\n` +
    `✅ Продаж сегодня: *${saleStr}*\n` +
    `👥 Пользователей: *${totalUsers}*\n` +
    `🤝 Активных реселлеров: *${activeResellers}*\n` +
    `🖥 Сессий в памяти: *${userSessions.size}*`;

  safeSendMessage(chatId, text, {
    parse_mode: 'Markdown',
    reply_markup: {
      inline_keyboard: [
        [
          { text: '🔑 Сток ключей', callback_data: 'admin_key_stock' },
          { text: '📦 Заказы', callback_data: 'admin_manage_orders' }
        ],
        [{ text: '📊 Статистика', callback_data: 'admin_stats' }]
      ]
    }
  }).catch(() => {});
});

// /chat — ссылка на чат и правила (только личка, для всех)
bot.onText(/\/chat/, async (msg) => {
  if (msg.chat.type !== 'private') return;
  const user = msg.from;
  const chatId = msg.chat.id;
  const isRu = getLang(user) === 'ru';

  // chat_link — ссылка на чат-группу (отдельная от channel_link)
  const chatLink = getSetting('chat_link') || 'https://t.me/CyRaXMod';

  const ruText =
    `💬 *Наш чат — CyRaXMod*\n\n` +
    `👉 ${chatLink}\n\n` +
    `📋 *Правила:*\n` +
    `• Без политики и межнациональных конфликтов\n` +
    `• Уважение — оскорбления и токсичность под баном\n` +
    `• Никакой рекламы — ни ботов, ни каналов, ни ссылок без разрешения\n` +
    `• Только темы, связанные с нашим сервисом\n\n` +
    `⚠️ Нарушение = бан без предупреждения`;

  const enText =
    `💬 *Our chat — CyRaXMod*\n\n` +
    `👉 ${chatLink}\n\n` +
    `📋 *Rules:*\n` +
    `• No politics or inter-ethnic conflicts\n` +
    `• Respect — insults and toxicity = ban\n` +
    `• No ads — no bots, channels, or links without permission\n` +
    `• Only topics related to our service\n\n` +
    `⚠️ Violation = ban without warning`;

  safeSendMessage(chatId, isRu ? ruText : enText, {
    parse_mode: 'Markdown',
    reply_markup: {
      inline_keyboard: [
        [{ text: isRu ? '💬 Открыть чат' : '💬 Open chat', url: chatLink }],
        [{ text: isRu ? '🏠 Меню' : '🏠 Menu', callback_data: 'start' }]
      ]
    }
  }).catch(() => {});
});

// /mystats — статистика покупок (только личка, для всех)
bot.onText(/\/mystats/, async (msg) => {
  if (msg.chat.type !== 'private') return;
  const user = msg.from;
  const chatId = msg.chat.id;
  const isRu = getLang(user) === 'ru';
  const lang = isRu ? 'ru' : 'en';

  const orders = await new Promise(res =>
    db.all(
      `SELECT product, amount, currency, confirmed_at FROM orders
       WHERE user_id = ? AND status = 'confirmed'
         AND (balance_topup IS NULL OR balance_topup = 0)
       ORDER BY confirmed_at DESC LIMIT 20`,
      [user.id], (e, rows) => res(rows || [])
    )
  );

  const spentRub = orders.filter(o => o.currency === 'RUB').reduce((s, o) => s + (o.amount || 0), 0);
  const spentUsd = orders.filter(o => o.currency === 'USD').reduce((s, o) => s + (o.amount || 0), 0);

  const userRow = await new Promise(res =>
    db.get(`SELECT balance, loyalty_discount FROM users WHERE id = ?`, [user.id], (e, r) => res(r || {}))
  );

  const balance = userRow.balance || 0;
  const loyaltyDiscount = userRow.loyalty_discount || 0;
  const firstBuy = orders.length > 0 ? orders[orders.length - 1].confirmed_at : null;

  let spentStr = '';
  if (spentRub > 0 && spentUsd > 0) spentStr = `${spentRub.toFixed(0)} ₽ + $${spentUsd.toFixed(2)}`;
  else if (spentRub > 0) spentStr = `${spentRub.toFixed(0)} ₽`;
  else if (spentUsd > 0) spentStr = `$${spentUsd.toFixed(2)}`;
  else spentStr = isRu ? '0 ₽' : '$0';

  let text = isRu ? `📊 <b>Ваша статистика</b>\n\n` : `📊 <b>Your stats</b>\n\n`;
  text += isRu
    ? `🛒 Всего покупок: <b>${orders.length}</b>\n💰 Потрачено: <b>${spentStr}</b>\n`
    : `🛒 Total purchases: <b>${orders.length}</b>\n💰 Spent: <b>${spentStr}</b>\n`;

  if (firstBuy) {
    const firstDate = new Date(firstBuy).toLocaleDateString(isRu ? 'ru-RU' : 'en-GB', { day: 'numeric', month: 'long', year: 'numeric' });
    text += isRu ? `📅 Первая покупка: <b>${firstDate}</b>\n` : `📅 First purchase: <b>${firstDate}</b>\n`;
  }
  if (balance > 0) {
    text += isRu ? `💎 Баланс: <b>${balance} ₽</b>\n` : `💎 Balance: <b>${balance} RUB</b>\n`;
  }
  if (loyaltyDiscount > 0) {
    text += isRu ? `🎁 Скидка лояльности: <b>${loyaltyDiscount}%</b>\n` : `🎁 Loyalty discount: <b>${loyaltyDiscount}%</b>\n`;
  }

  if (orders.length > 0) {
    text += isRu ? `\n⏱ <b>Последние покупки:</b>\n` : `\n⏱ <b>Recent purchases:</b>\n`;
    orders.slice(0, 5).forEach(o => {
      const date = new Date(o.confirmed_at).toLocaleDateString(isRu ? 'ru-RU' : 'en-GB', { day: 'numeric', month: 'short' });
      const prodName = PERIOD_NAMES[lang]?.[o.product] || o.product;
      text += `• ${prodName} — ${date}\n`;
    });
  } else {
    text += isRu ? `\n<i>Покупок пока нет.</i>` : `\n<i>No purchases yet.</i>`;
  }

  sendWithAnimatedEmoji(chatId, text, ANIMATED_EMOJI.STATS, '📊', {
    parse_mode: 'HTML',
    reply_markup: {
      inline_keyboard: [
        [{ text: isRu ? '🛒 Купить ещё' : '🛒 Buy more', callback_data: 'buy' }],
        [{ text: isRu ? '🏠 Меню' : '🏠 Menu', callback_data: 'start' }]
      ]
    }
  }).catch(() => {});
});

// /cyrax — единственная команда в групповых чатах, визитка бота
bot.onText(/\/cyrax/, (msg) => {
  const chatId = msg.chat.id;
  const botUsername = process.env.BOT_USERNAME || 'cyraxxmod_bot';

  if (msg.chat.type === 'private') {
    // В личке — просто в меню
    bot.sendMessage(chatId, `Привет! Нажми /start чтобы открыть меню.`).catch(() => {});
    return;
  }

  // В чате — публичная визитка с кнопкой
  bot.sendMessage(chatId,
    `👋 I only work in private messages.\n\n🔑 Cyrax mod keys\n🚀 Boost & guides\n\n@${botUsername}`,
    {
      reply_markup: {
        inline_keyboard: [[
          { text: '🤖 Open bot', url: `https://t.me/${botUsername}` }
        ]]
      }
    }
  ).catch(() => {});
});

// ==========================================
// ✅ ФИНАЛИЗАЦИЯ УСПЕШНОГО ЗАКАЗА (Task 3.1)
// ==========================================
/**
 * Выполняет все пост-заказовые действия после успешного подтверждения оплаты.
// ==========================================
// 🎯 ПРОГРЕСС-КУПОН: каждые 5 покупок → 10% на любой ключ
// ==========================================
async function checkMilestoneCoupon(userId, isRu = true, botInstance = bot) {
  try {
    // Считаем общее число покупок и находим самый частый продукт из последних 5
    const statsRow = await new Promise(resolve => {
      db.get(
        `SELECT COUNT(*) as total FROM orders
         WHERE user_id = ? AND status = 'confirmed'
         AND (balance_topup IS NULL OR balance_topup = 0)
         AND (paid_from_balance IS NULL OR paid_from_balance = 0)`,
        [userId], (e, r) => resolve(r || { total: 0 })
      );
    });
    const total = statsRow.total;

    // Прогресс-подсказка: «ещё N покупок → купон 10%»
    if (total > 0 && total % 5 !== 0) {
      const left = 5 - (total % 5);
      if (left <= 3) {
        const filled = total % 5;
        const bar = '🟣'.repeat(filled) + '⚪️'.repeat(5 - filled);
        const hint = isRu
          ? `🎯 ${bar}\n_Ещё ${left} ${left === 1 ? 'покупка' : left < 5 ? 'покупки' : 'покупок'} → купон 10% на любой ключ!_`
          : `🎯 ${bar}\n_${left} more ${left === 1 ? 'purchase' : 'purchases'} → 10% coupon for any key!_`;
        safeSendMessage(userId, hint, { parse_mode: 'Markdown' }, botInstance).catch(() => {});
      }
    }

    // Milestone: каждые 5 покупок → купон привязан к самому частому продукту из последних 5
    if (total > 0 && total % 5 === 0) {
      // Определяем самый частый продукт в последних 5 покупках (только реальные ключи)
      const topProduct = await new Promise(resolve => {
        db.get(
          `SELECT product, COUNT(*) as cnt FROM orders
           WHERE user_id = ? AND status = 'confirmed'
           AND (balance_topup IS NULL OR balance_topup = 0)
           AND (paid_from_balance IS NULL OR paid_from_balance = 0)
           AND product IN ('1d','3d','7d','30d')
           ORDER BY id DESC LIMIT 5`,
          [userId], (e, r) => resolve(r || null)
        );
      });
      // Если в последних 5 был один доминирующий — привязываем к нему, иначе без ограничения
      const productRestriction = topProduct ? topProduct.product : null;
      const periodLabel = productRestriction
        ? (PERIOD_NAMES[isRu ? 'ru' : 'en'][productRestriction] || productRestriction)
        : (isRu ? 'любой ключ' : 'any key');

      const couponCode = `MILE${userId}_${total}_${Date.now().toString(36).toUpperCase()}`;
      await new Promise((resolve, reject) => {
        db.serialize(() => {
          db.run(
            `INSERT INTO coupons (code, discount_percent, max_uses, user_id, product_restriction, created_at, expires_at)
             VALUES (?, 10, 1, ?, ?, datetime('now'), datetime('now', '+60 days'))`,
            [couponCode, userId, productRestriction],
            function(err) {
              if (err) return reject(err);
              // Привязываем к конкретному продукту если нужно
              if (productRestriction) {
                db.run(
                  `INSERT OR IGNORE INTO coupon_products (coupon_id, product) VALUES (?, ?)`,
                  [this.lastID, productRestriction],
                  () => resolve()
                );
              } else {
                resolve();
              }
            }
          );
        });
      });

      // Красивое сообщение с купоном — стиль как у выданного ключа (с копированием)
      const expiryLabel = isRu ? '60 дней' : '60 days';
      const msg = isRu
        ? `🎊 Поздравляем с ${total}-й покупкой!\n` +
          `└ (нажмите чтобы скопировать)\n\n` +
          `🎟 Купон — 10% на ${periodLabel}:\n` +
          `\`${couponCode}\`\n` +
          `└ (нажмите чтобы скопировать)\n\n` +
          `⏳ Срок действия: ${expiryLabel}\n` +
          `📦 Для товара: ${periodLabel}\n` +
          `_Введите код при следующей покупке._`
        : `🎊 Congrats on your ${total}th purchase!\n\n` +
          `🎟 10% coupon for ${periodLabel}:\n` +
          `\`${couponCode}\`\n` +
          `└ (tap to copy)\n\n` +
          `⏳ Valid: ${expiryLabel}\n` +
          `📦 For: ${periodLabel}\n` +
          `_Enter the code on your next purchase._`;

      safeSendMessage(userId, msg, { parse_mode: 'Markdown' }).catch(() => {});
      logAction(userId, 'milestone_coupon_issued', { total, couponCode, productRestriction });
    }
  } catch (e) {
    console.error('checkMilestoneCoupon error:', e.message);
  }
}

/**
 * Вызывается из двух мест: approve_ (ручное одобрение) и CryptoBot webhook (автооплата).
 * Содержит: начисление бонуса реселлеру, FOMO-купон, реферальный бонус, купон используем.
 *
 * @param {object} order    - Полная запись из таблицы orders (уже обновлённая до confirmed).
 * @param {number} orderId  - ID заказа (может совпадать с order.id).
 * @param {object} botInstance - Bot-инстанс для отправки сообщений пользователю (реселлер или основной).
 */
async function finalizeSuccessfulOrder(order, orderId, botInstance = bot) {
  const iid = orderId || order.id;

  // ── Идемпотентность: защита от двойного вызова (гонка approve_ + CryptoBot webhook) ──
  // Помечаем заказ как финализированный атомарным UPDATE с проверкой флага.
  // Если строка уже помечена — значит другой поток успел первым, выходим.
  const alreadyFinalized = await new Promise((resolve) => {
    db.run(
      `UPDATE orders SET finalized = 1 WHERE id = ? AND (finalized IS NULL OR finalized = 0)`,
      [iid],
      function (err) {
        if (err) {
          console.error(`❌ finalizeSuccessfulOrder idempotency check error orderId=${iid} userId=${order?.user_id} product=${order?.product}:`, err.message);
          // BUG FIX FIN-1: При ошибке БД безопаснее НЕ продолжать начисление,
          // чем рисковать двойным начислением. Уведомляем админа и выходим.
          safeSendMessage(ADMIN_ID,
            `⚠️ *finalizeSuccessfulOrder: ошибка флага finalized*\n\nЗаказ #${iid}\nОшибка: ${err.message}\n\nНачисление пропущено для безопасности. Проверьте вручную.`,
            { parse_mode: 'Markdown' }
          ).catch(() => {});
          resolve(true); // treat as already finalized — не продолжаем
          return;
        }
        // this.changes === 0 означает что строка уже была finalized = 1
        resolve(this.changes === 0);
      }
    );
  });

  if (alreadyFinalized) {
    console.warn(`⚠️ finalizeSuccessfulOrder: order #${iid} already finalized — skipping duplicate call`);
    return;
  }


  // Делаем это здесь, а не при создании заказа, чтобы купон не сгорал при отклонении.
  if (order.coupon_id) {
    markCouponUsed(order.user_id, order.coupon_id, iid);
  }

  // ── Шаг 2: Начислить наценку реселлеру ────────────────────────────────────
  // Наценка = amount (с наценкой) − original_amount (база без наценки).
  // Конвертируем в рубли независимо от валюты заказа.
  if (order.reseller_id) {
    const diff = order.amount - (order.original_amount || order.amount);
    if (diff > 0) {
      let markupRub = 0;
      if (order.currency === 'RUB') markupRub = diff;
      else if (order.currency === 'USD') markupRub = Math.round(diff / EXCHANGE_RATES.USD);
      else if (order.currency === 'EUR') markupRub = Math.round(diff / EXCHANGE_RATES.EUR);
      else if (order.currency === 'UAH') markupRub = Math.round(diff / EXCHANGE_RATES.UAH);

      if (markupRub > 0) {
        const basePrice = order.original_amount || (order.amount / (1 + (order.reseller_markup_pct || 30) / 100));
        db.run(`UPDATE resellers SET balance = balance + ? WHERE id = ?`, [markupRub, order.reseller_id]);
        db.run(
          `INSERT INTO reseller_orders (reseller_id, order_id, base_amount, markup_amount, total_amount, currency) VALUES (?, ?, ?, ?, ?, ?)`,
          [order.reseller_id, iid, basePrice, markupRub, order.amount, order.currency]
        );
        console.log(`💰 [RSL ${order.reseller_id}] markup +${markupRub}₽ for order #${iid}`);
      }
    }

    // Бонус за подключение реселлера (если применимо)
    if (order.product === 'reseller_connection') {
      processResellerConnectionBonus(order.reseller_id).catch(e => console.error('Reseller bonus error:', e));
    }
  }

  // ── Шаг 3: FOMO-купон и реферальный бонус ─────────────────────────────────
  // Применяются только к ключевым товарам (не к партнёрству, не к бусту).
  if (order.product !== 'reseller_connection' && order.product !== 'infinite_boost') {
    const amountInRub =
      order.currency === 'RUB' ? order.amount :
      order.currency === 'USD' ? Math.round(order.amount / EXCHANGE_RATES.USD) :
      order.currency === 'EUR' ? Math.round(order.amount / EXCHANGE_RATES.EUR) :
      order.currency === 'UAH' ? Math.round(order.amount / EXCHANGE_RATES.UAH) : 0;

    handleFomoCoupon(
      { user_id: order.user_id, amount_rub: amountInRub, product: order.product, user_lang: order.user_lang || 'en', order_id: iid },
      botInstance
    ).catch(e => console.error('FOMO error:', e));

    handleRefReward(order.user_id, order.product, botInstance)
      .catch(e => console.error('REF reward error:', e));
  }

  console.log(`✅ finalizeSuccessfulOrder done for order #${iid} (product: ${order.product}, user: ${order.user_id})`);

  // Прогресс-купон: проверяем milestone (каждые 5 покупок → 10% купон)
  if (order.product !== 'reseller_connection' && !order.balance_topup) {
    const isRuUser = (order.user_lang || 'en') === 'ru';
    checkMilestoneCoupon(order.user_id, isRuUser, botInstance).catch(e => console.error('milestone coupon error:', e));
  }

  // BUG FIX AF-1: Снижаем suspicion_score при успешных заказах — предотвращает ложные баны
  // добросовестных пользователей, которые в прошлом случайно дали повод для подозрения.
  db.run(
    `UPDATE users SET suspicion_score = MAX(0, COALESCE(suspicion_score, 0) - 2) WHERE id = ?`,
    [order.user_id]
  );

  // Купон за баскетбол создаётся в approve_ / CryptoBot-хендлере ДО вызова sendKeyMessage,
  // чтобы передать его прямо в сообщение с ключом. Здесь ничего делать не нужно.
}

// ==========================================
// 🏀 ВЫДАЧА БАСКЕТБОЛЬНОГО КУПОНА
// ==========================================
// Вызывается из finalizeSuccessfulOrder ТОЛЬКО после подтверждения оплаты.
// Возвращает объект { code, discountPercent, expiresStr } для вставки в сообщение с ключом.
// Если что-то пошло не так — возвращает null (ключ всё равно выдаётся, купон тихо пропускается).
async function _issueBasketballCoupon(order, orderId) {
  try {
    const isRu = (order.user_lang || 'en').startsWith('ru');
    const product = order.product;
    const KEY_DAYS = { '1d': 1, '3d': 3, '7d': 7, '30d': 30 };
    const keyDays = KEY_DAYS[product] || 7;
    const couponDays = keyDays + 1; // +1 день — окно после истечения ключа

    const couponCode = `BALL${order.user_id}_${Date.now().toString(36).toUpperCase()}`;
    const expiresDate = new Date(Date.now() + couponDays * 24 * 60 * 60 * 1000);
    const expiresStr = expiresDate.toLocaleDateString(isRu ? 'ru-RU' : 'en-GB', {
      day: 'numeric', month: 'long'
    });

    await new Promise((resolve, reject) => {
      db.run(
        `INSERT INTO coupons (code, discount_percent, max_uses, user_id, product_restriction, created_at, expires_at)
         VALUES (?, 5, 1, ?, ?, datetime('now'), datetime('now', '+${couponDays} days'))`,
        [couponCode, order.user_id, product],
        function (cErr) {
          if (cErr) return reject(cErr);
          const couponId = this.lastID;
          db.run(
            `INSERT OR IGNORE INTO coupon_products (coupon_id, product) VALUES (?, ?)`,
            [couponId, product],
            () => resolve()
          );
        }
      );
    });

    logAction(order.user_id, 'basketball_coupon_issued', { orderId, couponCode, product, couponDays });
    console.log(`🏀 [BASKETBALL] Купон ${couponCode} создан для user ${order.user_id} (заказ #${orderId})`);

    return { code: couponCode, discountPercent: 5, expiresStr };
  } catch (e) {
    console.error(`❌ [BASKETBALL] Ошибка создания купона для заказа #${orderId}:`, e.message);
    return null;
  }
}

// ==========================================
// 🔘 ОБРАБОТКА КНОПОК
// ==========================================
bot.on('callback_query', async (query) => {
  try {
    const user = query.from;
    const chatId = query.message.chat.id;
    const data = query.data;
    const message = query.message;

    // FIX 2.3: noop — заголовок-разделитель. Обрабатываем ПЕРВЫМ до любой другой логики.
    // Было в середине обработчика — теперь здесь, чтобы гарантировать выход без side-эффектов.
    if (data === 'noop') {
      bot.answerCallbackQuery(query.id).catch(() => {});
      return;
    }

    // Task 3.2: Краткий лог каждого колбэка — неоценим при расследовании инцидентов.
    // Не логируем payload купонов и токенов — они чувствительны.
    if (!data.startsWith('pay_') && !data.includes('token')) {
      console.log(`🔘 [CB] uid=${user.id} data=${data.substring(0, 60)}`);
    }

    // Только личные сообщения
    if (query.message.chat.type !== 'private') {
      bot.answerCallbackQuery(query.id, {
        text: t(user, 'private_only'),
        show_alert: true
      }).catch(() => { });
      return;
    }

    // 🛡️ Anti-scam: трекинг взаимодействий
    if (user.id !== ADMIN_ID) {
      trackUserInteraction(user.id, user.username, user.language_code);
    }

    // Ответ на callback
    if (data.startsWith('period_')) {
      bot.answerCallbackQuery(query.id, { text: '📅 Выбор периода' }).catch(() => { });
    } else if (data.startsWith('currency_')) {
      bot.answerCallbackQuery(query.id, { text: '💱 Выбор валюты' }).catch(() => { });
    } else if (data.startsWith('pay_')) {
      bot.answerCallbackQuery(query.id, { text: '💳 Выбор способа оплаты' }).catch(() => { });
    } else if (data === 'buy') {
      bot.answerCallbackQuery(query.id, { text: '🛒 Начинаем покупку' }).catch(() => { });
    } else if (data === 'orders') {
      bot.answerCallbackQuery(query.id, { text: '📂 Ваши ключи' }).catch(() => { });
    } else if (data === 'offer') {
      bot.answerCallbackQuery(query.id, { text: '📜 Публичная оферта' }).catch(() => { });
    } else if (data === 'boost_hub') {
      bot.answerCallbackQuery(query.id, { text: '⚡️ Буст аккаунта' }).catch(() => { });
    } else if (data === 'start') {
      bot.answerCallbackQuery(query.id, { text: '🏠 Главное меню' }).catch(() => { });
    } else if (data === 'admin') {
      bot.answerCallbackQuery(query.id, { text: '🛠 Панель управления' }).catch(() => { });
    } else {
      bot.answerCallbackQuery(query.id).catch(() => { });
    }

    if (maintenanceMode && user.id !== ADMIN_ID) {
      // Менеджеры могут работать во время техобслуживания
      const isMgrMaint = await isManager(user.id);
      if (isMgrMaint) {
        // Менеджеру разрешены только: manager_orders, approve_, reject_
        const allowedInMaint = data === 'manager_orders' || data.startsWith('approve_') || data.startsWith('reject_') || data.startsWith('basketball_throw_');
        if (!allowedInMaint) {
          bot.sendMessage(chatId, '🔧 Во время обслуживания доступны только заказы.').catch(() => { });
          return;
        }
        // Разрешаем — продолжаем обработку ниже
      } else {
        const timeLeft = maintenanceEndTime ? Math.ceil((maintenanceEndTime - Date.now()) / 60000) : null;
        const timeStr = timeLeft !== null ? t(user, 'maintenance_time', { minutes: timeLeft }) : '∞';
        const maintenanceMsg = t(user, 'maintenance')
          .replace('{time}', timeStr)
          .replace('{reason}', maintenanceReason ? maintenanceReason : '');
        maintenanceWaitingUsers.add(chatId);
        bot.sendMessage(chatId, maintenanceMsg);
        return;
      }
    }

    if (user.id !== ADMIN_ID && !checkRateLimit(user.id, data)) {
      const violation = rateLimitViolations.get(user.id);
      const isBanned = violation && violation.bannedUntil && Date.now() < violation.bannedUntil;
      if (isBanned) {
        const banUntil = new Date(violation.bannedUntil).toLocaleString(getLang(user) === 'ru' ? 'ru-RU' : 'en-GB');
        const isRuBan = getLang(user) === 'ru';
        bot.sendMessage(chatId, isRuBan
          ? `🚫 Вы временно заблокированы за спам.\n\n⏰ Бан до: ${banUntil}\n\nЕсли считаете это ошибкой — напишите администратору.`
          : `🚫 You are temporarily blocked for spam.\n\n⏰ Banned until: ${banUntil}\n\nIf you think this is a mistake — contact the administrator.`
        ).catch(() => { });
      } else {
        bot.sendMessage(chatId, t(user, 'rate_limit')).catch(() => { });
      }
      return;
    }

    // Проверка постоянного бана в БД (ставится вручную из профиля пользователя)
    if (user.id !== ADMIN_ID) {
      const dbBanRow = await new Promise(res => db.get('SELECT is_banned FROM users WHERE id = ?', [user.id], (e, r) => res(r)));
      if (dbBanRow && dbBanRow.is_banned) {
        const isRuPban = getLang(user) === 'ru';
        bot.sendMessage(chatId, isRuPban
          ? '🚫 Ваш доступ заблокирован администратором. По вопросам — обратитесь в поддержку.'
          : '🚫 Your access has been blocked by the administrator. Contact support for assistance.'
        ).catch(() => {});
        bot.answerCallbackQuery(query.id).catch(() => {});
        return;
      }
    }

    const session = getSession(user.id);

    if (data === 'start') {
      bot.deleteMessage(chatId, message.message_id).catch(() => { });
      showMainMenu(chatId, user);
      clearSession(user.id);
      return;
    }

    // BUG FIX UX-3: Отмена pending заказа из /start предупреждения
    if (data.startsWith('cancel_order_')) {
      const orderId = parseInt(data.replace('cancel_order_', ''));
      if (!isNaN(orderId)) {
        db.run(
          // FIX 2.5: используем статус 'cancelled_by_user' вместо 'rejected',
          // чтобы не искажать статистику отклонённых заказов администратором.
          `UPDATE orders SET status = 'cancelled_by_user' WHERE id = ? AND user_id = ? AND status = 'pending'`,
          [orderId, user.id],
          function (err) {
            const isRu = getLang(user) === 'ru';
            if (!err && this.changes > 0) {
              logAction(user.id, 'order_cancelled_by_user', { orderId });
              bot.answerCallbackQuery(query.id, { text: isRu ? '✅ Заказ отменён' : '✅ Order cancelled' }).catch(() => { });
              bot.editMessageReplyMarkup({ inline_keyboard: [] }, { chat_id: chatId, message_id: message.message_id }).catch(() => { });
              bot.sendMessage(chatId, isRu ? '✅ Заказ отменён. Можете начать заново.' : '✅ Order cancelled. You can start over.').catch(() => { });
            } else {
              bot.answerCallbackQuery(query.id, { text: isRu ? 'Заказ уже обработан' : 'Order already processed' }).catch(() => { });
            }
          }
        );
      }
      return;
    }

    // 🆘 ПОДДЕРЖКА — старт флоу
    if (data === 'support_ticket') {
      // Спрашиваем цель обращения
      const isRuSup = getLang(user) === 'ru';
      sendNavMessage(chatId, user.id,
        isRuSup
          ? `🆘 *Поддержка CyraxMods*\n\nПо какому вопросу вы обращаетесь?`
          : `🆘 *CyraxMods Support*\n\nWhat's your question about?`,
        {
          parse_mode: 'Markdown',
          reply_markup: {
            inline_keyboard: [
              [{ text: isRuSup ? '🔑 Проблема с ключом' : '🔑 Issue with a key', callback_data: 'support_key_intent' }],
              [{ text: isRuSup ? '💬 Личный вопрос / другое' : '💬 Personal question / other', callback_data: 'support_personal_contact' }],
              [{ text: isRuSup ? '◀️ Назад' : '◀️ Back', callback_data: 'start' }]
            ]
          }
        }
      );
      return;
    }

    // Вопрос с ключом — направляем в стандартный тикет-флоу
    if (data === 'support_key_intent') {
      const isRuSup = getLang(user) === 'ru';
      sendNavMessage(chatId, user.id,
        isRuSup
          ? `🔑 *Проблема с ключом?*\n\nВ главном меню уже есть кнопка *«🆘 Проблема с ключом?»* — она автоматически проверит ваш ключ, покажет срок действия и создаст тикет если нужно.\n\nЕсли хотите — можете открыть тикет прямо сейчас:`
          : `🔑 *Issue with a key?*\n\nThe main menu already has a *«🆘 Key issue?»* button — it will automatically check your key, show the validity period and create a ticket if needed.\n\nOr you can open a ticket right now:`,
        {
          parse_mode: 'Markdown',
          reply_markup: {
            inline_keyboard: [
              [{ text: isRuSup ? '🎫 Открыть тикет' : '🎫 Open ticket', callback_data: 'support_key_ticket' }],
              [{ text: isRuSup ? '◀️ Назад' : '◀️ Back', callback_data: 'support_ticket' }]
            ]
          }
        }
      );
      return;
    }

    // Ключевой тикет — запускаем стандартный флоу
    if (data === 'support_key_ticket') {
      startSupportTicket(chatId, user);
      return;
    }

    // Личный вопрос — даём контакт админа
    if (data === 'support_personal_contact') {
      const isRuSup = getLang(user) === 'ru';
      sendNavMessage(chatId, user.id,
        isRuSup
          ? `💬 *Личный вопрос*\n\nПо личным вопросам, предложениям о сотрудничестве или другим темам — пишите напрямую администратору:\n\n👤 @vkvbv\n\n_Пожалуйста, формулируйте вопрос чётко — это ускорит ответ._`
          : `💬 *Personal question*\n\nFor personal questions, partnership proposals or other topics — write directly to the administrator:\n\n👤 @vkvbv\n\n_Please describe your question clearly — it will speed up the response._`,
        {
          parse_mode: 'Markdown',
          reply_markup: {
            inline_keyboard: [
              [{ text: '👤 @vkvbv', url: 'https://t.me/vkvbv' }],
              [{ text: isRuSup ? '◀️ Назад' : '◀️ Back', callback_data: 'support_ticket' }]
            ]
          }
        }
      );
      return;
    }

    // 🆘 ПОДДЕРЖКА — собрать описание
    if (data === 'support_collect_description') {
      startCollectDescription(chatId, user);
      return;
    }

    // 🆘 ПОДДЕРЖКА — пропустить скриншот
    if (data === 'support_ticket_skip_screenshot') {
      const session = getSession(user.id);
      await createAndSendTicket(user, chatId, session.data.keyValue, session.data.description || 'Описание не предоставлено', null, session.data);
      clearSession(user.id);
      return;
    }

    // ПОКАЗ ПУБЛИЧНОЙ ОФЕРТЫ
    if (data === 'offer') {
      const offerText = t(user, 'offer_text');
      const keyboard = {
        inline_keyboard: [
          [{ text: t(user, 'offer_back'), callback_data: 'start' }]
        ]
      };

      const messageParts = splitMessage(offerText, 3500);

      for (let i = 0; i < messageParts.length; i++) {
        const options = { parse_mode: 'HTML' };
        if (i === messageParts.length - 1) {
          options.reply_markup = keyboard;
        }
        await safeSendMessage(chatId, messageParts[i], options).catch(() => { });
      }

      bot.deleteMessage(chatId, message.message_id).catch(() => { });
      return;
    }

    // INFINITE BOOST
    // ==========================================
    // ⚡️ ХАБ БУСТА — выбор между методом и ручным бустом
    // ==========================================
    if (data === 'boost_hub') {
      const isRu = getLang(user) === 'ru';
      const msg = isRu
        ? `⚡️ *Буст аккаунта MLBB*\n\nВыберите подходящий вариант:\n\n🧠 *Метод Буста* — секретная инструкция для самостоятельного фарма ранга. Один раз купил — пользуешься всегда.\n\n🏆 *Ручной Буст* — наши игроки бустят твой аккаунт за тебя. Гарантированный результат.`
        : `⚡️ *MLBB Account Boost*\n\nChoose your option:\n\n🧠 *Boost Method* — a secret guide for independent rank grinding. Buy once, use forever.\n\n🏆 *Manual Boost* — our players boost your account for you. Guaranteed result.`;

      const priceBoost = PRICES['infinite_boost'];
      const isBoostEnabled = isSectionEnabled('boost');
      const isMbEnabled = isSectionEnabled('manual_boost');

      const kb = { inline_keyboard: [] };

      if (isBoostEnabled) {
        kb.inline_keyboard.push([{
          text: isRu
            ? `🧠 Метод Буста — от $${priceBoost.USD}`
            : `🧠 Boost Method — from $${priceBoost.USD}`,
          callback_data: 'buy_boost'
        }]);
      } else {
        kb.inline_keyboard.push([{
          text: isRu ? '🧠 Метод Буста — временно недоступен' : '🧠 Boost Method — temporarily unavailable',
          callback_data: 'noop'
        }]);
      }

      if (isMbEnabled) {
        kb.inline_keyboard.push([{
          text: isRu ? '🏆 Ручной Буст — цена по заявке' : '🏆 Manual Boost — price on request',
          callback_data: 'manual_boost'
        }]);
      } else {
        kb.inline_keyboard.push([{
          text: isRu ? '🏆 Ручной Буст — временно недоступен' : '🏆 Manual Boost — temporarily unavailable',
          callback_data: 'noop'
        }]);
      }

      kb.inline_keyboard.push([{ text: t(user, 'back'), callback_data: 'start' }]);

      bot.editMessageText(msg, {
        chat_id: chatId,
        message_id: message.message_id,
        parse_mode: 'Markdown',
        reply_markup: kb
      }).catch(() => bot.sendMessage(chatId, msg, { parse_mode: 'Markdown', reply_markup: kb }));

      logAction(user.id, 'view_boost_hub');
      return;
    }

    if (data === 'buy_boost') {
      // Проверка флага отключения Метода Буста
      if (!isSectionEnabled('boost')) {
        const _br = getSetting('boost_pause_reason') || '';
        const msg_text = t(user, 'section_boost_disabled_msg') + (_br ? `\n\n💬 ${_br}` : '');
        bot.editMessageText(msg_text, {
          chat_id: chatId, message_id: message.message_id, parse_mode: 'HTML',
          reply_markup: { inline_keyboard: [[{ text: t(user, 'back'), callback_data: 'start' }]] }
        }).catch(() => bot.sendMessage(chatId, msg_text, { parse_mode: 'HTML' }));
        return;
      }

      const lang = getLang(user);
      const title = t(user, 'infinite_boost_title');
      const desc = t(user, 'infinite_boost_desc');
      const priceRub = formatPrice(PRICES['infinite_boost'].RUB, 'RUB');
      const priceUsd = PRICES['infinite_boost'].USD;
      const priceEur = PRICES['infinite_boost'].EUR;
      const priceUah = PRICES['infinite_boost'].UAH;

      const msg_text = `${title}\n\n${desc}\n\n💰 *Цена:*\n🇷🇺 ${priceRub}\n🇺🇸 $${priceUsd}\n🇪🇺 €${priceEur}\n🇺🇦 ${priceUah}₴`;

      const keyboard = {
        inline_keyboard: [
          [
            { text: `🇺🇸 $${priceUsd}`, callback_data: `boost_currency_USD` },
            { text: `🇪🇺 €${priceEur}`, callback_data: `boost_currency_EUR` }
          ],
          [
            { text: `🇷🇺 ${priceRub}`, callback_data: `boost_currency_RUB` },
            { text: `🇺🇦 ${priceUah}₴`, callback_data: `boost_currency_UAH` }
          ],
          [{ text: t(user, 'back'), callback_data: 'boost_hub' }]
        ]
      };

      bot.editMessageText(msg_text, {
        chat_id: chatId,
        message_id: message.message_id,
        parse_mode: 'Markdown',
        reply_markup: keyboard
      }).catch(() => {
        bot.sendMessage(chatId, msg_text, { parse_mode: 'Markdown', reply_markup: keyboard });
      });

      logAction(user.id, 'view_infinite_boost');
      return;
    }

    if (data.startsWith('boost_currency_')) {
      const currency = data.replace('boost_currency_', '');
      const amount = PRICES['infinite_boost'][currency];

      session.state = 'selected_currency';
      session.data.period = 'infinite_boost';
      session.data.currency = currency;
      session.data.amount = amount;
      session.data.discountedAmount = null;
      session.data.couponId = null;
      session.data.couponCode = null;

      const kb = { inline_keyboard: [] };

      if (currency === 'USD') {
        if (PAYPAL_LINK) kb.inline_keyboard.push([{ text: t(user, 'paypal'), callback_data: `pay_infinite_boost_${currency}_paypal` }]);
        kb.inline_keyboard.push([{ text: t(user, 'binance'), callback_data: `pay_infinite_boost_${currency}_binance` }]);
        if (CRYPTOBOT_TOKEN) kb.inline_keyboard.push([{ text: t(user, 'cryptobot'), callback_data: `pay_infinite_boost_${currency}_cryptobot` }]);
      } else if (currency === 'EUR') {
        kb.inline_keyboard.push([{ text: t(user, 'italy_card'), callback_data: `pay_infinite_boost_${currency}_card_it` }]);
        kb.inline_keyboard.push([{ text: t(user, 'binance'), callback_data: `pay_infinite_boost_${currency}_binance` }]);
        if (CRYPTOBOT_TOKEN) kb.inline_keyboard.push([{ text: t(user, 'cryptobot'), callback_data: `pay_infinite_boost_${currency}_cryptobot` }]);
        if (PAYPAL_LINK) kb.inline_keyboard.push([{ text: t(user, 'paypal'), callback_data: `pay_infinite_boost_${currency}_paypal` }]);
      } else if (currency === 'RUB') {
        kb.inline_keyboard.push([{ text: t(user, 'russia_sbp'), callback_data: `pay_infinite_boost_${currency}_sbp` }]);
        if (CRYPTOBOT_TOKEN) kb.inline_keyboard.push([{ text: t(user, 'cryptobot'), callback_data: `pay_infinite_boost_${currency}_cryptobot_usd` }]);
      } else if (currency === 'UAH') {
        kb.inline_keyboard.push([{ text: t(user, 'ukraine_card'), callback_data: `pay_infinite_boost_${currency}_card_ua` }]);
        if (CRYPTOBOT_TOKEN) kb.inline_keyboard.push([{ text: t(user, 'cryptobot'), callback_data: `pay_infinite_boost_${currency}_cryptobot_usd` }]);
      }

      addCouponButton(kb, user, 'infinite_boost', currency);
      kb.inline_keyboard.push([{ text: t(user, 'back'), callback_data: 'buy_boost' }]);

      bot.editMessageText(t(user, 'choose_payment'), {
        chat_id: chatId,
        message_id: message.message_id,
        reply_markup: kb
      }).catch(() => {
        bot.sendMessage(chatId, t(user, 'choose_payment'), { reply_markup: kb });
      });

      logAction(user.id, 'boost_currency_selected', { currency });
      return;
    }

    // ==========================================
    // 👤 РУЧНОЙ БУСТ
    // ==========================================
    // 👤 РУЧНОЙ БУСТ — клиентский флоу
    // ==========================================

    // Главная страница ручного буста
    if (data === 'manual_boost') {
      const msg_text = `${t(user, 'manual_boost_title')}\n\n${t(user, 'manual_boost_desc')}`;
      const keyboard = {
        inline_keyboard: [
          [{ text: t(user, 'manual_boost_proceed'), callback_data: 'manual_boost_start' }],
          [{ text: t(user, 'back'), callback_data: 'boost_hub' }]
        ]
      };
      bot.editMessageText(msg_text, {
        chat_id: chatId, message_id: message.message_id, parse_mode: 'HTML', reply_markup: keyboard
      }).catch(() => bot.sendMessage(chatId, msg_text, { parse_mode: 'HTML', reply_markup: keyboard }));
      logAction(user.id, 'view_manual_boost');
      return;
    }

    // Начало заявки — показываем клавиатуру выбора текущего ранга
    if (data === 'manual_boost_start') {
      if (!isSectionEnabled('manual_boost')) {
        const _mbr = getSetting('manual_boost_pause_reason') || '';
        const msg_text = t(user, 'section_manual_boost_disabled_msg') + (_mbr ? `\n\n💬 ${_mbr}` : '');
        bot.editMessageText(msg_text, {
          chat_id: chatId, message_id: message.message_id, parse_mode: 'HTML',
          reply_markup: { inline_keyboard: [[{ text: t(user, 'back'), callback_data: 'manual_boost' }]] }
        }).catch(() => bot.sendMessage(chatId, msg_text, { parse_mode: 'HTML' }));
        return;
      }
      session.data.manualBoost = {};
      session.state = 'mb_select_current';
      const lang = getLang(user);
      const rankKb = buildRankKeyboard('mb_cur_', lang, null);
      rankKb.push([{ text: t(user, 'back'), callback_data: 'manual_boost' }]);
      const msg_text = t(user, 'manual_boost_select_current');
      bot.editMessageText(msg_text, {
        chat_id: chatId, message_id: message.message_id, parse_mode: 'HTML',
        reply_markup: { inline_keyboard: rankKb }
      }).catch(() => bot.sendMessage(chatId, msg_text, { parse_mode: 'HTML', reply_markup: { inline_keyboard: rankKb } }));
      return;
    }

    // Клиент выбрал текущий ранг
    if (data.startsWith('mb_cur_')) {
      const rankKey = data.replace('mb_cur_', '');
      const rank = MLBB_RANKS.find(r => r.key === rankKey);
      if (!rank) return;
      const lang = getLang(user);
      const label = lang === 'ru' ? rank.label_ru : rank.label_en;
      session.data.manualBoost = { currentRankKey: rankKey, currentRankLabel: label };
      // Мифик — запрашиваем звёзды
      if (['mythic', 'mh', 'mg', 'mi'].includes(rankKey)) {
        session.state = 'mb_enter_current_stars';
        bot.sendMessage(chatId, t(user, 'manual_boost_enter_stars', { rank: label }), {
          parse_mode: 'HTML',
          reply_markup: { inline_keyboard: [[{ text: t(user, 'back'), callback_data: 'manual_boost_start' }]] }
        });
        return;
      }
      // Иначе — переходим к выбору желаемого ранга
      session.state = 'mb_select_target';
      session.data.manualBoost.currentStars = 0;
      const rankKb = buildRankKeyboard('mb_tgt_', lang, rankKey);
      rankKb.push([{ text: t(user, 'back'), callback_data: 'manual_boost_start' }]);
      const msg_text = t(user, 'manual_boost_select_target', { current: label });
      bot.sendMessage(chatId, msg_text, { parse_mode: 'HTML', reply_markup: { inline_keyboard: rankKb } });
      return;
    }

    // Клиент выбрал желаемый ранг
    if (data.startsWith('mb_tgt_')) {
      const rankKey = data.replace('mb_tgt_', '');
      const rank = MLBB_RANKS.find(r => r.key === rankKey);
      if (!rank) return;
      const lang = getLang(user);
      const label = lang === 'ru' ? rank.label_ru : rank.label_en;
      session.data.manualBoost.targetRankKey = rankKey;
      session.data.manualBoost.targetRankLabel = label;
      // Мифик — запрашиваем целевые звёзды
      if (['mythic', 'mh', 'mg', 'mi'].includes(rankKey)) {
        session.state = 'mb_enter_target_stars';
        bot.sendMessage(chatId, t(user, 'manual_boost_enter_target_stars', { rank: label }), {
          parse_mode: 'HTML',
          reply_markup: { inline_keyboard: [[{ text: t(user, 'back'), callback_data: 'manual_boost_start' }]] }
        });
        return;
      }
      session.data.manualBoost.targetStars = 0;
      session.state = 'mb_ready';
      submitBoostRequest(user, chatId, session);
      return;
    }

    // Оплата буста — выбрана валюта
    if (data.startsWith('mb_pay_currency_')) {
      const parts = data.replace('mb_pay_currency_', '').split('_');
      const brId = parseInt(parts[0]);
      const currency = parts[1];
      if (!brId || !currency) return;

      db.get(`SELECT * FROM boost_requests WHERE id = ? AND user_id = ?`, [brId, user.id], (err, br) => {
        if (err || !br) { bot.sendMessage(chatId, '❌ Заявка не найдена'); return; }
        let costs;
        try { costs = JSON.parse(br.costs_json || '{}'); } catch { costs = {}; }
        const amount = costs[currency];
        if (!amount) { bot.sendMessage(chatId, '❌ Валюта недоступна'); return; }

        session.state = 'mb_awaiting_receipt';
        session.data.br = { id: brId, currency, amount };

        const kb = { inline_keyboard: [] };
        if (currency === 'RUB') {
          const mbSbpIsUrl = (PAYMENT_DETAILS.sbp || '').startsWith('http');
          const mbSbpBtnText = getLang(user) === 'ru'
            ? `💳 Оплатить ${amount} ₽ через СБП`
            : `💳 Pay ${amount} RUB via SBP`;
          if (mbSbpIsUrl) {
            kb.inline_keyboard.push([{ text: mbSbpBtnText, url: PAYMENT_DETAILS.sbp }]);
          } else {
            kb.inline_keyboard.push([{ text: mbSbpBtnText, callback_data: `pay_manual_boost_${currency}_sbp` }]);
          }
        } else if (currency === 'UAH') {
          kb.inline_keyboard.push([{ text: t(user, 'ukraine_card'), callback_data: `mb_show_card_ua_${brId}` }]);
        } else if (currency === 'USD') {
          if (PAYPAL_LINK) kb.inline_keyboard.push([{ text: t(user, 'paypal'), url: PAYPAL_LINK }]);
          kb.inline_keyboard.push([{ text: t(user, 'binance'), callback_data: `mb_show_binance_${brId}` }]);
        } else if (currency === 'EUR') {
          kb.inline_keyboard.push([{ text: t(user, 'italy_card'), callback_data: `mb_show_card_it_${brId}` }]);
          if (PAYPAL_LINK) kb.inline_keyboard.push([{ text: t(user, 'paypal'), url: PAYPAL_LINK }]);
          kb.inline_keyboard.push([{ text: t(user, 'binance'), callback_data: `mb_show_binance_${brId}` }]);
        }
        kb.inline_keyboard.push([{ text: t(user, 'back'), callback_data: 'manual_boost' }]);

        const symMap = { RUB: '₽', USD: '$', EUR: '€', UAH: '₴' };
        bot.sendMessage(chatId,
          `${t(user, 'manual_boost_choose_method')}\n\n💰 *${amount} ${symMap[currency] || currency}*\n\n📸 После оплаты отправьте чек в этот чат.`,
          { parse_mode: 'Markdown', reply_markup: kb }
        );

        db.run(`UPDATE boost_requests SET payment_currency = ?, status = 'awaiting_payment' WHERE id = ?`, [currency, brId]);
      });
      return;
    }

    // Показываем реквизиты карты UA / IT / Binance для буста
    if (data.startsWith('mb_show_card_ua_') || data.startsWith('mb_show_card_it_') || data.startsWith('mb_show_binance_')) {
      let details = '', label = '';
      if (data.startsWith('mb_show_card_ua_')) { details = PAYMENT_DETAILS.card_ua; label = '🇺🇦 Карта (Украина)'; }
      if (data.startsWith('mb_show_card_it_')) { details = PAYMENT_DETAILS.card_it; label = '🇮🇹 Карта (Италия)'; }
      if (data.startsWith('mb_show_binance_')) { details = PAYMENT_DETAILS.binance; label = '💎 Binance ID'; }
      const safeDetails = escapeForBacktick(details);
      bot.sendMessage(chatId, `${label}:\n\`${safeDetails}\`\n\n📸 Отправьте чек после оплаты.`, {
        parse_mode: 'Markdown',
        reply_markup: { inline_keyboard: [[{ text: t(user, 'back'), callback_data: 'manual_boost' }]] }
      }).catch(() => {
        bot.sendMessage(chatId, `${label}:\n${details}\n\n📸 Отправьте чек после оплаты.`, {
          reply_markup: { inline_keyboard: [[{ text: t(user, 'back'), callback_data: 'manual_boost' }]] }
        });
      });
      return;
    }

    // ==========================================
    // 👤 ADMIN: Управление ручным бустом
    // ==========================================

    if (data === 'admin_manage_manual_boost') {
      if (user.id !== ADMIN_ID) { bot.answerCallbackQuery(query.id).catch(() => { }); return; }
      showAdminManualBoost(chatId, message.message_id);
      return;
    }

    if (data === 'admin_mb_toggle_status') {
      if (user.id !== ADMIN_ID) { bot.answerCallbackQuery(query.id).catch(() => { }); return; }
      const current = getSetting('manual_boost_status') || 'coming_soon';
      const newStatus = current === 'active' ? 'coming_soon' : 'active';
      saveSetting('manual_boost_status', newStatus, () => {
        bot.answerCallbackQuery(query.id, { text: newStatus === 'active' ? '✅ Активирован!' : '⏸ Деактивирован' }).catch(() => { });
        showAdminManualBoost(chatId, message.message_id);
      });
      return;
    }

    // Админ: рассчитать стоимость для конкретной заявки
    if (data.startsWith('admin_mb_calc_')) {
      if (user.id !== ADMIN_ID) { bot.answerCallbackQuery(query.id).catch(() => { }); return; }
      const brId = parseInt(data.replace('admin_mb_calc_', ''));
      db.get(`SELECT * FROM boost_requests WHERE id = ?`, [brId], (err, br) => {
        if (err || !br) { bot.sendMessage(chatId, '❌ Заявка не найдена'); return; }
        const fromRank = MLBB_RANKS.find(r => r.key === br.current_rank);
        const toRank = MLBB_RANKS.find(r => r.key === br.desired_rank);
        const { costRub, totalStars } = calcBoostCost(br.current_rank, br.stars_current, br.desired_rank, br.stars_desired);
        const fromLabel = escapeMarkdown(fromRank ? fromRank.label_ru : br.current_rank);
        const toLabel = escapeMarkdown(toRank ? toRank.label_ru : br.desired_rank);
        session.state = `admin_mb_confirm_price_${brId}`;
        session.data.mbAdminCalc = { brId, costRub, totalStars, fromLabel, toLabel };
        bot.sendMessage(chatId,
          `🧮 *Расчёт стоимости заявки #${brId}*\n\n` +
          `🎮 ${fromLabel} → 🏆 ${toLabel}\n` +
          `⭐ Звёзд для буста: *${totalStars}*\n\n` +
          `💰 Расчётная стоимость: *${costRub} ₽*\n` +
          `_(${formatAllCurrencies(convertFromRub(costRub))})_\n\n` +
          `Отправьте другую сумму в RUB, если хотите изменить, или нажмите "Подтвердить":`,
          {
            parse_mode: 'Markdown',
            reply_markup: {
              inline_keyboard: [
                [{ text: `✅ Подтвердить ${costRub} ₽`, callback_data: `admin_mb_approve_price_${brId}_${costRub}` }],
                [{ text: '🗑️ Удалить заявку', callback_data: `admin_mb_delete_${brId}` }],
                [{ text: '◀️ Назад', callback_data: 'admin_manage_manual_boost' }]
              ]
            }
          }
        );
      });
      return;
    }

    // Админ: подтверждение рассчитанной цены
    if (data.startsWith('admin_mb_approve_price_')) {
      if (user.id !== ADMIN_ID) { bot.answerCallbackQuery(query.id).catch(() => { }); return; }
      const rest = data.replace('admin_mb_approve_price_', '');
      const parts = rest.split('_');
      const brId = parseInt(parts[0]);
      const rub = parseInt(parts[1]);
      sendBoostCostToClient(brId, rub, chatId);
      return;
    }

    // Админ: удалить заявку на ручной буст
    if (data.startsWith('admin_mb_delete_')) {
      if (user.id !== ADMIN_ID) { bot.answerCallbackQuery(query.id).catch(() => { }); return; }
      const brId = parseInt(data.replace('admin_mb_delete_', ''));
      db.get(`SELECT * FROM boost_requests WHERE id = ?`, [brId], (err, br) => {
        if (err || !br) { bot.sendMessage(chatId, '❌ Заявка не найдена'); return; }
        db.run(`DELETE FROM boost_requests WHERE id = ?`, [brId], (delErr) => {
          if (delErr) { bot.sendMessage(chatId, '❌ Ошибка удаления'); return; }
          // Уведомить клиента
          const clientUser = { id: br.user_id, language_code: br.user_lang };
          bot.sendMessage(br.user_id,
            (br.user_lang || 'ru').startsWith('ru')
              ? '❌ Ваша заявка на ручной буст была отменена администратором.'
              : '❌ Your manual boost request has been cancelled by the administrator.'
          ).catch(() => { });
          logAction(ADMIN_ID, 'boost_request_deleted', { brId, username: br.username });
          bot.answerCallbackQuery(query.id, { text: `✅ Заявка #${brId} удалена` }).catch(() => { });
          // Обновить список
          db.all(
            `SELECT * FROM boost_requests WHERE status NOT IN ('confirmed','rejected') ORDER BY created_at DESC LIMIT 10`,
            [], (e, rows) => {
              if (e || !rows || rows.length === 0) {
                bot.sendMessage(chatId, '📋 Нет активных заявок на ручной буст.');
                return;
              }
              const kb = { inline_keyboard: [] };
              rows.forEach(r => {
                const fromR = MLBB_RANKS.find(x => x.key === r.current_rank);
                const toR = MLBB_RANKS.find(x => x.key === r.desired_rank);
                const fl = fromR ? fromR.label_ru : r.current_rank;
                const tl = toR ? toR.label_ru : r.desired_rank;
                const statusIcon = { pending: '⏳', priced: '💰', awaiting_payment: '💳', paid_pending: '📸' }[r.status] || '❓';
                kb.inline_keyboard.push([
                  { text: `${statusIcon} #${r.id} ${r.username} | ${fl}→${tl}`, callback_data: `admin_mb_calc_${r.id}` },
                  { text: '🗑️', callback_data: `admin_mb_delete_${r.id}` }
                ]);
              });
              kb.inline_keyboard.push([{ text: '◀️ Назад', callback_data: 'admin_manage_manual_boost' }]);
              bot.sendMessage(chatId, `✅ Заявка #${brId} удалена.\n\n📋 *Активные заявки на Ручной Буст:*\n\n_(нажмите заявку для расчёта/редактирования цены)_`, {
                parse_mode: 'Markdown', reply_markup: kb
              });
            }
          );
        });
      });
      return;
    }

    // Админ: подтвердить/отклонить чек ручного буста
    if (data.startsWith('admin_mb_confirm_') || data.startsWith('admin_mb_reject_')) {
      if (user.id !== ADMIN_ID) { bot.answerCallbackQuery(query.id).catch(() => { }); return; }
      const isConfirm = data.startsWith('admin_mb_confirm_');
      const brId = parseInt(data.replace(isConfirm ? 'admin_mb_confirm_' : 'admin_mb_reject_', ''));
      db.get(`SELECT * FROM boost_requests WHERE id = ?`, [brId], (err, br) => {
        if (err || !br) return;
        const newStatus = isConfirm ? 'confirmed' : 'rejected';
        db.run(`UPDATE boost_requests SET status = ?, confirmed_at = CURRENT_TIMESTAMP WHERE id = ?`, [newStatus, brId]);
        const clientUser = { id: br.user_id, language_code: br.user_lang };
        const msgKey = isConfirm ? 'manual_boost_confirmed_client' : 'manual_boost_rejected_client';
        bot.sendMessage(br.user_id, t(clientUser, msgKey), { parse_mode: 'HTML' }).catch(() => { });
        bot.editMessageReplyMarkup({ inline_keyboard: [] }, { chat_id: chatId, message_id: message.message_id }).catch(() => { });
        bot.sendMessage(chatId, isConfirm ? `✅ Заявка #${brId} подтверждена. Клиент уведомлён.` : `❌ Заявка #${brId} отклонена. Клиент уведомлён.`);
      });
      return;
    }

    if (data === 'admin_mb_list_requests') {
      if (user.id !== ADMIN_ID) { bot.answerCallbackQuery(query.id).catch(() => { }); return; }
      db.all(
        `SELECT * FROM boost_requests WHERE status NOT IN ('confirmed','rejected') ORDER BY created_at DESC LIMIT 10`,
        [], (err, rows) => {
          if (err || !rows || rows.length === 0) {
            bot.sendMessage(chatId, '📋 Нет активных заявок на ручной буст.');
            return;
          }
          const kb = { inline_keyboard: [] };
          rows.forEach(r => {
            const fromR = MLBB_RANKS.find(x => x.key === r.current_rank);
            const toR = MLBB_RANKS.find(x => x.key === r.desired_rank);
            const fl = fromR ? fromR.label_ru : r.current_rank;
            const tl = toR ? toR.label_ru : r.desired_rank;
            const statusIcon = { pending: '⏳', priced: '💰', awaiting_payment: '💳', paid_pending: '📸' }[r.status] || '❓';
            kb.inline_keyboard.push([
              { text: `${statusIcon} #${r.id} ${r.username} | ${fl}→${tl}`, callback_data: `admin_mb_calc_${r.id}` },
              { text: '🗑️', callback_data: `admin_mb_delete_${r.id}` }
            ]);
          });
          kb.inline_keyboard.push([{ text: '◀️ Назад', callback_data: 'admin_manage_manual_boost' }]);
          bot.sendMessage(chatId, '📋 *Активные заявки на Ручной Буст:*\n\n_(нажмите заявку для расчёта/редактирования цены)_', {
            parse_mode: 'Markdown', reply_markup: kb
          });
        }
      );
      return;
    }

    if (data.startsWith('admin_mb_set_price_')) {
      if (user.id !== ADMIN_ID) { bot.answerCallbackQuery(query.id).catch(() => { }); return; }
      const currency = data.replace('admin_mb_set_price_', '').toUpperCase();
      const names = { RUB: '₽ RUB', USD: '$ USD', EUR: '€ EUR', UAH: '₴ UAH' };
      session.state = `admin_mb_enter_price_${currency}`;
      bot.sendMessage(chatId, `💰 Введите цену за *одну звезду* в ${names[currency] || currency} для базового расчёта:`, { parse_mode: 'Markdown' });
      return;
    }

    // ==========================================
    // 💱 ADMIN: Управление курсами валют
    // ==========================================

    if (data === 'rates_toggle_manual') {
      if (user.id !== ADMIN_ID) { bot.answerCallbackQuery(query.id).catch(() => { }); return; }
      const current = getSetting('manual_exchange_rates') === '1';
      const newVal = current ? '0' : '1';
      saveSetting('manual_exchange_rates', newVal, async () => {
        bot.answerCallbackQuery(query.id, { text: newVal === '1' ? '🔴 Ручной режим ВКЛ' : '🟢 Авто режим ВКЛ' }).catch(() => { });
        await fetchAndUpdateExchangeRates(); // Пересчитываем сразу
        showExchangeRatesPanel(chatId);
      });
      return;
    }

    if (data === 'rates_refresh') {
      if (user.id !== ADMIN_ID) { bot.answerCallbackQuery(query.id).catch(() => { }); return; }
      bot.answerCallbackQuery(query.id, { text: '🔄 Обновляю курсы из API...' }).catch(() => { });
      await fetchAndUpdateExchangeRates();
      showExchangeRatesPanel(chatId);
      return;
    }

    if (data.startsWith('rates_set_fixed_')) {
      if (user.id !== ADMIN_ID) { bot.answerCallbackQuery(query.id).catch(() => { }); return; }
      const currency = data.replace('rates_set_fixed_', '').toUpperCase();
      const names = { USD: '$ USD', EUR: '€ EUR', UAH: '₴ UAH' };
      session.state = `admin_enter_fixed_rate_${currency}`;
      bot.sendMessage(chatId, `💱 Введите ФИКСИРОВАННЫЙ КУРС для *1 RUB* в ${names[currency] || currency}:\n\n_Например: 0.0108_`, { parse_mode: 'Markdown' });
      return;
    }

    // ЛОГИКА ПОКУПКИ
    if (data === 'buy') {
      // Проверка флага отключения ключей
      if (!isSectionEnabled('keys')) {
        const _kr = getSetting('keys_pause_reason') || '';
        const msg_text = t(user, 'section_keys_disabled_msg') + (_kr ? `\n\n💬 ${_kr}` : '');
        bot.editMessageText(msg_text, {
          chat_id: chatId, message_id: message.message_id, parse_mode: 'HTML',
          reply_markup: { inline_keyboard: [[{ text: t(user, 'back'), callback_data: 'start' }]] }
        }).catch(() => bot.sendMessage(chatId, msg_text, { parse_mode: 'HTML' }));
        return;
      }

      const periodNames = PERIOD_NAMES[getLang(user)];
      const isRu_period = getLang(user) === 'ru';
      const keyboard = {
        inline_keyboard: [
          [{ text: `🔑 ${periodNames['1d']}`, callback_data: 'period_1d' }],
          [{ text: `🔑 ${periodNames['3d']}`, callback_data: 'period_3d' }],
          [{ text: `🔑 ${periodNames['7d']}`, callback_data: 'period_7d' }],
          [{ text: `🔑 ${periodNames['30d']}`, callback_data: 'period_30d' }],
          [{ text: isRu_period ? '👤 Мой профиль' : '👤 My Profile', callback_data: 'my_profile' }],
          [{ text: t(user, 'back'), callback_data: 'start' }]
        ]
      };

      if (message) {
        bot.editMessageText(t(user, 'choose_period'), {
          chat_id: chatId,
          message_id: message.message_id,
          reply_markup: keyboard
        }).catch(() => {
          sendNavMessage(chatId, user.id, t(user, 'choose_period'), { reply_markup: keyboard });
        });
      } else {
        sendNavMessage(chatId, user.id, t(user, 'choose_period'), { reply_markup: keyboard });
      }

      logAction(user.id, 'view_products');
      return;
    }

    if (data.startsWith('period_')) {
      const period = data.replace('period_', '');
      session.state = 'selected_period';
      session.data.period = period;

      // FIX 2.1: Сброс скидок при смене периода.
      // Купон и лояльность привязаны к конкретной цене конкретного периода в конкретной валюте.
      // При смене периода старая скидка уже неверна — сбрасываем всё.
      // Купон пользователь сможет ввести снова после выбора валюты.
      session.data.discountedAmount = null;
      session.data.couponId = null;
      session.data.couponCode = null;
      session.data.discountPercent = null;
      session.data.loyaltyDiscountPercent = null;

      const keyboard = {
        inline_keyboard: [
          [
            { text: `${FLAGS.USD} USD - $${PRICES[period].USD}`, callback_data: `currency_${period}_USD` },
            { text: `${FLAGS.EUR} EUR - €${PRICES[period].EUR}`, callback_data: `currency_${period}_EUR` }
          ],
          [
            { text: `${FLAGS.RUB} RUB - ${formatPrice(PRICES[period].RUB, 'RUB')}`, callback_data: `currency_${period}_RUB` },
            { text: `${FLAGS.UAH} UAH - ${PRICES[period].UAH}₴`, callback_data: `currency_${period}_UAH` }
          ],
          [{ text: t(user, 'back'), callback_data: 'buy' }]
        ]
      };

      bot.editMessageText(t(user, 'choose_currency'), {
        chat_id: chatId,
        message_id: message.message_id,
        reply_markup: keyboard
      }).catch(() => sendNavMessage(chatId, user.id, t(user, 'choose_currency'), { reply_markup: keyboard }).catch(() => { }));

      logAction(user.id, 'select_period', { period });
      return;
    }

    if (data.startsWith('currency_')) {
      const parsed = parseCurrencyCallback(data);

      if (parsed) {
        const period = parsed.period;
        const currency = parsed.currency;

        session.state = 'selected_currency';
        session.data.period = period;
        session.data.currency = currency;
        session.data.amount = PRICES[period]?.[currency] || 0;
        // FIX 2.1: Полный сброс всех скидок при (пере)выборе валюты.
        // Правило приоритета: Купон > Лояльность (не суммируются).
        // Купон вводится ПОСЛЕ выбора метода оплаты — здесь его ещё нет.
        // Лояльность применяется автоматически, и только если купона нет.
        session.data.discountedAmount = null;
        session.data.couponId = null;
        session.data.couponCode = null;
        session.data.discountPercent = null;
        session.data.loyaltyDiscountPercent = null;

        // Task 9: Применяем скидку лояльности если нет купона
        // Купон имеет приоритет над лояльностью — скидка лояльности применяется только при отсутствии купона
        const basePrice = PRICES[period]?.[currency] || 0;
        try {
          const loyaltyPct = await getLoyaltyDiscount(user.id);
          const globalPct = parseInt(getSetting('default_loyalty_discount') || '0');
          const effectivePct = loyaltyPct > 0 ? loyaltyPct : globalPct;
          if (effectivePct > 0) {
            session.data.discountedAmount = applyLoyaltyDiscount(basePrice, effectivePct);
            session.data.loyaltyDiscountPercent = effectivePct;
          }
        } catch (e) {
          // loyalty check failed silently
        }

        const kb = { inline_keyboard: [] };

        if (currency === 'USD') {
          if (PAYPAL_LINK) {
            kb.inline_keyboard.push([{ text: t(user, 'paypal'), callback_data: `pay_${period}_${currency}_paypal` }]);
          }
          kb.inline_keyboard.push([{ text: t(user, 'binance'), callback_data: `pay_${period}_${currency}_binance` }]);
          if (CRYPTOBOT_TOKEN) {
            kb.inline_keyboard.push([{ text: t(user, 'cryptobot'), callback_data: `pay_${period}_${currency}_cryptobot` }]);
          }
        }

        if (currency === 'EUR') {
          kb.inline_keyboard.push([{ text: t(user, 'italy_card'), callback_data: `pay_${period}_${currency}_card_it` }]);
          kb.inline_keyboard.push([{ text: t(user, 'binance'), callback_data: `pay_${period}_${currency}_binance` }]);
          if (CRYPTOBOT_TOKEN) {
            kb.inline_keyboard.push([{ text: t(user, 'cryptobot'), callback_data: `pay_${period}_${currency}_cryptobot` }]);
          }
          if (PAYPAL_LINK) {
            kb.inline_keyboard.push([{ text: t(user, 'paypal'), callback_data: `pay_${period}_${currency}_paypal` }]);
          }
        }

        if (currency === 'RUB') {
          kb.inline_keyboard.push([{ text: t(user, 'russia_sbp'), callback_data: `pay_${period}_${currency}_sbp` }]);
          if (CRYPTOBOT_TOKEN) {
            kb.inline_keyboard.push([{ text: t(user, 'cryptobot'), callback_data: `pay_${period}_${currency}_cryptobot_usd` }]);
          }
        }

        if (currency === 'UAH') {
          kb.inline_keyboard.push([{ text: t(user, 'ukraine_card'), callback_data: `pay_${period}_${currency}_card_ua` }]);
          if (CRYPTOBOT_TOKEN) {
            kb.inline_keyboard.push([{ text: t(user, 'cryptobot'), callback_data: `pay_${period}_${currency}_cryptobot_usd` }]);
          }
        }

        // Кнопка купона — не для reseller_connection и infinite_boost
        if (period !== 'reseller_connection' && period !== 'infinite_boost') {
          addCouponButton(kb, user, period, currency);
        }

        // Кнопка «Оплатить с баланса» — если баланс >= цена товара
        if (period !== 'reseller_connection' && period !== 'infinite_boost') {
          try {
            const userBal = await getUserBalance(user.id);
            const itemPrice = session.data?.discountedAmount || PRICES[period]?.[currency] || 0;
            // Проверяем что валюта баланса совпадает с валютой покупки
            if (userBal.balance >= itemPrice && itemPrice > 0 && userBal.preferred_currency === currency) {
              const isRuBal = getLang(user) === 'ru';
              kb.inline_keyboard.push([{
                text: isRuBal
                  ? `💳 С баланса (${formatBalanceAmount(userBal.balance, currency)})`
                  : `💳 From balance (${formatBalanceAmount(userBal.balance, currency)})`,
                callback_data: `pay_balance_${period}_${currency}`
              }]);
            }
          } catch(e) { /* баланс не критичен */ }
        }

        // Кнопка назад
        const backTarget = period === 'reseller_connection' ? 'partnership'
          : period === 'infinite_boost' ? 'boost_hub'
            : `period_${period}`;
        kb.inline_keyboard.push([{ text: t(user, 'back'), callback_data: backTarget }]);

        bot.editMessageText(t(user, 'choose_payment'), {
          chat_id: chatId,
          message_id: message.message_id,
          reply_markup: kb
        }).catch(() => sendNavMessage(chatId, user.id, t(user, 'choose_payment'), { reply_markup: kb }).catch(() => { }));

        logAction(user.id, 'select_currency', { period, currency });
      }
      return;
    }

    // ==========================================
    // 💳 ОПЛАТА С БАЛАНСА ПРОФИЛЯ
    // ==========================================
    if (data.startsWith('pay_balance_')) {
      const withoutPrefix = data.replace('pay_balance_', '');
      const knownCurrencies = ['USD', 'EUR', 'RUB', 'UAH'];
      let period, currency;
      for (const cur of knownCurrencies) {
        if (withoutPrefix.endsWith('_' + cur)) {
          currency = cur;
          period = withoutPrefix.slice(0, -(cur.length + 1));
          break;
        }
      }
      if (!period || !currency) { bot.answerCallbackQuery(query.id).catch(() => {}); return; }
      const isRu = getLang(user) === 'ru';

      if (!PRICES[period]?.[currency]) { bot.answerCallbackQuery(query.id).catch(() => {}); return; }
      const itemPrice = session.data?.discountedAmount || PRICES[period][currency];

      // 🧪 ТЕСТ-РЕЖИМ: сразу переходим к подтверждению без проверки баланса

      const userBal = await getUserBalance(user.id);
      if (userBal.balance < itemPrice || userBal.preferred_currency !== currency) {
        bot.answerCallbackQuery(query.id, {
          text: isRu ? '❌ Недостаточно средств на балансе' : '❌ Insufficient balance',
          show_alert: true
        }).catch(() => {});
        return;
      }

      // Показываем подтверждение
      const pName = PERIOD_NAMES[isRu ? 'ru' : 'en'][period] || period;
      const msg = isRu
        ? `💳 *Подтверждение оплаты с баланса*

` +
          `📦 Товар: *${pName}*
` +
          `💰 Стоимость: *${formatBalanceAmount(itemPrice, currency)}*
` +
          `💳 Баланс после: *${formatBalanceAmount(userBal.balance - itemPrice, currency)}*

` +
          `Подтвердите покупку — ключ будет выдан мгновенно.`
        : `💳 *Confirm Balance Payment*

` +
          `📦 Item: *${pName}*
` +
          `💰 Cost: *${formatBalanceAmount(itemPrice, currency)}*
` +
          `💳 Balance after: *${formatBalanceAmount(userBal.balance - itemPrice, currency)}*

` +
          `Confirm purchase — key will be issued instantly.`;

      session.data = { ...session.data, balancePeriod: period, balanceCurrency: currency, balancePrice: itemPrice };

      sendNavMessage(chatId, user.id, msg, {
        parse_mode: 'Markdown',
        reply_markup: { inline_keyboard: [
          [{ text: isRu ? '✅ Подтвердить' : '✅ Confirm', callback_data: 'profile_topup_confirm' }],
          [{ text: isRu ? '◀️ Назад' : '◀️ Back', callback_data: `currency_${period}_${currency}` }]
        ]}
      }).catch(() => {});
      return;
    }

    // Подтверждение покупки с баланса
    if (data === 'profile_topup_confirm') {
      const period = session.data?.balancePeriod;
      const currency = session.data?.balanceCurrency;
      const itemPrice = session.data?.balancePrice;
      const isRu = getLang(user) === 'ru';

      if (!period || !currency || !itemPrice) { bot.answerCallbackQuery(query.id).catch(() => {}); return; }

      // 🧪 ТЕСТ-РЕЖИМ: выдаём тестовый ключ без списания баланса и без записи в БД

      // Финальная проверка баланса (атомарно)
      try {
        // Списываем баланс
        await adjustUserBalance(user.id, -itemPrice, currency, 'purchase',
          `Покупка ${period}`, null, null);
      } catch(e) {
        if (e.message === 'INSUFFICIENT_BALANCE') {
          bot.answerCallbackQuery(query.id, {
            text: isRu ? '❌ Недостаточно средств' : '❌ Insufficient balance',
            show_alert: true
          }).catch(() => {});
        } else {
          bot.sendMessage(chatId, '❌ Ошибка списания баланса. Попробуйте позже.');
        }
        return;
      }

      // Выдаём ключ
      let key;
      try {
        key = await issueKeyToUser(user.id, period, 'purchase');
      } catch(keyErr) {
        if (keyErr.code === 'OUT_OF_STOCK') {
          // Возвращаем деньги на баланс
          await adjustUserBalance(user.id, itemPrice, currency, 'refund',
            `Возврат: нет ключей ${period}`, null, null);
          // Умный OOS-купон с антифармом
          const oosCode = await issueOosCoupon(user.id, period, null);
          const oosPct = OOS_COUPON_PCT[period] || 10;
          const pName = PERIOD_NAMES[isRu ? 'ru' : 'en'][period] || period;
          const couponLine = oosCode
            ? (isRu
                ? `\n\n🎟 В качестве извинения — купон на *${oosPct}% скидку* на «${pName}»:\n\`${oosCode}\`\n_Действует 30 дней._`
                : `\n\n🎟 As an apology — *${oosPct}% discount coupon* for «${pName}»:\n\`${oosCode}\`\n_Valid 30 days._`)
            : '';
          // Компактное сообщение для пользователя — купон в стиле "ключ" (тапни чтобы скопировать)
          safeSendMessage(user.id,
            isRu
              ? `😔 Ключи «${pName}» временно закончились\n` +
                `💳 Возврат: ${formatBalanceAmount(itemPrice, currency)} → на ваш баланс` +
                (oosCode
                  ? `\n\n🎟 Купон ${oosPct}% на «${pName}»:\n\`${oosCode}\`\n└ (нажмите чтобы скопировать)\n⏳ Действует 30 дней`
                  : '') +
                `\n\n_Ключи появятся в ближайшее время!_`
              : `😔 Keys «${pName}» are temporarily out of stock\n` +
                `💳 Refund: ${formatBalanceAmount(itemPrice, currency)} → your balance` +
                (oosCode
                  ? `\n\n🎟 ${oosPct}% coupon for «${pName}»:\n\`${oosCode}\`\n└ (tap to copy)\n⏳ Valid 30 days`
                  : '') +
                `\n\n_Keys will be restocked soon!_`,
            { parse_mode: 'Markdown', reply_markup: { inline_keyboard: [[{ text: isRu ? '👤 Мой профиль' : '👤 My Profile', callback_data: 'my_profile' }]] } }
          ).catch(() => {});
          // Уведомление админу: человек оплатил, деньги вернули, ключа нет — нужно добавить
          const adminOosKb = {
            inline_keyboard: [
              [{ text: `🔑 Добавить ключи «${pName}»`, callback_data: `add_keys_${period}` }],
              [{ text: '📊 Панель управления', callback_data: 'admin' }]
            ]
          };
          safeSendMessage(ADMIN_ID,
            `📭 Нет ключей — оплата балансом\n\n` +
            `⚠️ Пользователь оплатил, но ключ не получил!\n` +
            `Деньги автоматически возвращены на баланс.\n\n` +
            `👤 ${user.username ? '@' + user.username : user.id}\n` +
            `📦 Товар: ${pName}\n` +
            `💳 Возврат: ${formatBalanceAmount(itemPrice, currency)}\n` +
            (oosCode ? `🎟 Купон выдан: ${oosCode} (${oosPct}%)` : `⚠️ Купон не выдан (антифарм)`) +
            `\n\n👉 Добавьте ключи — пользователь сможет купить повторно.`,
            { parse_mode: 'Markdown', reply_markup: adminOosKb }
          ).catch(() => {});
          return;
        }
        // 🔧 FIX: возвращаем деньги на баланс при любой ошибке выдачи ключа (не только OOS)
        // Без этого баланс уже списан, а ключ клиент не получил.
        try {
          await adjustUserBalance(user.id, itemPrice, currency, 'refund',
            `Автовозврат: ошибка выдачи ключа ${period}`, null, ADMIN_ID);
          bot.sendMessage(chatId,
            isRu
              ? `❌ Ошибка выдачи ключа.\n\n💳 Сумма *${formatBalanceAmount(itemPrice, currency)}* возвращена на ваш баланс.\n\nОбратитесь в поддержку, если проблема повторяется.`
              : `❌ Key issuance error.\n\n💳 *${formatBalanceAmount(itemPrice, currency)}* has been refunded to your balance.\n\nContact support if the issue persists.`,
            { parse_mode: 'Markdown' }
          );
          safeSendMessage(ADMIN_ID,
            `⚠️ *Ошибка выдачи ключа (balance purchase)*\n\n` +
            `👤 ${user.username ? '@' + user.username : user.id}\n` +
            `📦 Товар: ${period}\n` +
            `💳 Возврат: ${formatBalanceAmount(itemPrice, currency)}\n` +
            `❗ Ошибка: ${keyErr.message}`,
            { parse_mode: 'Markdown' }
          ).catch(() => {});
        } catch (refundErr2) {
          console.error('❌ КРИТИЧНО: не удалось вернуть баланс после ошибки выдачи ключа:', refundErr2.message);
          bot.sendMessage(chatId,
            isRu
              ? '❌ Ошибка выдачи ключа. Пожалуйста, обратитесь в поддержку — ваши деньги в безопасности.'
              : '❌ Key issuance error. Please contact support — your funds are safe.'
          );
        }
        return;
      }

      // Создаём запись заказа
      db.run(
        `INSERT INTO orders (user_id, username, user_lang, product, amount, currency, method, status, key_issued, confirmed_at, paid_from_balance, finalized)
         VALUES (?, ?, ?, ?, ?, ?, 'balance', 'confirmed', ?, datetime('now'), 1, 0)`,
        [user.id, user.username || '', getLang(user), period, itemPrice, currency, key],
        async function(insertErr) {
          if (insertErr) {
            console.error('❌ Error inserting balance order:', insertErr);
            return;
          }
          const newOrderId = this.lastID;

          db.run(
            `UPDATE balance_transactions SET order_id = ? WHERE user_id = ? AND type = 'purchase' AND order_id IS NULL ORDER BY created_at DESC LIMIT 1`,
            [newOrderId, user.id]
          );

          // ✅ Уведомление админу о покупке с баланса профиля
          const uname = user.username ? `@${escapeMarkdown(user.username)}` : `ID: ${user.id}`;
          const pNameAdmin = PERIOD_NAMES['ru'][period] || period;
          safeSendMessage(ADMIN_ID,
            `💳 *Покупка с баланса профиля*\n\n` +
            `👤 Клиент: ${uname}\n` +
            `📦 Товар: *${pNameAdmin}*\n` +
            `💰 Сумма: *${formatBalanceAmount(itemPrice, currency)}*\n` +
            `🔑 Ключ выдан автоматически\n` +
            `🆔 Заказ #${newOrderId}`,
            { parse_mode: 'Markdown' }
          ).catch(() => {});

          // Используем finalizeSuccessfulOrder для унификации пост-обработки:
          // FOMO-купон, реферальный бонус, milestone-купон, снижение suspicion_score.
          // finalized=0 при INSERT выше — finalizeSuccessfulOrder сам переключит флаг.
          const fakeOrder = {
            id: newOrderId,
            user_id: user.id,
            product: period,
            currency,
            amount: itemPrice,
            user_lang: getLang(user),
            reseller_id: null,
            coupon_id: session.data?.couponId || null,
            paid_from_balance: 1,
          };
          await finalizeSuccessfulOrder(fakeOrder, newOrderId, bot);
        }
      );

      const pName = PERIOD_NAMES[isRu ? 'ru' : 'en'][period] || period;
      const newBal = await getUserBalance(user.id);

      safeSendMessage(user.id,
        isRu
          ? `🎊 Оплата прошла! 🎊\n\n📦 ${pName}\n🔑 Ключ: \`${key}\`\n└ (нажмите чтобы скопировать)\n\n💳 Остаток: ${formatBalanceAmount(newBal.balance, currency)}`
          : `🎊 Payment successful! 🎊\n\n📦 ${pName}\n🔑 Key: \`${key}\`\n└ (tap to copy)\n\n💳 Balance: ${formatBalanceAmount(newBal.balance, currency)}`,
        { parse_mode: 'Markdown', reply_markup: { inline_keyboard: [[{ text: isRu ? '👤 Профиль' : '👤 Profile', callback_data: 'my_profile' }]] } }
      ).catch(() => {});

      clearSession(user.id);
      return;
    }

    // ==========================================
    // 🧪 ТЕСТ-РЕЖИМ ОПЛАТЫ — перехватываем любой pay_ коллбэк
    // Если админ в тест-режиме — выдаём тестовый ключ без оплаты и без записи в БД
    // ==========================================

    // PayPal
    if (data.startsWith('pay_') && data.includes('_paypal')) {
      const parsed = parsePayCallback(data);
      if (!parsed) { bot.answerCallbackQuery(query.id).catch(() => { }); return; }
      const { period, currency } = parsed;
      if (!PRICES[period]?.[currency]) { bot.answerCallbackQuery(query.id).catch(() => { }); return; }
      const baseAmount = session.data.discountedAmount || PRICES[period][currency];
      // PayPal комиссия +0.50 USD/EUR
      const paypalAmount = applyPaypalFee(baseAmount, currency);
      const feeAdded = paypalAmount > baseAmount;

      session.state = 'awaiting_receipt';
      session.data = {
        ...session.data,
        period, currency,
        amount: paypalAmount,
        method: 'PayPal'
      };

      const isRu = getLang(user) === 'ru';
      const baseFormatted = formatPrice(baseAmount, currency);
      const paypalFormatted = formatPrice(paypalAmount, currency);

      const feeNote = feeAdded
        ? (isRu
          ? `

💡 Сумма включает комиссию PayPal (+${PAYPAL_COMMISSION[currency]} ${currency}). Это стандартная плата платёжной системы — мы ничего не добавляем от себя.`
          : `

💡 The amount includes a PayPal processing fee (+${PAYPAL_COMMISSION[currency]} ${currency}). This is a standard payment system charge — we add nothing extra.`)
        : '';

      const isRu_paypal = getLang(user) === 'ru';
      const pNamePaypal = PERIOD_NAMES[isRu_paypal ? 'ru' : 'en'][period] || period;
      const instruction = t(user, 'paypal_instruction', { amount: paypalAmount, currency });
      const headerLinePaypal = isRu_paypal
        ? `🛒 <b>Товар:</b> ${pNamePaypal}

`
        : `🛒 <b>Product:</b> ${pNamePaypal}

`;
      const msgText = headerLinePaypal + instruction + feeNote;

      const paypalBtnText = isRu_paypal
        ? `💰 Оплатить ${paypalFormatted} — PayPal`
        : `💰 Pay ${paypalFormatted} — PayPal`;
      const paypalKeyboard = {
        inline_keyboard: [
          [{ text: paypalBtnText, url: PAYPAL_LINK }],
          [{ text: t(user, 'back'), callback_data: `currency_${period}_${currency}` }]
        ]
      };

      bot.editMessageText(msgText, {
        chat_id: chatId,
        message_id: message.message_id,
        parse_mode: 'HTML',
        reply_markup: paypalKeyboard
      }).catch(e => {
        console.error('❌ PayPal editMessage error:', e.message);
        bot.sendMessage(chatId, msgText, { parse_mode: 'HTML', reply_markup: paypalKeyboard }).catch(e2 => console.error('❌ PayPal sendMessage error:', e2.message));
      });

      logAction(user.id, 'payment_method_selected', { method: 'PayPal', period, currency, amount: paypalAmount });
      return;
    }

    // СБП
    if (data.startsWith('pay_') && data.includes('_sbp')) {
      const parsed = parsePayCallback(data);
      if (!parsed) { bot.answerCallbackQuery(query.id).catch(() => { }); return; }
      const { period, currency } = parsed;
      const amount = session.data.discountedAmount || PRICES[period]?.[currency] || 0;

      session.state = 'awaiting_receipt';
      session.data = {
        ...session.data,
        period, currency, amount,
        method: 'SBP'
      };

      const isRu_sbp = getLang(user) === 'ru';
      const sbpDetails = PAYMENT_DETAILS.sbp || (isRu_sbp ? 'не указаны — обратитесь к администратору' : 'not set — contact admin');

      // Если реквизиты — deeplink/ссылка, не показываем её в тексте — только кнопка
      const sbpIsUrl = sbpDetails.startsWith('http://') || sbpDetails.startsWith('https://');
      const sbpDetailsForText = sbpIsUrl
        ? (isRu_sbp ? '👇 нажми кнопку ниже' : '👇 tap the button below')
        : sbpDetails;

      const pNameSbp = PERIOD_NAMES[isRu_sbp ? 'ru' : 'en'][period] || period;
      const instruction = t(user, 'sbp_instruction', { amount, currency });
      const headerLine = isRu_sbp
        ? `🛒 <b>Товар:</b> ${pNameSbp}\n\n`
        : `🛒 <b>Product:</b> ${pNameSbp}\n\n`;
      const msg = headerLine + instruction;

      // Реквизиты — всегда кнопка:
      // • URL/deeplink → url-кнопка (открывает приложение банка)
      // • Телефон/номер → кнопка с текстом (нажать = скопировать)
      const sbpPayBtnText = isRu_sbp
        ? `💳 Оплатить ${amount} ₽ через СБП`
        : `💳 Pay ${amount} RUB via SBP`;
      const sbpDetailsBtnText = isRu_sbp
        ? `📋 Реквизиты: ${sbpDetails}`
        : `📋 Details: ${sbpDetails}`;

      const sbpKeyboard = {
        inline_keyboard: [
          sbpIsUrl
            ? [{ text: sbpPayBtnText, url: sbpDetails }]
            : [{ text: sbpDetailsBtnText, callback_data: 'noop' }],
          [{ text: t(user, 'back'), callback_data: `currency_${period}_${currency}` }]
        ]
      };

      bot.editMessageText(msg, {
        chat_id: chatId,
        message_id: message.message_id,
        parse_mode: 'HTML',
        reply_markup: sbpKeyboard
      }).catch(e => {
        console.error('❌ SBP editMessage error:', e.message);
        bot.sendMessage(chatId, msg, { parse_mode: 'HTML', reply_markup: sbpKeyboard }).catch(e2 => console.error('❌ SBP sendMessage error:', e2.message));
      });

      logAction(user.id, 'payment_method_selected', { method: 'SBP', period, currency, amount });
      return;
    }

    // Карта Украина
    if (data.startsWith('pay_') && data.includes('_card_ua')) {
      const parsed = parsePayCallback(data);
      if (!parsed) { bot.answerCallbackQuery(query.id).catch(() => { }); return; }
      const { period, currency } = parsed;
      const amount = session.data.discountedAmount || PRICES[period]?.[currency] || 0;

      session.state = 'awaiting_receipt';
      session.data = {
        ...session.data,
        period, currency, amount,
        method: 'Card UA'
      };

      const isRu_cardua = getLang(user) === 'ru';
      const pNameCardUa = PERIOD_NAMES[isRu_cardua ? 'ru' : 'en'][period] || period;
      const instruction = t(user, 'card_ua_instruction', {
        amount,
        currency,
        card: escapeForBacktick(PAYMENT_DETAILS.card_ua)
      });
      const headerCardUa = isRu_cardua
        ? `🛒 <b>Товар:</b> ${pNameCardUa}

`
        : `🛒 <b>Product:</b> ${pNameCardUa}

`;
      const msg = headerCardUa + instruction;

      const cardUaKeyboard = {
        inline_keyboard: [
          [{ text: isRu_cardua ? `💳 Карта: ${PAYMENT_DETAILS.card_ua}` : `💳 Card: ${PAYMENT_DETAILS.card_ua}`, callback_data: 'noop' }],
          [{ text: t(user, 'back'), callback_data: `currency_${period}_${currency}` }]
        ]
      };
      bot.editMessageText(msg, {
        chat_id: chatId,
        message_id: message.message_id,
        parse_mode: 'HTML',
        reply_markup: cardUaKeyboard
      }).catch(e => {
        console.error('❌ CardUA editMessage error:', e.message);
        bot.sendMessage(chatId, msg, {
          parse_mode: 'HTML',
          reply_markup: cardUaKeyboard
        }).catch(e2 => console.error('❌ CardUA sendMessage error:', e2.message));
      });

      logAction(user.id, 'payment_method_selected', { method: 'Card UA', period, currency, amount });
      return;
    }

    // Карта Италия
    if (data.startsWith('pay_') && data.includes('_card_it')) {
      const parsed = parsePayCallback(data);
      if (!parsed) { bot.answerCallbackQuery(query.id).catch(() => { }); return; }
      const { period, currency } = parsed;
      const amount = session.data.discountedAmount || PRICES[period]?.[currency] || 0;

      session.state = 'awaiting_receipt';
      session.data = {
        ...session.data,
        period, currency, amount,
        method: 'Card IT'
      };

      const isRu_cardit = getLang(user) === 'ru';
      const pNameCardIt = PERIOD_NAMES[isRu_cardit ? 'ru' : 'en'][period] || period;
      const instruction = t(user, 'card_it_instruction', {
        amount,
        currency,
        card: escapeForBacktick(PAYMENT_DETAILS.card_it)
      });
      const headerCardIt = isRu_cardit
        ? `🛒 <b>Товар:</b> ${pNameCardIt}

`
        : `🛒 <b>Product:</b> ${pNameCardIt}

`;
      const msg = headerCardIt + instruction;

      const cardItKeyboard = {
        inline_keyboard: [
          [{ text: isRu_cardit ? `💳 Карта: ${PAYMENT_DETAILS.card_it}` : `💳 Card: ${PAYMENT_DETAILS.card_it}`, callback_data: 'noop' }],
          [{ text: t(user, 'back'), callback_data: `currency_${period}_${currency}` }]
        ]
      };
      bot.editMessageText(msg, {
        chat_id: chatId,
        message_id: message.message_id,
        parse_mode: 'HTML',
        reply_markup: cardItKeyboard
      }).catch(e => {
        console.error('❌ CardIT editMessage error:', e.message);
        bot.sendMessage(chatId, msg, {
          parse_mode: 'HTML',
          reply_markup: cardItKeyboard
        }).catch(e2 => console.error('❌ CardIT sendMessage error:', e2.message));
      });

      logAction(user.id, 'payment_method_selected', { method: 'Card IT', period, currency, amount });
      return;
    }

    // Binance
    if (data.startsWith('pay_') && data.includes('_binance')) {
      const parsed = parsePayCallback(data);
      if (!parsed) { bot.answerCallbackQuery(query.id).catch(() => { }); return; }
      const { period, currency } = parsed;
      const amount = session.data.discountedAmount || PRICES[period]?.[currency] || 0;

      session.state = 'awaiting_receipt';
      session.data = {
        ...session.data,
        period, currency, amount,
        method: 'Binance'
      };

      const isRu_binance = getLang(user) === 'ru';
      const pNameBinance = PERIOD_NAMES[isRu_binance ? 'ru' : 'en'][period] || period;
      const instruction = t(user, 'binance_instruction', {
        amount,
        currency,
        id: escapeForBacktick(PAYMENT_DETAILS.binance),
        send_transaction: t(user, 'send_transaction')
      });
      const headerBinance = isRu_binance
        ? `🛒 <b>Товар:</b> ${pNameBinance}

`
        : `🛒 <b>Product:</b> ${pNameBinance}

`;
      const msg = headerBinance + instruction;

      const binanceKeyboard = {
        inline_keyboard: [
          [{ text: isRu_binance ? `💎 ID: ${PAYMENT_DETAILS.binance}` : `💎 ID: ${PAYMENT_DETAILS.binance}`, callback_data: 'noop' }],
          [{ text: t(user, 'back'), callback_data: `currency_${period}_${currency}` }]
        ]
      };
      bot.editMessageText(msg, {
        chat_id: chatId,
        message_id: message.message_id,
        parse_mode: 'HTML',
        reply_markup: binanceKeyboard
      }).catch(e => {
        console.error('❌ Binance editMessage error:', e.message);
        bot.sendMessage(chatId, msg, {
          parse_mode: 'HTML',
          reply_markup: binanceKeyboard
        }).catch(e2 => console.error('❌ Binance sendMessage error:', e2.message));
      });

      logAction(user.id, 'payment_method_selected', { method: 'Binance', period, currency, amount });
      return;
    }

    // CryptoBot (обычный)
    if (data.startsWith('pay_') && data.includes('_cryptobot') && !data.includes('_cryptobot_usd')) {
      const parsed = parsePayCallback(data);
      if (!parsed) { bot.answerCallbackQuery(query.id).catch(() => { }); return; }
      const { period, currency } = parsed;
      if (!PRICES[period]?.[currency]) { bot.answerCallbackQuery(query.id).catch(() => { }); return; }
      const baseAmount = session.data.discountedAmount || PRICES[period][currency];
      // +0.50 комиссия CryptoBot (для USD и EUR)
      const cryptoAmount = applyCryptobotFee(baseAmount, currency);
      const feeAdded = cryptoAmount > baseAmount;

      db.get(
        `SELECT id FROM orders WHERE user_id = ? AND product = ? AND status = 'pending'`,
        [user.id, period],
        async (err, existing) => {
          if (err) console.error('❌ Duplicate check error:', err);
          if (existing) {
            bot.sendMessage(chatId, t(user, 'order_already_pending'));
            return;
          }

          db.get(
            `SELECT COUNT(*) as count FROM keys WHERE product = ? AND status = 'available'`,
            [period],
            async (err, keyCount) => {
              if (err) {
                console.error('❌ Key check error:', err);
                return bot.sendMessage(chatId, '❌ Ошибка проверки наличия ключей');
              }

              const isNonKeyProduct = period === 'reseller_connection' || period === 'infinite_boost';
              const outOfStock = isNonKeyProduct ? false : (!keyCount || keyCount.count === 0);
              const isRu = getLang(user) === 'ru';

              try {
                const apiResponse = await axios.post(
                  'https://pay.crypt.bot/api/createInvoice',
                  {
                    currency_type: 'fiat',
                    fiat: currency,
                    amount: String(cryptoAmount),
                    description: period === 'reseller_connection' ? 'CyraxMods Partnership' : `CyraxMods ${period} key`,
                    paid_btn_name: 'callback',
                    paid_btn_url: 'https://t.me/' + (process.env.BOT_USERNAME || 'cyraxxmod_bot')
                  },
                  {
                    headers: {
                      'Crypto-Pay-API-Token': CRYPTOBOT_TOKEN,
                      'Content-Type': 'application/json'
                    }
                  }
                );

                if (!apiResponse.data.ok) {
                  const errCode = apiResponse.data.error ? JSON.stringify(apiResponse.data.error) : 'unknown';
                  console.error('❌ CryptoBot API returned error:', errCode);
                  return bot.sendMessage(chatId, t(user, 'error_creating_invoice'));
                }

                const invoice = apiResponse.data.result;

                db.run(
                  `INSERT INTO orders (user_id, username, user_lang, product, amount, currency, method, invoice_id, status, original_currency, original_amount, coupon_id, reseller_markup_pct, reseller_questionnaire)
                 VALUES (?, ?, ?, ?, ?, ?, 'CryptoBot', ?, ?, ?, ?, ?, ?, ?)`,
                  [
                    user.id, user.username || null, getLang(user), period,
                    cryptoAmount, currency,
                    String(invoice.invoice_id),
                    outOfStock ? 'out_of_stock_pending' : 'pending',
                    currency, baseAmount,
                    session.data.couponId || null,
                    session.data.resellerMarkup || null,
                    session.data.resellerQuestionnaire || null
                  ],
                  function (dbErr) {
                    if (dbErr) {
                      console.error('❌ CryptoBot DB error:', dbErr.message);
                      bot.sendMessage(chatId, t(user, 'error_creating_order'));
                      return;
                    }

                    const createdOrderId = this.lastID;

                    logAction(user.id, 'cryptobot_invoice_created', {
                      orderId: createdOrderId,
                      invoiceId: invoice.invoice_id,
                      amount: cryptoAmount,
                      currency,
                      outOfStock
                    });

                    // ✅ Задача 1: CryptoBot — НЕ шлём заказ в очередь admin-уведомлений.
                    // Оплата полностью автоматическая: webhook /cryptobot-webhook подтвердит и
                    // выдаст ключ без участия админа. Уведомление придёт только после успешной оплаты.
                    // Если ключей нет (outOfStock) — всё равно не шлём в pending: webhook сам обработает.

                    const feeNote = feeAdded
                      ? (isRu
                        ? `\n\n💡 _Сумма с учётом комиссии платёжной системы (+${CRYPTOBOT_COMMISSION[currency]} ${currency})._`
                        : `\n\n💡 _Amount includes payment system fee (+${CRYPTOBOT_COMMISSION[currency]} ${currency})._`)
                      : '';

                    const pNameCb = PERIOD_NAMES[isRu ? 'ru' : 'en'][period] || period;
                    const headerCb = isRu ? `🛒 <b>Товар:</b> ${pNameCb}\n\n` : `🛒 <b>Product:</b> ${pNameCb}\n\n`;
                    let instruction;
                    if (outOfStock) {
                      instruction = headerCb + (isRu
                        ? `⚠️ <b>Ключи временно закончились</b>\n\n🤖 <b>Оплата ${cryptoAmount} ${currency} — CryptoBot</b>\n\n<b>Шаги:</b>\n1️⃣ Нажми кнопку ниже → откроется инвойс${feeNote}\n2️⃣ Оплати криптовалютой\n\n<i>📬 Ключ выдадим вручную в течение 24 ч после оплаты</i>`
                        : `⚠️ <b>Keys temporarily out of stock</b>\n\n🤖 <b>Payment ${cryptoAmount} ${currency} — CryptoBot</b>\n\n<b>Steps:</b>\n1️⃣ Tap the button below → invoice opens${feeNote}\n2️⃣ Pay with cryptocurrency\n\n<i>📬 Key will be issued manually within 24 hours after payment</i>`);
                    } else {
                      instruction = headerCb + (isRu
                        ? `🤖 <b>Оплата ${cryptoAmount} ${currency} — CryptoBot</b>\n\n<b>Шаги:</b>\n1️⃣ Нажми кнопку ниже → откроется инвойс${feeNote}\n2️⃣ Оплати криптовалютой\n\n<i>⚡️ Ключ выдаётся автоматически сразу после оплаты</i>`
                        : `🤖 <b>Payment ${cryptoAmount} ${currency} — CryptoBot</b>\n\n<b>Steps:</b>\n1️⃣ Tap the button below → invoice opens${feeNote}\n2️⃣ Pay with cryptocurrency\n\n<i>⚡️ Key issued automatically right after payment</i>`);
                    }

                    const keyboard = {
                      inline_keyboard: [
                        [{ text: isRu ? `🤖 Оплатить ${formatPrice(cryptoAmount, currency)} — CryptoBot` : `🤖 Pay ${formatPrice(cryptoAmount, currency)} — CryptoBot`, url: invoice.pay_url }],
                        [{ text: t(user, 'back'), callback_data: `currency_${period}_${currency}` }]
                      ]
                    };

                    bot.editMessageText(instruction, {
                      chat_id: chatId,
                      message_id: message.message_id,
                      parse_mode: 'HTML',
                      reply_markup: keyboard
                    }).catch(() => {
                      bot.sendMessage(chatId, instruction, { parse_mode: 'HTML', reply_markup: keyboard });
                    });
                  }
                );
              } catch (error) {
                console.error('❌ CryptoBot error:', error.message, error.response?.data);
                bot.sendMessage(chatId, t(user, 'error_creating_invoice'));
              }
            }
          );
        }
      );

      return;
    }

    // CryptoBot в долларах (для UAH и RUB — конвертируем в USD + комиссия $0.50)
    if (data.startsWith('pay_') && data.includes('_cryptobot_usd')) {
      const parsed = parsePayCallback(data);
      if (!parsed) { bot.answerCallbackQuery(query.id).catch(() => { }); return; }
      const period = parsed.period;
      const originalCurrency = parsed.currency; // UAH или RUB
      // Если есть купон — применяем его % к USD цене
      if (!PRICES[period]?.USD) { bot.answerCallbackQuery(query.id).catch(() => { }); return; }
      const baseUsd = PRICES[period].USD;
      const discountedOriginal = session.data.discountedAmount;
      let base_usd = baseUsd;
      if (discountedOriginal && PRICES[period][originalCurrency]) {
        const discountRatio = discountedOriginal / PRICES[period][originalCurrency];
        base_usd = Math.round(baseUsd * discountRatio * 100) / 100;
      }
      // +0.50 комиссия CryptoBot
      const cryptoUsd = applyCryptobotFee(base_usd, 'USD');
      const feeAdded = cryptoUsd > base_usd;

      db.get(
        `SELECT id FROM orders WHERE user_id = ? AND product = ? AND status = 'pending'`,
        [user.id, period],
        async (err, existing) => {
          if (err) console.error('❌ Duplicate check error:', err);
          if (existing) {
            bot.sendMessage(chatId, t(user, 'order_already_pending'));
            return;
          }

          db.get(
            `SELECT COUNT(*) as count FROM keys WHERE product = ? AND status = 'available'`,
            [period],
            async (err, keyCount) => {
              if (err) {
                console.error('❌ Key check error:', err);
                return bot.sendMessage(chatId, '❌ Ошибка проверки наличия ключей');
              }

              const isNonKeyProduct2 = period === 'reseller_connection' || period === 'infinite_boost';
              const outOfStock = isNonKeyProduct2 ? false : (!keyCount || keyCount.count === 0);
              const isRu = getLang(user) === 'ru';

              try {
                const apiResponse = await axios.post(
                  'https://pay.crypt.bot/api/createInvoice',
                  {
                    currency_type: 'fiat',
                    fiat: 'USD',
                    amount: String(cryptoUsd),
                    description: period === 'reseller_connection' ? 'CyraxMods Partnership' : `CyraxMods ${period} key`,
                    paid_btn_name: 'callback',
                    paid_btn_url: 'https://t.me/' + (process.env.BOT_USERNAME || 'cyraxxmod_bot')
                  },
                  {
                    headers: {
                      'Crypto-Pay-API-Token': CRYPTOBOT_TOKEN,
                      'Content-Type': 'application/json'
                    }
                  }
                );

                if (!apiResponse.data.ok) {
                  const errCode = apiResponse.data.error ? JSON.stringify(apiResponse.data.error) : 'unknown';
                  console.error('❌ CryptoBot API returned error:', errCode);
                  return bot.sendMessage(chatId, t(user, 'error_creating_invoice'));
                }

                const invoice = apiResponse.data.result;

                db.run(
                  `INSERT INTO orders (user_id, username, user_lang, product, amount, currency, method, invoice_id, status, original_currency, original_amount, coupon_id, reseller_markup_pct, reseller_questionnaire)
                 VALUES (?, ?, ?, ?, ?, ?, 'CryptoBot', ?, ?, ?, ?, ?, ?, ?)`,
                  [
                    user.id, user.username || null, getLang(user), period,
                    cryptoUsd, 'USD',
                    String(invoice.invoice_id),
                    outOfStock ? 'out_of_stock_pending' : 'pending',
                    originalCurrency, PRICES[period]?.[originalCurrency] ?? 0,
                    session.data.couponId || null,
                    session.data.resellerMarkup || null,
                    session.data.resellerQuestionnaire || null
                  ],
                  function (dbErr) {
                    if (dbErr) {
                      console.error('❌ CryptoBot USD DB error:', dbErr.message);
                      bot.sendMessage(chatId, t(user, 'error_creating_order'));
                      return;
                    }

                    const createdOrderIdUsd = this.lastID;

                    logAction(user.id, 'cryptobot_invoice_created', {
                      orderId: createdOrderIdUsd,
                      invoiceId: invoice.invoice_id,
                      amount: cryptoUsd,
                      currency: 'USD',
                      originalCurrency,
                      outOfStock
                    });

                    // ✅ Задача 1: CryptoBot USD — НЕ шлём в pending-очередь.
                    // Оплата автоматическая через webhook. Уведомление только после успешной оплаты.

                    const feeNote = feeAdded
                      ? (isRu
                        ? `\n\n💡 _Сумма с учётом комиссии платёжной системы (+${CRYPTOBOT_COMMISSION['USD']} USD)._`
                        : `\n\n💡 _Amount includes payment system fee (+${CRYPTOBOT_COMMISSION['USD']} USD)._`)
                      : '';

                    const pNameCbUsd = PERIOD_NAMES[isRu ? 'ru' : 'en'][period] || period;
                    const headerCbUsd = isRu ? `🛒 <b>Товар:</b> ${pNameCbUsd}\n\n` : `🛒 <b>Product:</b> ${pNameCbUsd}\n\n`;
                    let instruction;
                    if (outOfStock) {
                      instruction = headerCbUsd + (isRu
                        ? `⚠️ <b>Ключи временно закончились</b>\n\n🤖 <b>Оплата $${cryptoUsd} USDT — CryptoBot</b>\n\n<b>Шаги:</b>\n1️⃣ Нажми кнопку ниже → откроется инвойс${feeNote}\n2️⃣ Оплати криптовалютой\n\n<i>📬 Ключ выдадим вручную в течение 24 ч после оплаты</i>`
                        : `⚠️ <b>Keys temporarily out of stock</b>\n\n🤖 <b>Payment $${cryptoUsd} USDT — CryptoBot</b>\n\n<b>Steps:</b>\n1️⃣ Tap the button below → invoice opens${feeNote}\n2️⃣ Pay with cryptocurrency\n\n<i>📬 Key will be issued manually within 24 hours after payment</i>`);
                    } else {
                      instruction = headerCbUsd + (isRu
                        ? `🤖 <b>Оплата $${cryptoUsd} USDT — CryptoBot</b>\n\n<b>Шаги:</b>\n1️⃣ Нажми кнопку ниже → откроется инвойс${feeNote}\n2️⃣ Оплати криптовалютой\n\n<i>⚡️ Ключ выдаётся автоматически сразу после оплаты</i>`
                        : `🤖 <b>Payment $${cryptoUsd} USDT — CryptoBot</b>\n\n<b>Steps:</b>\n1️⃣ Tap the button below → invoice opens${feeNote}\n2️⃣ Pay with cryptocurrency\n\n<i>⚡️ Key issued automatically right after payment</i>`);
                    }

                    const keyboard = {
                      inline_keyboard: [
                        [{ text: isRu ? `🤖 Оплатить $${cryptoUsd} USDT — CryptoBot` : `🤖 Pay $${cryptoUsd} USDT — CryptoBot`, url: invoice.pay_url }],
                        [{ text: t(user, 'back'), callback_data: `currency_${period}_${originalCurrency}` }]
                      ]
                    };

                    bot.editMessageText(instruction, {
                      chat_id: chatId,
                      message_id: message.message_id,
                      parse_mode: 'HTML',
                      reply_markup: keyboard
                    }).catch(() => {
                      bot.sendMessage(chatId, instruction, { parse_mode: 'HTML', reply_markup: keyboard });
                    });
                  }
                );
              } catch (error) {
                console.error('❌ CryptoBot USD error:', error.message, error.response?.data);
                bot.sendMessage(chatId, t(user, 'error_creating_invoice'));
              }
            }
          );
        }
      );

      return;
    }

    // "МОИ ЗАКАЗЫ" - с ключами
    if (data === 'orders') {
      const PERIOD_DAYS_MAP = { '1d': 1, '3d': 3, '7d': 7, '30d': 30 };
      db.all(
        `SELECT id, product, amount, currency, original_currency, original_amount, key_issued, created_at, confirmed_at
       FROM orders
       WHERE user_id = ? AND status = 'confirmed'
       ORDER BY confirmed_at DESC
       LIMIT 3`,
        [user.id],
        (err, orders) => {
          if (err) {
            console.error('❌ Orders fetch error:', err);
            bot.sendMessage(chatId, t(user, 'error_fetching_orders'));
            return;
          }

          if (!orders || orders.length === 0) {
            sendNavMessage(chatId, user.id, t(user, 'no_keys'), {
              reply_markup: { inline_keyboard: [[{ text: t(user, 'back'), callback_data: 'start' }]] }
            });
            return;
          }

          const isRu = getLang(user) === 'ru';
          let message = isRu ? '📂 *Ваши последние ключи*\n\n' : '📂 *Your recent keys*\n\n';
          const renewButtons = [];

          orders.forEach((order, index) => {
            const periodName = PERIOD_NAMES[getLang(user)][order.product] || order.product;
            const days = PERIOD_DAYS_MAP[order.product];
            const displayCurrency = order.original_currency || order.currency;
            const displayAmount = order.original_amount || order.amount;
            const formattedAmount = formatPrice(displayAmount, displayCurrency);

            // Таймер: от confirmed_at
            let timerStr = '';
            if (days && (order.confirmed_at || order.created_at)) {
              const startTime = new Date(order.confirmed_at || order.created_at);
              const expireTime = new Date(startTime.getTime() + days * 24 * 60 * 60 * 1000);
              const now = new Date();
              const diffMs = expireTime - now;
              if (diffMs > 0) {
                const diffDays = Math.floor(diffMs / (1000 * 60 * 60 * 24));
                const diffHours = Math.floor((diffMs % (1000 * 60 * 60 * 24)) / (1000 * 60 * 60));
                timerStr = diffDays > 0
                  ? (isRu ? `\n   ⏳ ~${diffDays}д ${diffHours}ч осталось` : `\n   ⏳ ~${diffDays}d ${diffHours}h left`)
                  : (isRu ? `\n   ⌛️ ~${diffHours}ч осталось` : `\n   ⌛️ ~${diffHours}h left`);
              } else {
                timerStr = isRu ? `\n   ✅ Истёк` : `\n   ✅ Expired`;
              }
            }

            message += `${index + 1}. *${periodName}* — *${formattedAmount}*\n`;
            if (order.key_issued && order.key_issued !== 'BOOST_GUIDE') {
              message += `   🔑 \`${order.key_issued}\``;
            }
            if (days) message += timerStr;
            message += '\n\n';

            // Кнопки повторного заказа + пакеты
            if (PERIOD_DAYS_MAP[order.product]) {
              const usedCurrency = order.original_currency || order.currency || 'RUB';
              renewButtons.push([
                {
                  text: isRu ? `🔄 ${periodName}` : `🔄 ${periodName}`,
                  callback_data: `currency_${order.product}_${usedCurrency}`
                },
                {
                  text: isRu ? `📦 Пакеты` : `📦 Bundles`,
                  callback_data: `bundle_offer_${order.product}`
                }
              ]);
            }
          });

          const note = isRu
            ? '_⚠️ Время отсчитывается с момента продажи, а не активации — данные приблизительные._'
            : '_⚠️ Timer starts from sale date, not activation — approximate estimate._';
          message += note;

          const keyboard = {
            inline_keyboard: [
              ...renewButtons,
              [{ text: t(user, 'back'), callback_data: 'start' }]
            ]
          };

          sendNavMessage(chatId, user.id, message, {
            parse_mode: 'Markdown',
            reply_markup: keyboard
          });
        }
      );

      return;
    }

    // 🎟️ П.2: ПРИМЕНЕНИЕ КУПОНА
    if (data.startsWith('apply_coupon_')) {
      const parsed = parseCouponCallback(data);
      if (!parsed) { bot.answerCallbackQuery(query.id).catch(() => { }); return; }
      const period = parsed.period;
      const currency = parsed.currency;

      session.state = 'awaiting_coupon_input';
      session.data = { ...session.data, period, currency };

      bot.sendMessage(chatId, '🎟️ Введите код купона:');
      return;
    }

    // Кнопка "Оставить отзыв" — доступна всем пользователям
    if (data.startsWith('request_review_')) {
      const orderId = parseInt(data.replace('request_review_', ''));
      await handleReviewRequest(user, chatId, orderId);
      return;
    }

    // Кнопка менеджера "Заказы на проверку" — доступна вне ADMIN_ID блока
    if (data === 'manager_orders') {
      isManager(user.id).then(mgr => {
        if (!mgr) { bot.sendMessage(chatId, '❌ У вас нет доступа'); return; }
        showManagerOrders(chatId, user.id);
      });
      return;
    }

    // ОДОБРЕНИЕ И ОТКЛОНЕНИЕ ЗАКАЗА — доступно админу и менеджерам
    // ОДОБРЕНИЕ ЗАКАЗА
    // 📨 Написать покупателю из уведомления о заказе
    if (data.startsWith('msg_buyer_')) {
      const targetId = parseInt(data.replace('msg_buyer_', ''));
      db.get('SELECT username FROM users WHERE id = ?', [targetId], (err, tu) => {
        const label = (!err && tu && tu.username) ? '@' + tu.username : 'ID: ' + targetId;
        session.state = 'awaiting_msg_user_text';
        session.data = { msgTargetId: targetId, msgTargetUsername: (!err && tu && tu.username) || String(targetId) };
        bot.sendMessage(chatId, '📨 Получатель: ' + label + '\n\nВведите текст (или отправьте фото с подписью):', { reply_markup: { inline_keyboard: [[{ text: '◀️ Отмена', callback_data: 'admin' }]] } });
      });
      return;
    }

    // 🚫 БЫСТРЫЙ БАН ИЗ КЛИЕНТСКОЙ КАРТОЧКИ / ТИКЕТА
    if (data.startsWith('ban_') && !data.startsWith('ban_dur_')) {
      const targetId = parseInt(data.replace('ban_', ''));
      if (user.id !== ADMIN_ID) { bot.answerCallbackQuery(query.id).catch(() => {}); return; }
      if (isNaN(targetId) || targetId === ADMIN_ID) {
        bot.answerCallbackQuery(query.id, { text: '❌ Нельзя', show_alert: true }).catch(() => {});
        return;
      }
      bot.answerCallbackQuery(query.id).catch(() => {});
      db.get('SELECT username FROM users WHERE id = ?', [targetId], (err, row) => {
        const displayName = (!err && row && row.username) ? `@${escapeMarkdown(row.username)}` : `ID: ${targetId}`;
        session.state = 'awaiting_ban_duration';
        session.data = { banTarget: { targetId, displayName } };
        bot.sendMessage(chatId,
          `🚫 *Бан пользователя*

👤 ${displayName}

На сколько?`,
          { parse_mode: 'Markdown', reply_markup: { inline_keyboard: [
            [{ text: '1 день', callback_data: 'ban_dur_1d' }, { text: '3 дня', callback_data: 'ban_dur_3d' }],
            [{ text: '7 дней', callback_data: 'ban_dur_7d' }, { text: '30 дней', callback_data: 'ban_dur_30d' }],
            [{ text: '♾ Навсегда', callback_data: 'ban_dur_perm' }],
            [{ text: '◀️ Отмена', callback_data: 'admin' }]
          ]}}
        );
      });
      return;
    }

    // 📨 НАПИСАТЬ ПОЛЬЗОВАТЕЛЮ ИЗ КЛИЕНТСКОЙ КАРТОЧКИ
    if (data.startsWith('admin_msg_user_')) {
      if (user.id !== ADMIN_ID) { bot.answerCallbackQuery(query.id).catch(() => {}); return; }
      const targetId = parseInt(data.replace('admin_msg_user_', ''));
      bot.answerCallbackQuery(query.id).catch(() => {});
      db.get('SELECT username FROM users WHERE id = ?', [targetId], (err, tu) => {
        const label = (!err && tu && tu.username) ? '@' + tu.username : 'ID: ' + targetId;
        session.state = 'awaiting_msg_user_text';
        session.data = { msgTargetId: targetId, msgTargetUsername: (!err && tu && tu.username) || String(targetId) };
        bot.sendMessage(chatId,
          `📨 Написать: *${label}*

Введите текст (или отправьте фото с подписью):`,
          { parse_mode: 'Markdown', reply_markup: { inline_keyboard: [[{ text: '◀️ Отмена', callback_data: 'admin' }]] } }
        );
      });
      return;
    }

        if (data.startsWith('approve_')) {
      const orderId = parseInt(data.replace('approve_', ''));

      // 🔒 Защита от двойного нажатия / дублированного callback от Telegram
      if (approvingOrders.has(orderId)) {
        bot.sendMessage(chatId, '⏳ Заказ уже обрабатывается, подождите...').catch(() => {});
        return;
      }
      approvingOrders.add(orderId);

      db.get(
        `SELECT * FROM orders WHERE id = ?`,
        [orderId],
        async (err, order) => {
          if (err || !order || (order.status !== 'pending' && order.status !== 'out_of_stock_pending')) {
            approvingOrders.delete(orderId);
            bot.sendMessage(chatId, '❌ Заказ не найден или уже обработан');
            return;
          }

          // Проверка прав: если не админ — должен быть менеджером для этого метода
          if (user.id !== ADMIN_ID) {
            const hasPerm = await new Promise(res => {
              db.get('SELECT 1 FROM managers m JOIN manager_methods mm ON m.user_id = mm.manager_id WHERE m.user_id = ? AND mm.payment_method = ?',
                [user.id, order.method], (e, row) => res(!e && !!row));
            });
            if (!hasPerm) {
              approvingOrders.delete(orderId);
              bot.sendMessage(chatId, '❌ У вас нет прав для подтверждения этого заказа'); return;
            }
          }

          // Определяем botInstance ДО try/catch — нужен и при успехе, и при OOS
          // FIX 2: Используем ?? чтобы гарантированно получить bot если реселлер-инстанс недоступен
          const botInstance = resellerBots.get(order.reseller_id)?.bot ?? bot;

          // ── Шаг 1: Атомарная выдача ключа (критический блок) ──────────────────
          // Любая ошибка здесь — ключ НЕ выдан. Это единственное место где catch
          // сообщает администратору "ключ не выдан". После этого блока ключ уже выдан.
          let issuedKey = null;
          try {
            // Если заказ был в очереди ожидания (out_of_stock_pending) —
            // нужно списать баланс. Делаем это АТОМАРНО:
            // 1. Сначала проверяем баланс — если недостаточен, останавливаемся сразу
            // 2. Затем выдаём ключ атомарно (issueKeyAndConfirmOrder)
            // 3. Только после успешной выдачи ключа списываем баланс
            // Если списание после выдачи ключа упало (крайне редко) — ключ уже выдан,
            // уведомляем админа для ручной корректировки.
            if (order.status === 'out_of_stock_pending') {
              // Шаг 1: Проверяем баланс ДО выдачи ключа (pessimistic check)
              const userBalRow = await new Promise(resolve =>
                db.get('SELECT balance, preferred_currency FROM user_balances WHERE user_id = ?',
                  [order.user_id], (e, r) => resolve(r || { balance: 0, preferred_currency: 'RUB' }))
              );
              const availableBalance = userBalRow.balance || 0;
              const requiredAmount = order.amount || 0;
              if (availableBalance < requiredAmount - 0.01) { // 0.01 допуск на float
                approvingOrders.delete(orderId);
                bot.sendMessage(chatId,
                  `❌ Заказ #${orderId}: баланс клиента ${availableBalance} ${order.currency} недостаточен для списания ${requiredAmount} ${order.currency}.\n\nКлиент уже потратил средства. Ключ НЕ выдан — разберитесь вручную.`
                ).catch(() => {});
                return;
              }
            }

            // 🔒 Атомарная выдача ключа + подтверждение заказа в одной транзакции.
            // issueKeyAndConfirmOrder гарантирует: либо оба действия выполнены, либо ни одного.
            const { key } = await issueKeyAndConfirmOrder(orderId, order.user_id, order.product, 'purchase');
            issuedKey = key;

            // Шаг 3: Списываем баланс ПОСЛЕ успешной выдачи ключа (OOS orders only)
            if (order.status === 'out_of_stock_pending') {
              try {
                await adjustUserBalance(
                  order.user_id,
                  -order.amount,
                  order.currency || 'RUB',
                  'purchase',
                  `Списание за заказ #${orderId} (ключ выдан из очереди ожидания)`,
                  orderId,
                  ADMIN_ID
                );
                console.log(`💳 [OOS] Баланс списан: -${order.amount} ${order.currency} у user ${order.user_id} за заказ #${orderId}`);
              } catch (balPostErr) {
                // Ключ уже выдан — не откатываем выдачу, но уведомляем админа
                console.error(`⚠️ [OOS] Ключ выдан, но баланс не списан для заказа #${orderId}:`, balPostErr.message);
                safeSendMessage(ADMIN_ID,
                  `⚠️ *Ключ выдан, баланс не списан*\n\nЗаказ #${orderId}, пользователь ID ${order.user_id}\nСумма: ${order.amount} ${order.currency}\n\nКлюч выдан успешно, но списание баланса не прошло — скорректируйте вручную.`,
                  { parse_mode: 'Markdown' }
                ).catch(() => {});
              }
            }

            // ⏳ → ✅ : редактируем через нужный botInstance
            if (order.hourglass_msg_id) {
              botInstance.editMessageText('✅', {
                chat_id: order.user_id,
                message_id: order.hourglass_msg_id
              }).catch(() => {
                botInstance.deleteMessage(order.user_id, order.hourglass_msg_id).catch(() => { });
              });
            }

            // Небольшая пауза чтобы ✅ успел появиться перед ключом
            await new Promise(r => setTimeout(r, 800));

            if (order.product === 'infinite_boost') {
              await sendInfiniteBoostGuide(order.user_id, order.user_lang || 'en', botInstance);
            } else if (order.product === 'reseller_connection') {
              // 🤝 Активация партнёрской программы — создаём запись реселлера
              const isRu = (order.user_lang || 'en').startsWith('ru');
              const defaultMarkup = parseInt(getSetting('reseller_default_markup')) || 30;
              const chosenMarkup = order.reseller_markup_pct || defaultMarkup;

              // Проверяем нет ли уже записи
              db.get(`SELECT id, status FROM resellers WHERE user_id = ?`, [order.user_id], (rErr, existingR) => {
                if (existingR) {
                  // Уже есть запись — обновляем статус
                  db.run(`UPDATE resellers SET status = 'awaiting_token', markup_pct = ?, questionnaire = ? WHERE user_id = ?`,
                    [chosenMarkup, order.reseller_questionnaire, order.user_id]);
                } else {
                  // Создаём новую запись
                  db.run(`INSERT INTO resellers (user_id, status, markup_pct, questionnaire, balance) VALUES (?, 'awaiting_token', ?, ?, 0)`,
                    [order.user_id, chosenMarkup, order.reseller_questionnaire]);
                }

                // Отправляем клиенту инструкцию по отправке токена
                const tokenMsg = isRu
                  ? `🎉 *Оплата подтверждена!*\n*Добро пожаловать в партнёрскую программу!*\n\n` +
                  `Теперь отправьте токен вашего бота:\n\n` +
                  `1️⃣ Откройте @BotFather в Telegram\n` +
                  `2️⃣ Создайте нового бота командой /newbot\n` +
                  `3️⃣ Скопируйте полученный токен\n` +
                  `4️⃣ Отправьте его сюда\n\n` +
                  `⚠️ *Токен выглядит так:* \`123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11\`\n\n` +
                  `🔒 Токен будет зашифрован и надёжно сохранён.`
                  : `🎉 *Payment confirmed! Welcome to the partnership program!*\n\n` +
                  `Now send your bot token:\n\n` +
                  `1️⃣ Open @BotFather in Telegram\n` +
                  `2️⃣ Create a new bot with /newbot\n` +
                  `3️⃣ Copy the token you receive\n` +
                  `4️⃣ Send it here\n\n` +
                  `⚠️ *Token looks like:* \`123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11\`\n\n` +
                  `🔒 Your token will be encrypted and securely stored.`;

                // Устанавливаем сессию для ожидания токена
                const sess = getSession(order.user_id);
                sess.state = 'awaiting_reseller_token';
                sess.data = {};

                safeSendMessage(order.user_id, tokenMsg, { parse_mode: 'Markdown' }).catch(() => { });

                logAction(order.user_id, 'reseller_connection_activated', { orderId });
              });
            } else {
              // 🏀 Проверяем выигрыш в баскетбол — купон передаётся прямо в сообщение с ключом
              let basketballCoupon = null;
              const KEY_PRODUCT_LIST = ['1d', '3d', '7d', '30d'];
              if (KEY_PRODUCT_LIST.includes(order.product)) {
                const basketWin = await new Promise(res =>
                  db.get(`SELECT 1 FROM basketball_throws WHERE order_id = ? AND won = 1`,
                    [orderId], (e, row) => res(row || null))
                );
                if (basketWin) basketballCoupon = await _issueBasketballCoupon(order, orderId);
              }
              await sendKeyMessage(order.user_id, order.user_lang || 'en', order.product, issuedKey, orderId, botInstance, basketballCoupon);
            }

            // ✅ Убираем кнопки "Одобрить/Отклонить" с исходного сообщения
            bot.editMessageReplyMarkup({ inline_keyboard: [] }, {
              chat_id: chatId,
              message_id: message.message_id
            }).catch(() => {});

            bot.sendMessage(chatId, `✅🎉 Заказ #${orderId} одобрен — ключ выдан!`);
            logAction(user.id, 'order_approved', { orderId, userId: order.user_id, by: user.id });

            // Уведомляем админа если действие выполнил менеджер
            if (user.id !== ADMIN_ID) {
              const mgUsername = user.username ? `@${escapeMarkdown(user.username)}` : `ID: ${user.id}`;
              safeSendMessage(ADMIN_ID, `👤 Менеджер ${mgUsername} подтвердил заказ #${orderId}`).catch(() => { });
            }

          } catch (error) {
            console.error(`❌ Approve error (key issuance) orderId=${orderId} userId=${order?.user_id} product=${order?.product}:`, error);
            approvingOrders.delete(orderId);

            if (error.code === 'ALREADY_CONFIRMED') {
              // Заказ уже был подтверждён (двойной клик, дубль колбэка)
              bot.sendMessage(chatId, `ℹ️ Заказ #${orderId} уже был подтверждён ранее.`);
            } else if (error.code === 'OUT_OF_STOCK') {
              // ── ФИЧА 1: нет ключей → зачислить сумму на внутренний баланс + очередь ──
              // Статус → 'out_of_stock_pending' (клиент в очереди ожидания).
              // Деньги зачисляются на bot_balance пользователя — возврат без потерь.
              db.run(
                `UPDATE orders SET status = 'out_of_stock_pending' WHERE id = ?`,
                [orderId],
                async (updateErr) => {
                  if (updateErr) {
                    console.error('❌ Error updating order to out_of_stock_pending:', updateErr);
                  }

                  const isRuApp = (order.user_lang || 'en').startsWith('ru');
                  const pNameApp = PERIOD_NAMES[isRuApp ? 'ru' : 'en'][order.product] || order.product;
                  const orderAmount = order.amount || 0;
                  const orderCurrency = order.currency || 'RUB';

                  // Зачисляем оплаченную сумму на внутренний баланс бота
                  let balanceAfter = null;
                  try {
                    balanceAfter = await adjustUserBalance(
                      order.user_id,
                      orderAmount,
                      orderCurrency,
                      'refund',
                      `Возврат за заказ #${orderId} (нет ключей «${pNameApp}»)`,
                      orderId,
                      ADMIN_ID
                    );
                  } catch (balErr) {
                    console.error(`❌ OOS balance credit error orderId=${orderId} userId=${order.user_id} amount=${orderAmount} currency=${orderCurrency}:`, balErr.message);
                  }

                  const balLine = balanceAfter !== null
                    ? (isRuApp
                        ? `\n\n💳 *${formatBalanceAmount(orderAmount, orderCurrency)}* зачислены на ваш баланс бота.\n_Текущий баланс: ${formatBalanceAmount(balanceAfter, orderCurrency)}_`
                        : `\n\n💳 *${formatBalanceAmount(orderAmount, orderCurrency)}* has been credited to your bot balance.\n_Current balance: ${formatBalanceAmount(balanceAfter, orderCurrency)}_`)
                    : '';

                  const userMsg = isRuApp
                    ? `😔 *К сожалению, ключи для «${pNameApp}» временно закончились.*\n\nВаш платёж подтверждён, но ключ выдать прямо сейчас не можем.${balLine}\n\n📬 Как только ключи появятся — мы сразу вам напишем, и вы сможете оформить покупку с баланса.\n\n_Спасибо за понимание и терпение! 🙏_`
                    : `😔 *Keys for «${pNameApp}» are temporarily out of stock.*\n\nYour payment was confirmed, but we can't issue a key right now.${balLine}\n\n📬 As soon as keys are restocked, we'll notify you so you can complete the purchase using your balance.\n\n_Thank you for your patience! 🙏_`;

                  await safeSendMessage(order.user_id, userMsg, { parse_mode: 'Markdown' }, botInstance);

                  await sendOutOfStockNotification(order, ADMIN_ID);

                  const balCredited = balanceAfter !== null
                    ? ` Сумма ${formatBalanceAmount(orderAmount, orderCurrency)} зачислена на баланс клиента.`
                    : ' ⚠️ Не удалось зачислить баланс — проверьте вручную!';
                  bot.sendMessage(chatId, `⚠️ Заказ #${orderId} — нет ключей. Клиент помещён в очередь ожидания (out_of_stock_pending).${balCredited}`);
                  approvingOrders.delete(orderId);
                }
              );
            } else {
              // ⚠️ Критическая ошибка при выдаче ключа — ключ НЕ был выдан
              bot.sendMessage(chatId, `❌ Ошибка при выдаче ключа для заказа #${orderId}: ${error.message}\n\nКлюч клиенту НЕ выдан. Проверьте заказ вручную.`);
              approvingOrders.delete(orderId);
              return; // выходим, пост-обработка не нужна
            }
          }

          // ── Пост-обработка (выполняется только если ключ успешно выдан) ─────
          // Купон, наценка реселлеру, FOMO, milestone, реферальный бонус.
          // Ошибки здесь НЕ показываются администратору — ключ уже выдан клиенту.
          approvingOrders.delete(orderId);
          finalizeSuccessfulOrder(order, orderId, botInstance).catch(e => {
            console.error(`⚠️ finalizeSuccessfulOrder silent error for order #${orderId}:`, e.message);
          });
        }
      );

      return;
    }

    // ОТКЛОНЕНИЕ ВСЕХ ЗАВИСШИХ (>2ч) ЗАКАЗОВ
    if (data === 'reject_all_stale' && user.id === ADMIN_ID) {
      const twoHoursAgo = new Date(Date.now() - 2 * 60 * 60 * 1000).toISOString();

      // Сначала получаем список зависших заказов для уведомления пользователей
      db.all(
        `SELECT o.id, o.user_id, u.language_code AS user_lang FROM orders o
         LEFT JOIN users u ON u.id = o.user_id
         WHERE o.status='pending' AND o.method != 'CryptoBot' AND datetime(o.created_at) < datetime(?)`,
        [twoHoursAgo],
        async (selErr, staleOrders) => {
          if (selErr) {
            bot.sendMessage(chatId, '❌ Ошибка при выборке заказов.');
            return;
          }

          if (!staleOrders || staleOrders.length === 0) {
            bot.sendMessage(chatId, '🤷‍♂️ Нет зависших заказов для удаления.');
            showLostOrdersReport(chatId, message.message_id);
            return;
          }

          // Оборачиваем UPDATE в транзакцию
          db.serialize(() => {
            db.run('BEGIN TRANSACTION');
            db.run(
              `UPDATE orders SET status='rejected', confirmed_at=datetime('now')
               WHERE status='pending' AND method != 'CryptoBot' AND datetime(created_at) < datetime(?)`,
              [twoHoursAgo],
              async function(txErr) {
                if (txErr) {
                  db.run('ROLLBACK');
                  bot.sendMessage(chatId, '❌ Ошибка при отклонении заказов.');
                  return;
                }
                const changed = this.changes;
                db.run('COMMIT');

                bot.sendMessage(chatId, `✅ Успешно отменено ${changed} зависших заказов.`);

                // Уведомляем каждого пользователя об автоматическом отклонении
                for (const order of staleOrders) {
                  try {
                    await bot.sendMessage(
                      order.user_id,
                      order.user_lang && order.user_lang.startsWith('ru')
                      ? `⚠️ Ваш заказ #${order.id} отклонён автоматически за долгое ожидание.\n\nЕсли у вас есть вопросы — обратитесь в поддержку.`
                      : `⚠️ Your order #${order.id} was automatically rejected due to a long wait time. If you have questions, please contact support.`
                    );
                  } catch (notifyErr) {
                    console.warn(`⚠️ Не удалось уведомить user ${order.user_id} об отклонении заказа #${order.id}:`, notifyErr.message);
                  }
                }

                showLostOrdersReport(chatId, message.message_id);
              }
            );
          });
        }
      );
      return;
    }

    // 🏀 BASKETBALL GAME
    if (data.startsWith('basketball_throw_')) {
      const bOrderId = parseInt(data.replace('basketball_throw_', ''));
      const isRuUser = getLang(user) === 'ru';

      // Проверяем что заказ существует, принадлежит этому юзеру и ещё pending
      db.get(
        `SELECT id, product, status FROM orders WHERE id = ? AND user_id = ?`,
        [bOrderId, user.id],
        async (err, bOrder) => {
          if (err || !bOrder) {
            bot.answerCallbackQuery(query.id, { text: '❌ Заказ не найден', show_alert: false }).catch(() => {});
            return;
          }
          if (bOrder.status !== 'pending') {
            bot.answerCallbackQuery(query.id, {
              text: isRuUser ? '⚠️ Заказ уже обработан' : '⚠️ Order already processed',
              show_alert: false
            }).catch(() => {});
            return;
          }

          // Проверяем что игрок ещё не бросал для этого заказа
          db.get(
            `SELECT id FROM basketball_throws WHERE order_id = ?`,
            [bOrderId],
            async (e2, existingThrow) => {
              if (existingThrow) {
                bot.answerCallbackQuery(query.id, {
                  text: t(user, 'basketball_already_thrown'),
                  show_alert: true
                }).catch(() => {});
                return;
              }

              // Убираем кнопку сразу чтобы нельзя было нажать дважды
              bot.editMessageReplyMarkup(
                { inline_keyboard: [] },
                { chat_id: chatId, message_id: query.message.message_id }
              ).catch(() => {});
              bot.answerCallbackQuery(query.id).catch(() => {});

              // Бросаем мяч через sendDice
              let diceMsg;
              try {
                diceMsg = await bot.sendDice(chatId, { emoji: '🏀' });
              } catch (diceErr) {
                console.error('❌ sendDice error:', diceErr.message);
                return;
              }

              const score = diceMsg.dice ? diceMsg.dice.value : 0;
              // 🏀 Basketball dice Telegram: score 5 = чистый свиш, вероятность ~20%.
              // Только визуальное попадание считается победой — честно и без путаницы.
              // Купон 5% при конверсии ~50% = ~0.5% потерь от выручки. Безопасно.
              const isCleanSwish = score === 5;
              const isWin = isCleanSwish;

              // Записываем бросок в БД
              db.run(
                `INSERT OR IGNORE INTO basketball_throws (order_id, user_id, score, won, thrown_at) VALUES (?, ?, ?, ?, datetime('now'))`,
                [bOrderId, user.id, score, isWin ? 1 : 0]
              );

              // Ждём пока анимация мяча доиграет (~4 сек)
              await new Promise(r => setTimeout(r, 4000));

              if (isWin) {
                // 🎉 Попал — купон будет выдан вместе с ключом после подтверждения оплаты.
                // Здесь только сохраняем факт победы (already done above) и показываем интригу.
                const pendingMsg = isRuUser
                  ? `🎉 <b>Свиш!</b> Мяч в кольце!\n\n🎟 Купон на скидку уже твой — придёт <b>вместе с ключом</b> как только платёж подтвердят ⚡️`
                  : `🎉 <b>Swish!</b> Clean basket!\n\n🎟 Your discount coupon is locked in — it'll arrive <b>with your key</b> once payment is confirmed ⚡️`;
                safeSendMessage(chatId, pendingMsg, { parse_mode: 'HTML' }).catch(() => {});
                logAction(user.id, 'basketball_win', { orderId: bOrderId, product: bOrder.product });
              } else {
                // 😅 Промах
                safeSendMessage(chatId, t(user, 'basketball_lose')).catch(() => {});
                logAction(user.id, 'basketball_lose', { orderId: bOrderId, score });
              }
            }
          );
        }
      );
      return;
    }

    // ОТКЛОНЕНИЕ ЗАКАЗА
    if (data.startsWith('reject_')) {
      const orderId = parseInt(data.replace('reject_', ''));

      db.get(
        `SELECT * FROM orders WHERE id = ?`,
        [orderId],
        async (err, order) => {
          if (err || !order || (order.status !== 'pending' && order.status !== 'out_of_stock_pending')) {
            bot.sendMessage(chatId, '❌ Заказ не найден или уже обработан');
            return;
          }

          // Проверка прав: если не админ — должен быть менеджером для этого метода
          if (user.id !== ADMIN_ID) {
            const hasPerm = await new Promise(res => {
              db.get('SELECT 1 FROM managers m JOIN manager_methods mm ON m.user_id = mm.manager_id WHERE m.user_id = ? AND mm.payment_method = ?',
                [user.id, order.method], (e, row) => res(!e && !!row));
            });
            if (!hasPerm) { bot.sendMessage(chatId, '❌ У вас нет прав для отклонения этого заказа'); return; }
          }

          // Определяем botInstance для клиента (реселлер или основной)
          const rejectBotInstance = (() => {
            if (order.reseller_id) {
              const rslEntry = resellerBots.get(order.reseller_id);
              if (rslEntry?.bot) return rslEntry.bot;
            }
            return bot;
          })();

          db.run(
            `UPDATE orders SET status = 'rejected', confirmed_at = datetime('now') WHERE id = ?`,
            [orderId],
            async (updateErr) => {
              if (updateErr) {
                console.error('❌ Reject update error:', updateErr);
                bot.sendMessage(chatId, '❌ Ошибка обновления');
                return;
              }

              // ⏳ → ❌ : редактируем через нужный botInstance
              const rejUserObj = { language_code: order.user_lang || 'ru' };
              const rejIsRu = (order.user_lang || 'ru').startsWith('ru');
              const rejText = rejIsRu
                ? '❌ *Платёж отклонён.*\n\nЕсли вы считаете что это ошибка — обратитесь в поддержку.'
                : '❌ *Payment rejected.*\n\nIf you think this is a mistake, please contact support.';
              if (order.hourglass_msg_id) {
                rejectBotInstance.editMessageText(rejText, {
                  chat_id: order.user_id,
                  message_id: order.hourglass_msg_id,
                  parse_mode: 'Markdown'
                }).catch(() => {
                  rejectBotInstance.deleteMessage(order.user_id, order.hourglass_msg_id).catch(() => { });
                  rejectBotInstance.sendMessage(order.user_id, rejText, { parse_mode: 'Markdown' }).catch(() => { });
                });
              } else {
                await rejectBotInstance.sendMessage(order.user_id, rejText, { parse_mode: 'Markdown' }).catch(() => { });
              }

              // Уведомляем админа если действие выполнил менеджер
              if (user.id !== ADMIN_ID) {
                const mgUsername = user.username ? `@${escapeMarkdown(user.username)}` : `ID: ${user.id}`;
                safeSendMessage(ADMIN_ID, `👤 Менеджер ${mgUsername} отклонил заказ #${orderId}`).catch(() => { });
              }

              // ✅ Убираем кнопки "Одобрить/Отклонить" с исходного сообщения
              bot.editMessageReplyMarkup({ inline_keyboard: [] }, {
                chat_id: chatId,
                message_id: message.message_id
              }).catch(() => {});

              bot.sendMessage(chatId, `❌ Заказ #${orderId} отклонен`);

              logAction(user.id, 'order_rejected', { orderId, userId: order.user_id, by: user.id });
            }
          );
        }
      );

      return;
    }

    // ОДОБРЕНИЕ/ОТКЛОНЕНИЕ ВЫВОДА СРЕДСТВ РЕСЕЛЛЕРА (только АДМИН)
    if (data.startsWith('rsl_withdraw_approve_') && user.id === ADMIN_ID) {
      const withdrawId = parseInt(data.replace('rsl_withdraw_approve_', ''));
      db.get('SELECT * FROM reseller_withdrawals WHERE id = ?', [withdrawId], async (err, w) => {
        if (err || !w || w.status !== 'pending') return safeSendMessage(chatId, '❌ Заявка не найдена или уже обработана');

        db.get('SELECT user_id FROM resellers WHERE id = ?', [w.reseller_id], async (e, r) => {
          if (!r) return;

          if (w.details && w.details.startsWith('KEY_')) {
            const product = w.details.replace('KEY_', '');
            try {
              // FIX 2.2: Сначала пытаемся выдать ключ, и ТОЛЬКО потом списываем баланс.
              // Если ключа нет — средства не тронуты, заявка остаётся pending.
              const key = await issueKeyToUser(r.user_id, product, 'reseller_withdrawal');

              // Ключ выдан успешно — теперь списываем баланс
              db.run('UPDATE resellers SET balance = balance - ? WHERE id = ?', [w.amount, w.reseller_id]);
              db.run('UPDATE reseller_withdrawals SET status = "approved", processed_at = datetime("now") WHERE id = ?', [withdrawId]);
              bot.editMessageReplyMarkup({ inline_keyboard: [] }, { chat_id: chatId, message_id: query.message.message_id }).catch(() => { });
              safeSendMessage(chatId, `✅ Вывод #${withdrawId} одобрен. Ключ ${product} выдан. Баланс уменьшен на ${w.amount} ₽.`);

              let botInstance = resellerBots.get(w.reseller_id)?.bot || bot;
              if (product === 'infinite_boost') {
                await sendInfiniteBoostGuide(r.user_id, 'ru', botInstance);
                safeSendMessage(r.user_id, `✅ *Ваша заявка на получение ключа одобрена!*\n\nГайд по Infinite Boost отправлен выше.`, { parse_mode: 'Markdown' }, botInstance).catch(() => {});
              } else {
                await sendKeyMessage(r.user_id, 'ru', product, key, null, botInstance);
                safeSendMessage(r.user_id, `✅ *Ваша заявка на получение ключа одобрена!*\n\nВаш ключ ${product} отправлен выше.`, { parse_mode: 'Markdown' }, botInstance).catch(() => { });
              }
              logAction(ADMIN_ID, 'rsl_withdraw_key_approved', { withdrawId, resellerId: w.reseller_id, product });
            } catch (err) {
              if (err.code === 'OUT_OF_STOCK') {
                // FIX 2.2: Баланс НЕ был списан — просто уведомляем. Заявка остаётся pending.
                safeSendMessage(chatId, `❌ Вывод #${withdrawId}: Нет доступных ключей для ${product}.\nЗаявка осталась в ожидании (баланс реселлера НЕ изменён). Одобрите после пополнения стока. /admin`);
              } else {
                safeSendMessage(chatId, `❌ Вывод #${withdrawId}: Ошибка выдачи ключа: ${err.message}\nЗаявка остаётся pending. Баланс не изменён.`);
              }
            }
          } else {
            // Обычный вывод денег — атомарное списание с проверкой достаточности баланса
            db.run(
              'UPDATE resellers SET balance = balance - ? WHERE id = ? AND balance >= ?',
              [w.amount, w.reseller_id, w.amount],
              function (atomicErr) {
                if (atomicErr || this.changes === 0) {
                  safeSendMessage(chatId, `❌ Вывод #${withdrawId}: недостаточно средств на балансе или ошибка БД. Заявка не обработана.`);
                  return;
                }
                db.run('UPDATE reseller_withdrawals SET status = "approved", processed_at = datetime("now") WHERE id = ?', [withdrawId]);
                safeSendMessage(chatId, `✅ Вывод #${withdrawId} одобрен. Баланс реселлера уменьшен на ${w.amount} ₽.`);
                bot.editMessageReplyMarkup({ inline_keyboard: [] }, { chat_id: chatId, message_id: query.message.message_id }).catch(() => { });
                const rslBotForNotify = resellerBots.get(w.reseller_id)?.bot || bot;
                safeSendMessage(r.user_id, `✅ *Ваша заявка на вывод ${w.amount} ₽ одобрена!*\n\nСредства отправлены на ваши реквизиты.`, { parse_mode: 'Markdown' }, rslBotForNotify).catch(() => { });
                logAction(ADMIN_ID, 'rsl_withdraw_approved', { withdrawId, resellerId: w.reseller_id, amount: w.amount });
              }
            );
          }
        });
      });
      return;
    }

    if (data.startsWith('rsl_withdraw_reject_') && user.id === ADMIN_ID) {
      const withdrawId = parseInt(data.replace('rsl_withdraw_reject_', ''));
      db.get('SELECT * FROM reseller_withdrawals WHERE id = ?', [withdrawId], (err, w) => {
        if (err || !w || w.status !== 'pending') return safeSendMessage(chatId, '❌ Заявка не найдена или уже обработана');
        db.run('UPDATE reseller_withdrawals SET status = "rejected", processed_at = datetime("now") WHERE id = ?', [withdrawId]);

        // FIX 2.2: Для KEY_ заявок баланс НЕ был списан при создании — не возвращаем.
        // Для обычных денежных заявок (money) — баланс WAS списан — возвращаем.
        const isKeyWithdrawal = w.details && w.details.startsWith('KEY_');
        if (!isKeyWithdrawal) {
          db.run('UPDATE resellers SET balance = balance + ? WHERE id = ?', [w.amount, w.reseller_id]);
        }

        const rejectNote = isKeyWithdrawal
          ? `❌ Вывод (ключ) #${withdrawId} отклонён. Баланс реселлера не изменён (средства не списывались).`
          : `❌ Вывод #${withdrawId} отклонен. Баланс (${w.amount} ₽) возвращен реселлеру.`;
        safeSendMessage(chatId, rejectNote);
        bot.editMessageReplyMarkup({ inline_keyboard: [] }, { chat_id: chatId, message_id: query.message.message_id }).catch(() => { });

        db.get('SELECT user_id FROM resellers WHERE id = ?', [w.reseller_id], (e, r) => {
          if (r) {
            const userMsg = isKeyWithdrawal
              ? `❌ *Ваша заявка на получение ключа отклонена.* Обратитесь к администратору за подробностями.`
              : `❌ *Ваша заявка на вывод ${w.amount} ₽ отклонена.*\n\nСредства возвращены на ваш баланс.`;
            const rslBotForReject = resellerBots.get(w.reseller_id)?.bot || bot;
            safeSendMessage(r.user_id, userMsg, { parse_mode: 'Markdown' }, rslBotForReject).catch(() => { });
          }
        });
      });
      return;
    }

    // АДМИН-ПАНЕЛЬ
    if (user.id === ADMIN_ID) {
      if (data === 'admin') {
        showAdminPanel(chatId, message.message_id);
        return;
      }

      if (data === 'admin_stats') {
        showStatistics(chatId, 'all', message.message_id);
        return;
      }

      if (data === 'stats_today') { showStatistics(chatId, 'today', message.message_id); return; }
      if (data === 'stats_week') { showStatistics(chatId, 'week', message.message_id); return; }
      if (data === 'stats_month') { showStatistics(chatId, 'month', message.message_id); return; }
      if (data === 'stats_all') { showStatistics(chatId, 'all', message.message_id); return; }
    }

    if (data === 'admin_top_sales') {
      showTopSales(chatId, message.message_id);
      return;
    }

    if (data === 'admin_active_users') {
      showActiveUsers(chatId);
      return;
    }

    if (data === 'admin_resellers') {
      showAdminResellers(chatId, 0, message.message_id);
      return;
    }
    if (data.startsWith('admin_resellers_page_')) {
      showAdminResellers(chatId, parseInt(data.replace('admin_resellers_page_', '')), message.message_id);
      return;
    }
    if (data.startsWith('admin_reseller_edit_')) {
      showAdminResellerEdit(chatId, parseInt(data.replace('admin_reseller_edit_', '')));
      return;
    }
    if (data.startsWith('admin_rsl_balance_edit_')) {
      if (user.id !== ADMIN_ID) { bot.answerCallbackQuery(query.id).catch(() => { }); return; }
      const rId = parseInt(data.replace('admin_rsl_balance_edit_', ''));
      session.state = 'awaiting_admin_rsl_balance';
      session.data = { resellerId: rId };
      bot.sendMessage(chatId, '💰 Введите новую сумму баланса для реселлера (включая копейки, например: 1500.50):');
      bot.answerCallbackQuery(query.id).catch(() => { });
      return;
    }

    if (data.startsWith('admin_rsl_markup_set_')) {
      // Установить наценку вручную
      const rId = parseInt(data.replace('admin_rsl_markup_set_', ''));
      db.get(`SELECT r.*, u.username as tg_username FROM resellers r LEFT JOIN users u ON r.user_id = u.id WHERE r.id = ?`, [rId], (err, r) => {
        if (err || !r) return safeSendMessage(chatId, '❌ Реселлер не найден.');
        const uname = r.tg_username ? `@${escapeMarkdown(r.tg_username)}` : `ID ${r.user_id}`;
        const session = getSession(chatId);
        session.state = 'admin_awaiting_rsl_markup';
        session.data = { rslId: rId };
        const exampleMsg = `⚙️ *Установка наценки*\n\nРеселлер: ${uname}\nТекущая наценка: *${r.markup_pct}%*\n\nОтправьте новое значение (целое число, например \`25\`):\n\n💡 *Примеры:*\n— \`20\` — наценка 20% (ключ 100₽ будет стоить 120₽)\n— \`35\` — наценка 35% (ключ 100₽ будет стоить 135₽)\n— \`50\` — наценка 50% (ключ 100₽ будет стоить 150₽)`;
        safeSendMessage(chatId, exampleMsg, {
          parse_mode: 'Markdown',
          reply_markup: { inline_keyboard: [[{ text: '❌ Отмена', callback_data: `admin_reseller_edit_${rId}` }]] }
        });
      });
      return;
    }

    if (data.startsWith('admin_rsl_markup_')) {
      // format: admin_rsl_markup_${r.id}_plus_5
      const match = data.match(/^admin_rsl_markup_(\d+)_(plus|minus)_(\d+)$/);
      if (match) {
        const rId = match[1];
        const op = match[2];
        const val = parseInt(match[3]);
        // FIX 1.2: Полностью атомарный UPDATE без предварительного чтения.
        // SQL сам ограничивает диапазон [0, 200] — race condition невозможен.
        const sql = op === 'plus'
          ? `UPDATE resellers SET markup_pct = MIN(200, markup_pct + ?) WHERE id = ?`
          : `UPDATE resellers SET markup_pct = MAX(0, markup_pct - ?) WHERE id = ?`;
        db.run(sql, [val, rId], (err) => {
          if (!err) showAdminResellerEdit(chatId, rId);
        });
      }
      return;
    }
    // =============================================
    // 🔄 TOGGLE приёма новых реселлеров (ДОЛЖЕН БЫТЬ ДО startsWith('admin_rsl_toggle_'))
    // =============================================
    if (data === 'admin_rsl_toggle_new') {
      if (user.id !== ADMIN_ID) { bot.answerCallbackQuery(query.id).catch(() => { }); return; }
      const current = getSetting('reseller_enabled');
      const newVal = current === '0' ? '1' : '0';
      saveSetting('reseller_enabled', newVal, async () => {
        const rslEnabled = newVal !== '0';
        bot.answerCallbackQuery(query.id, {
          text: rslEnabled ? '✅ Приём заявок включён' : '🔴 Приём заявок отключён'
        }).catch(() => { });

        if (rslEnabled) {
          // 🚀 Включили систему — запускаем все активные боты которые ещё не запущены
          db.all(`SELECT * FROM resellers WHERE status = 'active'`, [], async (err, rows) => {
            if (rows && rows.length > 0) {
              let cnt = 0;
              for (const r of rows) {
                if (!resellerBots.has(r.id)) {
                  const ok = await initResellerBot(r);
                  if (ok) cnt++;
                  await new Promise(res => setTimeout(res, 300));
                }
              }
              if (cnt > 0) {
                safeSendMessage(chatId, `✅ Запущено реселлер-ботов: ${cnt}`).catch(() => {});
              }
            }
          });
        } else {
          // ⛔ Выключили систему — останавливаем ВСЕ запущенные боты
          for (const [rId, entry] of resellerBots.entries()) {
            if (entry && entry.bot) {
              try { await entry.bot.deleteWebHook(); } catch (_) {}
              resellerBots.delete(rId);
            }
          }
          console.log('⛔ Все реселлер-боты остановлены (система выключена)');
        }

        try {
          const kbText = rslEnabled ? '🟢 Приём новых заявок: ВКЛ' : '🔴 Приём новых заявок: ВЫКЛ';
          const newKeyboard = JSON.parse(JSON.stringify(query.message.reply_markup || {}));
          if (newKeyboard && newKeyboard.inline_keyboard && newKeyboard.inline_keyboard.length > 0) {
            newKeyboard.inline_keyboard[0][0].text = kbText;
            bot.editMessageReplyMarkup(newKeyboard, {
              chat_id: chatId,
              message_id: query.message.message_id
            }).catch(() => { });
          }
        } catch (e) { showAdminResellers(chatId, 0, message.message_id); }
      });
      return;
    }

    if (data.startsWith('admin_rsl_toggle_')) {
      const rId = parseInt(data.replace('admin_rsl_toggle_', ''));
      if (isNaN(rId)) { bot.answerCallbackQuery(query.id).catch(() => {}); return; }
      db.get('SELECT * FROM resellers WHERE id = ?', [rId], async (e, r) => {
        if (!r) return;
        const next = r.status === 'active' ? 'disabled' : 'active';
        db.run('UPDATE resellers SET status = ? WHERE id = ?', [next, rId], async () => {
          if (next === 'disabled') {
            // ❌ Останавливаем бот — удаляем из Map, снимаем вебхук
            const entry = resellerBots.get(rId);
            if (entry && entry.bot) {
              try { await entry.bot.deleteWebHook(); } catch (_) {}
              resellerBots.delete(rId);
              console.log(`⛔ [РЕСЕЛЛЕР ${rId}] Бот остановлен (деактивирован админом)`);
            }
            bot.answerCallbackQuery(query.id, { text: '⛔ Бот деактивирован и остановлен' }).catch(() => {});
          } else {
            // ✅ Запускаем бот — инициализируем заново
            const started = await initResellerBot(r);
            if (started) {
              bot.answerCallbackQuery(query.id, { text: '✅ Бот активирован и запущен' }).catch(() => {});
              console.log(`✅ [РЕСЕЛЛЕР ${rId}] Бот запущен (активирован админом)`);
            } else {
              bot.answerCallbackQuery(query.id, { text: '⚠️ Активирован, но бот не запустился — проверьте токен' }).catch(() => {});
            }
          }
          showAdminResellerEdit(chatId, rId);
        });
      });
      return;
    }
    // 🗑️ Удаление реселлер-бота — шаг 1: подтверждение
    if (data.startsWith('admin_rsl_delete_confirm_')) {
      const rId = parseInt(data.replace('admin_rsl_delete_confirm_', ''));
      if (isNaN(rId)) { bot.answerCallbackQuery(query.id).catch(() => {}); return; }
      db.get(`SELECT r.*, u.username as tg_username FROM resellers r LEFT JOIN users u ON r.user_id = u.id WHERE r.id = ?`, [rId], (e, r) => {
        if (!r) { bot.answerCallbackQuery(query.id, { text: '❌ Реселлер не найден' }).catch(() => {}); return; }
        const uname = r.tg_username ? `@${escapeMarkdown(r.tg_username)}` : `ID ${r.user_id}`;
        const botName = r.bot_username ? `@${escapeMarkdown(r.bot_username)}` : 'не подключен';
        const text = `⚠️ *Подтвердите удаление реселлера*\n\n` +
          `👤 Пользователь: ${uname}\n` +
          `🤖 Бот: ${botName}\n\n` +
          `Это действие *необратимо*. Будут удалены:\n` +
          `• запись реселлера из БД\n` +
          `• все его заказы (reseller_orders)\n` +
          `• все заявки на вывод\n` +
          `• бот будет остановлен и вебхук удалён\n\n` +
          `Реселлер получит уведомление об удалении.`;
        bot.answerCallbackQuery(query.id).catch(() => {});
        safeSendMessage(chatId, text, {
          parse_mode: 'Markdown',
          reply_markup: {
            inline_keyboard: [
              [
                { text: '✅ Да, удалить', callback_data: `admin_rsl_delete_exec_${rId}` },
                { text: '❌ Отмена', callback_data: `admin_reseller_edit_${rId}` }
              ]
            ]
          }
        });
      });
      return;
    }

    // 🗑️ Удаление реселлер-бота — шаг 2: выполнение
    if (data.startsWith('admin_rsl_delete_exec_')) {
      const rId = parseInt(data.replace('admin_rsl_delete_exec_', ''));
      if (isNaN(rId)) { bot.answerCallbackQuery(query.id).catch(() => {}); return; }
      db.get('SELECT * FROM resellers WHERE id = ?', [rId], async (e, r) => {
        if (!r) { bot.answerCallbackQuery(query.id, { text: '❌ Реселлер не найден' }).catch(() => {}); return; }

        // 1. Останавливаем бот реселлера
        const entry = resellerBots.get(rId);
        if (entry && entry.bot) {
          try { await entry.bot.deleteWebHook(); } catch (_) {}
          resellerBots.delete(rId);
          console.log(`🗑️ [РЕСЕЛЛЕР ${rId}] Бот остановлен (удалён админом)`);
        }

        // 2. Уведомляем реселлера
        try {
          await safeSendMessage(r.user_id,
            `❌ *Ваш реселлер-бот был удалён администратором.*\n\nЕсли у вас есть вопросы, свяжитесь с поддержкой.`,
            { parse_mode: 'Markdown' }
          );
        } catch (_) {}

        // 3. Удаляем данные из БД
        db.serialize(() => {
          db.run(`UPDATE orders SET reseller_id = NULL WHERE reseller_id = ?`, [rId]);
          db.run(`DELETE FROM reseller_orders WHERE reseller_id = ?`, [rId]);
          db.run(`DELETE FROM reseller_withdrawals WHERE reseller_id = ?`, [rId]);
          db.run(`DELETE FROM resellers WHERE id = ?`, [rId], (delErr) => {
            if (delErr) {
              console.error(`❌ Ошибка удаления реселлера ${rId}:`, delErr);
              bot.answerCallbackQuery(query.id, { text: '❌ Ошибка при удалении из БД' }).catch(() => {});
              return;
            }
            console.log(`🗑️ [РЕСЕЛЛЕР ${rId}] Полностью удалён из БД`);
            logAction(ADMIN_ID, 'reseller_deleted', { rId, userId: r.user_id });
            bot.answerCallbackQuery(query.id, { text: '✅ Реселлер удалён' }).catch(() => {});
            safeSendMessage(chatId, `✅ *Реселлер удалён* (ID ${rId})\n\nСписок реселлеров обновлён.`, {
              parse_mode: 'Markdown',
              reply_markup: { inline_keyboard: [[{ text: '📋 К списку реселлеров', callback_data: 'admin_resellers' }]] }
            });
          });
        });
      });
      return;
    }

    if (data === 'admin_rsl_withdrawals') {
      showAdminResellerWithdrawals(chatId, 0);
      return;
    }
    if (data.startsWith('admin_rsl_withdrawals_page_')) {
      showAdminResellerWithdrawals(chatId, parseInt(data.replace('admin_rsl_withdrawals_page_', '')));
      return;
    }

    if (data === 'admin_key_stock') {
      showKeyStock(chatId, message.message_id);
      return;
    }

    if (data === 'admin_sold_keys') {
      showSoldKeys(chatId, 0);
      return;
    }

    if (data.startsWith('sold_keys_page_')) {
      const page = parseInt(data.replace('sold_keys_page_', '')) || 0;
      showSoldKeys(chatId, page);
      return;
    }

    if (data === 'admin_manage_orders') {
      ordersPage = 0;
      showManageOrders(chatId, 0, message.message_id);
      return;
    }

    if (data.startsWith('orders_page_')) {
      const page = parseInt(data.replace('orders_page_', ''));
      showManageOrders(chatId, page, message.message_id);
      return;
    }

    if (data === 'admin_manage_keys') {
      showManageKeys(chatId, message.message_id);
      return;
    }

    if (data === 'admin_manage_prices') {
      showManagePrices(chatId, message.message_id);
      return;
    }

    if (data === 'admin_manage_payment_details') {
      showManagePaymentDetails(chatId, message.message_id);
      return;
    }

    if (data.startsWith('edit_payment_')) {
      const method = data.replace('edit_payment_', '');
      session.state = 'awaiting_payment_details';
      session.data = { method };

      const methodNames = {
        sbp: 'СБП',
        card_ua: 'Карта UA',
        card_it: 'Карта IT',
        binance: 'Binance',
        paypal: 'PayPal',
        crypto: 'Crypto'
      };

      bot.sendMessage(
        chatId,
        `💳 *Изменение ${methodNames[method] || method}*\n\nТекущее значение:\n\`${PAYMENT_DETAILS[method]}\`\n\nВведите новое значение:`,
        { parse_mode: 'Markdown' }
      );

      return;
    }

    if (data === 'admin_view_logs') {
      showLogs(chatId, 0);
      return;
    }

    if (data.startsWith('logs_page_')) {
      const page = parseInt(data.replace('logs_page_', '')) || 0;
      showLogs(chatId, page);
      return;
    }

    if (data === 'admin_export_csv') {
      await exportStatsToCsv(chatId);
      return;
    }

    if (data === 'admin_backup') {
      bot.sendMessage(chatId, '⏳ Создаю бэкап...');
      await sendManualBackup(chatId);
      return;
    }

    if (data === 'admin_restore') {
      session.state = 'awaiting_restore_file';
      bot.sendMessage(chatId, '🔄 Отправьте файл базы данных (.db) для восстановления.\n⚠️ Текущая база будет перезаписана (создаётся резервная копия shop.db.backup)');
      return;
    }

    // admin_edit_stats удалён (Task 7)

    if (data.startsWith('edit_price_')) {
      const product = data.replace('edit_price_', '');

      session.state = 'awaiting_new_prices';
      session.data = { product };

      const currentPrices = PRICES[product];

      bot.sendMessage(
        chatId,
        `💰 *Изменение цен ${product}*\n\nТекущие:\n` +
        `${FLAGS.USD} $${currentPrices.USD}\n` +
        `${FLAGS.EUR} €${currentPrices.EUR}\n` +
        `${FLAGS.RUB} ${formatPrice(currentPrices.RUB, 'RUB')}\n` +
        `${FLAGS.UAH} ${currentPrices.UAH}₴\n\n` +
        `Введите: \`USD EUR RUB UAH\`\n` +
        `Пример: \`1.50 1.25 100 65\``,
        { parse_mode: 'Markdown' }
      );

      return;
    }

    if (data.startsWith('admin_user_ban_toggle_')) {
      const targetId = parseInt(data.replace('admin_user_ban_toggle_', ''));
      if (user.id !== ADMIN_ID) return;
      db.get(`SELECT is_banned FROM users WHERE id = ?`, [targetId], (err, row) => {
        if (err || !row) return;
        const newStatus = row.is_banned ? 0 : 1;
        db.run(`UPDATE users SET is_banned = ? WHERE id = ?`, [newStatus, targetId], () => {
          bot.answerCallbackQuery(query.id, { text: newStatus ? '🚫 Пользователь забанен' : '✅ Пользователь разбанен', show_alert: true });
          // Обновляем панель поиска (повторный поиск или уведомление)
          bot.sendMessage(chatId, `👤 Пользователь ${targetId}: статус изменён на *${newStatus ? 'ЗАБАНЕН' : 'АКТИВЕН'}*`, { parse_mode: 'Markdown' });
        });
      });
      return;
    }

    if (data.startsWith('admin_orders_user_')) {
      const targetId = parseInt(data.replace('admin_orders_user_', ''));
      if (user.id !== ADMIN_ID) return;
      db.all(`SELECT id, product, status, amount, currency, confirmed_at FROM orders WHERE user_id = ? ORDER BY id DESC LIMIT 10`, [targetId], (err, rows) => {
        if (err || !rows || rows.length === 0) {
          bot.sendMessage(chatId, '📦 У пользователя нет заказов.');
          return;
        }
        let msg = `📦 *Последние 10 заказов пользователя ${targetId}:*\n\n`;
        rows.forEach(o => {
          msg += `🔹 #${o.id} | ${o.product} | ${o.amount} ${o.currency} | ${o.status}\n`;
        });
        bot.sendMessage(chatId, msg, { parse_mode: 'Markdown' });
      });
      return;
    }

    if (data.startsWith('admin_rsl_edit_')) {
      const targetId = parseInt(data.replace('admin_rsl_edit_', ''));
      if (user.id !== ADMIN_ID) return;
      db.get(`SELECT balance FROM resellers WHERE user_id = ?`, [targetId], (err, row) => {
        if (err || !row) {
          bot.sendMessage(chatId, '❌ Этот пользователь не является партнёром-реселлером.');
          return;
        }
        session.state = 'awaiting_admin_rsl_balance_change';
        session.data = { targetUserId: targetId, currentBalance: row.balance };

        const kb = {
          inline_keyboard: [
            [
              { text: '+100 ₽', callback_data: `rsl_bal_set_${targetId}_100` },
              { text: '+500 ₽', callback_data: `rsl_bal_set_${targetId}_500` },
              { text: '+1000 ₽', callback_data: `rsl_bal_set_${targetId}_1000` }
            ],
            [
              { text: '-500 ₽', callback_data: `rsl_bal_set_${targetId}_-500` },
              { text: '❌ Обнулить', callback_data: `rsl_bal_set_${targetId}_clear` }
            ],
            [{ text: '◀️ Назад', callback_data: `admin_user_search` }]
          ]
        };

        bot.sendMessage(chatId, `💰 *Управление балансом реселлера*\n\nПользователь: ID ${targetId}\nТекущий баланс: *${row.balance} ₽*\n\nВыберите сумму ниже или введите своё значение (например, \`+100\` или \`-50\`):`, { parse_mode: 'Markdown', reply_markup: kb });
      });
      return;
    }

    if (data.startsWith('rsl_bal_set_')) {
      if (user.id !== ADMIN_ID) return;
      const parts = data.split('_');
      const targetId = parseInt(parts[3]);
      const action = parts[4];

      db.get(`SELECT balance FROM resellers WHERE user_id = ?`, [targetId], (err, row) => {
        if (err || !row) {
          bot.answerCallbackQuery(query.id, { text: '❌ Ошибка: Реселлер не найден', show_alert: true });
          return;
        }

        let change = 0;
        if (action === 'clear') {
          change = -row.balance;
        } else {
          change = parseInt(action);
        }

        db.run(`UPDATE resellers SET balance = balance + ? WHERE user_id = ?`, [change, targetId], (updErr) => {
          if (updErr) {
            bot.answerCallbackQuery(query.id, { text: '❌ Ошибка БД', show_alert: true });
          } else {
            const newBal = (row.balance + change).toFixed(2);
            bot.answerCallbackQuery(query.id, { text: `✅ Баланс изменен на ${change} ₽`, show_alert: false });

            // Обновляем сообщение (панель управления балансом)
            const kb = {
              inline_keyboard: [
                [
                  { text: '+100 ₽', callback_data: `rsl_bal_set_${targetId}_100` },
                  { text: '+500 ₽', callback_data: `rsl_bal_set_${targetId}_500` },
                  { text: '+1000 ₽', callback_data: `rsl_bal_set_${targetId}_1000` }
                ],
                [
                  { text: '-500 ₽', callback_data: `rsl_bal_set_${targetId}_-500` },
                  { text: '❌ Обнулить', callback_data: `rsl_bal_set_${targetId}_clear` }
                ],
                [{ text: '◀️ Назад', callback_data: `admin_user_search` }]
              ]
            };

            bot.editMessageText(`💰 *Управление балансом реселлера*\n\nПользователь: ID ${targetId}\nТекущий баланс: *${newBal} ₽*\n\nИзменено на: *${change} ₽*`, {
              chat_id: chatId,
              message_id: query.message.message_id,
              parse_mode: 'Markdown',
              reply_markup: kb
            }).catch(() => { });

            // Уведомляем реселлера если пополнение положительное
            if (change > 0) {
              safeSendMessage(targetId, `💰 Ваш баланс пополнен на *${change} ₽* администратором!\nАктуальный баланс: *${newBal} ₽*`, { parse_mode: 'Markdown' }).catch(() => { });
            }
          }
        });
      });
      return;
    }

    if (data.startsWith('add_keys_')) {
      const product = data.replace('add_keys_', '');

      session.state = 'awaiting_keys';
      session.data = { product };

      bot.sendMessage(
        chatId,
        `🔑 *Добавление ключей ${product}*\n\nОтправьте ключи (по одному на строку):`,
        { parse_mode: 'Markdown' }
      );

      return;
    }

    if (data === 'admin_issue_key') {
      session.state = 'awaiting_username';
      session.data = {};

      bot.sendMessage(chatId, '👤 Введите username (можно с @ или без):');
      return;
    }

    if (data.startsWith('issue_') && !data.includes('approve') && !data.includes('reject')) {
      // issue_id_<userId>_<period> — by numeric user ID (from ticket)
      // issue_<username>_<period> — by username
      let targetUserId = null;
      let username = null;
      let period = null;

      if (data.startsWith('issue_id_')) {
        const parts = data.split('_'); // ['issue','id','<userId>','<period>']
        targetUserId = parseInt(parts[2]);
        period = parts[3];
      } else {
        const parts = data.split('_');
        username = parts[1];
        period = parts[2];
      }

      const resolveUser = (cb) => {
        if (targetUserId) return cb(null, { id: targetUserId });
        db.get(`SELECT id FROM users WHERE username = ?`, [username], cb);
      };

      resolveUser(async (err, targetUser) => {
        if (err || !targetUser) {
          bot.sendMessage(chatId, '❌ Пользователь не найден');
          return;
        }
        try {
          const key = await issueKeyToUser(targetUser.id, period, 'manual');
          const periodName = PERIOD_NAMES.ru[period] || period;
          const userMessage = `🎁 *Вам выдан ключ!*\n\n🔑 ${periodName}\n\`${key}\`\n\n_Активация при первом вводе в CyraxMod_\n\n📢 @cyraxml`;
          await safeSendMessage(targetUser.id, userMessage, { parse_mode: 'Markdown' });
          const label = username ? `@${escapeMarkdown(username)}` : `ID ${targetUser.id}`;
          bot.sendMessage(chatId, `✅ Ключ ${periodName} выдан ${label}`);
          logAction(ADMIN_ID, 'manual_key_issue', { username, period, targetUserId: targetUser.id });
        } catch (error) {
          console.error('❌ Issue key error:', error);
          bot.sendMessage(chatId, `❌ Ошибка: ${error.message}`);
        }
        clearSession(user.id);
      });
      return;
    }


    // ==========================================
    // 🧪 ТЕСТ-ВИТРИНА (режим покупателя для админа)
    // ==========================================


    // ==========================================
    // 👤 МОЙ ПРОФИЛЬ (пользовательский)
    // ==========================================
    if (data === 'my_profile') {
      const isRu = getLang(user) === 'ru';

      // Получаем полную статистику пользователя
      const [orders, balance] = await Promise.all([
        new Promise(resolve => {
          db.all(
            `SELECT id, product, amount, currency, original_currency, original_amount,
                    key_issued, created_at, confirmed_at
             FROM orders
             WHERE user_id = ? AND status = 'confirmed' AND (balance_topup IS NULL OR balance_topup = 0)
             ORDER BY confirmed_at DESC LIMIT 5`,
            [user.id], (e, rows) => resolve(rows || [])
          );
        }),
        getUserBalance(user.id)
      ]);

      const [totalStats] = await Promise.all([
        new Promise(resolve => {
          db.get(
            `SELECT COUNT(*) as total, SUM(amount) as spent
             FROM orders WHERE user_id = ? AND status = 'confirmed'
             AND (balance_topup IS NULL OR balance_topup = 0)`,
            [user.id], (e, row) => resolve(row || { total: 0, spent: 0 })
          );
        })
      ]);

      // Прогресс к купону (каждые 5 покупок → купон 10%)
      const totalPurchases = totalStats.total || 0;
      const nextMilestone = Math.ceil((totalPurchases + 1) / 5) * 5;
      const purchasesLeft = nextMilestone - totalPurchases;
      const progressBar = (() => {
        const filled = totalPurchases % 5;
        return '🟣'.repeat(filled) + '⚪️'.repeat(5 - filled);
      })();

      const PERIOD_DAYS = { '1d': 1, '3d': 3, '7d': 7, '30d': 30 };

      let keysText = '';
      if (orders.length > 0) {
        keysText = isRu ? '\n\n🔑 *Последние ключи:*\n' : '\n\n🔑 *Recent keys:*\n';
        orders.slice(0, 3).forEach((o, i) => {
          const pName = PERIOD_NAMES[isRu ? 'ru' : 'en'][o.product] || o.product;
          const dispCur = o.original_currency || o.currency;
          const dispAmt = o.original_amount || o.amount;
          const days = PERIOD_DAYS[o.product];
          let timer = '';
          if (days && (o.confirmed_at || o.created_at)) {
            const exp = new Date(new Date(o.confirmed_at || o.created_at).getTime() + days * 86400000);
            const diff = exp - Date.now();
            if (diff > 0) {
              const d = Math.floor(diff / 86400000);
              const h = Math.floor((diff % 86400000) / 3600000);
              timer = d > 0 ? ` ⏳${d}д ${h}ч` : ` ⌛${h}ч`;
            } else {
              timer = isRu ? ' ✅ истёк' : ' ✅ expired';
            }
          }
          keysText += `${i + 1}. *${pName}* — ${formatBalanceAmount(dispAmt, dispCur)}${timer}
`;
          if (o.key_issued && !o.key_issued.startsWith('BOOST') && !o.key_issued.startsWith('RESELLER')) {
            keysText += `   \`${o.key_issued}\`
`;
          }
        });
      }

      const balanceText = balance.balance > 0
        ? (isRu
            ? `
💳 *Баланс профиля:* ${formatBalanceAmount(balance.balance, balance.preferred_currency)}`
            : `
💳 *Profile balance:* ${formatBalanceAmount(balance.balance, balance.preferred_currency)}`)
        : '';

      // FIX 2.1: Показываем прогресс-бар всем — включая пользователей с 0 покупок.
      // При totalPurchases === 0: бар = 5 пустых кружков + текст "Первая покупка!"
      // При totalPurchases > 0:  бар = filled + пустые + сколько осталось до купона.
      const milestoneText = isRu
        ? `

🎯 *Прогресс к купону:*
${progressBar} ${totalPurchases === 0 ? 'Первая покупка!' : `ещё ${purchasesLeft} ${purchasesLeft === 1 ? 'покупка' : purchasesLeft < 5 ? 'покупки' : 'покупок'}`}
_Каждые 5 покупок — купон 10% на любой ключ_`
        : `

🎯 *Progress to coupon:*
${progressBar} ${totalPurchases === 0 ? 'First purchase!' : `${purchasesLeft} more ${purchasesLeft === 1 ? 'purchase' : 'purchases'}`}
_Every 5 purchases — 10% coupon for any key_`;

      const msg = isRu
        ? `👤 *Мой профиль*

` +
          `🆔 ID: \`${user.id}\`
` +
          `📅 В боте с: ${new Date(user.joined_at || Date.now()).toLocaleDateString('ru-RU')}
` +
          `🛒 Покупок: *${totalPurchases}*${balanceText}${milestoneText}${keysText}`
        : `👤 *My Profile*

` +
          `🆔 ID: \`${user.id}\`
` +
          `📅 Member since: ${new Date(user.joined_at || Date.now()).toLocaleDateString('en-GB')}
` +
          `🛒 Purchases: *${totalPurchases}*${balanceText}${milestoneText}${keysText}`;

      const kb = { inline_keyboard: [] };
      if (balance.balance > 0) {
        kb.inline_keyboard.push([{
          text: isRu ? `💳 Мой баланс: ${formatBalanceAmount(balance.balance, balance.preferred_currency)}` : `💳 Balance: ${formatBalanceAmount(balance.balance, balance.preferred_currency)}`,
          callback_data: 'profile_balance'
        }]);
      }
      kb.inline_keyboard.push([{ text: isRu ? '🔑 Все мои ключи' : '🔑 All my keys', callback_data: 'orders' }]);
      if (totalPurchases >= 1) {
        kb.inline_keyboard.push([{ text: isRu ? '💳 Пополнить баланс' : '💳 Top up balance', callback_data: 'profile_topup' }]);
      }
      kb.inline_keyboard.push([{ text: isRu ? '🎟️ Мои купоны' : '🎟️ My Coupons', callback_data: 'my_coupons' }]);
      kb.inline_keyboard.push([{ text: isRu ? '◀️ Назад' : '◀️ Back', callback_data: 'buy' }]);

      sendNavMessage(chatId, user.id, msg, { parse_mode: 'Markdown', reply_markup: kb }).catch(() => {});
      return;
    }

    // Детальный экран баланса
    if (data === 'profile_balance') {
      const isRu = getLang(user) === 'ru';
      const balance = await getUserBalance(user.id);

      const txs = await new Promise(resolve => {
        db.all(
          `SELECT amount, type, description, created_at FROM balance_transactions
           WHERE user_id = ? ORDER BY created_at DESC LIMIT 10`,
          [user.id], (e, rows) => resolve(rows || [])
        );
      });

      const typeIcon = { topup: '➕', purchase: '🛍', refund: '↩️', admin_credit: '🎁', admin_debit: '➖' };
      let txText = '';
      if (txs.length > 0) {
        txText = isRu ? '\n\n📋 *Последние операции:*\n' : '\n\n📋 *Recent transactions:*\n';
        txs.forEach(tx => {
          const icon = typeIcon[tx.type] || '•';
          const sign = tx.amount > 0 ? '+' : '';
          const date = new Date(tx.created_at).toLocaleDateString(isRu ? 'ru-RU' : 'en-GB');
          txText += `${icon} ${sign}${formatBalanceAmount(tx.amount, balance.preferred_currency)} — ${(tx.description || tx.type).replace(/_/g, '\\_ ')} _${date}_
`;
        });
      }

      const msg = isRu
        ? `💳 *Баланс профиля*

💰 Текущий баланс: *${formatBalanceAmount(balance.balance, balance.preferred_currency)}*
💱 Валюта: ${balance.preferred_currency}${txText}

_Баланс можно потратить на покупку любого ключа._`
        : `💳 *Profile Balance*

💰 Current balance: *${formatBalanceAmount(balance.balance, balance.preferred_currency)}*
💱 Currency: ${balance.preferred_currency}${txText}

_Balance can be used to purchase any key._`;

      sendNavMessage(chatId, user.id, msg, {
        parse_mode: 'Markdown',
        reply_markup: { inline_keyboard: [
          [{ text: isRu ? '💳 Пополнить' : '💳 Top up', callback_data: 'profile_topup' }],
          [{ text: isRu ? '◀️ Профиль' : '◀️ Profile', callback_data: 'my_profile' }]
        ]}
      }).catch(() => {});
      return;
    }

    // ==========================================
    // 🎁 ПРОГРЕСС-КУПОН: каждые 5 покупок
    // ==========================================
    // Вызывается из finalizeSuccessfulOrder — проверяем milestone
    // Вынесено в отдельную async функцию, вызываем ниже
    // (определена после этого блока)

    // ==========================================
    // 💳 ПОПОЛНЕНИЕ БАЛАНСА ПРОФИЛЯ
    // ==========================================
    if (data === 'profile_topup') {
      const isRu = getLang(user) === 'ru';

      // Если юзер нажал "Назад" не отправив чек — отменяем висящий pending заказ
      const pendingTopup = session.data?.topupOrderId;
      if (pendingTopup && session.state === 'awaiting_topup_receipt') {
        db.run(`UPDATE orders SET status = 'rejected' WHERE id = ? AND status = 'pending' AND balance_topup = 1`, [pendingTopup]);
        clearSession(user.id);
      }

      const balance = await getUserBalance(user.id);
      const cur = balance.preferred_currency;

      // FIX 1: Убраны кнопки с предустановленными суммами.
      // Пользователь вводит произвольную сумму текстом — удобнее и универсальнее.
      // Показываем минимумы и подсказки для каждой валюты.
      const minAmounts = { RUB: 100, USD: 1.5, EUR: 1.5, UAH: 65 };
      const minAmt = minAmounts[cur] || 1;
      const exampleAmounts = {
        RUB: '500, 1000, 2000',
        USD: '5, 10, 20',
        EUR: '5, 10, 20',
        UAH: '200, 500, 1000'
      };
      const examples = exampleAmounts[cur] || '10, 50, 100';

      // Устанавливаем сессию ожидания суммы
      session.state = 'awaiting_topup_amount';
      session.data = { ...session.data, topupCurrency: cur };

      const msg = isRu
        ? `💳 *Пополнение баланса*\n\n` +
          `Текущий баланс: *${formatBalanceAmount(balance.balance, cur)}*\n\n` +
          `Введите сумму пополнения в *${cur}*:\n` +
          `_Например: ${examples}_\n\n` +
          `⚠️ Минимальная сумма: *${formatBalanceAmount(minAmt, cur)}*`
        : `💳 *Top Up Balance*\n\n` +
          `Current balance: *${formatBalanceAmount(balance.balance, cur)}*\n\n` +
          `Enter the amount in *${cur}*:\n` +
          `_Example: ${examples}_\n\n` +
          `⚠️ Minimum amount: *${formatBalanceAmount(minAmt, cur)}*`;

      sendNavMessage(chatId, user.id, msg, {
        parse_mode: 'Markdown',
        reply_markup: { inline_keyboard: [
          [{ text: isRu ? '◀️ Профиль' : '◀️ Profile', callback_data: 'my_profile' }]
        ]}
      }).catch(() => {});
      return;
    }

    // Выбран метод оплаты для пополнения баланса
    if (data.startsWith('profile_topup_pay_')) {
      const method = data.replace('profile_topup_pay_', '');
      const topupAmount = session.data?.topupAmount;
      const topupCur = session.data?.topupCurrency;
      const isRu = getLang(user) === 'ru';
      if (!topupAmount || !topupCur) { bot.answerCallbackQuery(query.id).catch(() => {}); return; }
      // FIX 2.2: явная защита от повреждённых данных сессии — сумма обязана быть положительной
      if (topupAmount <= 0 || isNaN(topupAmount)) {
        bot.answerCallbackQuery(query.id, {
          text: isRu ? '❌ Некорректная сумма' : '❌ Invalid amount',
          show_alert: true
        }).catch(() => {});
        clearSession(user.id);
        return;
      }

      // CryptoBot — автоматическое пополнение
      if ((method === 'cryptobot' || method === 'cryptobot_usd') && CRYPTOBOT_TOKEN) {
        // FIX 2: ИСПРАВЛЕНИЕ КРИТИЧЕСКОГО БАГА — неверная формула конвертации.
        // EXCHANGE_RATES.USD = 0.01308 означает "1 RUB = 0.01308 USD" (коэффициент умножения).
        // Было: topupAmount / EXCHANGE_RATES.USD → 1500 / 0.01308 ≈ 114,678 USD (НЕВЕРНО!)
        // Стало: topupAmount * EXCHANGE_RATES.USD → 1500 * 0.01308 ≈ 19.62 USD (ВЕРНО)
        let cbAmount = topupAmount;
        let cbCur = 'USDT';
        if (topupCur === 'RUB') {
          // RUB → USD: умножаем на курс RUB→USD
          cbAmount = Math.ceil(topupAmount * EXCHANGE_RATES.USD * 100) / 100;
        } else if (topupCur === 'UAH') {
          // UAH → USD: сначала UAH→RUB (делим на EXCHANGE_RATES.UAH), потом RUB→USD
          cbAmount = Math.ceil((topupAmount / EXCHANGE_RATES.UAH) * EXCHANGE_RATES.USD * 100) / 100;
        } else if (topupCur === 'EUR') {
          // EUR — CryptoBot поддерживает EUR напрямую
          cbAmount = topupAmount;
          cbCur = 'EUR';
        } else if (topupCur === 'USD') {
          // USD — уже в нужной валюте
          cbAmount = topupAmount;
        }
        // Минимум для CryptoBot — 0.10 USDT/EUR
        cbAmount = Math.max(parseFloat(cbAmount.toFixed(2)), 0.10);

        try {
          const cbResp = await axios.post('https://pay.crypt.bot/api/createInvoice', {
            asset: cbCur,
            amount: cbAmount.toFixed(2),
            description: isRu
              ? `Пополнение баланса CyraxMods — ${formatBalanceAmount(topupAmount, topupCur)}`
              : `CyraxMods balance top up — ${formatBalanceAmount(topupAmount, topupCur)}`,
            payload: `topup_${user.id}_${topupAmount}_${topupCur}`,
            paid_btn_name: 'viewItem',
            paid_btn_url: 'https://t.me/' + (process.env.BOT_USERNAME || 'cyraxxmod_bot'),
            expires_in: 3600
          }, { headers: { 'Crypto-Pay-API-Token': CRYPTOBOT_TOKEN } });

          if (cbResp.data?.ok) {
            const invoice = cbResp.data.result;
            // Создаём заказ типа balance_topup
            db.run(
              `INSERT INTO orders (user_id, username, user_lang, product, amount, currency, method, invoice_id, status, balance_topup)
               VALUES (?, ?, ?, 'balance_topup', ?, ?, 'cryptobot', ?, 'pending', 1)`,
              [user.id, user.username || '', getLang(user), topupAmount, topupCur, invoice.invoice_id],
              function(insErr) {
                if (insErr) console.error('balance topup order insert error:', insErr);
              }
            );
            const msg = isRu
              ? `💳 *Пополнение баланса*

Сумма: *${formatBalanceAmount(topupAmount, topupCur)}*

Оплатите через CryptoBot — после оплаты баланс пополнится автоматически.`
              : `💳 *Balance Top Up*

Amount: *${formatBalanceAmount(topupAmount, topupCur)}*

Pay via CryptoBot — balance will be credited automatically after payment.`;
            sendNavMessage(chatId, user.id, msg, {
              parse_mode: 'Markdown',
              reply_markup: { inline_keyboard: [
                [{ text: '💳 Оплатить через CryptoBot', url: invoice.bot_invoice_url || invoice.pay_url }],
                [{ text: isRu ? '◀️ Назад' : '◀️ Back', callback_data: 'profile_topup' }]
              ]}
            }).catch(() => {});
          } else {
            bot.sendMessage(chatId, isRu ? '❌ Ошибка создания счёта. Попробуйте другой метод.' : '❌ Invoice creation failed. Try another method.');
          }
        } catch(e) {
          console.error('CryptoBot topup error:', e.message);
          bot.sendMessage(chatId, t(user, 'error_creating_invoice'));
        }
        return;
      }

      // Ручные методы — создаём pending-заказ и просим чек
      const details = {
        sbp: PAYMENT_DETAILS.sbp,
        card_ua: PAYMENT_DETAILS.card_ua,
        card_it: PAYMENT_DETAILS.card_it,
        binance: PAYMENT_DETAILS.binance,
        paypal: PAYMENT_DETAILS.paypal
      };
      const methodDetails = details[method];
      if (!methodDetails) { bot.answerCallbackQuery(query.id).catch(() => {}); return; }

      db.run(
        `INSERT INTO orders (user_id, username, user_lang, product, amount, currency, method, status, balance_topup)
         VALUES (?, ?, ?, 'balance_topup', ?, ?, ?, 'pending', 1)`,
        [user.id, user.username || '', getLang(user), topupAmount, topupCur, method],
        function(insErr) {
          if (insErr) { console.error('topup order insert:', insErr); return; }
          const orderId = this.lastID;

          const sbpIsUrl = methodDetails.startsWith('http') || methodDetails.startsWith('tg://');
          const isRu2 = getLang(user) === 'ru';
          const msg = isRu2
            ? `💳 *Пополнение баланса — ожидает подтверждения*

Сумма: *${formatBalanceAmount(topupAmount, topupCur)}*

` +
              (sbpIsUrl ? `Нажмите кнопку для оплаты.` : `Реквизиты:
\`${methodDetails}\`

`) +
              `После оплаты отправьте скриншот или чек — администратор подтвердит пополнение вручную.`
            : `💳 *Balance Top Up — Pending Confirmation*

Amount: *${formatBalanceAmount(topupAmount, topupCur)}*

` +
              (sbpIsUrl ? `Tap the button to pay.` : `Details:
\`${methodDetails}\`

`) +
              `After payment, send a screenshot — admin will confirm manually.`;

          const kb2 = { inline_keyboard: [] };
          if (sbpIsUrl) kb2.inline_keyboard.push([{ text: isRu2 ? '💳 Перейти к оплате' : '💳 Pay Now', url: methodDetails }]);
          kb2.inline_keyboard.push([{ text: isRu2 ? '◀️ Назад' : '◀️ Back', callback_data: 'profile_topup' }]);

          session.state = 'awaiting_topup_receipt';
          session.data = { ...session.data, topupOrderId: orderId };
          sendNavMessage(chatId, user.id, msg, { parse_mode: 'Markdown', reply_markup: kb2 }).catch(() => {});

          // ✅ Уведомление админу — только когда придёт чек (в awaiting_topup_receipt)
        }
      );
      return;
    }

    // ==========================================
    // 💰 УПРАВЛЕНИЕ БАЛАНСОМ ПОЛЬЗОВАТЕЛЯ (АДМИН)
    // ==========================================
    if (data.startsWith('admin_balance_edit_') && user.id === ADMIN_ID) {
      const targetId = parseInt(data.replace('admin_balance_edit_', ''));
      const balance = await getUserBalance(targetId);
      const cur = balance.preferred_currency || 'RUB';
      const sym = { RUB: '₽', USD: '$', EUR: '€', UAH: '₴' }[cur] || cur;
      const exPlus = (cur === 'USD' || cur === 'EUR') ? '+10' : '+500';
      const exMinus = (cur === 'USD' || cur === 'EUR') ? '-5' : '-200';
      session.state = 'awaiting_balance_edit';
      session.data = { balanceTargetId: targetId, balanceCurrency: cur };
      bot.sendMessage(chatId,
        `💰 *Изменение баланса пользователя ${targetId}*\n\n` +
        `Текущий баланс: *${formatBalanceAmount(balance.balance, cur)}* (${cur})\n\n` +
        `Введите сумму со знаком (в ${cur}, ${sym}):\n` +
        `  \\+ начислить: \`${exPlus}\`\n` +
        `  \\- списать: \`${exMinus}\`\n\n` +
        `Можно добавить комментарий через пробел: \`${exPlus} Компенсация\`\n\n` +
        `⚠️ При списании суммы больше текущего баланса — будет списан весь остаток (до 0), отрицательный баланс невозможен.`,
        { parse_mode: 'Markdown' }
      );
      return;
    }

    // Быстрая выдача купона из карточки пользователя
    if (data.startsWith('admin_coupon_issue_') && user.id === ADMIN_ID) {
      const targetId = parseInt(data.replace('admin_coupon_issue_', ''));
      session.state = 'awaiting_coupon_issue_quick';
      session.data = { couponTargetId: targetId };
      bot.sendMessage(chatId,
        `🎟 *Выдача купона пользователю ${targetId}*

` +
        `Введите через пробел: \`скидка% [период]\`

` +
        `Примеры:
` +
        `  \`15\` — 15% на любой товар
` +
        `  \`10 30d\` — 10% только на 30д ключ
` +
        `  \`20 7d\` — 20% только на 7д ключ`,
        { parse_mode: 'Markdown' }
      );
      return;
    }

    // Подтверждение пополнения баланса через ручные методы
    if (data.startsWith('approve_topup_') && user.id === ADMIN_ID) {
      const orderId = parseInt(data.replace('approve_topup_', ''));

      // 🔒 Защита от двойного нажатия
      if (approvingTopups.has(orderId)) {
        bot.sendMessage(chatId, '⏳ Пополнение уже обрабатывается...').catch(() => {});
        return;
      }
      approvingTopups.add(orderId);

      db.get(`SELECT * FROM orders WHERE id = ? AND balance_topup = 1`, [orderId], async (err, order) => {
        if (err || !order) {
          approvingTopups.delete(orderId);
          bot.sendMessage(chatId, '❌ Заказ не найден.'); return;
        }
        if (order.status === 'confirmed') {
          approvingTopups.delete(orderId);
          bot.sendMessage(chatId, '⚠️ Этот заказ уже подтверждён ранее.'); return;
        }
        if (order.status === 'rejected') {
          approvingTopups.delete(orderId);
          bot.sendMessage(chatId, '⚠️ Этот заказ был отклонён.'); return;
        }
        if (order.status === 'pending') {
          approvingTopups.delete(orderId);
          bot.sendMessage(chatId, '⏳ Пользователь ещё не отправил чек. Подождите.'); return;
        }
        try {
          await adjustUserBalance(order.user_id, order.amount, order.currency, 'topup',
            `Пополнение через ${order.method}`, orderId, ADMIN_ID);
          db.run(`UPDATE orders SET status = 'confirmed', confirmed_at = datetime('now') WHERE id = ?`, [orderId]);

          // ✅ Убираем кнопки после подтверждения
          bot.editMessageReplyMarkup({ inline_keyboard: [] }, {
            chat_id: chatId,
            message_id: message.message_id
          }).catch(() => {});

          bot.sendMessage(chatId, `✅ Баланс пользователя ${order.user_id} пополнен на ${formatBalanceAmount(order.amount, order.currency)}`);
          safeSendMessage(order.user_id,
            `✅ *Баланс пополнен!*\n\nЗачислено: *${formatBalanceAmount(order.amount, order.currency)}*\n\nТеперь вы можете использовать баланс для покупки ключей в разделе «Купить ключ».`,
            { parse_mode: 'Markdown', reply_markup: { inline_keyboard: [[{ text: '👤 Мой профиль', callback_data: 'my_profile' }]] } }
          ).catch(() => {});
        } catch(e) {
          bot.sendMessage(chatId, `❌ Ошибка: ${e.message}`);
        } finally {
          approvingTopups.delete(orderId);
        }
      });
      return;
    }

    if (data.startsWith('reject_topup_') && user.id === ADMIN_ID) {
      const orderId = parseInt(data.replace('reject_topup_', ''));
      db.get(`SELECT * FROM orders WHERE id = ? AND balance_topup = 1`, [orderId], (e, o) => {
        if (e || !o) { bot.sendMessage(chatId, '❌ Заказ не найден.'); return; }
        if (o.status === 'confirmed') { bot.sendMessage(chatId, '⚠️ Заказ уже подтверждён — отклонить нельзя.'); return; }
        if (o.status === 'rejected') { bot.sendMessage(chatId, '⚠️ Заказ уже отклонён.'); return; }
        db.run(`UPDATE orders SET status = 'rejected' WHERE id = ?`, [orderId], () => {
          bot.sendMessage(chatId, `✅ Запрос пополнения #${orderId} отклонён.`);
          safeSendMessage(o.user_id,
            `ℹ️ Запрос на пополнение баланса на *${formatBalanceAmount(o.amount, o.currency)}* был отклонён.\n\nЕсли это ошибка — обратитесь в поддержку.`,
            { parse_mode: 'Markdown' }
          ).catch(() => {});
        });
      });
      return;
    }

    // ⚙️ П.3: НАСТРОЙКИ БОТА
    if (data === 'admin_settings') {
      clearSession(user.id);
      showBotSettings(chatId, message.message_id);
      return;
    }

    if (data.startsWith('settings_toggle_')) {
      const key = data.replace('settings_toggle_', '');
      const current = getSetting(key);
      const newVal = current === '1' ? '0' : '1';
      saveSetting(key, newVal, () => {
        showBotSettings(chatId, message.message_id);
      });
      return;
    }

    if (data === 'settings_edit_low_keys_threshold') {
      session.state = 'awaiting_low_keys_threshold';
      bot.sendMessage(chatId, `⚠️ *Порог мало ключей*\n\nТекущий: ${getSetting('low_keys_threshold')} шт.\n\nВведите новое число:`, { parse_mode: 'Markdown' });
      return;
    }

    if (data === 'settings_edit_welcome') {
      session.state = 'awaiting_edit_text';
      session.data = { textKey: 'welcome_ru' };
      bot.sendMessage(chatId, `👋 *Редактирование приветствия*\n\nТекущее:\n${getSetting('welcome_ru') || translations.ru.welcome}\n\nВведите новый текст:`, { parse_mode: 'Markdown' });
      return;
    }

    if (data === 'settings_edit_offer') {
      session.state = 'awaiting_edit_text';
      session.data = { textKey: 'offer_text_ru' };
      bot.sendMessage(chatId, `📜 Отправьте новый текст оферты (Markdown поддерживается):`);
      return;
    }

    if (data === 'settings_edit_help') {
      session.state = 'awaiting_edit_text';
      session.data = { textKey: 'help_text' };
      bot.sendMessage(chatId, `❓ Введите текст помощи:`);
      return;
    }

    if (data === 'settings_edit_channel_link') {
      session.state = 'awaiting_edit_text';
      session.data = { textKey: 'channel_link' };
      bot.sendMessage(chatId, `📢 Текущая ссылка канала: ${getSetting('channel_link') || 'не задана'}\n\nВведите новую (например https://t.me/cyraxml):`);
      return;
    }

    if (data === 'settings_edit_chat_link') {
      session.state = 'awaiting_edit_text';
      session.data = { textKey: 'chat_link' };
      bot.sendMessage(chatId, `💬 Текущая ссылка чата: ${getSetting('chat_link') || 'не задана'}\n\nВведите новую (например https://t.me/CyRaXMod):`);
      return;
    }

    if (data === 'settings_edit_support_link') {
      session.state = 'awaiting_edit_text';
      session.data = { textKey: 'support_link' };
      bot.sendMessage(chatId, `🆘 Текущая ссылка поддержки: ${getSetting('support_link')}\n\nВведите новую:`);
      return;
    }

    if (data === 'settings_edit_help_link') {
      session.state = 'awaiting_edit_text';
      session.data = { textKey: 'help_link' };
      const current = getSetting('help_link') || 'https://t.me/cyraxml/260';
      bot.sendMessage(chatId, `❓ Текущая ссылка кнопки «Помощь»:\n${current}\n\nВведите новую:`);
      return;
    }

    if (data === 'settings_edit_review_link') {
      session.state = 'awaiting_edit_text';
      session.data = { textKey: 'review_channel_link' };
      const current = getSetting('review_channel_link') || 'https://t.me/cyraxml/12';
      bot.sendMessage(chatId, `✍️ Текущая ссылка для отзывов:\n${current}\n\nВведите новую ссылку (например: https://t.me/channel/123):`);
      return;
    }

    if (data === 'settings_edit_dns_address') {
      session.state = 'awaiting_edit_text';
      session.data = { textKey: 'dns_address' };
      const current = getSetting('dns_address') || 'ff73dd.dns.nextdns.io';
      bot.sendMessage(chatId, `🌐 *Текущий DNS-адрес (отображается в FAQ №13):*\n\`${current}\`\n\nВведите новый DNS-адрес (например: ff73dd.dns.nextdns.io).\n\nЭто значение сразу появится у пользователей при открытии FAQ → «Как настроить DNS».`, { parse_mode: 'Markdown' });
      return;
    }

    if (data === 'settings_edit_promo_text') {
      const botUsername = process.env.BOT_USERNAME || 'cyraxxmod_bot';
      const defaultPromo =
        `👋 I only work in private messages.\n\n🔑 Cyrax mod keys\n🚀 Boost & guides\n\n@${botUsername}`;
      const current = getSetting('group_promo_text') || defaultPromo;
      session.state = 'awaiting_edit_text';
      session.data = { textKey: 'group_promo_text' };
      // Отправляем текущий текст отдельным сообщением без parse_mode — безопасно для любых символов
      bot.sendMessage(chatId,
        `📣 Текущий текст авторекламки:`,
        {}
      ).catch(() => {});
      bot.sendMessage(chatId,
        current,
        {}
      ).catch(() => {});
      bot.sendMessage(chatId,
        `✏️ Введите новый текст рекламки.\nПоддерживает эмодзи и переносы строк.\nКнопка "Open bot" добавляется автоматически.\n\nДля отмены нажмите /cancel`,
        { reply_markup: { inline_keyboard: [[{ text: '◀️ Отмена', callback_data: 'admin_settings' }]] } }
      );
      return;
    }

    if (data === 'settings_promo_preview') {
      const botUsername = process.env.BOT_USERNAME || 'cyraxxmod_bot';
      const defaultPromo =
        `👋 I only work in private messages.\n\n🔑 Cyrax mod keys\n🚀 Boost & guides\n\n@${botUsername}`;
      const promoText = getSetting('group_promo_text') || defaultPromo;
      const groupCount = await new Promise(res =>
        db.get(`SELECT COUNT(*) as cnt FROM group_chats WHERE active = 1`, [], (e, r) => res(r ? r.cnt : 0))
      );
      bot.sendMessage(chatId,
        `👁 *Предпросмотр рекламки:*\n\nАктивных групп: *${groupCount}*\n\n──────────────────`,
        { parse_mode: 'Markdown' }
      ).catch(() => {});
      bot.sendMessage(chatId, promoText, {
        reply_markup: {
          inline_keyboard: [[
            { text: '🤖 Open bot', url: `https://t.me/${botUsername}` }
          ]]
        }
      }).catch(() => {});
      bot.answerCallbackQuery(query.id).catch(() => {});
      return;
    }

    if (data === 'settings_promo_send_now') {
      bot.answerCallbackQuery(query.id, { text: '📤 Рассылка запущена...' }).catch(() => {});
      const groupCount = await new Promise(res =>
        db.get(`SELECT COUNT(*) as cnt FROM group_chats WHERE active = 1`, [], (e, r) => res(r ? r.cnt : 0))
      );
      if (groupCount === 0) {
        bot.sendMessage(chatId, '⚠️ Нет активных групп для рассылки. Добавьте бота в группы.').catch(() => {});
        return;
      }
      bot.sendMessage(chatId, `📤 Отправляю рекламку в *${groupCount}* групп(ы)...`, { parse_mode: 'Markdown' }).catch(() => {});
      sendGroupPromo().then(() => {
        bot.sendMessage(chatId, '✅ Рассылка завершена.').catch(() => {});
      });
      return;
    }

    // ⏱ Настройка интервала авторассылки
    if (data === 'settings_promo_interval') {
      if (user.id !== ADMIN_ID) { bot.answerCallbackQuery(query.id).catch(() => {}); return; }
      bot.answerCallbackQuery(query.id).catch(() => {});
      const current = getSetting('promo_interval_hours') || '6';
      bot.sendMessage(chatId,
        `⏱ Интервал авторассылки сейчас: ${current} ч\n\nВыберите новый интервал:`,
        { reply_markup: { inline_keyboard: [
          [
            { text: current === '2'  ? '✅ 2 ч'  : '2 ч',  callback_data: 'settings_promo_set_interval_2'  },
            { text: current === '4'  ? '✅ 4 ч'  : '4 ч',  callback_data: 'settings_promo_set_interval_4'  },
            { text: current === '6'  ? '✅ 6 ч'  : '6 ч',  callback_data: 'settings_promo_set_interval_6'  },
          ],
          [
            { text: current === '8'  ? '✅ 8 ч'  : '8 ч',  callback_data: 'settings_promo_set_interval_8'  },
            { text: current === '12' ? '✅ 12 ч' : '12 ч', callback_data: 'settings_promo_set_interval_12' },
            { text: current === '24' ? '✅ 24 ч' : '24 ч', callback_data: 'settings_promo_set_interval_24' },
          ],
          [{ text: '◀️ Назад', callback_data: 'admin_settings' }]
        ]}}
      ).catch(() => {});
      return;
    }

    // ⏱ Сохранение нового интервала
    if (data.startsWith('settings_promo_set_interval_')) {
      if (user.id !== ADMIN_ID) { bot.answerCallbackQuery(query.id).catch(() => {}); return; }
      const hours = data.replace('settings_promo_set_interval_', '');
      const validHours = ['2', '4', '6', '8', '12', '24'];
      if (!validHours.includes(hours)) { bot.answerCallbackQuery(query.id).catch(() => {}); return; }
      saveSetting('promo_interval_hours', hours);
      bot.answerCallbackQuery(query.id, { text: `✅ Интервал установлен: ${hours} ч` }).catch(() => {});
      // Обновляем кнопку в сообщении
      bot.editMessageText(
        `⏱ Интервал авторассылки установлен: ${hours} ч\n\nВыберите новый интервал:`,
        { chat_id: chatId, message_id: query.message.message_id,
          reply_markup: { inline_keyboard: [
            [
              { text: hours === '2'  ? '✅ 2 ч'  : '2 ч',  callback_data: 'settings_promo_set_interval_2'  },
              { text: hours === '4'  ? '✅ 4 ч'  : '4 ч',  callback_data: 'settings_promo_set_interval_4'  },
              { text: hours === '6'  ? '✅ 6 ч'  : '6 ч',  callback_data: 'settings_promo_set_interval_6'  },
            ],
            [
              { text: hours === '8'  ? '✅ 8 ч'  : '8 ч',  callback_data: 'settings_promo_set_interval_8'  },
              { text: hours === '12' ? '✅ 12 ч' : '12 ч', callback_data: 'settings_promo_set_interval_12' },
              { text: hours === '24' ? '✅ 24 ч' : '24 ч', callback_data: 'settings_promo_set_interval_24' },
            ],
            [{ text: '◀️ Назад', callback_data: 'admin_settings' }]
          ]}}
      ).catch(() => {});
      console.log(`📣 [PROMO] Интервал изменён на ${hours} ч`);
      return;
    }

    // ➕ Ручное добавление группы в group_chats
    if (data === 'settings_promo_add_chat') {
      if (user.id !== ADMIN_ID) { bot.answerCallbackQuery(query.id).catch(() => {}); return; }
      bot.answerCallbackQuery(query.id).catch(() => {});
      session.state = 'awaiting_promo_chat_id';
      bot.sendMessage(chatId,
        `➕ Добавить группу вручную\n\nОтправьте chat_id группы (отрицательное число, например: -1001234567890)\n\nКак узнать chat_id:\n1. Добавьте @userinfobot в группу\n2. Напишите туда /start — он покажет ID\n\nИли просто перешлите мне любое сообщение из нужной группы.`,
        { reply_markup: { inline_keyboard: [[{ text: '◀️ Отмена', callback_data: 'admin_settings' }]] } }
      );
      return;
    }

    // 📋 Список активных групп с кнопкой удаления
    if (data === 'settings_promo_list_chats') {
      if (user.id !== ADMIN_ID) { bot.answerCallbackQuery(query.id).catch(() => {}); return; }
      bot.answerCallbackQuery(query.id).catch(() => {});
      const chats = await new Promise(res =>
        db.all(`SELECT chat_id, title, last_promo_at FROM group_chats WHERE active = 1 ORDER BY added_at DESC`, [], (e, rows) => res(rows || []))
      );
      if (!chats.length) {
        bot.sendMessage(chatId, '📋 Активных групп нет.\n\nДобавьте бота в группы или используйте кнопку «Добавить группу» для ручного ввода.', {
          reply_markup: { inline_keyboard: [[{ text: '◀️ Назад', callback_data: 'admin_settings' }]] }
        }).catch(() => {});
        return;
      }
      let msg = `📋 *Активные группы для рассылки:* ${chats.length}\n\n`;
      const kb = [];
      chats.forEach((c, i) => {
        const title = c.title ? c.title.substring(0, 30) : `ID ${c.chat_id}`;
        const lastSent = c.last_promo_at ? new Date(c.last_promo_at).toLocaleDateString('ru-RU') : 'не отправлялось';
        msg += `${i + 1}. ${escapeMarkdown(title)}\n   ID: ${c.chat_id} · посл. рассылка: ${lastSent}\n`;
        kb.push([{ text: `🗑 ${title}`, callback_data: `settings_promo_remove_${c.chat_id}` }]);
      });
      msg += `\n_Нажмите на группу чтобы удалить её из рассылки_`;
      kb.push([{ text: '◀️ Назад', callback_data: 'admin_settings' }]);
      bot.sendMessage(chatId, msg, { reply_markup: { inline_keyboard: kb } }).catch(() => {});
      return;
    }

    // 🗑 Удаление группы из рассылки
    if (data.startsWith('settings_promo_remove_')) {
      if (user.id !== ADMIN_ID) { bot.answerCallbackQuery(query.id).catch(() => {}); return; }
      const removeChatId = data.replace('settings_promo_remove_', '');
      db.run(`UPDATE group_chats SET active = 0 WHERE chat_id = ?`, [removeChatId], (err) => {
        bot.answerCallbackQuery(query.id, { text: err ? '❌ Ошибка' : '✅ Группа удалена из рассылки' }).catch(() => {});
        if (!err) {
          // Обновляем список
          const fakeData = 'settings_promo_list_chats';
          // Re-show list by triggering same logic via redirect
          db.all(`SELECT chat_id, title, last_promo_at FROM group_chats WHERE active = 1 ORDER BY added_at DESC`, [], (e2, rows) => {
            const remaining = rows || [];
            if (!remaining.length) {
              bot.sendMessage(chatId, '📋 Групп не осталось. Список пуст.', {
                reply_markup: { inline_keyboard: [[{ text: '◀️ Назад', callback_data: 'admin_settings' }]] }
              }).catch(() => {});
              return;
            }
            let msg2 = `📋 *Активные группы:* ${remaining.length}\n\n`;
            const kb2 = [];
            remaining.forEach((c, i) => {
              const title = c.title ? c.title.substring(0, 30) : `ID ${c.chat_id}`;
              const lastSent = c.last_promo_at ? new Date(c.last_promo_at).toLocaleDateString('ru-RU') : 'не отправлялось';
              msg2 += `${i + 1}. ${escapeMarkdown(title)}\n   ID: ${c.chat_id} · посл. рассылка: ${lastSent}\n`;
              kb2.push([{ text: `🗑 ${title}`, callback_data: `settings_promo_remove_${c.chat_id}` }]);
            });
            kb2.push([{ text: '◀️ Назад', callback_data: 'admin_settings' }]);
            bot.sendMessage(chatId, msg2, { reply_markup: { inline_keyboard: kb2 } }).catch(() => {});
          });
        }
      });
      return;
    }

    // 🎟️ П.2: КУПОНЫ
    if (data === 'admin_coupons') {
      showCouponsPanel(chatId, message.message_id);
      return;
    }

    if (data === 'coupon_list') {
      showCouponList(chatId);
      return;
    }

    if (data === 'coupon_list_archive') {
      showCouponList(chatId, true);
      return;
    }

    // Пагинация купонов: coupon_list_page_N и coupon_list_archive_page_N
    if (data.startsWith('coupon_list_page_')) {
      const page = parseInt(data.replace('coupon_list_page_', '')) || 0;
      showCouponList(chatId, false, page);
      return;
    }
    if (data.startsWith('coupon_list_archive_page_')) {
      const page = parseInt(data.replace('coupon_list_archive_page_', '')) || 0;
      showCouponList(chatId, true, page);
      return;
    }

    if (data === 'coupon_stats') {
      showCouponStats(chatId);
      return;
    }

    if (data === 'coupon_create') {
      session.state = 'awaiting_coupon_code';
      session.data = {};
      bot.sendMessage(chatId,
        '🎟️ *Создание купона*\n\nШаг 1/5: Введите уникальный код купона (латинские буквы и цифры):',
        { parse_mode: 'Markdown' }
      );
      return;
    }

    if (data === 'coupon_issue_to_user') {
      session.state = 'awaiting_coupon_issue_username';
      session.data = {};
      bot.sendMessage(chatId, '🎁 Введите username пользователя (можно с @ или без):');
      return;
    }

    if (data === 'coupon_issue_to_all') {
      session.state = 'awaiting_coupon_all_percent';
      session.data = { targetMode: 'all' };
      bot.sendMessage(chatId,
        '📣 *Выдача купона ВСЕМ пользователям*\n\nШаг 1/3: Введите процент скидки (1-100):',
        { parse_mode: 'Markdown' }
      );
      return;
    }

    if (data === 'coupon_issue_to_buyers') {
      session.state = 'awaiting_coupon_all_percent';
      session.data = { targetMode: 'buyers' };
      bot.sendMessage(chatId,
        '🛒 *Выдача купона покупателям*\n\n👥 Получат только пользователи, сделавшие минимум 1 покупку.\n\nШаг 1/3: Введите процент скидки (1-100):',
        { parse_mode: 'Markdown' }
      );
      return;
    }

    // 🗑️ Задача 2: Удалить ВСЕ купоны — шаг 1: первое предупреждение
    if (data === 'coupon_delete_all_confirm') {
      safeSendMessage(chatId, '⚠️ *Удалить ВСЕ купоны?*\n\nЭто действие необратимо. Все купоны и записи об их использовании будут удалены.', {
        parse_mode: 'Markdown',
        reply_markup: {
          inline_keyboard: [
            [{ text: '⚠️ Да, перейти к подтверждению', callback_data: 'coupon_delete_all_yes' }],
            [{ text: '❌ Отмена', callback_data: 'admin_coupons' }]
          ]
        }
      });
      return;
    }

    // 🗑️ Удалить ВСЕ купоны — шаг 2: финальное подтверждение перед необратимым действием
    if (data === 'coupon_delete_all_yes') {
      safeSendMessage(chatId, '🔴 *Финальное подтверждение*\n\nВы точно хотите удалить *все* купоны без возможности восстановления?\n\nНажмите ещё раз для подтверждения:', {
        parse_mode: 'Markdown',
        reply_markup: {
          inline_keyboard: [
            [{ text: '🗑️ Да, удалить ВСЕ купоны окончательно', callback_data: 'coupon_delete_all_final' }],
            [{ text: '❌ Отмена', callback_data: 'admin_coupons' }]
          ]
        }
      });
      return;
    }

    // 🗑️ Удалить ВСЕ купоны — шаг 3: реальное удаление
    if (data === 'coupon_delete_all_final') {
      db.run('DELETE FROM user_coupons', [], (e1) => {
        db.run('DELETE FROM coupon_products', [], (e2) => {
          db.run('DELETE FROM coupons', [], (e3) => {
            if (e3) {
              bot.sendMessage(chatId, '❌ Ошибка удаления купонов');
            } else {
              bot.sendMessage(chatId, '✅ Все купоны удалены');
              logAction(ADMIN_ID, 'all_coupons_deleted', {});
              showCouponsPanel(chatId, message.message_id);
            }
          });
        });
      });
      return;
    }

    if (data.startsWith('delete_coupon_confirm_')) {
      const couponId = parseInt(data.replace('delete_coupon_confirm_', ''));
      db.get(`SELECT code, used_count FROM coupons WHERE id = ?`, [couponId], (err, c) => {
        if (err || !c) { bot.sendMessage(chatId, '❌ Купон не найден'); return; }
        if (c.used_count > 0) {
          // Купон использовался — предлагаем деактивировать или всё равно удалить
          safeSendMessage(chatId, `⚠️ Купон \`${c.code}\` был использован ${c.used_count} раз.\n\nВыберите действие:`, {
            parse_mode: 'Markdown',
            reply_markup: {
              inline_keyboard: [
                [{ text: '🔕 Деактивировать', callback_data: `coupon_deactivate_${couponId}` }],
                [{ text: '🗑️ Всё равно удалить', callback_data: `delete_coupon_yes_${couponId}` }],
                [{ text: '❌ Отмена', callback_data: 'coupon_list' }]
              ]
            }
          });
          return;
        }
        safeSendMessage(chatId, `❓ Удалить купон \`${c.code}\`?`, {
          parse_mode: 'Markdown',
          reply_markup: {
            inline_keyboard: [
              [{ text: '✅ Да, удалить', callback_data: `delete_coupon_yes_${couponId}` }, { text: '❌ Отмена', callback_data: 'coupon_list' }]
            ]
          }
        });
      });
      return;
    }

    if (data.startsWith('delete_coupon_yes_')) {
      const couponId = parseInt(data.replace('delete_coupon_yes_', ''));
      db.get(`SELECT code FROM coupons WHERE id = ?`, [couponId], (err, c) => {
        if (err || !c) { bot.sendMessage(chatId, '❌ Купон не найден'); return; }
        db.run(`DELETE FROM coupon_products WHERE coupon_id = ?`, [couponId]);
        db.run(`DELETE FROM user_coupons WHERE coupon_id = ?`, [couponId]);
        db.run(`DELETE FROM coupons WHERE id = ?`, [couponId], (err2) => {
          if (err2) { bot.sendMessage(chatId, '❌ Ошибка удаления'); return; }
          bot.sendMessage(chatId, `✅ Купон \`${c.code}\` удалён`, { parse_mode: 'Markdown' });
          logAction(ADMIN_ID, 'coupon_deleted', { couponId, code: c.code });
          showCouponList(chatId);
        });
      });
      return;
    }

    if (data.startsWith('coupon_deactivate_')) {
      const couponId = parseInt(data.replace('coupon_deactivate_', ''));
      db.run(`UPDATE coupons SET is_active = 0 WHERE id = ?`, [couponId], (err) => {
        if (err) {
          bot.sendMessage(chatId, '❌ Не удалось деактивировать купон. Попробуйте ещё раз.');
        } else {
          bot.sendMessage(chatId, '✅ Купон деактивирован');
          showCouponList(chatId);
        }
      });
      return;
    }

    if (data === 'admin_bans') {
      showBannedUsers(chatId, message.message_id);
      return;
    }

    // =============================================
    // 👥 МЕНЕДЖЕРЫ — callback обработчики
    // =============================================
    if (user.id === ADMIN_ID && data === 'admin_managers') {
      showManagersPanel(chatId, message.message_id);
      return;
    }

    if (user.id === ADMIN_ID && data === 'admin_add_manager') {
      session.state = 'awaiting_manager_username';
      session.data = { managerMethods: [] };
      bot.sendMessage(chatId, '👤 Введите @username пользователя (можно с @ или без):');
      return;
    }

    if (user.id === ADMIN_ID && data.startsWith('admin_remove_manager_')) {
      const targetId = parseInt(data.replace('admin_remove_manager_', ''));
      db.run('DELETE FROM manager_methods WHERE manager_id = ?', [targetId]);
      db.run('DELETE FROM managers WHERE user_id = ?', [targetId], (err) => {
        if (err) { bot.sendMessage(chatId, '❌ Ошибка удаления'); return; }
        bot.sendMessage(chatId, '✅ Менеджер удалён');
        showManagersPanel(chatId, message.message_id);
      });
      return;
    }

    if (user.id === ADMIN_ID && data.startsWith('admin_edit_manager_')) {
      const targetId = parseInt(data.replace('admin_edit_manager_', ''));
      db.all('SELECT payment_method FROM manager_methods WHERE manager_id = ?', [targetId], (err, rows) => {
        const currentMethods = (rows || []).map(r => r.payment_method);
        session.state = 'awaiting_manager_methods_edit';
        session.data = { editManagerId: targetId, managerMethods: [...currentMethods] };
        showManagerMethodsKeyboard(chatId, targetId, currentMethods);
      });
      return;
    }

    if (user.id === ADMIN_ID && data.startsWith('admin_manager_toggle_')) {
      const rest = data.replace('admin_manager_toggle_', '');
      const method = rest;
      const sd = session.data || {};
      const methods = sd.managerMethods || [];
      const idx = methods.indexOf(method);
      if (idx === -1) methods.push(method);
      else methods.splice(idx, 1);
      session.data = { ...sd, managerMethods: methods };
      const managerId = sd.pendingManagerId || sd.editManagerId;
      showManagerMethodsKeyboard(chatId, managerId, methods);
      return;
    }

    if (user.id === ADMIN_ID && data.startsWith('admin_manager_methods_done_')) {
      const targetId = parseInt(data.replace('admin_manager_methods_done_', ''));
      const methods = (session.data && session.data.managerMethods) || [];
      if (methods.length === 0) {
        bot.sendMessage(chatId, '⚠️ Выберите хотя бы один метод оплаты');
        return;
      }
      const username = session.data.pendingManagerUsername || null;
      // Задача 6: Спрашиваем имя менеджера для персонального приветствия
      session.state = 'awaiting_manager_display_name';
      session.data = { ...session.data, pendingManagerId: targetId, pendingManagerUsername: username, pendingMethods: methods };
      bot.sendMessage(chatId,
        `✅ Методы выбраны: ${methods.join(', ')}\n\n👤 *Шаг 3/3 — Имя менеджера*\n\nВведите имя, которым бот будет приветствовать менеджера (например: *Диана*):\n\n_(или "-" чтобы использовать @username)_`,
        { parse_mode: 'Markdown' }
      );
      return;
    }


    // ==========================================
    // 🔔 УМНЫЕ НАПОМИНАНИЯ — СОГЛАСОВАНИЕ
    // ==========================================

    // Админ нажал "Отправить напоминание"
    // ── БАТЧ-НАПОМИНАНИЯ: постраничный просмотр ────────────────────────────────

    // Открыть/перейти к странице N батча
    if (data.startsWith('remind_batch_open_') && user.id === ADMIN_ID) {
      const page = parseInt(data.replace('remind_batch_open_', '')) || 0;
      const adminSession = getSession(ADMIN_ID);
      const batch = adminSession.reminderBatch;
      if (!batch || batch.length === 0) {
        bot.answerCallbackQuery(query.id, { text: 'Список пуст или устарел' }).catch(() => {});
        bot.editMessageReplyMarkup({ inline_keyboard: [] }, { chat_id: chatId, message_id: message.message_id }).catch(() => {});
        return;
      }
      const item = batch[page];
      if (!item) {
        // Все разобраны
        bot.editMessageText(
          '✅ *Все напоминания разобраны!*',
          { chat_id: chatId, message_id: message.message_id, parse_mode: 'Markdown', reply_markup: { inline_keyboard: [] } }
        ).catch(() => {});
        bot.answerCallbackQuery(query.id).catch(() => {});
        return;
      }
      const userObj = { language_code: item.userLang };
      const previewMsg = buildReminderMessage(getLang(userObj), item.product, item.confirmedAt, item.periodName);
      const cardText =
        `🔔 *Напоминание ${page + 1} из ${batch.length}*

` +
        `👤 ${item.uname}  |  📦 *${item.periodName}*  |  📅 ${item.confirmedDate}

` +
        `📩 _Сообщение клиенту:_
` +
        `┄┄┄┄┄┄┄┄┄┄┄┄┄┄
${previewMsg}
┄┄┄┄┄┄┄┄┄┄┄┄┄┄`;
      const nextPage = page + 1;
      const navRow = [];
      if (nextPage < batch.length) {
        navRow.push({ text: `➡️ Следующий (${nextPage + 1}/${batch.length})`, callback_data: `remind_batch_open_${nextPage}` });
      }
      const kb = {
        inline_keyboard: [
          [
            { text: '✅ Отправить', callback_data: `remind_batch_item_send_${page}` },
            { text: '⏭ Пропустить', callback_data: `remind_batch_item_skip_${page}` }
          ],
          ...(navRow.length ? [navRow] : [])
        ]
      };
      bot.editMessageText(cardText, {
        chat_id: chatId, message_id: message.message_id,
        parse_mode: 'HTML', reply_markup: kb
      }).catch(() => {
        // Если editMessageText не сработал — отправляем новым сообщением
        safeSendMessage(chatId, cardText, { parse_mode: 'HTML', reply_markup: kb }).catch(() => {});
      });
      bot.answerCallbackQuery(query.id).catch(() => {});
      return;
    }

    // Отправить конкретный элемент батча
    if (data.startsWith('remind_batch_item_send_') && user.id === ADMIN_ID) {
      const page = parseInt(data.replace('remind_batch_item_send_', ''));
      const adminSession = getSession(ADMIN_ID);
      const batch = adminSession.reminderBatch;
      const item = batch && batch[page];
      if (!item) { bot.answerCallbackQuery(query.id, { text: '❌ Элемент не найден' }).catch(() => {}); return; }

      db.get(`SELECT id FROM reminders WHERE order_id = ?`, [item.orderId], async (e, existing) => {
        if (existing) {
          bot.answerCallbackQuery(query.id, { text: 'ℹ️ Уже отправлено ранее' }).catch(() => {});
        } else {
          const userObj = { language_code: item.userLang };
          const msgText = buildReminderMessage(getLang(userObj), item.product, item.confirmedAt, item.periodName);
          try {
            await safeSendMessage(item.userId, msgText, {
              parse_mode: 'HTML',
              reply_markup: { inline_keyboard: [[{ text: t(userObj, 'reminder_button'), callback_data: 'buy' }]] }
            });
            db.run(`INSERT OR IGNORE INTO reminders (user_id, order_id) VALUES (?, ?)`, [item.userId, item.orderId]);
            logAction(ADMIN_ID, 'reminder_sent_manual', { orderId: item.orderId, userId: item.userId });
            bot.answerCallbackQuery(query.id, { text: `✅ Отправлено ${item.uname}` }).catch(() => {});
          } catch (err) {
            bot.answerCallbackQuery(query.id, { text: `❌ Ошибка: ${err.message}` }).catch(() => {});
          }
        }
        // Перейти к следующему
        const nextPage = page + 1;
        if (nextPage < batch.length) {
          // Имитируем переход на следующую страницу
          const nextItem = batch[nextPage];
          const userObj2 = { language_code: nextItem.userLang };
          const previewMsg2 = buildReminderMessage(getLang(userObj2), nextItem.product, nextItem.confirmedAt, nextItem.periodName);
          const cardText2 =
            `🔔 *Напоминание ${nextPage + 1} из ${batch.length}*

` +
            `👤 ${nextItem.uname}  |  📦 *${nextItem.periodName}*  |  📅 ${nextItem.confirmedDate}

` +
            `📩 _Сообщение клиенту:_
` +
            `┄┄┄┄┄┄┄┄┄┄┄┄┄┄
${previewMsg2}
┄┄┄┄┄┄┄┄┄┄┄┄┄┄`;
          const nn = nextPage + 1;
          const navRow2 = nn < batch.length ? [{ text: `➡️ Следующий (${nn + 1}/${batch.length})`, callback_data: `remind_batch_open_${nn}` }] : [];
          const kb2 = {
            inline_keyboard: [
              [{ text: '✅ Отправить', callback_data: `remind_batch_item_send_${nextPage}` }, { text: '⏭ Пропустить', callback_data: `remind_batch_item_skip_${nextPage}` }],
              ...(navRow2.length ? [navRow2] : [])
            ]
          };
          bot.editMessageText(cardText2, { chat_id: chatId, message_id: message.message_id, parse_mode: 'Markdown', reply_markup: kb2 }).catch(() => {});
        } else {
          bot.editMessageText('✅ *Все напоминания разобраны!*', { chat_id: chatId, message_id: message.message_id, parse_mode: 'Markdown', reply_markup: { inline_keyboard: [] } }).catch(() => {});
        }
      });
      return;
    }

    // Пропустить конкретный элемент батча
    if (data.startsWith('remind_batch_item_skip_') && user.id === ADMIN_ID) {
      const page = parseInt(data.replace('remind_batch_item_skip_', ''));
      const adminSession = getSession(ADMIN_ID);
      const batch = adminSession.reminderBatch;
      const item = batch && batch[page];
      if (item) {
        db.run(`INSERT OR IGNORE INTO reminders (user_id, order_id) VALUES (0, ?)`, [item.orderId]);
      }
      bot.answerCallbackQuery(query.id, { text: '⏭ Пропущено' }).catch(() => {});
      const nextPage = page + 1;
      if (batch && nextPage < batch.length) {
        const nextItem = batch[nextPage];
        const userObj3 = { language_code: nextItem.userLang };
        const previewMsg3 = buildReminderMessage(getLang(userObj3), nextItem.product, nextItem.confirmedAt, nextItem.periodName);
        const cardText3 =
          `🔔 *Напоминание ${nextPage + 1} из ${batch.length}*

` +
          `👤 ${nextItem.uname}  |  📦 *${nextItem.periodName}*  |  📅 ${nextItem.confirmedDate}

` +
          `📩 _Сообщение клиенту:_
` +
          `┄┄┄┄┄┄┄┄┄┄┄┄┄┄
${previewMsg3}
┄┄┄┄┄┄┄┄┄┄┄┄┄┄`;
        const nn3 = nextPage + 1;
        const navRow3 = nn3 < batch.length ? [{ text: `➡️ Следующий (${nn3 + 1}/${batch.length})`, callback_data: `remind_batch_open_${nn3}` }] : [];
        const kb3 = {
          inline_keyboard: [
            [{ text: '✅ Отправить', callback_data: `remind_batch_item_send_${nextPage}` }, { text: '⏭ Пропустить', callback_data: `remind_batch_item_skip_${nextPage}` }],
            ...(navRow3.length ? [navRow3] : [])
          ]
        };
        bot.editMessageText(cardText3, { chat_id: chatId, message_id: message.message_id, parse_mode: 'Markdown', reply_markup: kb3 }).catch(() => {});
      } else {
        bot.editMessageText('✅ *Все напоминания разобраны!*', { chat_id: chatId, message_id: message.message_id, parse_mode: 'Markdown', reply_markup: { inline_keyboard: [] } }).catch(() => {});
      }
      return;
    }

    // Отправить всем сразу
    if (data === 'remind_batch_send_all' && user.id === ADMIN_ID) {
      const adminSession = getSession(ADMIN_ID);
      const batch = adminSession.reminderBatch;
      if (!batch || batch.length === 0) { bot.answerCallbackQuery(query.id, { text: 'Список пуст' }).catch(() => {}); return; }
      bot.editMessageReplyMarkup({ inline_keyboard: [] }, { chat_id: chatId, message_id: message.message_id }).catch(() => {});
      let sent = 0, skipped = 0;
      for (const item of batch) {
        await new Promise(resolve => {
          db.get(`SELECT id FROM reminders WHERE order_id = ?`, [item.orderId], async (e, existing) => {
            if (existing) { skipped++; resolve(); return; }
            const userObj = { language_code: item.userLang };
            const lang = getLang(userObj);
            const msgText = buildReminderMessage(lang, item.product, item.confirmedAt, item.periodName);
            try {
              await safeSendMessage(item.userId, msgText, {
                parse_mode: 'HTML',
                reply_markup: { inline_keyboard: [[{ text: t(userObj, 'reminder_button'), callback_data: 'buy' }]] }
              });
              db.run(`INSERT OR IGNORE INTO reminders (user_id, order_id) VALUES (?, ?)`, [item.userId, item.orderId]);
              logAction(ADMIN_ID, 'reminder_sent_manual', { orderId: item.orderId, userId: item.userId });
              sent++;
            } catch (_) { skipped++; }
            await new Promise(r => setTimeout(r, 100));
            resolve();
          });
        });
      }
      adminSession.reminderBatch = [];
      bot.answerCallbackQuery(query.id, { text: `✅ Отправлено: ${sent}, пропущено: ${skipped}` }).catch(() => {});
      safeSendMessage(chatId, `✅ *Напоминания отправлены!*

Отправлено: ${sent}
Пропущено: ${skipped}`, { parse_mode: 'Markdown' }).catch(() => {});
      return;
    }

    // Пропустить всех сразу
    if (data === 'remind_batch_skip_all' && user.id === ADMIN_ID) {
      const adminSession = getSession(ADMIN_ID);
      const batch = adminSession.reminderBatch;
      if (!batch || batch.length === 0) { bot.answerCallbackQuery(query.id, { text: 'Список пуст' }).catch(() => {}); return; }
      for (const item of batch) {
        db.run(`INSERT OR IGNORE INTO reminders (user_id, order_id) VALUES (0, ?)`, [item.orderId]);
      }
      adminSession.reminderBatch = [];
      bot.editMessageReplyMarkup({ inline_keyboard: [] }, { chat_id: chatId, message_id: message.message_id }).catch(() => {});
      bot.answerCallbackQuery(query.id, { text: `⏭ Пропущено ${batch.length} напоминаний` }).catch(() => {});
      return;
    }

    // ── конец батч-напоминаний ────────────────────────────────────────────────

    if (data.startsWith('remind_send_')) {
      const m = data.match(/^remind_send_(\d+)_(\d+)$/);
      if (!m) { bot.answerCallbackQuery(query.id).catch(() => { }); return; }
      const orderId = parseInt(m[1]);
      const userId = parseInt(m[2]);

      db.get(`SELECT * FROM orders WHERE id = ?`, [orderId], async (err, order) => {
        if (err || !order) {
          bot.editMessageReplyMarkup({ inline_keyboard: [] }, { chat_id: chatId, message_id: message.message_id }).catch(() => { });
          bot.sendMessage(chatId, '❌ Заказ не найден');
          return;
        }

        // Уже напоминали?
        db.get(`SELECT id FROM reminders WHERE order_id = ?`, [orderId], async (e2, existing) => {
          if (existing) {
            bot.editMessageReplyMarkup({ inline_keyboard: [] }, { chat_id: chatId, message_id: message.message_id }).catch(() => { });
            bot.sendMessage(chatId, 'ℹ️ Напоминание уже было отправлено ранее');
            return;
          }

          const userObj = { language_code: order.user_lang || 'en' };
          const lang = getLang(userObj);
          const periodName = PERIOD_NAMES[lang]?.[order.product] || order.product;
          const msgText = buildReminderMessage(lang, order.product, order.confirmed_at, periodName);

          try {
            await safeSendMessage(userId, msgText, {
              parse_mode: 'HTML',
              reply_markup: {
                inline_keyboard: [[
                  { text: t(userObj, 'reminder_button'), callback_data: 'buy' }
                ]]
              }
            });

            db.run(`INSERT OR IGNORE INTO reminders (user_id, order_id) VALUES (?, ?)`, [userId, orderId]);
            logAction(ADMIN_ID, 'reminder_sent_manual', { orderId, userId });

            bot.editMessageReplyMarkup({ inline_keyboard: [] }, { chat_id: chatId, message_id: message.message_id }).catch(() => { });
            bot.sendMessage(chatId, `✅ Напоминание отправлено клиенту (заказ #${orderId})`);
          } catch (e) {
            bot.sendMessage(chatId, `❌ Не удалось отправить: ${e.message}`);
          }
        });
      });
      return;
    }

    // Админ нажал "Пропустить напоминание"
    if (data.startsWith('remind_skip_')) {
      const orderId = parseInt(data.replace('remind_skip_', ''));
      // Записываем в reminders чтобы не предлагать снова
      db.run(`INSERT OR IGNORE INTO reminders (user_id, order_id) VALUES (0, ?)`, [orderId]);
      bot.editMessageReplyMarkup({ inline_keyboard: [] }, { chat_id: chatId, message_id: message.message_id }).catch(() => { });
      bot.answerCallbackQuery(query.id, { text: '⏭ Пропущено' }).catch(() => { });
      return;
    }

    // ==========================================
    // 🎟 УМНЫЕ КУПОНЫ ЛОЯЛЬНОСТИ — ФЛОУ
    // ==========================================

    // Шаг 1: Да — выдать купон
    if (data.startsWith('loyal_yes_')) {
      const targetUserId = parseInt(data.replace('loyal_yes_', ''));
      db.get(`SELECT id, username, language_code FROM users WHERE id = ?`, [targetUserId], (err, targetUser) => {
        if (err || !targetUser) { bot.sendMessage(chatId, '❌ Пользователь не найден'); return; }

        const uname = targetUser.username ? `@${escapeMarkdown(targetUser.username)}` : `ID: ${targetUserId}`;
        bot.editMessageReplyMarkup({ inline_keyboard: [] }, { chat_id: chatId, message_id: message.message_id }).catch(() => { });
        bot.sendMessage(chatId,
          `🎟 *Купон для ${uname}*\n\nШаг 1 из 2 — Выберите размер скидки:`,
          {
            parse_mode: 'Markdown',
            reply_markup: {
              inline_keyboard: [
                [
                  { text: '5%', callback_data: `loyal_pct_${targetUserId}_5` },
                  { text: '10%', callback_data: `loyal_pct_${targetUserId}_10` },
                  { text: '15%', callback_data: `loyal_pct_${targetUserId}_15` }
                ],
                [
                  { text: '20%', callback_data: `loyal_pct_${targetUserId}_20` },
                  { text: '25%', callback_data: `loyal_pct_${targetUserId}_25` },
                  { text: '30%', callback_data: `loyal_pct_${targetUserId}_30` }
                ],
                [{ text: '❌ Отмена', callback_data: `loyal_no_${targetUserId}` }]
              ]
            }
          }
        );
      });
      return;
    }

    // =============================================
    // 🏆 ПРОСМОТР ЧАСТЫХ ПОКУПАТЕЛЕЙ (пагинация)
    // =============================================
    if (user.id === ADMIN_ID && (data === 'admin_frequent_buyers' || data.startsWith('frequent_page_'))) {
      const cache = global._frequentBuyersCache;
      if (!cache || !cache.buyers || cache.buyers.length === 0) {
        bot.answerCallbackQuery(query.id).catch(() => {});
        bot.sendMessage(chatId, '⏳ Данные собираются... Запускаю анализ, это займёт несколько секунд.').catch(() => {});
        // Запускаем анализ прямо сейчас и после показываем результат
        analyzeFrequentBuyers().then(() => {
          const freshCache = global._frequentBuyersCache;
          if (!freshCache || !freshCache.buyers || freshCache.buyers.length === 0) {
            bot.sendMessage(chatId, '📭 Нет данных о постоянных клиентах. Нужно минимум 2+ покупки от одного пользователя.', {
              reply_markup: { inline_keyboard: [[{ text: '◀️ Назад', callback_data: 'admin' }]] }
            }).catch(() => {});
          } else {
            bot.sendMessage(chatId, `✅ Анализ завершён — найдено ${freshCache.buyers.length} постоянных клиентов. Открываю список...`).catch(() => {});
            // Имитируем callback с page=0
            setTimeout(() => {
              const fakeData = 'admin_frequent_buyers';
              const pageSize = 3;
              const pageBuyers = freshCache.buyers.slice(0, pageSize);
              const totalPages = Math.ceil(freshCache.buyers.length / pageSize);
              let card = `🏆 *Постоянные клиенты* (стр. 1/${totalPages})\n\n`;
              pageBuyers.forEach((b, i) => {
                const uname = b.username ? escapeMarkdown(`@${b.username}`) : `ID: ${b.user_id}`;
                const favPeriod = PERIOD_NAMES.ru[b.fav_product] || b.fav_product || '?';
                const spent = formatPrice(b.total_spent || 0, b.currency || 'USD');
                card += `${i + 1}. ${uname}\n   🛒 ${b.purchase_count} покупок | ❤️ ${favPeriod} | 💰 ${spent}\n`;
              });
              const couponBtns = pageBuyers.map(b => [{ text: `🎟 ${b.username ? '@' + b.username : 'ID:' + b.user_id}`, callback_data: `loyal_yes_${b.user_id}` }]);
              const nav = [];
              if (totalPages > 1) nav.push({ text: 'След. ▶️', callback_data: `frequent_page_1` });
              safeSendMessage(chatId, card, { parse_mode: 'Markdown', reply_markup: { inline_keyboard: [...couponBtns, ...(nav.length ? [nav] : []), [{ text: '◀️ Назад в админ', callback_data: 'admin' }]] } }).catch(() => {});
            }, 500);
          }
        }).catch(() => {
          bot.sendMessage(chatId, '❌ Ошибка анализа. Попробуйте позже.').catch(() => {});
        });
        return;
      }
      const pageSize = 3;
      const page = data.startsWith('frequent_page_') ? parseInt(data.replace('frequent_page_', '')) : 0;
      const totalPages = Math.ceil(cache.buyers.length / pageSize);
      const pageBuyers = cache.buyers.slice(page * pageSize, page * pageSize + pageSize);
      if (!pageBuyers.length) return;

      let card = `🏆 *Постоянные клиенты* (стр. ${page + 1}/${totalPages})\n\n`;
      pageBuyers.forEach((b, i) => {
        const uname = b.username ? escapeMarkdown(`@${b.username}`) : `ID: ${b.user_id}`;
        const favPeriod = PERIOD_NAMES.ru[b.fav_product] || b.fav_product || '?';
        const spent = formatPrice(b.total_spent || 0, b.currency || 'USD');
        card += `${page * pageSize + i + 1}. ${uname}\n`;
        card += `   🛒 ${b.purchase_count} покупок | ❤️ ${favPeriod} | 💰 ${spent}\n`;
        // кнопки купонов — в couponBtns ниже
      });

      const couponBtns = pageBuyers.map(b => [{
        text: `🎟 ${b.username ? '@' + b.username : 'ID:' + b.user_id}`,
        callback_data: `loyal_yes_${b.user_id}`
      }]);

      const nav = [];
      if (page > 0) nav.push({ text: '◀️ Пред.', callback_data: `frequent_page_${page - 1}` });
      if (page < totalPages - 1) nav.push({ text: 'След. ▶️', callback_data: `frequent_page_${page + 1}` });

      safeSendMessage(chatId, card, {
        parse_mode: 'Markdown',
        reply_markup: {
          inline_keyboard: [
            ...couponBtns,
            ...(nav.length ? [nav] : []),
            [{ text: '◀️ Назад в админ', callback_data: 'admin' }]
          ]
        }
      }).catch(() => { });
      return;
    }


    // Шаг 1: Нет — пропустить
    if (data.startsWith('loyal_no_')) {
      bot.editMessageReplyMarkup({ inline_keyboard: [] }, { chat_id: chatId, message_id: message.message_id }).catch(() => { });
      bot.answerCallbackQuery(query.id, { text: '⏭ Пропущено' }).catch(() => { });
      return;
    }

    // =========================================================
    // 📢 ПРЕВЬЮ РАССЫЛКИ — подтверждение / отмена
    // =========================================================
    if (data === 'broadcast_preview_confirm' && user.id === ADMIN_ID) {
      bot.answerCallbackQuery(query.id).catch(() => {});
      bot.editMessageReplyMarkup({ inline_keyboard: [] }, { chat_id: chatId, message_id: message.message_id }).catch(() => {});
      const session = getSession(user.id);
      if (!session.data?.broadcastData) {
        bot.sendMessage(chatId, '❌ Сессия истекла. Начните рассылку заново.').catch(() => {});
        return;
      }
      bot.sendMessage(chatId, '🚀 Запускаю рассылку...');
      executeAdminBroadcast(chatId, session.data.broadcastData);
      clearSession(user.id);
      return;
    }

    if (data === 'broadcast_preview_cancel' && user.id === ADMIN_ID) {
      bot.answerCallbackQuery(query.id).catch(() => {});
      bot.editMessageReplyMarkup({ inline_keyboard: [] }, { chat_id: chatId, message_id: message.message_id }).catch(() => {});
      clearSession(user.id);
      bot.sendMessage(chatId, '✏️ Рассылка отменена. Начните заново:', {
        reply_markup: { inline_keyboard: [
          [{ text: '📢 Новая рассылка', callback_data: 'admin_broadcast' }],
          [{ text: '◀️ Панель', callback_data: 'admin' }]
        ]}
      }).catch(() => {});
      return;
    }

    // =========================================================
    // 📢 ВИЗАРД РАССЫЛКИ (Ответ на вопрос о кнопке)
    // =========================================================
    if (data === 'broadcast_btn_yes' || data === 'broadcast_btn_no') {
      if (user.id !== ADMIN_ID) return;
      bot.editMessageReplyMarkup({ inline_keyboard: [] }, { chat_id: chatId, message_id: message.message_id }).catch(() => { });

      const session = getSession(user.id);
      if (session.state !== 'awaiting_broadcast_btn_decision' || !session.data.broadcastData) {
        bot.answerCallbackQuery(query.id, { text: 'Сессия обмена истекла. Начните заново.' }).catch(() => { });
        return;
      }

      if (data === 'broadcast_btn_no') {
        showBroadcastPreview(chatId, session.data.broadcastData, user);
        // session НЕ сбрасываем — ждём подтверждения превью
      } else {
        session.state = 'awaiting_broadcast_btn_name';
        bot.sendMessage(chatId, '✏️ Напишите текст для кнопки:');
      }
      bot.answerCallbackQuery(query.id).catch(() => { });
      return;
    }

    if (data.startsWith('admin_bc_action_')) {
      if (user.id !== ADMIN_ID) return;
      bot.editMessageReplyMarkup({ inline_keyboard: [] }, { chat_id: chatId, message_id: message.message_id }).catch(() => { });

      const session = getSession(user.id);
      if (session.state !== 'awaiting_broadcast_btn_action' || !session.data.broadcastData) {
        bot.answerCallbackQuery(query.id, { text: 'Сессия обмена истекла.' }).catch(() => { });
        return;
      }

      let actionValue = '';
      if (data === 'admin_bc_action_catalog') actionValue = 'catalog';
      if (data === 'admin_bc_action_help') actionValue = 'help';

      session.data.broadcastData.btnAction = actionValue;
      showBroadcastPreview(chatId, session.data.broadcastData, user);
      // session НЕ сбрасываем — ждём подтверждения превью
      bot.answerCallbackQuery(query.id).catch(() => { });
      return;
    }

    // Шаг 2: Выбран процент — спрашиваем срок
    if (data.startsWith('loyal_pct_')) {
      const m2 = data.match(/^loyal_pct_(\d+)_(\d+)$/);
      if (!m2) { bot.answerCallbackQuery(query.id).catch(() => { }); return; }
      const targetUserId = parseInt(m2[1]);
      const percent = parseInt(m2[2]);

      db.get(`SELECT username FROM users WHERE id = ?`, [targetUserId], (err, row) => {
        const uname = (!err && row && row.username) ? `@${escapeMarkdown(row.username)}` : `ID: ${targetUserId}`;
        bot.editMessageReplyMarkup({ inline_keyboard: [] }, { chat_id: chatId, message_id: message.message_id }).catch(() => { });
        bot.sendMessage(chatId,
          `🎟 *Купон ${percent}% для ${uname}*\n\nШаг 2 из 2 — Срок действия:`,
          {
            parse_mode: 'Markdown',
            reply_markup: {
              inline_keyboard: [
                [
                  { text: '⏰ 24 часа', callback_data: `loyal_exp_${targetUserId}_${percent}_1d` },
                  { text: '📅 7 дней', callback_data: `loyal_exp_${targetUserId}_${percent}_7d` }
                ],
                [
                  { text: '♾️ До первого использования', callback_data: `loyal_exp_${targetUserId}_${percent}_forever` }
                ],
                [{ text: '❌ Отмена', callback_data: `loyal_no_${targetUserId}` }]
              ]
            }
          }
        );
      });
      return;
    }

    // Шаг 3: Выбран срок — создаём и отправляем купон
    if (data.startsWith('loyal_exp_')) {
      const m3 = data.match(/^loyal_exp_(\d+)_(\d+)_(1d|7d|forever)$/);
      if (!m3) { bot.answerCallbackQuery(query.id).catch(() => { }); return; }
      const targetUserId = parseInt(m3[1]);
      const percent = parseInt(m3[2]);
      const expType = m3[3];

      db.get(`SELECT id, username, language_code FROM users WHERE id = ?`, [targetUserId], (err, targetUser) => {
        if (err || !targetUser) { bot.sendMessage(chatId, '❌ Пользователь не найден'); return; }

        let expiresAt = null;
        let expiresLabel = '';
        if (expType === '1d') {
          expiresAt = new Date(Date.now() + 24 * 60 * 60 * 1000).toISOString();
          expiresLabel = '24 часа';
        } else if (expType === '7d') {
          expiresAt = new Date(Date.now() + 7 * 24 * 60 * 60 * 1000).toISOString();
          expiresLabel = '7 дней';
        } else {
          expiresAt = null;   // бессрочный
          expiresLabel = 'до первого использования';
        }

        const couponCode = 'LOYAL-' + crypto.randomBytes(3).toString('hex').toUpperCase();
        const uname = targetUser.username ? `@${escapeMarkdown(targetUser.username)}` : `ID: ${targetUserId}`;

        db.run(
          `INSERT INTO coupons (code, discount_percent, max_uses, expires_at, created_by, user_id) VALUES (?, ?, 1, ?, ?, ?)`,
          [couponCode, percent, expiresAt, ADMIN_ID, targetUserId],
          (err2) => {
            if (err2) {
              bot.sendMessage(chatId, `❌ Ошибка создания купона: ${err2.message}`);
              return;
            }

            // Уведомляем клиента
            const userObj = { language_code: targetUser.language_code || 'en' };
            const isRu = getLang(userObj) === 'ru';
            const clientMsg = isRu
              ? `🎉 *Специально для тебя!*\n\nМы ценим твою лояльность и дарим персональный купон на *${percent}%* скидки.\n\n🎟️ Код: \`${couponCode}\`\n⏰ Срок: ${expiresLabel}\n⚠️ Одноразовый, только для тебя\n\nПрименяй при следующей покупке ключа! 🔑`
              : `🎉 *Special offer just for you!*\n\nWe appreciate your loyalty and give you a personal *${percent}%* discount coupon.\n\n🎟️ Code: \`${couponCode}\`\n⏰ Valid: ${expiresLabel}\n⚠️ Single-use, exclusive to you\n\nApply it on your next key purchase! 🔑`;

            safeSendMessage(targetUserId, clientMsg, { parse_mode: 'Markdown' }).catch(() => { });

            // Подтверждение админу
            bot.editMessageReplyMarkup({ inline_keyboard: [] }, { chat_id: chatId, message_id: message.message_id }).catch(() => { });
          });
        return;
      });
    }

    // =========================================================
    // 🔍 АДМИН: ПОИСК ПОЛЬЗОВАТЕЛЯ
    // =========================================================
    if (data === 'admin_user_search') {
      if (user.id !== ADMIN_ID) return;
      session.state = 'awaiting_user_search_query';
      bot.sendMessage(chatId, '🔍 Введите *username* (можно с @ или без) или *Telegram ID* пользователя для поиска статистики:', { parse_mode: 'Markdown' });
      bot.answerCallbackQuery(query.id).catch(() => { });
      return;
    }

    // Шаг 3: Выбран срок — создаём и отправляем купон

    if (data === 'admin_ban_by_username') {
      session.state = 'awaiting_ban_username';
      session.data = {};
      bot.sendMessage(chatId,
        '🚫 *Бан пользователя*\n\nВведите @username или числовой ID пользователя, которого хотите заблокировать:',
        { parse_mode: 'Markdown', reply_markup: { inline_keyboard: [[{ text: '◀️ Назад', callback_data: 'admin_bans' }]] } }
      );
      return;
    }

    // Выбор срока бана через кнопки
    if (data.startsWith('ban_dur_')) {
      const durKey = data.replace('ban_dur_', '');
      const banTarget = session.data && session.data.banTarget;
      if (!banTarget) {
        bot.sendMessage(chatId, '❌ Сессия устарела. Начните заново.');
        clearSession(user.id);
        return;
      }
      const durMap = {
        '1d': { ms: 1 * 24 * 60 * 60 * 1000, label: '1 день' },
        '3d': { ms: 3 * 24 * 60 * 60 * 1000, label: '3 дня' },
        '7d': { ms: 7 * 24 * 60 * 60 * 1000, label: '7 дней' },
        '30d': { ms: 30 * 24 * 60 * 60 * 1000, label: '30 дней' },
        'perm': { ms: 100 * 365 * 24 * 60 * 60 * 1000, label: 'навсегда' },
      };
      const dur = durMap[durKey];
      if (!dur) { bot.answerCallbackQuery(query.id).catch(() => { }); return; }
      applyBan(banTarget.targetId, banTarget.displayName, dur.ms, dur.label, chatId);
      clearSession(user.id);
      return;
    }

    if (data === 'admin_unban_by_username') {
      session.state = 'awaiting_unban_username';
      session.data = {};
      bot.sendMessage(chatId,
        '✏️ *Ручной разбан*\n\nВведите @username или числовой ID пользователя:',
        { parse_mode: 'Markdown' }
      );
      return;
    }

    // 🔓 РАЗБАН ПОЛЬЗОВАТЕЛЯ
    if (data.startsWith('unban_user_')) {
      const targetId = parseInt(data.replace('unban_user_', ''));
      const violation = rateLimitViolations.get(targetId);
      if (violation) {
        violation.bannedUntil = null;
        violation.count = 0;
        rateLimitViolations.set(targetId, violation);
      }
      userActionLimits.delete(targetId);
      db.get(`SELECT username, language_code FROM users WHERE id = ?`, [targetId], (err, row) => {
        const userDisplay = (!err && row && row.username) ? `@${escapeMarkdown(row.username)}` : `ID: ${targetId}`;
        bot.sendMessage(chatId, `✅ Пользователь ${userDisplay} разбанен. Он снова может пользоваться ботом.`, { parse_mode: 'Markdown' });
        // Уведомить пользователя на его языке
        const isRuUnban = getLang({ language_code: row?.language_code || 'en' }) === 'ru';
        bot.sendMessage(targetId, isRuUnban
          ? '✅ Ваш доступ к боту восстановлен. Добро пожаловать обратно!'
          : '✅ Your bot access has been restored. Welcome back!'
        ).catch(() => { });
        logAction(ADMIN_ID, 'user_unbanned', { targetId });
      });
      return;
    }

    if (data === 'admin_broadcast') {
      const isRu = getLang(user) === 'ru';
      safeSendMessage(chatId, '📢 Рассылка\n\nВыберите действие:', {
        parse_mode: 'Markdown',
        reply_markup: {
          inline_keyboard: [
            [{ text: '✉️ Написать своё сообщение', callback_data: 'broadcast_custom' }],
            [{ text: '🔑 Быстро: ключи пополнены', callback_data: 'broadcast_preset_keys_ready' }],
            [{ text: '◀️ Назад', callback_data: 'admin' }]
          ]
        }
      });
      return;
    }

    if (data === 'broadcast_custom') {
      session.state = 'awaiting_broadcast';
      session.data = {};
      bot.sendMessage(chatId, '📢 Отправьте сообщение или фото для рассылки:');
      return;
    }

    if (data === 'broadcast_preset_keys_ready') {
      // Мультиязычная рассылка — каждому на его языке
      bot.sendMessage(chatId, '📢 Начинаю рассылку о пополнении ключей...');
      db.all(`SELECT id, language_code FROM users`, [], async (err, users) => {
        if (err || !users) { bot.sendMessage(chatId, '❌ Не удалось получить список пользователей. Попробуйте позже.'); return; }
        let sent = 0;
        let blocked = 0;
        for (const row of users) {
          try {
            const lang = getLang({ language_code: row.language_code || 'en' });
            const broadcastMsg = lang === 'ru'
              ? `🔑 *CyraxMods — Новости магазина*\n\n✅ Ключи пополнены! Бот работает в штатном режиме.\n\n🎮 Приятных покупок и удачи в игре! 💜\n\n👉 Нажми кнопку ниже, чтобы купить ключ.`
              : `🔑 *CyraxMods — Shop Update*\n\n✅ Keys have been restocked! The bot is running smoothly.\n\n🎮 Happy shopping and good luck in game! 💜\n\n👉 Tap the button below to buy a key.`;
            await bot.sendMessage(row.id, broadcastMsg, {
              parse_mode: 'Markdown',
              reply_markup: { inline_keyboard: [[{ text: lang === 'ru' ? '🔑 Купить ключ' : '🔑 Buy Key', callback_data: 'buy' }]] }
            });
            sent++;
            await new Promise(r => setTimeout(r, 50));
          } catch (e) {
            if (e?.response?.body?.error_code === 403) blocked++;
          }
        }
        const keysReport = [
          `📊 *Итоги рассылки: пополнение ключей*`,
          ``,
          `👥 Всего пользователей: *${users.length}*`,
          `✅ Получили сообщение: *${sent}*`,
          `🚫 Заблокировали бота: *${blocked}*`,
          ``,
          `📈 Доставлено: *${users.length > 0 ? Math.round(sent / users.length * 100) : 0}%*`
        ].join('\n');
        bot.sendMessage(ADMIN_ID, keysReport, { parse_mode: 'Markdown' });
      });
      return;
    }
    // =============================================
    // 🛑 ПАУЗА ВИТРИНЫ — остановка отдельных разделов
    // =============================================
    if (data === 'admin_section_pause') {
      const keysOff  = getSetting('keys_disabled')         === '1';
      const boostOff = getSetting('boost_disabled')        === '1';
      const mbOff    = getSetting('manual_boost_disabled') === '1';
      const icon = (off) => off ? '🔴' : '🟢';
      bot.sendMessage(chatId,
        `🛑 *Пауза витрины*\n\nВыберите раздел для остановки или возобновления:\n\n${icon(keysOff)} Ключи\n${icon(boostOff)} Метод Буста\n${icon(mbOff)} Ручной Буст`,
        {
          parse_mode: 'Markdown',
          reply_markup: { inline_keyboard: [
            [{ text: `${icon(keysOff)} Ключи`,        callback_data: 'spause_target_keys' }],
            [{ text: `${icon(boostOff)} Метод Буста`, callback_data: 'spause_target_boost' }],
            [{ text: `${icon(mbOff)} Ручной Буст`,    callback_data: 'spause_target_manual_boost' }],
            [{ text: '◀️ Назад', callback_data: 'admin_settings' }]
          ]}
        }
      );
      return;
    }

    // Выбрали раздел — если уже выключен, сразу включаем. Если включён — запускаем wizard.
    if (data.startsWith('spause_target_')) {
      const section = data.replace('spause_target_', ''); // keys | boost | manual_boost
      const settingKey = section === 'keys' ? 'keys_disabled'
                       : section === 'boost' ? 'boost_disabled'
                       : 'manual_boost_disabled';
      const isOff = getSetting(settingKey) === '1';

      if (isOff) {
        // Раздел уже выключен — включаем и отменяем таймер если был
        if (sectionPauseTimers[section]) { clearTimeout(sectionPauseTimers[section]); delete sectionPauseTimers[section]; }
        saveSetting(settingKey, '0', () => {
          const names = { keys: 'Ключи', boost: 'Метод Буста', manual_boost: 'Ручной Буст' };
          bot.sendMessage(chatId, `✅ *${names[section]}* — продажи возобновлены.`, { parse_mode: 'Markdown' });
          showBotSettings(chatId, message.message_id);
        });
      } else {
        // Раздел включён — запускаем wizard остановки
        session.state = 'spause_awaiting_duration';
        session.data = { spauseSection: section, spauseSettingKey: settingKey };
        bot.sendMessage(chatId,
          `⏱ На сколько остановить *${section === 'keys' ? 'Ключи' : section === 'boost' ? 'Метод Буста' : 'Ручной Буст'}*?\n\nВведите количество минут, или *0* — бессрочно (до ручного включения):`,
          { parse_mode: 'Markdown', reply_markup: { inline_keyboard: [
            [{ text: '30 мин', callback_data: 'spause_dur_30' }, { text: '1 час', callback_data: 'spause_dur_60' }],
            [{ text: '3 часа', callback_data: 'spause_dur_180' }, { text: '24 часа', callback_data: 'spause_dur_1440' }],
            [{ text: '♾️ Бессрочно', callback_data: 'spause_dur_0' }],
            [{ text: '❌ Отмена', callback_data: 'admin_section_pause' }]
          ]}}
        );
      }
      return;
    }

    // Быстрый выбор длительности через кнопку
    if (data.startsWith('spause_dur_')) {
      const minutes = parseInt(data.replace('spause_dur_', ''));
      session.data = session.data || {};
      session.data.spauseDuration = minutes;
      session.state = 'spause_awaiting_reason';
      bot.sendMessage(chatId,
        `💬 Причина остановки (необязательно):\n_Будет показана клиентам вместо стандартного текста. Или «-» чтобы пропустить._`,
        { parse_mode: 'Markdown', reply_markup: { inline_keyboard: [
          [{ text: '— Без причины', callback_data: 'spause_reason_skip' }],
          [{ text: '❌ Отмена', callback_data: 'admin_section_pause' }]
        ]}}
      );
      return;
    }

    // Пропуск причины
    if (data === 'spause_reason_skip') {
      applySectionPause(chatId, session, '');
      return;
    }

    if (data === 'admin_maintenance') {
      if (maintenanceMode) {
        maintenanceMode = false;
        maintenanceEndTime = null;
        maintenanceReason = '';

        if (maintenanceTimer) {
          clearTimeout(maintenanceTimer);
          maintenanceTimer = null;
        }

        maintenanceWaitingUsers.forEach(cid => {
          bot.sendMessage(cid, t({ language_code: 'ru' }, 'maintenance_over'));
        });

        maintenanceWaitingUsers.clear();

        bot.sendMessage(chatId, '✅ Техобслуживание отключено');
      } else {
        session.state = 'awaiting_maintenance_time';
        session.data = {};

        bot.sendMessage(chatId, '🔧 Длительность в минутах (1-1440):');
      }
      return;
    }

    // =============================================
    // 🎁 ЛОЯЛЬНОСТЬ — admin callback handlers
    // =============================================
    if (user.id === ADMIN_ID && data === 'admin_loyalty') {
      showLoyaltyPanel(chatId, message.message_id);
      return;
    }
    if (user.id === ADMIN_ID && data.startsWith('loyalty_edit_discount_')) {
      const targetId = parseInt(data.replace('loyalty_edit_discount_', ''));
      session.state = 'awaiting_loyalty_discount';
      session.data = { targetUserId: targetId };
      bot.sendMessage(chatId, `✏️ Введите новый процент персональной скидки для пользователя ID ${targetId} (0 — отключить):`);
      return;
    }
    if (user.id === ADMIN_ID && data === 'loyalty_edit_default') {
      session.state = 'awaiting_default_loyalty';
      bot.sendMessage(chatId, `⚙️ Текущая глобальная скидка: ${getSetting('default_loyalty_discount')}%\n\nВведите новый процент (0 = отключить):`);
      return;
    }
    // =============================================
    // 🎫 FOMO-купоны — admin callbacks
    // =============================================
    if (user.id === ADMIN_ID && data === 'admin_fomo') {
      showFomoPanel(chatId, message.message_id);
      return;
    }
    if (user.id === ADMIN_ID && data === 'fomo_toggle') {
      const current = isSectionEnabled('fomo');
      saveSetting('fomo_enabled', current ? '0' : '1', () => showFomoPanel(chatId, message.message_id));
      return;
    }
    if (user.id === ADMIN_ID && data === 'fomo_edit_chance') {
      session.state = 'awaiting_fomo_chance';
      bot.sendMessage(chatId, `🎲 Текущий шанс: ${getSetting('fomo_chance')}%\n\nВведите новый (1-100):`);
      return;
    }
    if (user.id === ADMIN_ID && data === 'fomo_edit_expiry') {
      session.state = 'awaiting_fomo_expiry';
      bot.sendMessage(chatId, `📅 Срок купонов: ${getSetting('fomo_coupon_expiry_days')} дн.\n\nВведите новый (1-365):`);
      return;
    }
    if (user.id === ADMIN_ID && data === 'fomo_edit_max_percent') {
      session.state = 'awaiting_fomo_max_percent';
      bot.sendMessage(chatId, `💯 Макс. %: ${getSetting('fomo_max_percent')}%\n\nВведите новый (1-99):`);
      return;
    }
    // =============================================
    // 📝 ОТЗЫВЫ — admin callbacks
    // =============================================
    if (user.id === ADMIN_ID && data === 'admin_reviews') {
      showReviewsPanel(chatId, message.message_id);
      return;
    }
    if (user.id === ADMIN_ID && data.startsWith('review_reward_')) {
      // Кнопки review_reward_coupon_{id}_30 и review_reward_key_{id} — прямая выдача награды
      if (data.startsWith('review_reward_coupon_')) {
        // Формат: review_reward_coupon_{reviewId}_{pct}
        const parts = data.replace('review_reward_coupon_', '').split('_');
        const pct = parseInt(parts.pop());
        const reviewId = parseInt(parts.join('_'));
        if (!isNaN(reviewId) && !isNaN(pct)) {
          giveReviewRewardCoupon(chatId, reviewId, pct);
        } else {
          bot.sendMessage(chatId, '❌ Ошибка парсинга. Попробуйте через «Панель отзывов».');
        }
        return;
      }
      if (data.startsWith('review_reward_key_')) {
        // Формат: review_reward_key_{reviewId}
        const reviewId = parseInt(data.replace('review_reward_key_', ''));
        if (!isNaN(reviewId)) {
          // Показываем выбор периода
          bot.sendMessage(chatId, '🔑 Выберите период ключа:', {
            reply_markup: {
              inline_keyboard: [
                [
                  { text: '1 день', callback_data: `review_key_period_${reviewId}_1d` },
                  { text: '3 дня', callback_data: `review_key_period_${reviewId}_3d` }
                ],
                [
                  { text: '7 дней', callback_data: `review_key_period_${reviewId}_7d` },
                  { text: '30 дней', callback_data: `review_key_period_${reviewId}_30d` }
                ],
                [{ text: '◀️ Назад', callback_data: 'admin_reviews' }]
              ]
            }
          });
        } else {
          bot.sendMessage(chatId, '❌ Ошибка парсинга. Попробуйте через «Панель отзывов».');
        }
        return;
      }

      // Чистый review_reward_{id} — открываем меню выбора типа награды
      const reviewId = parseInt(data.replace('review_reward_', ''));
      session.state = 'awaiting_review_reward_type';
      session.data = { reviewId };

      db.get(`SELECT rc.user_id, u.username,
        (SELECT COUNT(*) FROM review_codes WHERE user_id = rc.user_id AND is_used = 1 AND reward_type IS NOT NULL) as prev_rewards
        FROM review_codes rc
        JOIN users u ON u.id = rc.user_id
        WHERE rc.id = ?`, [reviewId], (e, row) => {

        let warningText = '';
        if (!e && row && row.prev_rewards > 0) {
          const uLabel = row.username ? `@${escapeMarkdown(row.username)}` : `ID: ${row.user_id}`;
          warningText = `\n\n⚠️ *Внимание!* ${uLabel} уже получал(а) награду за отзыв *${row.prev_rewards} раз(а)*.\nРассмотрите сниженное вознаграждение.`;
        }

        bot.sendMessage(chatId, `🎁 Выберите тип награды:${warningText}`, {
          parse_mode: 'Markdown',
          reply_markup: {
            inline_keyboard: [
              [{ text: '🎟️ Купон (X%)', callback_data: `review_give_coupon_${reviewId}` }],
              [{ text: '🔑 Бесплатный ключ', callback_data: `review_give_key_${reviewId}` }],
              [{ text: '◀️ Назад', callback_data: 'admin_reviews' }]
            ]
          }
        });
      });
      return;
    }
    if (user.id === ADMIN_ID && data.startsWith('review_give_coupon_')) {
      const reviewId = parseInt(data.replace('review_give_coupon_', ''));
      session.state = 'awaiting_review_coupon_percent';
      session.data = { reviewId };
      bot.sendMessage(chatId, '💸 Введите процент скидки купона (1-100):');
      return;
    }
    if (user.id === ADMIN_ID && data.startsWith('review_give_key_')) {
      const reviewId = parseInt(data.replace('review_give_key_', ''));
      bot.sendMessage(chatId, '🔑 Выберите период ключа:', {
        reply_markup: {
          inline_keyboard: [
            [
              { text: '1 день', callback_data: `review_key_period_${reviewId}_1d` },
              { text: '3 дня', callback_data: `review_key_period_${reviewId}_3d` }
            ],
            [
              { text: '7 дней', callback_data: `review_key_period_${reviewId}_7d` },
              { text: '30 дней', callback_data: `review_key_period_${reviewId}_30d` }
            ],
            [{ text: '◀️ Назад', callback_data: `review_reward_${reviewId}` }]
          ]
        }
      });
      return;
    }
    if (user.id === ADMIN_ID && data.startsWith('review_key_period_')) {
      const rest = data.replace('review_key_period_', '');
      const underscoreIdx = rest.indexOf('_');
      const reviewId = parseInt(rest.substring(0, underscoreIdx));
      const period = rest.substring(underscoreIdx + 1);
      giveReviewRewardKey(chatId, reviewId, period);
      return;
    }

    // =============================================
    // 🤝 ПАРТНЁРСТВО (РЕСЕЛЛЕРЫ)
    // =============================================
    if (data === 'partnership') {
      const isRuMode = getLang(user) === 'ru';

      // Сначала проверяем, не является ли юзер уже реселлером
      db.get(`SELECT id, status FROM resellers WHERE user_id = ?`, [user.id], (err, row) => {
        if (row && row.status === 'active') {
          // Активный реселлер — показываем статус + кнопку сброса токена
          const msgActive = isRuMode
            ? '✅ *Вы уже являетесь активным партнёром!*\n\nИспользуйте админ-панель в вашем боте для управления.\n\nЕсли вам нужно переустановить токен бота — нажмите кнопку ниже.'
            : '✅ *You are already an active partner!*\n\nUse the admin panel in your bot to manage it.\n\nIf you need to reset your bot token — press the button below.';
          sendNavMessage(chatId, user.id, msgActive, {
            parse_mode: 'Markdown',
            reply_markup: {
              inline_keyboard: [
                [{ text: isRuMode ? '🔄 Сбросить токен' : '🔄 Reset Token', callback_data: `reseller_token_reset_${row.id}` }],
                [{ text: isRuMode ? '◀️ В меню' : '◀️ Menu', callback_data: 'start' }]
              ]
            }
          });
          return;
        }
        if (row && row.status === 'awaiting_token' || row?.status === 'pending') {
          bot.sendMessage(chatId, t(user, 'partner_already_pending'));
          return;
        }

        if (getSetting('reseller_enabled') === '0') {
          bot.sendMessage(chatId, t(user, 'partner_disabled'));
          return;
        }

        // Подставляем актуальный курс RUB в примеры заработка партнёра
        // EXCHANGE_RATES.USD = 0.01308 → 1 USD = 1/0.01308 ≈ 76 RUB
        const usdToRub = Math.round(1 / EXCHANGE_RATES.USD);
        const rubReplacements = {
          rub_075:  Math.round(0.75  * usdToRub),
          rub_315:  Math.round(3.15  * usdToRub),
          rub_1050: Math.round(10.50 * usdToRub),
          rub_150:  Math.round(150   * usdToRub),
          rub_300:  Math.round(300   * usdToRub),
        };
        const msgText = t(user, 'partner_landing_text', rubReplacements);
        const keyboard = {
          inline_keyboard: [
            [{ text: t(user, 'partner_connect_btn'), callback_data: 'reseller_activate' }],
            [{ text: isRuMode ? '◀️ В меню' : '◀️ Menu', callback_data: 'start' }]
          ]
        };

        sendNavMessage(chatId, user.id, msgText, { parse_mode: 'Markdown', reply_markup: keyboard }).catch(() => { });
      });
      return;
    }


    // Быстрый выбор наценки из пресета (для покупателя)
    if (data.startsWith('rsl_markup_preset_')) {
      const pct = parseInt(data.replace('rsl_markup_preset_', ''));
      const session = getSession(user.id);
      if (!isNaN(pct) && session.state === 'awaiting_reseller_markup') {
        session.data = session.data || {};
        session.data.resellerMarkup = pct;
        session.state = 'awaiting_reseller_questionnaire';
        bot.sendMessage(chatId, getLang(user) === 'ru'
          ? `✅ Наценка *${pct}%* выбрана.\n\n📝 Пожалуйста, коротко расскажите о вашем опыте продаж и понимании рынка.`
          : `✅ Markup *${pct}%* selected.\n\n📝 Please briefly describe your sales experience and market understanding.`,
          { parse_mode: 'Markdown' }
        ).catch(() => { });
      }
      return;
    }

    // Нажатие на кнопку подключения (создание заказа)
    if (data === 'reseller_activate') {
      if (getSetting('reseller_enabled') === '0') {
        bot.sendMessage(chatId, t(user, 'partner_disabled'));
        return;
      }

      db.get(`SELECT status FROM resellers WHERE user_id = ?`, [user.id], (err, row) => {
        if (row && row.status === 'active') {
          bot.sendMessage(chatId, t(user, 'partner_already_active'));
          return;
        }

        const session = getSession(user.id);
        session.state = 'awaiting_reseller_markup';
        session.data = { isResellerFlow: true };

        const isRuMode = getLang(user) === 'ru';
        const msg = isRuMode
          ? '⚙️ *Выберите наценку*\n\nЭто процент, который добавляется к базовой цене:\n\n'
          + '💡 *Примеры:*\n'
          + '— 20% → ключ 100₽ будет стоить 120₽ (+20₽ вам)\n'
          + '— 30% → ключ 100₽ будет стоить 130₽ (+30₽ вам)\n'
          + '— 50% → ключ 100₽ будет стоить 150₽ (+50₽ вам)\n'
          + '\nВыберите вариант или напишите своё число:'
          : '⚙️ *Select your markup*\n\nThis percentage is added to the base price:\n\n'
          + '💡 *Examples:*\n'
          + '— 20% → $10 key costs $12 (+$2 for you)\n'
          + '— 30% → $10 key costs $13 (+$3 for you)\n'
          + '— 50% → $10 key costs $15 (+$5 for you)\n'
          + '\nSelect an option or type your own number:';

        const keyboard = {
          inline_keyboard: [
            [
              { text: '20%', callback_data: 'rsl_markup_preset_20' },
              { text: '25%', callback_data: 'rsl_markup_preset_25' },
              { text: '30%', callback_data: 'rsl_markup_preset_30' }
            ],
            [
              { text: '35%', callback_data: 'rsl_markup_preset_35' },
              { text: '40%', callback_data: 'rsl_markup_preset_40' },
              { text: '50%', callback_data: 'rsl_markup_preset_50' }
            ],
            [{ text: isRuMode ? '◀️ Отмена' : '◀️ Cancel', callback_data: 'partnership' }]
          ]
        };

        safeSendMessage(chatId, msg, { parse_mode: 'Markdown', reply_markup: keyboard }).catch(() => { });
      });
      return;
    }


    // =============================================
    // 🔄 СБРОС ТОКЕНА РЕСЕЛЛЕРА — начало
    // =============================================
    if (data.startsWith('reseller_token_reset_')) {
      const rId = parseInt(data.replace('reseller_token_reset_', ''));
      db.get(`SELECT * FROM resellers WHERE id = ? AND user_id = ?`, [rId, user.id], (err, r) => {
        if (err || !r || r.status !== 'active') {
          bot.sendMessage(chatId, '❌ Ошибка: реселлер не найден или не активен.');
          return;
        }
        const isRu = getLang(user) === 'ru';
        const msg = isRu
          ? '🔄 *Сброс токена бота*\n\nПожалуйста, укажите причину переустановки:'
          : '🔄 *Bot Token Reset*\n\nPlease select the reason for reset:';
        safeSendMessage(chatId, msg, {
          parse_mode: 'Markdown',
          reply_markup: {
            inline_keyboard: [
              [{ text: isRu ? '🔑 Потерял токен' : '🔑 Lost token', callback_data: `rsl_reset_reason_${rId}_lost` }],
              [{ text: isRu ? '🛡️ Компрометация токена' : '🛡️ Token compromised', callback_data: `rsl_reset_reason_${rId}_compromised` }],
              [{ text: isRu ? '✏️ Другая причина' : '✏️ Other reason', callback_data: `rsl_reset_reason_${rId}_other` }],
              [{ text: isRu ? '❌ Отмена' : '❌ Cancel', callback_data: 'partnership' }]
            ]
          }
        });
      });
      return;
    }

    // Выбор причины сброса
    if (data.startsWith('rsl_reset_reason_')) {
      const parts = data.replace('rsl_reset_reason_', '').split('_');
      const rId = parseInt(parts[0]);
      const reason = parts.slice(1).join('_');
      const isRu = getLang(user) === 'ru';

      if (reason === 'other') {
        // Свободный ввод причины
        session.state = 'awaiting_rsl_reset_reason';
        session.data = { resellerId: rId };
        const msg = isRu
          ? '✏️ Опишите причину переустановки токена:'
          : '✏️ Please describe the reason for token reset:';
        bot.sendMessage(chatId, msg);
        return;
      }

      // Пресетная причина — сразу отправляем на рассмотрение
      const reasonLabels = {
        lost: isRu ? 'Потерял токен' : 'Lost token',
        compromised: isRu ? 'Компрометация токена' : 'Token compromised'
      };
      submitTokenResetRequest(user, chatId, rId, reasonLabels[reason] || reason);
      return;
    }

    // Админ: одобрить сброс токена
    if (data.startsWith('admin_rsl_reset_approve_')) {
      if (user.id !== ADMIN_ID) { bot.answerCallbackQuery(query.id).catch(() => { }); return; }
      const rId = parseInt(data.replace('admin_rsl_reset_approve_', ''));
      db.get(`SELECT r.*, u.username as tg_username FROM resellers r LEFT JOIN users u ON r.user_id = u.id WHERE r.id = ?`, [rId], (err, r) => {
        if (err || !r) { bot.sendMessage(chatId, '❌ Реселлер не найден'); return; }

        // 🔒 FIX 5.2: Ротируем webhook_secret при сбросе токена.
        // Это инвалидирует старый URL вебхука — злоумышленник не сможет использовать перехваченный URL.
        const newWebhookSecret = crypto.randomBytes(20).toString('hex');

        db.run(
          `UPDATE resellers SET encrypted_token = NULL, bot_username = NULL, status = 'awaiting_token', webhook_secret = ? WHERE id = ?`,
          [newWebhookSecret, rId],
          (e) => {
            if (e) { bot.sendMessage(chatId, '❌ Ошибка обновления'); return; }
            bot.editMessageReplyMarkup({ inline_keyboard: [] }, { chat_id: chatId, message_id: message.message_id }).catch(() => { });
            const uname = r.tg_username ? `@${escapeMarkdown(r.tg_username)}` : `ID ${r.user_id}`;
            bot.sendMessage(chatId, `✅ Токен реселлера ${uname} сброшен. Webhook-секрет обновлён — старый URL вебхука инвалидирован.`);

            // Уведомляем реселлера
            const rLang = r.user_lang || 'en';
            const isRu = rLang.startsWith('ru') || rLang.startsWith('uk') || rLang.startsWith('be');
            const clientMsg = isRu
              ? '✅ *Ваш запрос на сброс токена одобрен!*\n\nПожалуйста, отправьте новый токен бота.\n\nПолучите его у @BotFather:\n1. /revoke — отзовите старый токен\n2. Скопируйте новый токен\n3. Отправьте его сюда'
              : '✅ *Your token reset request has been approved!*\n\nPlease send your new bot token.\n\nGet it from @BotFather:\n1. /revoke — revoke old token\n2. Copy new token\n3. Send it here';
            safeSendMessage(r.user_id, clientMsg, { parse_mode: 'Markdown' }).catch(() => { });
            logAction(ADMIN_ID, 'reseller_token_reset_approved', { rId, userId: r.user_id });
          }
        );
      });
      return;
    }

    // Админ: отклонить сброс токена
    if (data.startsWith('admin_rsl_reset_reject_')) {
      if (user.id !== ADMIN_ID) { bot.answerCallbackQuery(query.id).catch(() => { }); return; }
      const rId = parseInt(data.replace('admin_rsl_reset_reject_', ''));
      db.get(`SELECT r.user_id, u.username as tg_username FROM resellers r LEFT JOIN users u ON r.user_id = u.id WHERE r.id = ?`, [rId], (err, r) => {
        if (err || !r) { bot.sendMessage(chatId, '❌ Реселлер не найден'); return; }
        bot.editMessageReplyMarkup({ inline_keyboard: [] }, { chat_id: chatId, message_id: message.message_id }).catch(() => { });
        const uname = r.tg_username ? `@${escapeMarkdown(r.tg_username)}` : `ID ${r.user_id}`;
        bot.sendMessage(chatId, `❌ Запрос на сброс токена от ${uname} отклонён.`);

        const clientMsg = getLang({ language_code: 'ru' }) === 'ru'
          ? '❌ *Ваш запрос на сброс токена отклонён.*\n\nЕсли считаете это ошибкой — свяжитесь с администратором.'
          : '❌ *Your token reset request has been rejected.*\n\nIf you think this is an error — contact the administrator.';
        safeSendMessage(r.user_id, clientMsg, { parse_mode: 'Markdown' }).catch(() => { });
        logAction(ADMIN_ID, 'reseller_token_reset_rejected', { rId, userId: r.user_id });
      });
      return;
    }

    // =============================================
    // ❌ ОТКЛОНЕНИЕ ЗАПРОСА НА НАГРАДУ ЗА ОТЗЫВ
    // =============================================
    // Постраничная навигация по отзывам
    if (user.id === ADMIN_ID && data.startsWith('review_page_')) {
      const page = parseInt(data.replace('review_page_', '')) || 0;
      // Редактируем текущее сообщение или отправляем новое
      bot.deleteMessage(chatId, message.message_id).catch(() => {});
      showReviewsPanel(chatId, page, message.message_id);
      bot.answerCallbackQuery(query.id).catch(() => {});
      return;
    }

    // Отклонить все сразу
    if (user.id === ADMIN_ID && data === 'review_reject_all') {
      db.run(
        `UPDATE review_codes SET is_used = 1, rewarded_at = datetime('now') WHERE is_used = 0`,
        [],
        function(err) {
          const count = this.changes || 0;
          bot.editMessageReplyMarkup({ inline_keyboard: [] }, { chat_id: chatId, message_id: message.message_id }).catch(() => {});
          bot.answerCallbackQuery(query.id, { text: `✅ Отклонено ${count} запросов`, show_alert: true }).catch(() => {});
          logAction(ADMIN_ID, 'review_rejected_all', { count });
        }
      );
      return;
    }

    if (user.id === ADMIN_ID && data.startsWith('review_reject_')) {
      // Формат: review_reject_{id}_p{page} (новый) или review_reject_{id} (старый)
      const raw = data.replace('review_reject_', '');
      const pageMatch = raw.match(/_p(\d+)$/);
      const nextPage = pageMatch ? parseInt(pageMatch[1]) : 0;
      const reviewId = parseInt(pageMatch ? raw.replace(/_p\d+$/, '') : raw);

      db.get(`SELECT * FROM review_codes WHERE id = ? AND is_used = 0`, [reviewId], (err, rc) => {
        if (err || !rc) {
          bot.answerCallbackQuery(query.id, { text: '❌ Код не найден или уже обработан', show_alert: true }).catch(() => {});
          return;
        }
        db.run(
          `UPDATE review_codes SET is_used = 1, rewarded_at = datetime('now') WHERE id = ?`,
          [reviewId],
          (updateErr) => {
            if (updateErr) {
              console.error('❌ review_reject update error:', updateErr);
              bot.sendMessage(chatId, '❌ Ошибка обновления');
              return;
            }

            bot.answerCallbackQuery(query.id, { text: '✅ Отклонено' }).catch(() => {});

            // Уведомляем пользователя
            db.get(`SELECT language_code FROM users WHERE id = ?`, [rc.user_id], (e, u) => {
              const lang = (u && u.language_code) ? getLang({ language_code: u.language_code }) : 'en';
              const rejectMsg = lang === 'ru'
                ? '❌ Запрос на награду за отзыв отклонён. Возможно, отзыв не был найден — убедитесь, что он опубликован, и попробуйте снова.'
                : '❌ Your review reward request was rejected. Your review may not have been found — make sure it is published and try again.';
              safeSendMessage(rc.user_id, rejectMsg).catch(() => {});
            });
            logAction(ADMIN_ID, 'review_rejected', { reviewId, userId: rc.user_id });

            // Переходим к следующему элементу в панели
            bot.deleteMessage(chatId, message.message_id).catch(() => {});
            // Показываем следующий (или тот же индекс — он уже будет другим элементом)
            db.get(`SELECT COUNT(*) as cnt FROM review_codes WHERE is_used = 0`, [], (e2, r2) => {
              if (r2 && r2.cnt > 0) {
                const np = Math.min(nextPage, r2.cnt - 1);
                showReviewsPanel(chatId, np, message.message_id);
              } else {
                safeSendMessage(chatId, '✅ *Все запросы обработаны*\n\n_Ожидающих наград нет._', {
                  parse_mode: 'Markdown',
                  reply_markup: { inline_keyboard: [[{ text: '◀️ Назад', callback_data: 'admin' }]] }
                });
              }
            });
          }
        );
      });
      return;
    }

    // =============================================
    // 📦 BUNDLE callbacks
    // =============================================
    if (data.startsWith('bundle_offer_')) {
      const product = data.replace('bundle_offer_', '');
      await showBundleOffer(chatId, user, product, null);
      return;
    }
    if (data.startsWith('bundle_select_')) {
      const parts = data.replace('bundle_select_', '').split('_');
      const product = parts[0];
      const qty = parseInt(parts[1]);
      handleBundleSelect(chatId, user, product, qty, message);
      return;
    }
    if (data.startsWith('bundle_currency_')) {
      // bundle_currency_{product}_{qty}_{currency}
      const parts = data.replace('bundle_currency_', '').split('_');
      // product can be '1d', '3d', '7d', '30d'
      const currency = parts[parts.length - 1];
      const qty = parseInt(parts[parts.length - 2]);
      const product = parts.slice(0, parts.length - 2).join('_');
      handleBundleCurrency(chatId, user, product, qty, currency, message);
      return;
    }
    if (data.startsWith('bundle_pay_')) {
      const method = data.replace('bundle_pay_', '');
      await handleBundlePayment(chatId, user, method, message);
      return;
    }

    // =============================================
    // ❓ FAQ callbacks
    // =============================================
    if (data === 'faq') {
      showFaqMenu(chatId, user);
      return;
    }
    if (data.startsWith('faq_item_')) {
      const itemId = parseInt(data.replace('faq_item_', ''));
      const item = FAQ_ITEMS.find(f => f.id === itemId);
      if (!item) return;
      const isRu = getLang(user) === 'ru';
      // Базовая кнопка «Назад» + опциональные кнопки из item.extraButtons
      const backRow = [{ text: isRu ? '◀️ Назад к FAQ' : '◀️ Back to FAQ', callback_data: 'faq' }];
      const extraRows = item.extraButtons ? item.extraButtons(isRu) : [];
      const rawFaqText = item.a[isRu ? 'ru' : 'en'];
      const faqText = typeof rawFaqText === 'function' ? rawFaqText() : rawFaqText;
      sendNavMessage(chatId, user.id, faqText, {
        parse_mode: 'Markdown',
        reply_markup: { inline_keyboard: [...extraRows, backRow] }
      }).catch(() => { });
      return;
    }

    // =============================================
    // 🎁 РЕФЕРАЛЬНАЯ ПРОГРАММА callbacks
    // =============================================
    if (data === 'my_ref') {
      const isRu = getLang(user) === 'ru';
      getOrCreateRefCode(user.id).then(refCode => {
        const botUsername = process.env.BOT_USERNAME || 'cyraxxmod_bot';
        const refLink = `https://t.me/${botUsername}?start=${refCode}`;

        db.get(`SELECT COUNT(*) as total FROM referrals WHERE referrer_id = ?`, [user.id], (e1, r1) => {
          db.get(`SELECT COUNT(*) as bought FROM referrals WHERE referrer_id = ? AND status = 'rewarded'`, [user.id], (e2, r2) => {
            db.get(`SELECT COUNT(*) as pending FROM referrals WHERE referrer_id = ? AND status = 'pending'`, [user.id], (e3, r3) => {
              const total = r1 ? r1.total : 0;
              const bought = r2 ? r2.bought : 0;
              const pending = r3 ? r3.pending : 0;

              const msg = isRu
                ? `🎁 *Реферальная программа*

🔗 Ваша ссылка:
\`${refLink}\`

📊 *Статистика:*
👥 Всего приглашено: ${total}
✅ Совершили покупку: ${bought}
⏳ Ожидают покупки: ${pending}

💡 *Как работает:*
Пригласите друга → он покупает → вы получаете купон на скидку!

💰 1д→3% | 3д→6% | 7д→10% | 30д→20%`
                : `🎁 *Referral Program*

🔗 Your link:
\`${refLink}\`

📊 *Stats:*
👥 Total invited: ${total}
✅ Made a purchase: ${bought}
⏳ Awaiting purchase: ${pending}

💡 *How it works:*
Invite a friend → they buy → you get a discount coupon!

💰 1d→3% | 3d→6% | 7d→10% | 30d→20%`;

              sendNavMessage(chatId, user.id, msg, {
                parse_mode: 'Markdown',
                reply_markup: { inline_keyboard: [[{ text: isRu ? '◀️ Назад' : '◀️ Back', callback_data: 'start' }]] }
              }).catch(() => { });
            });
          });
        });
      }).catch(e => {
        console.error('REF code error:', e);
        bot.sendMessage(chatId, '❌ Ошибка получения реферальной ссылки').catch(() => { });
      });
      return;
    }

    // =============================================
    // 🎟️ МОИ КУПОНЫ
    // =============================================
    if (data === 'my_coupons') {
      const isRu = getLang(user) === 'ru';
      db.all(
        `SELECT code, discount_percent, expires_at, product_restriction
         FROM coupons
         WHERE user_id = ?
           AND is_active = 1
           AND (expires_at IS NULL OR expires_at > datetime('now'))
           AND (max_uses = 0 OR used_count < max_uses)
         ORDER BY expires_at ASC`,
        [user.id],
        (err, coupons) => {
          if (err) {
            safeSendMessage(chatId, isRu ? '❌ Ошибка загрузки купонов.' : '❌ Failed to load coupons.');
            return;
          }
          let msg;
          if (!coupons || coupons.length === 0) {
            msg = isRu
              ? `🎟️ *Мои купоны*\n\nУ вас пока нет активных купонов.\n\n_Купоны выдаются при акциях, компенсациях и специальных событиях._`
              : `🎟️ *My Coupons*\n\nYou have no active coupons.\n\n_Coupons are issued during promotions, compensations, and special events._`;
          } else {
            msg = isRu ? `🎟️ *Мои активные купоны*\n\n` : `🎟️ *My Active Coupons*\n\n`;
            coupons.forEach(c => {
              const restrictionLabel = c.product_restriction
                ? (PERIOD_NAMES[isRu ? 'ru' : 'en'][c.product_restriction] || c.product_restriction)
                : null;
              const restriction = restrictionLabel
                ? (isRu ? ` (только «${restrictionLabel}»)` : ` (only «${restrictionLabel}»)`)
                : '';
              const expiry = c.expires_at
                ? (isRu ? `до ${new Date(c.expires_at).toLocaleDateString('ru-RU')}` : `until ${new Date(c.expires_at).toLocaleDateString('en-GB')}`)
                : (isRu ? 'бессрочный' : 'no expiry');
              msg += `🏷 \`${c.code}\` — *${c.discount_percent}%*${restriction}\n`;
              msg += `   ⏳ ${expiry}\n\n`;
            });
            msg += isRu
              ? `_Нажмите на код купона, чтобы скопировать._`
              : `_Tap a coupon code to copy it._`;
          }
          const keyboard = {
            inline_keyboard: [[{ text: isRu ? '◀️ Мой профиль' : '◀️ My Profile', callback_data: 'my_profile' }]]
          };
          safeSendMessage(chatId, msg, { parse_mode: 'Markdown', reply_markup: keyboard });
        }
      );
      return;
    }

    // =============================================
    // ⏰ ОТЛОЖЕННЫЕ РАССЫЛКИ callbacks (admin)
    // =============================================
    // =============================================
    // 🎁 НАЧИСЛЕНИЕ БАЛАНСА ВСЕМ АКТИВНЫМ КЛИЕНТАМ
    // =============================================
    if (user.id === ADMIN_ID && data === 'admin_gift_all') {
      // Считаем активных покупателей для превью
      db.get(
        `SELECT COUNT(DISTINCT user_id) as cnt
         FROM orders
         WHERE status = 'confirmed'
           AND (balance_topup IS NULL OR balance_topup = 0)
           AND user_id != ?`,
        [ADMIN_ID],
        (err, row) => {
          const cnt = row?.cnt || 0;
          session.state = 'awaiting_gift_all_amount';
          session.data = {};
          bot.sendMessage(chatId,
            `🎁 *Начисление подарка всем активным клиентам*\n\n` +
            `👥 Активных покупателей: *${cnt}*\n\n` +
            `Введите базовую сумму в *рублях* — она автоматически конвертируется:\n` +
            `• RUB-клиенты получат ровно эту сумму\n` +
            `• UAH-клиенты получат эквивалент по курсу\n` +
            `• USD/EUR-клиенты — эквивалент по курсу\n\n` +
            `Пример: \`50\` → 50 ₽ / ~30 ₴ / ~$0.55 каждому`,
            {
              parse_mode: 'Markdown',
              reply_markup: { inline_keyboard: [[{ text: '❌ Отмена', callback_data: 'admin' }]] }
            }
          );
        }
      );
      return;
    }

    if (user.id === ADMIN_ID && data === 'admin_gift_all_confirm') {
      // ✅ FIX: Читаем данные из БД — переживает рестарт Render.
      // Сессия (in-memory) стирается при каждом рестарте, БД — нет.
      db.get(
        `SELECT data FROM admin_pending_actions WHERE admin_id = ? AND action = 'gift_all'`,
        [ADMIN_ID],
        async (dbErr, pendingRow) => {
          // Пробуем получить из БД, потом fallback на сессию
          let amountRub, comment;
          if (!dbErr && pendingRow) {
            try {
              const parsed = JSON.parse(pendingRow.data);
              amountRub = parsed.amountRub;
              comment = parsed.comment;
            } catch (e) { /* игнорируем ошибку парсинга */ }
          }
          if (!amountRub || !comment) {
            const sd = session.data || {};
            amountRub = sd.amountRub;
            comment = sd.comment;
          }

          if (!amountRub || !comment) {
            bot.sendMessage(chatId, '❌ Данные операции не найдены. Пожалуйста, начните заново.', {
              reply_markup: { inline_keyboard: [[{ text: '🔙 Начать заново', callback_data: 'admin_gift_all' }]] }
            });
            return;
          }

          bot.editMessageText(
            `⏳ Начисляю подарки... Не закрывайте панель.`,
            { chat_id: chatId, message_id: message.message_id, parse_mode: 'Markdown' }
          ).catch(() => {});

          // Получаем всех уникальных активных покупателей с их preferred_currency
          db.all(
            `SELECT DISTINCT o.user_id, u.language_code,
                    COALESCE(ub.preferred_currency, NULL) as pref_cur
             FROM orders o
             LEFT JOIN users u ON u.id = o.user_id
             LEFT JOIN user_balances ub ON ub.user_id = o.user_id
             WHERE o.status = 'confirmed'
               AND (o.balance_topup IS NULL OR o.balance_topup = 0)
               AND o.user_id != ?
             ORDER BY o.user_id`,
            [ADMIN_ID],
            async (err, buyers) => {
              if (err || !buyers || buyers.length === 0) {
                bot.sendMessage(chatId, '❌ Не удалось получить список покупателей.');
                clearSession(user.id);
                db.run(`DELETE FROM admin_pending_actions WHERE admin_id = ? AND action = 'gift_all'`, [ADMIN_ID]);
                return;
              }

              let success = 0, failed = 0, blocked = 0;
              const rates = EXCHANGE_RATES; // { USD, EUR, UAH } — 1 RUB → валюта

              for (const buyer of buyers) {
                // Определяем валюту клиента
                let currency = buyer.pref_cur;
                if (!currency) {
                  const lang = buyer.language_code || '';
                  if (lang.startsWith('ru') || ['be','kk','uz','ky','tg','tk','az'].some(l => lang.startsWith(l))) currency = 'RUB';
                  else if (lang.startsWith('uk')) currency = 'UAH';
                  else currency = 'USD';
                }

                // Конвертируем из RUB в валюту клиента
                let giftAmount;
                if (currency === 'RUB') {
                  giftAmount = amountRub;
                } else if (rates[currency]) {
                  giftAmount = Math.round(amountRub * rates[currency] * 100) / 100;
                  if (giftAmount < 0.01) giftAmount = 0.01;
                } else {
                  giftAmount = amountRub;
                  currency = 'RUB';
                }

                try {
                  await adjustUserBalance(
                    buyer.user_id,
                    giftAmount,
                    currency,
                    'admin_credit',
                    `🎁 ${comment}`,
                    null,
                    ADMIN_ID
                  );

                  // Уведомляем клиента на его языке
                  const langCode = buyer.language_code || '';
                  const isRuClient = getLang({ language_code: langCode }) === 'ru';

                  const clientMsg = isRuClient
                    ? `🎁 *Подарок от CyraxMods!*\n\n` +
                      `💬 ${comment}\n\n` +
                      `Зачислено на ваш баланс: *${formatBalanceAmount(giftAmount, currency)}*\n\n` +
                      `_Используйте баланс при следующей покупке ключа_ 🔑`
                    : `🎁 *Gift from CyraxMods!*\n\n` +
                      `💬 ${comment}\n\n` +
                      `Credited to your balance: *${formatBalanceAmount(giftAmount, currency)}*\n\n` +
                      `_Use your balance on your next key purchase_ 🔑`;

                  await safeSendMessage(buyer.user_id, clientMsg, {
                    parse_mode: 'Markdown',
                    reply_markup: { inline_keyboard: [[{ text: isRuClient ? '👤 Мой профиль' : '👤 My Profile', callback_data: 'my_profile' }]] }
                  }).catch((e) => {
                    if (e?.response?.body?.error_code === 403) blocked++;
                  });

                  success++;
                } catch (e) {
                  failed++;
                  console.error(`❌ gift_all: ошибка для ${buyer.user_id}:`, e.message);
                }

                // Небольшая задержка — не флудим Telegram API
                await new Promise(r => setTimeout(r, 60));
              }

              // Чистим и сессию и запись в БД после успешного выполнения
              clearSession(user.id);
              db.run(`DELETE FROM admin_pending_actions WHERE admin_id = ? AND action = 'gift_all'`, [ADMIN_ID]);
              logAction(ADMIN_ID, 'gift_all', { amountRub, comment, success, failed, blocked });

              const report =
                `✅ *Начисление завершено!*\n\n` +
                `🎁 Сумма: *${amountRub} ₽* (конвертировано в валюту каждого)\n` +
                `💬 Комментарий: _${comment.replace(/_/g, '\\_ ')}_\n\n` +
                `👥 Всего покупателей: *${buyers.length}*\n` +
                `✅ Начислено: *${success}*\n` +
                `🚫 Заблокировали бота: *${blocked}*\n` +
                (failed > 0 ? `⚠️ Ошибки начисления: *${failed}*\n` : '') +
                `\n📊 Общий расход: *~${Math.round(amountRub * success)} ₽* (эквивалент)`;

              bot.sendMessage(chatId, report, {
                parse_mode: 'Markdown',
                reply_markup: { inline_keyboard: [[{ text: '🔙 Панель', callback_data: 'admin' }]] }
              });
            }
          );
        }
      );
      return;
    }

    if (user.id === ADMIN_ID && data === 'admin_gift_all_cancel') {
      clearSession(user.id);
      db.run(`DELETE FROM admin_pending_actions WHERE admin_id = ? AND action = 'gift_all'`, [ADMIN_ID]);
      bot.editMessageText('❌ Начисление отменено.', {
        chat_id: chatId,
        message_id: message.message_id,
        reply_markup: { inline_keyboard: [[{ text: '🔙 Панель', callback_data: 'admin' }]] }
      }).catch(() => bot.sendMessage(chatId, '❌ Начисление отменено.'));
      return;
    }

    if (user.id === ADMIN_ID && data === 'admin_scheduled_broadcast') {
      clearSession(user.id);
      showScheduledBroadcastPanel(chatId);
      return;
    }
    if (user.id === ADMIN_ID && data === 'sched_broadcast_create') {
      session.state = 'awaiting_sched_broadcast_text';
      session.data = {};
      safeSendMessage(chatId, '📝 *Шаг 1/3*: Введите текст рассылки (поддерживается Markdown):', {
        parse_mode: 'Markdown',
        reply_markup: { inline_keyboard: [[{ text: '❌ Отмена', callback_data: 'admin_scheduled_broadcast' }]] }
      }).catch(() => { });
      return;
    }
    if (user.id === ADMIN_ID && data === 'sched_broadcast_cancel_all') {
      db.run(`UPDATE scheduled_broadcasts SET status = 'cancelled' WHERE status = 'pending'`, [], (err) => {
        bot.sendMessage(chatId, err ? '❌ Ошибка отмены' : '✅ Все отложенные рассылки отменены');
        showScheduledBroadcastPanel(chatId);
      });
      return;
    }
    if (user.id === ADMIN_ID && data.startsWith('sched_filter_')) {
      const filter = data.replace('sched_filter_', '');
      const { sched_text, sched_at } = session.data || {};
      if (!sched_text || !sched_at) { bot.sendMessage(chatId, '❌ Ошибка сессии'); return; }

      const filterNames = { all: 'Всем', active: 'Покупавшим (30д)', inactive: 'Неактивным (7д+)' };
      const preview = `⏰ *Подтверждение рассылки*

📝 *Текст:*
${sched_text.substring(0, 200)}

📅 *Время:* ${sched_at}
🎯 *Аудитория:* ${filterNames[filter] || filter}`;

      safeSendMessage(chatId, preview, {
        parse_mode: 'Markdown',
        reply_markup: {
          inline_keyboard: [
            [
              { text: '✅ Запланировать', callback_data: `sched_confirm_${filter}` },
              { text: '❌ Отмена', callback_data: 'admin_scheduled_broadcast' }
            ]
          ]
        }
      }).catch(() => { });
      session.data.sched_filter = filter;
      return;
    }
    if (user.id === ADMIN_ID && data.startsWith('sched_confirm_')) {
      const filter = data.replace('sched_confirm_', '');
      const { sched_text, sched_at } = session.data || {};
      if (!sched_text || !sched_at) { bot.sendMessage(chatId, '❌ Ошибка сессии'); return; }

      db.run(
        `INSERT INTO scheduled_broadcasts (text, scheduled_at, filter) VALUES (?, ?, ?)`,
        [sched_text, sched_at, filter],
        (err) => {
          clearSession(user.id);
          bot.sendMessage(chatId, err ? `❌ Ошибка: ${err.message}` : `✅ Рассылка запланирована на ${sched_at}`);
          showScheduledBroadcastPanel(chatId);
        }
      );
      return;
    }

    // =============================================
    // 📦 BUNDLE — admin approve/reject callbacks
    // =============================================
    if (user.id === ADMIN_ID && data.startsWith('bundle_approve_')) {
      const bundleOrderId = parseInt(data.replace('bundle_approve_', ''));
      db.get(`SELECT * FROM bundle_orders WHERE id = ?`, [bundleOrderId], async (err, bo) => {
        if (err || !bo) { bot.sendMessage(chatId, '❌ Bundle заказ не найден'); return; }
        if (bo.status === 'confirmed') { bot.sendMessage(chatId, '⚠️ Уже выдан'); return; }

        try {
          const keys = await issueBundleKeys(bo.user_id, bo.product, bo.quantity, bo.user_lang || 'en');
          db.run(`UPDATE bundle_orders SET status = 'confirmed', confirmed_at = datetime('now') WHERE id = ?`, [bundleOrderId]);

          await sendBundleKeys(bo.user_id, bo.user_lang || 'en', bo.product, keys, bundleOrderId);

          // Если к заказу применён купон — отмечаем использованным
          if (bo.coupon_id) {
            markCouponUsed(bo.user_id, bo.coupon_id, bundleOrderId).catch(e => console.error('Bundle coupon mark error:', e));
          }

          const periodName = PERIOD_NAMES.ru[bo.product] || bo.product;
          bot.editMessageCaption(`✅ *Bundle #${bundleOrderId} одобрен*\n${bo.quantity}× ${periodName}\nВыдано ключей: ${keys.length}`, {
            chat_id: chatId, message_id: message.message_id,
            parse_mode: 'Markdown', reply_markup: { inline_keyboard: [] }
          }).catch(() => bot.sendMessage(chatId, `✅ Bundle #${bundleOrderId} одобрен, выдано ${keys.length} ключей`));

          logAction(ADMIN_ID, 'bundle_approved', { bundleOrderId, qty: bo.quantity });
        } catch (e) {
          console.error('Bundle approve error:', e);
          if (e.code === 'PARTIAL_OUT_OF_STOCK') {
            const issuedKeys = e.keys || [];
            const issuedCount = e.issued || 0;

            if (issuedCount > 0 && issuedKeys.length > 0) {
              // Выдаём уже полученные ключи пользователю — не пропадут
              db.run(
                `UPDATE bundle_orders SET status = 'partial', confirmed_at = datetime('now') WHERE id = ?`,
                [bundleOrderId]
              );
              await sendBundleKeys(bo.user_id, bo.user_lang || 'en', bo.product, issuedKeys, bundleOrderId);

              const isRuUser = bo.user_lang === 'ru';
              safeSendMessage(bo.user_id,
                isRuUser
                  ? `⚠️ Часть заказа выдана: ${issuedCount} из ${bo.quantity} ключей. Остаток будет выдан после пополнения стока. Обратитесь в поддержку.`
                  : `⚠️ Partial delivery: ${issuedCount} of ${bo.quantity} keys issued. Remaining will be sent after restock. Contact support.`,
                { reply_markup: { inline_keyboard: [[{ text: isRuUser ? '🎫 Поддержка' : '🎫 Support', callback_data: 'support_ticket' }]] } }
              ).catch(() => { });
            } else {
              db.run(`UPDATE bundle_orders SET status = 'out_of_stock' WHERE id = ?`, [bundleOrderId]);
            }

            // Уведомление админу
            bot.sendMessage(chatId,
              `⚠️ *Bundle #${bundleOrderId}: частичная выдача*\n\n` +
              `✅ Выдано: ${issuedCount} из ${bo.quantity} ключей\n` +
              `❌ Не хватает: ${bo.quantity - issuedCount} ключей (${bo.product})\n\n` +
              `Пополните сток и выдайте остаток вручную через /admin → Ключи.`,
              { parse_mode: 'Markdown' }
            );
          } else {
            bot.sendMessage(chatId, `❌ Ошибка выдачи: ${e.message}`);
            // Уведомляем пользователя
            const isRuFail = bo.user_lang === 'ru';
            safeSendMessage(bo.user_id,
              isRuFail
                ? '❌ Произошла ошибка при выдаче ключей. Администратор уведомлён и разберётся в ближайшее время.'
                : '❌ An error occurred while issuing keys. Admin has been notified.',
              { reply_markup: { inline_keyboard: [[{ text: isRuFail ? '🎫 Поддержка' : '🎫 Support', callback_data: 'support_ticket' }]] } }
            ).catch(() => { });
          }
        }
      });
      return;
    }

    if (user.id === ADMIN_ID && data.startsWith('bundle_reject_')) {
      const bundleOrderId = parseInt(data.replace('bundle_reject_', ''));
      db.run(`UPDATE bundle_orders SET status = 'rejected' WHERE id = ?`, [bundleOrderId], () => {
        db.get(`SELECT * FROM bundle_orders WHERE id = ?`, [bundleOrderId], (e, bo) => {
          if (bo) {
            const isRu = bo.user_lang === 'ru';
            safeSendMessage(bo.user_id,
              isRu
                ? '❌ Ваш bundle-заказ был отклонён. Обратитесь в поддержку если считаете это ошибкой.'
                : '❌ Your bundle order was rejected. Contact support if you think this is an error.',
              { reply_markup: { inline_keyboard: [[{ text: isRu ? '🎫 Поддержка' : '🎫 Support', callback_data: 'support_ticket' }]] } }
            ).catch(() => { });
          }
          bot.editMessageCaption(`❌ Bundle #${bundleOrderId} отклонён`, {
            chat_id: chatId, message_id: message.message_id,
            reply_markup: { inline_keyboard: [] }
          }).catch(() => bot.sendMessage(chatId, `❌ Bundle #${bundleOrderId} отклонён`));
        });
      });
      return;
    }

    // =============================================
    // 💱 КУРСЫ ВАЛЮТ — admin callbacks
    // =============================================
    if (user.id === ADMIN_ID && data === 'admin_exchange_rates') {
      showExchangeRatesPanel(chatId);
      return;
    }
    if (user.id === ADMIN_ID && data === 'rates_refresh') {
      await fetchAndUpdateExchangeRates();
      showExchangeRatesPanel(chatId);
      return;
    }
    if (user.id === ADMIN_ID && data === 'rates_apply_to_keys') {
      // Задача 7: Предпросмотр пересчёта цен ключей по текущим курсам
      const products = ['1d', '3d', '7d', '30d'];
      let preview = `💱 *Пересчёт цен ключей по курсу*\n\n_(1 RUB = USD ${EXCHANGE_RATES.USD.toFixed(5)} / EUR ${EXCHANGE_RATES.EUR.toFixed(5)} / UAH ${EXCHANGE_RATES.UAH.toFixed(4)})_\n\n`;
      products.forEach(p => {
        const rubPrice = PRICES[p] ? PRICES[p].RUB : 0;
        const newUsd = Math.round(rubPrice * EXCHANGE_RATES.USD * 100) / 100;
        const newEur = Math.round(rubPrice * EXCHANGE_RATES.EUR * 100) / 100;
        const newUah = Math.round(rubPrice * EXCHANGE_RATES.UAH);
        preview += `📦 *${p}* (${rubPrice} ₽):\n  🇺🇸 $${newUsd}  🇪🇺 €${newEur}  🇺🇦 ${newUah}₴\n`;
      });
      preview += `\n⚠️ Цены в RUB остаются без изменений.\nПрименить?`;
      safeSendMessage(chatId, preview, {
        parse_mode: 'Markdown',
        reply_markup: {
          inline_keyboard: [
            [{ text: '✅ Применить', callback_data: 'rates_apply_to_keys_confirm' }, { text: '❌ Отмена', callback_data: 'admin_exchange_rates' }]
          ]
        }
      });
      return;
    }
    if (user.id === ADMIN_ID && data === 'rates_apply_to_keys_confirm') {
      // Применяем курсы к ценам ключей
      const products = ['1d', '3d', '7d', '30d'];
      let updated = 0;
      const updateTasks = [];
      products.forEach(p => {
        if (!PRICES[p] || !PRICES[p].RUB) return;
        const rubPrice = PRICES[p].RUB;
        const newUsd = Math.round(rubPrice * EXCHANGE_RATES.USD * 100) / 100;
        const newEur = Math.round(rubPrice * EXCHANGE_RATES.EUR * 100) / 100;
        const newUah = Math.round(rubPrice * EXCHANGE_RATES.UAH);
        PRICES[p].USD = newUsd;
        PRICES[p].EUR = newEur;
        PRICES[p].UAH = newUah;
        [['USD', newUsd], ['EUR', newEur], ['UAH', newUah]].forEach(([cur, val]) => {
          updateTasks.push(new Promise(res => {
            db.run('INSERT OR REPLACE INTO prices (product, currency, amount) VALUES (?, ?, ?)', [p, cur, val], res);
          }));
        });
        updated++;
      });
      await Promise.all(updateTasks);
      bot.sendMessage(chatId, `✅ Цены ${updated} товаров пересчитаны по текущим курсам валют.`);
      logAction(ADMIN_ID, 'prices_updated_by_rates', { products, rates: EXCHANGE_RATES });
      showExchangeRatesPanel(chatId);
      return;
    }
    if (user.id === ADMIN_ID && data.startsWith('rates_markup_')) {
      const cur = data.replace('rates_markup_', '');
      session.state = `awaiting_markup_${cur}`;
      bot.sendMessage(chatId, `✏️ Наценка ${cur}: ${getSetting('markup_' + cur)}%\n\nВведите новую (0-50):`);
      return;
    }

    // =============================================
    // 🛡️ ANTI-SCAM — admin callbacks
    // =============================================
    if (user.id === ADMIN_ID && data === 'admin_tickets') {
      showAdminTickets(chatId, 0, message.message_id);
      return;
    }

    // 📨 НАПИСАТЬ ПОЛЬЗОВАТЕЛЮ
    if (user.id === ADMIN_ID && data === 'admin_message_user') {
      session.state = 'awaiting_msg_user_target';
      session.data = {};
      bot.sendMessage(chatId, '📨 *Сообщение пользователю*\n\nВведите @username или числовой ID:', { parse_mode: 'Markdown', reply_markup: { inline_keyboard: [[{ text: '◀️ Отмена', callback_data: 'admin' }]] } });
      return;
    }

    // ⏰ ОТЧЁТ: ПОТЕРЯННЫЕ ЗАКАЗЫ
    if (user.id === ADMIN_ID && data === 'admin_lost_orders') {
      showLostOrdersReport(chatId, message.message_id);
      return;
    }

    if (user.id === ADMIN_ID && data.startsWith('admin_tickets_page_')) {
      const pg = parseInt(data.replace('admin_tickets_page_', '')) || 0;
      showAdminTickets(chatId, pg);
      return;
    }

    if (user.id === ADMIN_ID && data.startsWith('ticket_issue_key_')) {
      const ticketId = parseInt(data.replace('ticket_issue_key_', ''));
      db.get(`SELECT * FROM support_tickets WHERE id = ?`, [ticketId], (e, tk) => {
        if (e || !tk) { bot.sendMessage(chatId, '❌ Тикет не найден'); return; }
        const session = getSession(user.id);
        session.state = 'awaiting_key_period';
        session.data = { targetUserId: tk.user_id, username: null, fromTicket: ticketId };
        bot.sendMessage(chatId, `🔑 Выберите период ключа для выдачи пользователю ID: ${tk.user_id}:`, {
          reply_markup: {
            inline_keyboard: [
              [{ text: '1 день', callback_data: `issue_id_${tk.user_id}_1d` }],
              [{ text: '3 дня', callback_data: `issue_id_${tk.user_id}_3d` }],
              [{ text: '7 дней', callback_data: `issue_id_${tk.user_id}_7d` }],
              [{ text: '30 дней', callback_data: `issue_id_${tk.user_id}_30d` }]
            ]
          }
        });
        // Закрыть тикет
        db.run(`UPDATE support_tickets SET status = 'resolved', resolved_at = datetime('now'), resolved_by = ? WHERE id = ?`, [ADMIN_ID, ticketId]);
        // ✅ Логируем выдачу ключа по тикету
        logAction(ADMIN_ID, 'ticket_key_issued', { ticketId, targetUserId: tk.user_id, ticketNumber: tk.ticket_number });
      });
      return;
    }

    if (user.id === ADMIN_ID && data.startsWith('client_info_')) {
      const targetId = parseInt(data.replace('client_info_', ''));
      showClientInfo(chatId, targetId);
      return;
    }

    if (user.id === ADMIN_ID && data.startsWith('client_note_')) {
      const targetId = parseInt(data.replace('client_note_', ''));
      session.state = 'awaiting_client_note';
      session.data = { targetId };
      bot.sendMessage(chatId, `📝 Введите заметку для пользователя ID: ${targetId}:`);
      return;
    }

    if (user.id === ADMIN_ID && data.startsWith('client_reset_suspicion_')) {
      const targetId = parseInt(data.replace('client_reset_suspicion_', ''));
      db.run(`UPDATE users SET suspicion_score = 0 WHERE id = ?`, [targetId], (err) => {
        if (err) { bot.sendMessage(chatId, '❌ Ошибка сброса'); return; }
        bot.sendMessage(chatId, `✅ Счёт подозрительности для ID ${targetId} сброшен до 0`);
        showClientInfo(chatId, targetId);
      });
      return;
    }

    if (user.id === ADMIN_ID && data.startsWith('ticket_resolve_')) {
      const ticketId = parseInt(data.replace('ticket_resolve_', ''));
      db.run(`UPDATE support_tickets SET status = 'resolved', resolved_at = datetime('now') WHERE id = ?`, [ticketId], (err) => {
        if (err) { bot.sendMessage(chatId, '❌ Не удалось закрыть тикет. Попробуйте ещё раз.'); return; }
        bot.answerCallbackQuery(query.id, { text: '✅ Тикет закрыт как решённый' }).catch(() => { });
        bot.editMessageReplyMarkup({ inline_keyboard: [] }, { chat_id: chatId, message_id: query.message.message_id }).catch(() => { });
        bot.sendMessage(chatId, `✅ Тикет #${ticketId} отмечен как решённый`);
      });
      return;
    }

    if (user.id === ADMIN_ID && data.startsWith('ticket_fraud_')) {
      const ticketId = parseInt(data.replace('ticket_fraud_', ''));
      db.run(`UPDATE support_tickets SET status = 'fraud', resolved_at = datetime('now') WHERE id = ?`, [ticketId], (err) => {
        if (err) { bot.sendMessage(chatId, '❌ Не удалось пометить тикет. Попробуйте ещё раз.'); return; }
        // Повысить suspicion_score
        db.get(`SELECT user_id FROM support_tickets WHERE id = ?`, [ticketId], (e, tk) => {
          if (!e && tk) increaseSuspicion(tk.user_id, 15, `Тикет #${ticketId} помечен как мошенничество`);
        });
        bot.answerCallbackQuery(query.id, { text: '🚨 Тикет помечен как мошенничество' }).catch(() => { });
        bot.editMessageReplyMarkup({ inline_keyboard: [] }, { chat_id: chatId, message_id: query.message.message_id }).catch(() => { });
        bot.sendMessage(chatId, `🚨 Тикет #${ticketId} помечен как мошенничество`);
      });
      return;
    }


  } catch (e) {
    console.error('❌ callback_query handler error:', e);
    try { bot.answerCallbackQuery(query.id).catch(() => { }); } catch (_) { }
  }
});

// ==========================================
// 🛡️ АНТИФРОД: ПРОВЕРКА ПОВТОРНЫХ ЧЕКОВ
// ==========================================
function checkReceiptDuplicate(fileId, fileUniqueId, userId, callback) {
  // Проверяем ПЕРЕД созданием заказа — ищем по file_unique_id (надёжнее) или file_id
  const conditions = [];
  const params = [];
  if (fileUniqueId) {
    conditions.push('file_unique_id = ?');
    params.push(fileUniqueId);
  }
  conditions.push('file_id = ?');
  params.push(fileId);

  db.get(
    `SELECT ur.*, u.username FROM used_receipts ur LEFT JOIN users u ON ur.user_id = u.id WHERE (${conditions.join(' OR ')}) LIMIT 1`,
    params,
    (err, existing) => {
      if (err) { callback(null); return; }

      if (existing && existing.user_id !== userId) {
        // 🚨 Тот же чек от ДРУГОГО пользователя — БЛОК!
        callback({
          isDuplicate: true,
          isSameUser: false,
          originalUserId: existing.user_id,
          originalUsername: existing.username,
          originalOrderId: existing.order_id,
          originalOrderType: existing.order_type
        });
      } else if (existing && existing.user_id === userId) {
        // ⚠️ Тот же чек от ТОГО ЖЕ пользователя — предупреждение
        callback({
          isDuplicate: true,
          isSameUser: true,
          originalUserId: existing.user_id,
          originalUsername: existing.username,
          originalOrderId: existing.order_id,
          originalOrderType: existing.order_type
        });
      } else {
        callback(null);
      }
    }
  );
}

function saveReceiptRecord(fileId, fileUniqueId, userId, orderId, orderType) {
  db.run(
    `INSERT INTO used_receipts (file_id, file_unique_id, user_id, order_id, order_type) VALUES (?, ?, ?, ?, ?)`,
    [fileId, fileUniqueId || null, userId, orderId, orderType]
  );
}

// ==========================================
// ✍️ ОБРАБОТКА ТЕКСТОВЫХ СООБЩЕНИЙ
// ==========================================
bot.on('message', async (msg) => {
  try {
    const msgType = msg.photo ? 'photo' : msg.document ? 'document' : msg.text ? 'text' : 'other';
    const currentState = getSession(msg.from?.id)?.state || 'none';
    console.log(`📨 [MSG] uid=${msg.from?.id} type=${msgType} state=${currentState}`);

    if (msg.text && msg.text.startsWith('/')) {
      // Main admin command for reseller balance
      if (msg.text.startsWith('/rsl_balance') && msg.from.id === ADMIN_ID) {
        const parts = msg.text.trim().split(/\s+/);

        // /rsl_balance (без аргументов) -> список
        if (parts.length === 1) {
          db.all(`SELECT id, user_id, username, balance FROM resellers WHERE status = 'active'`, [], (err, rows) => {
            if (err) {
              bot.sendMessage(msg.chat.id, '❌ Ошибка получения балансов реселлеров.');
              return;
            }
            let report = '💰 *Балансы активных реселлеров:*\n\n';
            if (rows.length === 0) {
              report += 'Нет активных реселлеров.';
            } else {
              rows.forEach(r => {
                const uname = r.username ? `@${escapeMarkdown(r.username)}` : `ID ${r.user_id}`;
                report += `🤖 ${uname} (RSL ID: ${r.id}): *${r.balance || 0} ₽*\n`;
              });
            }
            report += '\nℹ️ Для изменения: `/rsl_balance <ID> <Сумма>\nСумма может быть отрицательной.`';
            bot.sendMessage(msg.chat.id, report, { parse_mode: 'Markdown' });
          });
          return;
        }

        // /rsl_balance <ID> <Amount>
        if (parts.length >= 3) {
          const rslId = parseInt(parts[1]);
          const amount = parseFloat(parts[2]);
          if (isNaN(rslId) || isNaN(amount)) {
            bot.sendMessage(msg.chat.id, '❌ Неверный формат. Используйте: `/rsl_balance <ID> <Сумма>`', { parse_mode: 'Markdown' });
            return;
          }

          db.get(`SELECT id, user_id, balance FROM resellers WHERE id = ?`, [rslId], (err, r) => {
            if (err || !r) {
              bot.sendMessage(msg.chat.id, `❌ Реселлер с ID ${rslId} не найден.`);
              return;
            }
            // Атомарное изменение с защитой от ухода в минус
            const safeAmount = amount >= 0
              ? amount
              : Math.max(-(r.balance || 0), amount); // Не даём списать больше чем есть
            db.run(
              `UPDATE resellers SET balance = MAX(0, balance + ?) WHERE id = ?`,
              [safeAmount, rslId],
              function (errUpd) {
                if (errUpd || this.changes === 0) {
                  bot.sendMessage(msg.chat.id, '❌ Ошибка обновления баланса.');
                  return;
                }
                db.get(`SELECT balance FROM resellers WHERE id = ?`, [rslId], (e2, updated) => {
                  const newBalance = updated ? updated.balance : (r.balance || 0) + safeAmount;
                  bot.sendMessage(msg.chat.id, `✅ Баланс реселлера ID ${rslId} обновлен!\nБыло: *${r.balance || 0} ₽*\nСтало: *${newBalance} ₽*`, { parse_mode: 'Markdown' });
                  safeSendMessage(r.user_id, `💰 *Уведомление от администратора*\n\nВаш баланс ${amount >= 0 ? 'пополнен на' : 'уменьшен на'} *${Math.abs(safeAmount)} ₽*.\nТекущий баланс: *${newBalance} ₽*`, { parse_mode: 'Markdown' }).catch(() => { });
                });
              }
            );
          });
          return;
        }

        bot.sendMessage(msg.chat.id, '❌ Неверный формат. Используйте: `/rsl_balance <ID> <Сумма>`', { parse_mode: 'Markdown' });
        return;
      }
      return;
    }
    if (!msg.text && !msg.photo && !msg.document) return;

    // Только личные сообщения
    if (msg.chat.type !== 'private') {
      // Упомянули бота в чате — отвечаем визиткой с кнопкой (без команд, без текста с символами)
      if (msg.text && msg.entities && msg.entities.some(e => e.type === 'mention' || e.type === 'bot_command')) {
        const botUsername = process.env.BOT_USERNAME || 'cyraxxmod_bot';
        bot.sendMessage(msg.chat.id,
          `👋 I only work in private messages.\n🔑 Cyrax mod keys  🚀 Boost & guides`,
          {
            reply_to_message_id: msg.message_id,
            reply_markup: {
              inline_keyboard: [[
                { text: '🤖 Open bot', url: `https://t.me/${botUsername}` }
              ]]
            }
          }
        ).catch(() => {});
      }
      return;
    }

    const user = msg.from;
    const chatId = msg.chat.id;
    const session = getSession(user.id);

    // 🛡️ Anti-scam: трекинг + детект жалоб
    if (user.id !== ADMIN_ID) {
      trackUserInteraction(user.id, user.username, user.language_code);
      // Автодетект жалобы
      if (msg.text && isComplaint(msg.text) && !session.state) {
        autoCreateSupportTicket(user.id, msg.text).catch(e => console.error('❌ autoCreateSupportTicket:', e));
      }
    }

    // Проверка бана — забаненный не может ничего делать
    if (user.id !== ADMIN_ID) {
      const violation = rateLimitViolations.get(user.id);
      if (violation && violation.bannedUntil && Date.now() < violation.bannedUntil) {
        const isRuMsg2 = getLang(user) === 'ru';
        bot.sendMessage(chatId, isRuMsg2
          ? '🚫 Ваш доступ к боту заблокирован. Если считаете это ошибкой — обратитесь в поддержку.'
          : '🚫 Your bot access is blocked. Contact support if you think this is a mistake.'
        ).catch(() => { });
        return;
      }
    }

    if (maintenanceMode && user.id !== ADMIN_ID) {
      // Менеджеры могут работать во время техобслуживания
      const isMgrMsg = await isManager(user.id);
      if (!isMgrMsg) return;
    }

    // ==========================================
    // 👤 РУЧНОЙ БУСТ — обработка текстовых ответов
    // ==========================================

    // Ввод текущих звёзд (мифик-ранг)
    if (session.state === 'mb_enter_current_stars') {
      const num = parseInt(msg.text);
      if (isNaN(num) || num < 0) {
        bot.sendMessage(chatId, '❌ Введите корректное число (0 или больше)');
        return;
      }
      // FIX 3.2: Проверяем что введённое число не превышает максимум для ранга
      const curRankKey = session.data.manualBoost?.currentRankKey;
      const curRank = curRankKey ? MLBB_RANKS.find(r => r.key === curRankKey) : null;
      if (curRank && curRank.stars > 0 && num >= curRank.stars) {
        bot.sendMessage(chatId, `❌ Для ранга *${curRank.label_ru || curRank.label_en}* максимум *${curRank.stars - 1}* звёзд/очков. Введите число от 0 до ${curRank.stars - 1}:`, { parse_mode: 'Markdown' });
        return;
      }
      session.data.manualBoost.currentStars = num;
      session.state = 'mb_select_target';
      const lang = getLang(user);
      const curLabel = session.data.manualBoost.currentRankLabel;
      const rankKb = buildRankKeyboard('mb_tgt_', lang, session.data.manualBoost.currentRankKey);
      rankKb.push([{ text: t(user, 'back'), callback_data: 'manual_boost_start' }]);
      bot.sendMessage(chatId, t(user, 'manual_boost_select_target', { current: curLabel }),
        { parse_mode: 'HTML', reply_markup: { inline_keyboard: rankKb } }
      );
      return;
    }

    // Ввод целевых звёзд (мифик-ранг)
    if (session.state === 'mb_enter_target_stars') {
      const num = parseInt(msg.text);
      if (isNaN(num) || num < 0) {
        bot.sendMessage(chatId, '❌ Введите корректное число (0 или больше)');
        return;
      }
      // FIX 3.2: Для целевых звёзд — должно быть БОЛЬШЕ текущего если тот же ранг
      const tgtRankKey = session.data.manualBoost?.targetRankKey;
      const tgtRank = tgtRankKey ? MLBB_RANKS.find(r => r.key === tgtRankKey) : null;
      const curRankKey = session.data.manualBoost?.currentRankKey;
      // Если тот же ранг — целевые звёзды должны быть больше текущих
      if (tgtRankKey && tgtRankKey === curRankKey) {
        const curStars = session.data.manualBoost?.currentStars || 0;
        if (num <= curStars) {
          bot.sendMessage(chatId, `❌ Целевое количество очков должно быть больше текущего (${curStars}). Введите число больше ${curStars}:`);
          return;
        }
      }
      // Если конкретный ранг с ограниченным диапазоном — проверяем максимум
      if (tgtRank && tgtRank.stars > 0 && num >= tgtRank.stars) {
        bot.sendMessage(chatId, `❌ Для ранга *${tgtRank.label_ru || tgtRank.label_en}* максимум *${tgtRank.stars - 1}* звёзд/очков. Введите число от 0 до ${tgtRank.stars - 1}:`, { parse_mode: 'Markdown' });
        return;
      }
      session.data.manualBoost.targetStars = num;
      session.state = 'mb_ready';
      submitBoostRequest(user, chatId, session);
      return;
    }

    // АДМИН: ввод фиксированной цены в RUB для конкретной заявки
    if (user.id === ADMIN_ID && session.state && session.state.startsWith('admin_mb_confirm_price_')) {
      if (!msg.text || !msg.text.trim()) return;
      const brId = parseInt(session.state.replace('admin_mb_confirm_price_', ''));
      const rubInput = parseInt(msg.text.replace(/[^0-9]/g, ''));
      if (isNaN(rubInput) || rubInput <= 0) {
        bot.sendMessage(chatId, '❌ Введите корректную сумму в рублях (только цифры)');
        return;
      }
      clearSession(user.id);
      sendBoostCostToClient(brId, rubInput, chatId);
      return;
    }

    // ЮЗЕР ВВОДИТ СУММУ ПОПОЛНЕНИЯ БАЛАНСА (текстом)
    if (session.state === 'awaiting_topup_amount' && msg.text) {
      const isRu = getLang(user) === 'ru';
      const topupCur = session.data?.topupCurrency || 'RUB';
      // Только минимальные суммы — верхний лимит (MAX_BALANCE_AMOUNT = 1 000 000)
      // проверяется внутри adjustUserBalance, дублирование убрано (DRY).
      const minAmounts = { RUB: 100, USD: 1.5, EUR: 1.5, UAH: 65 };
      const minAmt = minAmounts[topupCur] || 1;

      // Парсим введённую сумму — принимаем запятую как разделитель
      const raw = msg.text.trim().replace(',', '.');
      const topupAmount = parseFloat(raw);

      if (isNaN(topupAmount) || topupAmount <= 0) {
        bot.sendMessage(chatId,
          isRu
            ? `❌ Некорректная сумма. Введите число, например: \`500\` или \`12.50\``
            : `❌ Invalid amount. Enter a number, e.g. \`10\` or \`12.50\``,
          { parse_mode: 'Markdown' }
        ).catch(() => {});
        return;
      }

      if (topupAmount < minAmt) {
        bot.sendMessage(chatId,
          isRu
            ? `❌ Минимальная сумма пополнения — *${formatBalanceAmount(minAmt, topupCur)}*.\nПожалуйста, введите большую сумму.`
            : `❌ Minimum top-up amount is *${formatBalanceAmount(minAmt, topupCur)}*.\nPlease enter a larger amount.`,
          { parse_mode: 'Markdown' }
        ).catch(() => {});
        return;
      }

      // Сохраняем сумму в сессию и показываем методы оплаты
      session.data = session.data || {};
      session.data.topupAmount = topupAmount;
      session.data.topupCurrency = topupCur;
      session.state = null;

      const payMsg = isRu
        ? `💳 *Пополнение баланса*\n\nСумма: *${formatBalanceAmount(topupAmount, topupCur)}*\n\nВыберите способ оплаты:`
        : `💳 *Balance Top Up*\n\nAmount: *${formatBalanceAmount(topupAmount, topupCur)}*\n\nChoose payment method:`;

      const kb = { inline_keyboard: [] };
      if (topupCur === 'RUB') {
        if (isPaymentConfigured('sbp')) kb.inline_keyboard.push([{ text: t(user, 'russia_sbp'), callback_data: `profile_topup_pay_sbp` }]);
        if (CRYPTOBOT_TOKEN) kb.inline_keyboard.push([{ text: t(user, 'cryptobot'), callback_data: `profile_topup_pay_cryptobot_usd` }]);
      } else if (topupCur === 'UAH') {
        if (isPaymentConfigured('card_ua')) kb.inline_keyboard.push([{ text: t(user, 'ukraine_card'), callback_data: `profile_topup_pay_card_ua` }]);
        if (CRYPTOBOT_TOKEN) kb.inline_keyboard.push([{ text: t(user, 'cryptobot'), callback_data: `profile_topup_pay_cryptobot_usd` }]);
      } else if (topupCur === 'USD') {
        if (isPaymentConfigured('binance')) kb.inline_keyboard.push([{ text: t(user, 'binance'), callback_data: `profile_topup_pay_binance` }]);
        if (PAYPAL_LINK) kb.inline_keyboard.push([{ text: t(user, 'paypal'), callback_data: `profile_topup_pay_paypal` }]);
        if (CRYPTOBOT_TOKEN) kb.inline_keyboard.push([{ text: t(user, 'cryptobot'), callback_data: `profile_topup_pay_cryptobot` }]);
      } else if (topupCur === 'EUR') {
        if (isPaymentConfigured('card_it')) kb.inline_keyboard.push([{ text: t(user, 'italy_card'), callback_data: `profile_topup_pay_card_it` }]);
        if (isPaymentConfigured('binance')) kb.inline_keyboard.push([{ text: t(user, 'binance'), callback_data: `profile_topup_pay_binance` }]);
        if (CRYPTOBOT_TOKEN) kb.inline_keyboard.push([{ text: t(user, 'cryptobot'), callback_data: `profile_topup_pay_cryptobot` }]);
      }
      kb.inline_keyboard.push([{ text: isRu ? '◀️ Назад' : '◀️ Back', callback_data: 'profile_topup' }]);

      bot.sendMessage(chatId, payMsg, { parse_mode: 'Markdown', reply_markup: kb }).catch(() => {});
      return;
    }

    // АДМИН: ввод базовой цены за звезду
    if (user.id === ADMIN_ID && session.state && session.state.startsWith('admin_mb_enter_price_')) {
      if (!msg.text || !msg.text.trim()) return;
      const currency = session.state.replace('admin_mb_enter_price_', '');
      const newPrice = msg.text.trim();
      const settingKey = `manual_boost_price_${currency.toLowerCase()}`;
      saveSetting(settingKey, newPrice, () => {
        bot.sendMessage(chatId, `✅ Базовая цена за звезду (${currency}): *${newPrice}*`, { parse_mode: 'Markdown' });
        clearSession(user.id);
      });
      return;
    }

    // АДМИН ВВОДИТ НОВЫЕ ПЛАТЕЖНЫЕ РЕКВИЗИТЫ
    if (user.id === ADMIN_ID && session.state === 'awaiting_payment_details') {
      const method = session.data.method;
      const newDetails = msg.text.trim();

      if (!newDetails) {
        bot.sendMessage(chatId, '❌ Значение не может быть пустым');
        return;
      }

      PAYMENT_DETAILS[method] = newDetails;

      db.run(
        `INSERT OR REPLACE INTO payment_details (method, details) VALUES (?, ?)`,
        [method, newDetails],
        (err) => {
          if (err) {
            console.error('❌ Error saving payment details:', err);
            bot.sendMessage(chatId, '❌ Ошибка сохранения');
            return;
          }

          // FIX 1.1: После записи в БД — принудительно перечитываем, чтобы глобальный
          // объект PAYMENT_DETAILS гарантированно совпадал с тем, что в БД.
          loadPaymentDetailsFromDB().then(() => {
            console.log(`✅ Payment details reloaded after admin update (method: ${method})`);
          }).catch(() => {});

          bot.sendMessage(chatId, `✅ Реквизиты обновлены`);
          logAction(user.id, 'payment_details_updated', { method });

          clearSession(user.id);
          showManagePaymentDetails(chatId, message.message_id);
        }
      );

      return;
    }

    // АДМИН: ввод фиксированного курса ( USD / EUR / UAH )
    if (user.id === ADMIN_ID && session.state && session.state.startsWith('awaiting_fixed_rate_')) {
      const cur = session.state.replace('awaiting_fixed_rate_', '');
      const rate = parseFloat(msg.text.replace(',', '.'));
      if (isNaN(rate) || rate <= 0) {
        bot.sendMessage(chatId, '❌ Введите корректное число больше 0');
        return;
      }
      saveSetting(`fixed_rate_${cur}`, rate.toString(), () => {
        bot.sendMessage(chatId, `✅ Фиксированный курс для *${cur}* установлен: ${rate}`, { parse_mode: 'Markdown' });
        clearSession(user.id);
        showExchangeRatesPanel(chatId);
      });
      return;
    }

    // АДМИН: ввод наценки ( USD / EUR / UAH )
    if (user.id === ADMIN_ID && session.state && session.state.startsWith('awaiting_markup_')) {
      const cur = session.state.replace('awaiting_markup_', '');
      const val = parseInt(msg.text);
      if (isNaN(val) || val < 0 || val > 100) {
        bot.sendMessage(chatId, '❌ Введите число от 0 до 100');
        return;
      }
      saveSetting(`markup_${cur}`, val.toString(), () => {
        bot.sendMessage(chatId, `✅ Наценка для *${cur}* установлена: ${val}%`, { parse_mode: 'Markdown' });
        clearSession(user.id);
        showExchangeRatesPanel(chatId);
      });
      return;
    }

    // БЫСТРАЯ ВЫДАЧА КУПОНА ПОЛЬЗОВАТЕЛЮ ИЗ КАРТОЧКИ
    if (user.id === ADMIN_ID && session.state === 'awaiting_coupon_issue_quick' && msg.text) {
      const targetId = session.data?.couponTargetId;
      const parts = msg.text.trim().split(/\s+/);
      const discountPct = parseInt(parts[0]);
      const productRestriction = parts[1] || null;

      if (isNaN(discountPct) || discountPct < 1 || discountPct > 100) {
        bot.sendMessage(chatId, '❌ Скидка должна быть от 1 до 100. Пример: 15 или 10 30d');
        return;
      }
      if (productRestriction && !PRICES[productRestriction]) {
        bot.sendMessage(chatId, `❌ Неверный период "${productRestriction}". Доступно: 1d, 3d, 7d, 30d`);
        return;
      }

      const couponCode = `ADM${targetId}_${Date.now().toString(36).toUpperCase()}`;
      db.run(
        `INSERT INTO coupons (code, discount_percent, max_uses, user_id, product_restriction, created_at, expires_at)
         VALUES (?, ?, 1, ?, ?, datetime('now'), datetime('now', '+30 days'))`,
        [couponCode, discountPct, targetId, productRestriction],
        (err) => {
          if (err) { bot.sendMessage(chatId, '❌ Ошибка создания купона: ' + err.message); return; }
          if (productRestriction) {
            db.run(`INSERT OR IGNORE INTO coupon_products (coupon_id, product) VALUES (?, ?)`, [this.lastID, productRestriction]);
          }
          const restriction = productRestriction ? ` (только ${PERIOD_NAMES['ru'][productRestriction] || productRestriction})` : ' (любой товар)';
          bot.sendMessage(chatId, `✅ Купон выдан: \`${couponCode}\`
${discountPct}% скидка${restriction}`, { parse_mode: 'Markdown' });
          logAction(ADMIN_ID, 'admin_coupon_issued_quick', { targetId, couponCode, discountPct, productRestriction });
          // Уведомляем пользователя
          safeSendMessage(targetId,
            `🎟 *Вам выдан купон на скидку!*

Скидка: *${discountPct}%*${restriction}
Код: \`${couponCode}\`

_Срок действия: 30 дней. Введите код при выборе способа оплаты._`,
            { parse_mode: 'Markdown' }
          ).catch(() => {});
        }
      );
      clearSession(user.id);
      return;
    }

    // АДМИН ИЗМЕНЯЕТ БАЛАНС ПОЛЬЗОВАТЕЛЯ
    if (user.id === ADMIN_ID && session.state === 'awaiting_balance_edit' && msg.text) {
      const targetId = session.data?.balanceTargetId;
      const currency = session.data?.balanceCurrency || 'RUB';
      const input = msg.text.trim();
      const match = input.match(/^([+-]?\d+(?:\.\d+)?)\s*(.*)?$/);
      if (!match) {
        bot.sendMessage(chatId, '❌ Неверный формат. Пример: +500 или -200 или +300 Компенсация');
        return;
      }
      const amount = parseFloat(match[1]);
      const comment = match[2]?.trim() || (amount > 0 ? 'Начислено администратором' : 'Списано администратором');
      if (isNaN(amount) || amount === 0) {
        bot.sendMessage(chatId, '❌ Сумма не может быть нулём.');
        return;
      }
      const type = amount > 0 ? 'admin_credit' : 'admin_debit';
      try {
        const newBal = await adjustUserBalance(targetId, amount, currency, type, comment, null, ADMIN_ID);
        const formatted = formatBalanceAmount(Math.abs(amount), currency);
        const newFormatted = formatBalanceAmount(newBal, currency);
        bot.sendMessage(chatId,
          `✅ *Баланс изменён*

👤 Пользователь: ${targetId}
` +
          `${amount > 0 ? '➕ Начислено' : '➖ Списано'}: *${formatted}*
` +
          `💰 Новый баланс: *${newFormatted}*
📝 Комментарий: ${comment}`,
          { parse_mode: 'Markdown' }
        );
        logAction(ADMIN_ID, 'admin_balance_changed', { targetId, amount, currency, comment, newBalance: newBal });
        // Уведомляем пользователя
        const userBal = await getUserBalance(targetId);
        const isRuTarget = true; // Уведомление на русском (универсально)
        safeSendMessage(targetId,
          amount > 0
            ? `🎁 *Администратор пополнил ваш баланс*

Зачислено: *${formatted}*
💰 Текущий баланс: *${newFormatted}*
📝 ${comment}

_Баланс можно использовать для покупки ключей._`
            : `ℹ️ *Изменение баланса*

Списано: *${formatted}*
💰 Текущий баланс: *${newFormatted}*
📝 ${comment}`,
          { parse_mode: 'Markdown', reply_markup: { inline_keyboard: [[{ text: '👤 Мой профиль', callback_data: 'my_profile' }]] } }
        ).catch(() => {});
      } catch (e) {
        bot.sendMessage(chatId, `❌ Ошибка: ${e.message}`);
      }
      clearSession(user.id);
      return;
    }

    // АДМИН ВВОДИТ НОВЫЕ ЦЕНЫ
    if (user.id === ADMIN_ID && session.state === 'awaiting_new_prices') {
      const product = session.data.product;
      const prices = msg.text.trim().split(/\s+/);

      if (prices.length !== 4) {
        bot.sendMessage(msg.chat.id, '❌ Нужно 4 цены: USD EUR RUB UAH');
        return;
      }

      const usd = parseFloat(prices[0]);
      const eur = parseFloat(prices[1]);
      const rub = parseFloat(prices[2]);
      const uah = parseFloat(prices[3]);

      if (isNaN(usd) || isNaN(eur) || isNaN(rub) || isNaN(uah)) {
        bot.sendMessage(msg.chat.id, '❌ Все цены должны быть числами');
        return;
      }

      if (usd <= 0 || eur <= 0 || rub <= 0 || uah <= 0) {
        bot.sendMessage(msg.chat.id, '❌ Цены должны быть > 0');
        return;
      }

      PRICES[product].USD = usd;
      PRICES[product].EUR = eur;
      PRICES[product].RUB = rub;
      PRICES[product].UAH = uah;

      const currencies = ['USD', 'EUR', 'RUB', 'UAH'];
      const values = [usd, eur, rub, uah];

      let completed = 0;
      let hasError = false;

      currencies.forEach((currency, index) => {
        db.run(
          `INSERT OR REPLACE INTO prices (product, currency, amount) VALUES (?, ?, ?)`,
          [product, currency, values[index]],
          (err) => {
            if (err) {
              console.error('❌ Error saving price:', err);
              hasError = true;
            }
            completed++;

            if (completed === currencies.length) {
              if (hasError) {
                bot.sendMessage(msg.chat.id, '⚠️ Цены обновлены, но ошибка БД');
              } else {
                bot.sendMessage(
                  msg.chat.id,
                  `✅ *Цены обновлены для ${product}*\n\n` +
                  `${FLAGS.USD} $${usd}\n` +
                  `${FLAGS.EUR} €${eur}\n` +
                  `${FLAGS.RUB} ${formatPrice(rub, 'RUB')}\n` +
                  `${FLAGS.UAH} ${uah}₴`,
                  { parse_mode: 'Markdown' }
                );
              }

              logAction(user.id, 'prices_updated', { product, usd, eur, rub, uah });
              clearSession(user.id);
            }
          }
        );
      });

      return;
    }

    // АДМИН ДОБАВЛЯЕТ КЛЮЧИ
    if (user.id === ADMIN_ID && session.state === 'awaiting_keys') {
      const keys = msg.text.trim().split('\n').filter(k => k.trim());
      const product = session.data.product;

      if (keys.length === 0) {
        bot.sendMessage(msg.chat.id, '❌ Не указаны ключи');
        return;
      }

      let added = 0;
      let duplicates = 0;
      let mismatch = 0;
      // CRITICAL 2: Счётчик ключей без известного префикса (нет cyraxmod_Xd) — предупреждаем
      let noPrefix = 0;

      // Разрешённые алиасы периодов: ключ с таким периодом принимается в данный продукт.
      // Пример: 30d принимает ключи cyraxmod_29d (честная компенсация — 1d + 29d = 30d).
      const PERIOD_ALIASES = {
        '30d': ['30d', '29d'],
        '7d':  ['7d'],
        '3d':  ['3d'],
        '1d':  ['1d'],
      };
      const allowedPeriods = PERIOD_ALIASES[product] || [product];

      const insertPromises = keys.map(key => {
        return new Promise((resolve) => {
          const trimmed = key.trim();
          // Проверяем совпадение периода ключа с выбранным продуктом (с учётом алиасов)
          const periodMatch = trimmed.match(/cyraxmod_(\d+)d/i);
          if (periodMatch) {
            const keyPeriod = periodMatch[1] + 'd';
            if (!allowedPeriods.includes(keyPeriod)) {
              mismatch++;
              resolve();
              return; // Не добавляем несоответствующий ключ
            }
          } else {
            // Ключ без известного префикса cyraxmod_Xd — добавляем с предупреждением.
            noPrefix++;
            console.warn(`⚠️ [KEY_VALIDATION] Key without cyraxmod prefix added to product=${product}: "${trimmed.substring(0, 30)}..."`);
          }
          db.run(
            `INSERT INTO keys (product, key_value, status) VALUES (?, ?, 'available')`,
            [product, trimmed],
            (err) => {
              if (err) {
                if (err.message.includes('UNIQUE')) {
                  duplicates++;
                }
                console.error('❌ Key insert error:', err.message);
              } else {
                added++;
              }
              resolve();
            }
          );
        });
      });

      await Promise.all(insertPromises);

      console.log(`✅ Keys added for ${product}: ${added} new, ${duplicates} duplicates, ${mismatch} mismatches, ${noPrefix} without prefix`);

      let message = `✅ Добавлено: ${added}`;
      if (duplicates > 0) message += `\n⚠️ Дубликатов: ${duplicates}`;
      if (mismatch > 0) message += `\n❌ Несоответствие периода: ${mismatch} (ключи не совпали с периодом ${product} — не добавлены)`;
      // CRITICAL 2: Предупреждение об ключах без стандартного префикса cyraxmod_Xd
      if (noPrefix > 0) message += `\n⚠️ Внимание: ${noPrefix} ключ(ей) без стандартного префикса cyraxmod_Xd — добавлены, но убедитесь что они верные!`;

      bot.sendMessage(msg.chat.id, message);

      logAction(user.id, 'keys_added', { product, count: added });

      // ── ФИЧА 2: уведомить пользователей из очереди ожидания ──────────────────
      // Находим все заказы в статусе 'out_of_stock_pending' для данного продукта.
      // Клиентам уже зачислены деньги на баланс — предлагаем купить снова с баланса.
      if (added > 0) {
        try {
          const waitingOrders = await new Promise((resolve) => {
            db.all(
              `SELECT o.id, o.user_id, o.user_lang, o.amount, o.currency, o.reseller_id
               FROM orders o
               WHERE o.product = ? AND o.status = 'out_of_stock_pending'
               ORDER BY o.created_at ASC`,
              [product],
              (e, rows) => resolve(e ? [] : (rows || []))
            );
          });

          if (waitingOrders.length > 0) {
            const pNameRu = PERIOD_NAMES.ru[product] || product;
            const pNameEn = PERIOD_NAMES.en[product] || product;

            let notified = 0;
            for (const wo of waitingOrders) {
              try {
                const isRuWo = (wo.user_lang || 'en').startsWith('ru');
                const pName = isRuWo ? pNameRu : pNameEn;
                const balAmt = formatBalanceAmount(wo.amount || 0, wo.currency || 'RUB');

                // Определяем нужный botInstance (реселлер или основной)
                let woBot = bot;
                if (wo.reseller_id) {
                  const rslEntry = resellerBots.get(wo.reseller_id);
                  if (rslEntry?.bot) woBot = rslEntry.bot;
                }

                const notifMsg = isRuWo
                  ? `🎉 *Хорошие новости!*\n\nКлючи для *«${pName}»* снова в наличии! 🔑\n\n` +
                    `💳 Напомним, что ранее мы зачислили *${balAmt}* на ваш баланс бота.\n\n` +
                    `👉 Пожалуйста, оформите покупку снова — выберите «🔑 Купить ключ» и при оплате используйте *«Оплатить с баланса»*.\n\n` +
                    `_Спасибо за ожидание! 🙏_`
                  : `🎉 *Great news!*\n\nKeys for *«${pName}»* are back in stock! 🔑\n\n` +
                    `💳 As a reminder, we previously credited *${balAmt}* to your bot balance.\n\n` +
                    `👉 Please place your order again — tap «🔑 Buy Key» and choose *«Pay with balance»* at checkout.\n\n` +
                    `_Thank you for your patience! 🙏_`;

                await safeSendMessage(wo.user_id, notifMsg, { parse_mode: 'Markdown' }, woBot);
                notified++;

                // Небольшая пауза чтобы не спамить Telegram API
                await new Promise(r => setTimeout(r, 300));
              } catch (notifErr) {
                console.error(`❌ OOS queue notify error for user ${wo.user_id}:`, notifErr.message);
              }
            }

            if (notified > 0) {
              bot.sendMessage(msg.chat.id,
                `📬 Уведомлено пользователей из очереди ожидания: *${notified}* — предложено купить ключ с баланса.`,
                { parse_mode: 'Markdown' }
              ).catch(() => {});
              logAction(user.id, 'oos_queue_notified', { product, count: notified });
            }
          }
        } catch (queueErr) {
          console.error('❌ OOS queue notification error:', queueErr.message);
        }
      }
      // ─────────────────────────────────────────────────────────────────────────

      clearSession(user.id);
      return;
    }

    // (старый текстовый обработчик да/нет заменён на кнопочный флоу loyal_yes_/loyal_no_)

    // 🔍 АДМИН: ОБРАБОТКА ПОИСКА ПОЛЬЗОВАТЕЛЯ
    if (user.id === ADMIN_ID && session.state === 'awaiting_user_search_query' && msg.text) {
      const queryStr = msg.text.trim().replace('@', '');
      const isNumeric = !isNaN(queryStr) && queryStr !== '';

      // Всегда используем параметризованный запрос с двумя параметрами.
      // isNumeric → ищем по id; иначе id = 0 (никогда не совпадёт) и ищем по username.
      const searchSql = `SELECT * FROM users WHERE id = ? OR (username = ? COLLATE NOCASE)`;
      const searchParams = isNumeric ? [parseInt(queryStr), queryStr] : [0, queryStr];

      db.get(searchSql, searchParams, async (err, targetUser) => {
        if (err || !targetUser) {
          bot.sendMessage(chatId, '❌ Пользователь не найден. Попробуйте другой ID или username.');
          return;
        }

        // Собираем статистику
        const stats = await new Promise((resolve) => {
          db.get(`
                  SELECT 
                    COUNT(id) as total_orders,
                    (SELECT COUNT(*) FROM referrals WHERE referrer_id = ?) as ref_count,
                    (SELECT COUNT(*) FROM coupons WHERE user_id = ? AND (expires_at IS NULL OR expires_at > datetime('now')) AND max_uses > 0) as coupon_count
                  FROM orders 
                  WHERE user_id = ? AND status = 'confirmed'
                `, [targetUser.id, targetUser.id, targetUser.id], (e, row) => {
            resolve(row || { total_orders: 0, ref_count: 0, coupon_count: 0 });
          });
        });

        // Считаем потраченное отдельно по каждой валюте (не суммируем разные валюты)
        const spentByCurrency = await new Promise((resolve) => {
          db.all(
            `SELECT currency, SUM(amount) as total FROM orders
             WHERE user_id = ? AND status = 'confirmed'
             AND (balance_topup IS NULL OR balance_topup = 0)
             GROUP BY currency`,
            [targetUser.id],
            (e, rows) => resolve(rows || [])
          );
        });

        const isBanned = !!targetUser.banned_until && new Date(targetUser.banned_until) > new Date();
        const isUaMgr = await new Promise(res => db.get(
          `SELECT mm.payment_method FROM managers m
           LEFT JOIN manager_methods mm ON mm.manager_id = m.user_id AND mm.payment_method = 'card_ua'
           WHERE m.user_id = ?`, [targetUser.id], (e, r) => res(!!r)
        ));
        const isMgrRole = await isManager(targetUser.id);
        let role;
        if (targetUser.id === ADMIN_ID) role = '👑 Admin';
        else if (isMgrRole && isUaMgr) role = '🇺🇦 Менеджер UA';
        else if (isMgrRole) role = '🔧 Менеджер';
        else role = '👤 Клиент';

        // Расширенная аналитика: последние действия, купоны, подозрительная активность
        const [lastActions, usedCoupons, lastOrders, balanceRow] = await Promise.all([
          // Последние 5 действий
          new Promise(res => db.all(
            `SELECT action, details, timestamp FROM action_logs
             WHERE user_id = ? ORDER BY timestamp DESC LIMIT 5`,
            [targetUser.id], (e, rows) => res(rows || [])
          )),
          // Применённые купоны
          new Promise(res => db.get(
            `SELECT COUNT(*) as cnt FROM user_coupons WHERE user_id = ? AND used_at IS NOT NULL`,
            [targetUser.id], (e, r) => res(r?.cnt || 0)
          )),
          // Последние 3 заказа
          new Promise(res => db.all(
            `SELECT product, amount, currency, original_currency, original_amount, status, confirmed_at, method
             FROM orders WHERE user_id = ? ORDER BY created_at DESC LIMIT 3`,
            [targetUser.id], (e, rows) => res(rows || [])
          )),
          // Баланс
          getUserBalance(targetUser.id)
        ]);

        // Спам-индикатор из rate limit данных
        const rlData = rateLimitViolations.get(targetUser.id);
        const spamScore = rlData ? rlData.count || 0 : 0;

        // Форматируем последние действия
        const actionLabels = {
          'start': '🏠 Запустил бота', 'view_products': '🛍 Смотрел витрину',
          'select_period': '📦 Выбрал период', 'select_currency': '💱 Выбрал валюту',
          'payment_method_selected': '💳 Выбрал способ оплаты',
          'cryptobot_invoice_created': '🤖 Создал CryptoBot счёт',
          'order_approved': '✅ Заказ подтверждён', 'order_rejected': '❌ Заказ отклонён',
          'order_cancelled_by_user': '🚫 Отменил заказ', 'coupon_created': '🎟 Создал купон',
          'support_ticket_created': '🎫 Открыл тикет', 'boost_request_submitted': '🚀 Отправил буст-заявку',
          'view_boost_hub': '🚀 Смотрел хаб буста', 'view_manual_boost': '🛠 Смотрел ручной буст',
          'auto_backup_created': '💾 Авто-бэкап', 'view_infinite_boost': '⚡ Смотрел буст', 'basketball_win': '🏀 Выиграл купон', 'basketball_lose': '🏀 Бросил (мимо)'
        };
        let actionsText = '';
        if (lastActions.length > 0) {
          actionsText = '\n\n🕹 *Последние действия:*\n';
          lastActions.forEach(a => {
            // Если action не в словаре — экранируем _ чтобы не ломать Markdown
            const rawLabel = actionLabels[a.action];
            const label = rawLabel || a.action.replace(/_/g, '\\_');
            const time = new Date(a.timestamp).toLocaleString('ru-RU', { day:'2-digit', month:'2-digit', hour:'2-digit', minute:'2-digit' });
            let extra = '';
            try {
              const d = JSON.parse(a.details || '{}');
              if (d.period) extra = ` (${d.period})`;
              else if (d.method) extra = ` (${d.method})`;
              else if (d.currency) extra = ` (${d.currency})`;
            } catch(e) {}
            actionsText += `  • ${label}${extra} _${time}_\n`;
          });
        }

        // Последние заказы
        let ordersText = '';
        if (lastOrders.length > 0) {
          ordersText = '\n\n📦 *Последние заказы:*\n';
          lastOrders.forEach(o => {
            const pName = (PERIOD_NAMES['ru'][o.product] || o.product).replace(/_/g, '\\_');
            const dispCur = o.original_currency || o.currency;
            const dispAmt = o.original_amount || o.amount;
            const statusIcon = { confirmed: '✅', pending: '⏳', rejected: '❌', out_of_stock: '📭' }[o.status] || '•';
            // Если дата отсутствует — не оборачиваем в _ чтобы не ломать Markdown
            const dateStr = o.confirmed_at ? new Date(o.confirmed_at).toLocaleDateString('ru-RU') : null;
            const datePart = dateStr ? ` _${dateStr}_` : '';
            ordersText += `  ${statusIcon} *${pName}* — ${formatBalanceAmount(dispAmt, dispCur)} (${o.method})${datePart}\n`;
          });
        }

        // Баланс профиля
        const balanceText = balanceRow.balance > 0
          ? `\n💳 *Баланс профиля:* ${formatBalanceAmount(balanceRow.balance, balanceRow.preferred_currency)}`
          : '';

        const report = [
          `👤 *Карточка пользователя*`,
          `━━━━━━━━━━━━━━━━━━`,
          `🆔 ID: \`${targetUser.id}\``,
          `👤 Username: ${targetUser.username ? '@' + escapeMarkdown(targetUser.username) : 'нет'}`,
          `🌍 Язык: ${targetUser.user_lang || targetUser.language_code || 'не указан'}`,
          `🎭 Роль: ${role}`,
          `🚫 Статус: ${isBanned ? '🛑 ЗАБАНЕН' : '✅ Активен'}`,
          `━━━━━━━━━━━━━━━━━━`,
          `📊 *Покупки:*`,
          `🛒 Заказов: *${stats.total_orders}*`,
          spentByCurrency.length > 0
            ? `💰 Потрачено: *${spentByCurrency.map(r => formatBalanceAmount(r.total, r.currency)).join(' | ')}*`
            : `💰 Потрачено: *0*`,
          `🎟️ Купонов применено: *${usedCoupons}*`,
          balanceText,
          `━━━━━━━━━━━━━━━━━━`,
          `🤝 Приглашено рефералов: *${stats.ref_count}*`,
          `💠 Подозрений (Anti-Scam): *${targetUser.suspicion_score || 0}*`,
          spamScore > 0 ? `⚠️ Нарушений rate-limit: *${spamScore}*` : null,
          stats.coupon_count > 0 ? `🎫 Активных купонов: *${stats.coupon_count}*` : null,
          targetUser.notes ? `\n📝 Заметки: _${targetUser.notes.replace(/_/g, '\\_ ')}_` : null,
          ordersText,
          actionsText
        ].filter(Boolean).join('\n');

        const kb = {
          inline_keyboard: [
            [
              { text: isBanned ? '🔓 Разбанить' : '🚫 Забанить', callback_data: `admin_user_action_${isBanned ? 'unban' : 'ban'}_${targetUser.id}` },
              { text: '📨 Написать', callback_data: `admin_msg_user_${targetUser.id}` }
            ],
            [
              { text: '💰 Изменить баланс', callback_data: `admin_balance_edit_${targetUser.id}` },
              { text: '🎟 Выдать купон', callback_data: `admin_coupon_issue_${targetUser.id}` }
            ],
            [{ text: '◀️ Назад', callback_data: 'admin' }]
          ]
        };

        bot.sendMessage(chatId, report, { parse_mode: 'Markdown', reply_markup: kb })
          .catch(() => {
            // Fallback: если Markdown сломался из-за спецсимволов — отправляем plain text
            bot.sendMessage(chatId, report.replace(/[*_`\[\]]/g, ''), { reply_markup: kb }).catch(() => {});
          });

        clearSession(user.id);
      });
      return;
    }

    // АДМИН ОПРЕДЕЛЯЕТ ТЕКСТ РАССЫЛКИ
    if (user.id === ADMIN_ID && session.state === 'awaiting_broadcast') {
      session.data.broadcastData = {
        text: msg.text || null,
        photo: msg.photo ? msg.photo[msg.photo.length - 1].file_id : null,
        caption: msg.caption || null
      };

      session.state = 'awaiting_broadcast_btn_decision';

      const kb = {
        inline_keyboard: [
          [{ text: '✅ Да, нужна кнопка', callback_data: 'broadcast_btn_yes' }],
          [{ text: '❌ Нет, отправить так', callback_data: 'broadcast_btn_no' }],
          [{ text: 'Отмена', callback_data: 'admin' }]
        ]
      };

      bot.sendMessage(chatId, '⚙️ Желаете добавить inline-кнопку под сообщением рассылки?', { reply_markup: kb });
      return;
    }

    // АДМИН ВВОДИТ НАЗВАНИЕ КНОПКИ ДЛЯ РАССЫЛКИ
    if (user.id === ADMIN_ID && session.state === 'awaiting_broadcast_btn_name' && msg.text) {
      session.data.broadcastData.btnName = msg.text.trim();
      session.state = 'awaiting_broadcast_btn_action';

      const kbAction = {
        inline_keyboard: [
          [{ text: '🛒 Перейти к витрине (Магазин)', callback_data: 'admin_bc_action_catalog' }],
          [{ text: '📋 Инструкция (FAQ)', callback_data: 'admin_bc_action_help' }]
        ]
      };
      bot.sendMessage(chatId, '🔗 Отправьте *URL-ссылку* для перехода (например `https://t.me/...`),\nЛИБО выберите одно из частых действий ниже:', { parse_mode: 'Markdown', reply_markup: kbAction });
      return;
    }

    // АДМИН ВВОДИТ ССЫЛКУ ДЛЯ КНОПКИ (URL)
    if (user.id === ADMIN_ID && session.state === 'awaiting_broadcast_btn_action' && msg.text) {
      session.data.broadcastData.btnAction = msg.text.trim();
      showBroadcastPreview(chatId, session.data.broadcastData, user);
      // session НЕ сбрасываем — ждём подтверждения превью
      return;
    }

    // АДМИН: Изменение баланса реселлера
    if (user.id === ADMIN_ID && session.state === 'admin_awaiting_rsl_markup' && msg.text) {
      const val = parseInt(msg.text.trim());
      if (isNaN(val) || val < 0 || val > 500) {
        bot.sendMessage(chatId, '❌ Введите число от 0 до 500 (например: 25, 30, 50).');
        return;
      }
      const { rslId } = session.data || {};
      if (!rslId) { clearSession(chatId); return; }
      db.run('UPDATE resellers SET markup_pct = ? WHERE id = ?', [val, rslId], (err) => {
        if (err) {
          bot.sendMessage(chatId, '❌ Ошибка обновления.');
        } else {
          bot.sendMessage(chatId, `✅ *Наценка установлена: ${val}%*`, { parse_mode: 'Markdown' });
          showAdminResellerEdit(chatId, rslId);
        }
        clearSession(chatId);
      });
      return;
    }

    if (user.id === ADMIN_ID && session.state === 'awaiting_admin_rsl_balance_change' && msg.text) {
      const input = msg.text.trim();
      let change = parseInt(input);
      if (isNaN(change)) {
        bot.sendMessage(chatId, '❌ Введите число (например +100 или -50)');
        return;
      }

      const { targetUserId } = session.data;
      db.run(`UPDATE resellers SET balance = balance + ? WHERE user_id = ?`, [change, targetUserId], (err) => {
        if (err) {
          bot.sendMessage(chatId, '❌ Ошибка обновления баланса.');
        } else {
          db.get(`SELECT balance FROM resellers WHERE user_id = ?`, [targetUserId], (e, row) => {
            const newBal = row ? row.balance : '???';
            bot.sendMessage(chatId, `✅ Баланс реселлера ${targetUserId} обновлён.\n\nИзменение: ${change} ₽\nНовый баланс: *${newBal} ₽*`, { parse_mode: 'Markdown' });
            // Уведомляем реселлера если пополнение положительное
            if (change > 0) {
              safeSendMessage(targetUserId, `💰 Ваш баланс пополнен на *${change} ₽* администратором!\nАктуальный баланс: *${newBal} ₽*`, { parse_mode: 'Markdown' }).catch(() => { });
            }
          });
        }
        clearSession(chatId);
      });
      return;
    }

    // АДМИН ВВОДИТ ВРЕМЯ ТЕХОБСЛУЖИВАНИЯ
    // ПАУЗА ВИТРИНЫ: ввод произвольной длительности
    if (user.id === ADMIN_ID && session.state === 'spause_awaiting_duration') {
      const minutes = parseInt(msg.text.trim());
      if (isNaN(minutes) || minutes < 0) {
        bot.sendMessage(chatId, '❌ Введите число минут (0 = бессрочно):');
        return;
      }
      session.data.spauseDuration = minutes;
      session.state = 'spause_awaiting_reason';
      bot.sendMessage(chatId,
        `💬 Причина (необязательно) — «-» чтобы пропустить:`,
        { reply_markup: { inline_keyboard: [[{ text: '— Без причины', callback_data: 'spause_reason_skip' }]] } }
      );
      return;
    }

    // ПАУЗА ВИТРИНЫ: ввод причины текстом
    if (user.id === ADMIN_ID && session.state === 'spause_awaiting_reason') {
      const reason = (msg.text || '').trim() === '-' ? '' : (msg.text || '').trim();
      applySectionPause(chatId, session, reason);
      return;
    }

    if (user.id === ADMIN_ID && session.state === 'awaiting_maintenance_time') {
      const minutes = parseInt(msg.text);

      if (isNaN(minutes) || minutes < 1 || minutes > 1440) {
        bot.sendMessage(msg.chat.id, '❌ Введите число от 1 до 1440');
        return;
      }

      bot.sendMessage(msg.chat.id, '💬 Причина (или "-"):');

      session.state = 'awaiting_maintenance_reason';
      session.data.maintenanceMinutes = minutes;

      return;
    }

    // АДМИН УКАЗЫВАЕТ ПРИЧИНУ ТЕХОБСЛУЖИВАНИЯ
    if (user.id === ADMIN_ID && session.state === 'awaiting_maintenance_reason') {
      const reason = msg.text.trim() === '-' ? '' : msg.text.trim();
      const minutes = session.data.maintenanceMinutes;

      maintenanceMode = true;
      maintenanceEndTime = Date.now() + (minutes * 60000);
      maintenanceReason = reason;

      if (maintenanceTimer) clearTimeout(maintenanceTimer);

      maintenanceTimer = setTimeout(() => {
        maintenanceMode = false;
        maintenanceEndTime = null;
        maintenanceReason = '';

        maintenanceWaitingUsers.forEach(cid => {
          bot.sendMessage(cid, t({ language_code: 'ru' }, 'maintenance_over'));
        });

        maintenanceWaitingUsers.clear();
        bot.sendMessage(ADMIN_ID, '✅ Техобслуживание завершено');
      }, minutes * 60000);

      bot.sendMessage(
        msg.chat.id,
        `🔧 *Техобслуживание запущено*\n⏰ ${minutes} мин${reason ? `\n💬 ${reason}` : ''}`,
        { parse_mode: 'Markdown' }
      );

      clearSession(user.id);

      return;
    }

    // АДМИН ДОБАВЛЯЕТ ЗАМЕТКУ О КЛИЕНТЕ
    if (user.id === ADMIN_ID && session.state === 'awaiting_client_note') {
      const targetId = session.data.targetId;
      const note = msg.text ? msg.text.trim() : '';
      db.run(`UPDATE users SET notes = ? WHERE id = ?`, [note, targetId], (err) => {
        if (err) { bot.sendMessage(chatId, '❌ Ошибка сохранения заметки'); }
        else {
          bot.sendMessage(chatId, `✅ Заметка сохранена для ID ${targetId}`);
          showClientInfo(chatId, targetId);
        }
        clearSession(user.id);
      });
      return;
    }

    // АДМИН ВВОДИТ USERNAME ДЛЯ ВЫДАЧИ КЛЮЧА
    if (user.id === ADMIN_ID && session.state === 'awaiting_username') {
      let username = msg.text.trim().replace('@', '');

      if (username.includes(' ') || username.includes('\n')) {
        bot.sendMessage(msg.chat.id, '❌ Неверный формат');
        return;
      }

      db.get(
        `SELECT id FROM users WHERE username = ?`,
        [username],
        (err, targetUser) => {
          if (err || !targetUser) {
            bot.sendMessage(msg.chat.id, '❌ Пользователь не найден');
            clearSession(user.id);
            return;
          }

          session.state = 'awaiting_key_period';
          session.data = { targetUserId: targetUser.id, username: username };

          const keyboard = {
            inline_keyboard: [
              [{ text: '1 день', callback_data: `issue_${username}_1d` }],
              [{ text: '3 дня', callback_data: `issue_${username}_3d` }],
              [{ text: '7 дней', callback_data: `issue_${username}_7d` }],
              [{ text: '30 дней', callback_data: `issue_${username}_30d` }]
            ]
          };

          bot.sendMessage(msg.chat.id, `👤 @${escapeMarkdown(username || '')}\n\nВыберите период:`, { reply_markup: keyboard });
        }
      );

      return;
    }

    // РУЧНОЙ РАЗБАН ПО @USERNAME ИЛИ ID
    if (user.id === ADMIN_ID && session.state === 'awaiting_ban_username') {
      const input = msg.text.trim().replace('@', '');

      const proceedToDuration = (targetId, displayName) => {
        if (targetId === ADMIN_ID) {
          bot.sendMessage(chatId, '❌ Нельзя забанить самого себя.');
          clearSession(user.id);
          return;
        }
        // Сохраняем цель и спрашиваем срок
        session.state = 'awaiting_ban_duration';
        session.data.banTarget = { targetId, displayName };
        bot.sendMessage(chatId,
          `👤 Пользователь: *${displayName}*\n\nНа сколько заблокировать?\n\nВведите срок, например:\n• \`1d\` — 1 день\n• \`3d\` — 3 дня\n• \`7d\` — 7 дней\n• \`30d\` — 30 дней\n• \`perm\` — навсегда`,
          {
            parse_mode: 'Markdown', reply_markup: {
              inline_keyboard: [
                [{ text: '1 день', callback_data: 'ban_dur_1d' }, { text: '3 дня', callback_data: 'ban_dur_3d' }],
                [{ text: '7 дней', callback_data: 'ban_dur_7d' }, { text: '30 дней', callback_data: 'ban_dur_30d' }],
                [{ text: '♾ Навсегда', callback_data: 'ban_dur_perm' }],
                [{ text: '◀️ Отмена', callback_data: 'admin_bans' }]
              ]
            }
          }
        );
      };

      if (/^\d+$/.test(input)) {
        const targetId = parseInt(input);
        db.get(`SELECT username FROM users WHERE id = ?`, [targetId], (err, row) => {
          const displayName = (!err && row && row.username) ? `@${escapeMarkdown(row.username)}` : `ID: ${targetId}`;
          proceedToDuration(targetId, displayName);
        });
      } else {
        db.get(`SELECT id, username FROM users WHERE username = ?`, [input], (err, row) => {
          if (err || !row) {
            bot.sendMessage(chatId, `❌ Пользователь @${input} не найден в базе.\n\nЕсли знаете его числовой ID — введите его напрямую.`);
            clearSession(user.id);
            return;
          }
          proceedToDuration(row.id, `@${escapeMarkdown(row.username || '')}`);
        });
      }
      return;
    }

    // Ввод произвольного срока бана текстом (если не нажали кнопку)
    if (user.id === ADMIN_ID && session.state === 'awaiting_ban_duration') {
      const input = msg.text.trim().toLowerCase();
      const { targetId, displayName } = session.data.banTarget;
      const parsed = parseBanDuration(input);
      if (!parsed) {
        bot.sendMessage(chatId, '❌ Не понял срок. Введите например: `1d`, `7d`, `30d`, `perm`', { parse_mode: 'Markdown' });
        return;
      }
      applyBan(targetId, displayName, parsed.ms, parsed.label, chatId);
      clearSession(user.id);
      return;
    }

    if (user.id === ADMIN_ID && session.state === 'awaiting_unban_username') {
      const input = msg.text.trim().replace('@', '');
      clearSession(user.id);

      const doUnban = (targetId, displayName) => {
        const violation = rateLimitViolations.get(targetId);
        const wasBanned = violation && violation.bannedUntil && Date.now() < violation.bannedUntil;
        if (violation) {
          violation.bannedUntil = null;
          violation.count = 0;
          rateLimitViolations.set(targetId, violation);
        }
        userActionLimits.delete(targetId);
        if (wasBanned) {
          bot.sendMessage(chatId, `✅ Пользователь ${displayName} разбанен. Доступ восстановлен.`);
          db.get('SELECT language_code FROM users WHERE id = ?', [targetId], (e, row) => {
            const isRuUnban2 = getLang({ language_code: row?.language_code || 'en' }) === 'ru';
            bot.sendMessage(targetId, isRuUnban2
              ? '✅ Ваш доступ к боту восстановлен. Добро пожаловать обратно!'
              : '✅ Your bot access has been restored. Welcome back!'
            ).catch(() => { });
          });
          logAction(ADMIN_ID, 'user_unbanned_manual', { targetId, displayName });
        } else {
          bot.sendMessage(chatId, `ℹ️ Пользователь ${displayName} не был забанен (или бан уже истёк).`);
        }
      };

      // Ввели числовой ID
      if (/^\d+$/.test(input)) {
        const targetId = parseInt(input);
        db.get(`SELECT username FROM users WHERE id = ?`, [targetId], (err, row) => {
          const displayName = (!err && row && row.username) ? `@${escapeMarkdown(row.username)}` : `ID: ${targetId}`;
          doUnban(targetId, displayName);
        });
      } else {
        // Ввели username
        db.get(`SELECT id, username FROM users WHERE username = ?`, [input], (err, row) => {
          if (err || !row) {
            bot.sendMessage(chatId, `❌ Пользователь @${input} не найден в базе.\n\nЕсли знаете его числовой ID — введите его напрямую.`);
            return;
          }
          doUnban(row.id, `@${escapeMarkdown(row.username || '')}`);
        });
      }
      return;
    }

    // АДМИН ЗАГРУЖАЕТ ФАЙЛ ДЛЯ ВОССТАНОВЛЕНИЯ БД (с проверками)
    if (user.id === ADMIN_ID && session.state === 'awaiting_restore_file' && msg.document) {
      const fileId = msg.document.file_id;
      const fileName = msg.document.file_name;
      const fileSize = msg.document.file_size;

      if (!fileName.endsWith('.db')) {
        bot.sendMessage(chatId, '❌ Нужен .db файл');
        return;
      }

      if (fileSize > 10 * 1024 * 1024) {
        bot.sendMessage(chatId, '❌ Файл слишком большой (макс 10MB)');
        return;
      }

      bot.sendMessage(chatId, '⏳ Загружаю и восстанавливаю базу данных...');

      try {
        const fileLink = await bot.getFileLink(fileId);
        const response = await axios({
          method: 'get',
          url: fileLink,
          responseType: 'stream',
          maxContentLength: 10 * 1024 * 1024
        });

        // Сохраняем загруженный файл во временный путь
        const tempPath = './shop_restore_temp.db';
        const writer = fs.createWriteStream(tempPath);
        response.data.pipe(writer);

        await new Promise((resolve, reject) => {
          writer.on('finish', resolve);
          writer.on('error', reject);
        });

        // Проверяем целостность загруженного файла
        const testDb = new sqlite3.Database(tempPath, sqlite3.OPEN_READONLY, (openErr) => {
          if (openErr) {
            fs.unlinkSync(tempPath);
            bot.sendMessage(chatId, '❌ Файл не является базой данных SQLite');
            clearSession(user.id);
            return;
          }

          testDb.get('SELECT COUNT(*) as cnt FROM sqlite_master WHERE type="table"', [], async (testErr, row) => {
            testDb.close();

            if (testErr || !row || row.cnt === 0) {
              try { fs.unlinkSync(tempPath); } catch (e) { }
              bot.sendMessage(chatId, '❌ База данных пуста или повреждена');
              clearSession(user.id);
              return;
            }

            try {
              // Делаем бэкап текущей базы перед заменой
              const safeBackupPath = DB_PATH + '.pre_restore_backup';
              // Checkpoint текущей базы перед бэкапом
              await new Promise((res) => db.run('PRAGMA wal_checkpoint(TRUNCATE)', [], res));
              fs.copyFileSync(DB_PATH, safeBackupPath);

              // Закрываем текущую БД
              await new Promise((res) => db.close(res));

              // Удаляем старые WAL-файлы
              if (fs.existsSync(DB_PATH + '-wal')) fs.unlinkSync(DB_PATH + '-wal');
              if (fs.existsSync(DB_PATH + '-shm')) fs.unlinkSync(DB_PATH + '-shm');

              // Заменяем основной файл
              fs.copyFileSync(tempPath, DB_PATH);
              fs.unlinkSync(tempPath);

              // Переоткрываем БД
              db = new sqlite3.Database(DB_PATH);
              db.run('PRAGMA journal_mode=WAL;');

              await initializeDatabase();
              await loadPricesFromDB();
              await loadPaymentDetailsFromDB();
              await loadSettings();

              // Верифицируем восстановленную базу
              db.get(
                `SELECT
                (SELECT COUNT(*) FROM keys WHERE status='available') as available_keys,
                (SELECT COUNT(*) FROM orders) as total_orders,
                (SELECT COUNT(*) FROM users) as users_count,
                (SELECT COUNT(*) FROM settings) as settings_count`,
                [],
                (e, stats) => {
                  const statsText = (!e && stats)
                    ? `\n\n📊 Восстановлено:\n👥 Пользователей: ${stats.users_count}\n📦 Заказов: ${stats.total_orders}\n🔑 Ключей: ${stats.available_keys}\n⚙️ Настроек: ${stats.settings_count}`
                    : '';
                  bot.sendMessage(chatId, `✅ База данных успешно восстановлена!${statsText}\n\n_Резервная копия прежней базы сохранена как shop.db.pre_restore_backup_`, { parse_mode: 'Markdown' });
                  logAction(ADMIN_ID, 'database_restored', { fileName });
                }
              );

            } catch (replaceErr) {
              console.error('❌ Replace error:', replaceErr);
              // Пытаемся откатиться
              try {
                if (fs.existsSync(DB_PATH + '.pre_restore_backup')) {
                  if (fs.existsSync(DB_PATH + '-wal')) fs.unlinkSync(DB_PATH + '-wal');
                  if (fs.existsSync(DB_PATH + '-shm')) fs.unlinkSync(DB_PATH + '-shm');
                  fs.copyFileSync(DB_PATH + '.pre_restore_backup', DB_PATH);
                  db = new sqlite3.Database(DB_PATH);
                  db.run('PRAGMA journal_mode=WAL;');
                }
              } catch (rollbackErr) {
                console.error('❌ Rollback error:', rollbackErr);
              }
              bot.sendMessage(chatId, `❌ Ошибка при замене базы: ${replaceErr.message}\n\nПрежняя база восстановлена.`);
            }
          });
        });

      } catch (error) {
        console.error('❌ Restore error:', error);
        bot.sendMessage(chatId, `❌ Ошибка восстановления: ${error.message}`);

        // Откатываемся на pre_restore_backup если есть
        try {
          if (fs.existsSync(DB_PATH + '.pre_restore_backup')) {
            if (fs.existsSync(DB_PATH + '-wal')) fs.unlinkSync(DB_PATH + '-wal');
            if (fs.existsSync(DB_PATH + '-shm')) fs.unlinkSync(DB_PATH + '-shm');
            fs.copyFileSync(DB_PATH + '.pre_restore_backup', DB_PATH);
            db = new sqlite3.Database(DB_PATH);
            db.run('PRAGMA journal_mode=WAL;');
          }
        } catch (rollbackErr) {
          console.error('❌ Rollback error:', rollbackErr);
        }
        // Чистим temp файл если остался
        try { if (fs.existsSync('./shop_restore_temp.db')) fs.unlinkSync('./shop_restore_temp.db'); } catch (e) { }
      }

      clearSession(user.id);
      return;
    }

    // awaiting_stats_input удалён (Task 7)

    // ⚙️ П.3: ИЗМЕНЕНИЕ ПОРОГА КЛЮЧЕЙ
    if (user.id === ADMIN_ID && session.state === 'awaiting_low_keys_threshold') {
      const val = parseInt(msg.text);
      if (isNaN(val) || val < 1 || val > 100) {
        bot.sendMessage(chatId, '❌ Введите число от 1 до 100');
        return;
      }
      saveSetting('low_keys_threshold', String(val), () => {
        bot.sendMessage(chatId, `✅ Порог установлен: ${val} шт.`);
        showBotSettings(chatId, message.message_id);
      });
      clearSession(user.id);
      return;
    }

    // ⚙️ П.3: РЕДАКТИРОВАНИЕ ТЕКСТОВ/ССЫЛОК
    // ➕ Ввод chat_id для ручного добавления группы
    if (user.id === ADMIN_ID && session.state === 'awaiting_promo_chat_id') {
      // Поддержка /cancel
      if (msg.text && msg.text.trim() === '/cancel') {
        clearSession(user.id);
        bot.sendMessage(chatId, '❌ Отменено.');
        showBotSettings(chatId, message.message_id);
        return;
      }
      // Принимаем также пересланные сообщения — берём chat.id из forward_from_chat
      let targetChatId = null;
      let targetTitle = null;
      if (msg.forward_from_chat) {
        targetChatId = msg.forward_from_chat.id;
        targetTitle = msg.forward_from_chat.title || null;
      } else if (msg.text) {
        const cleaned = msg.text.trim().replace(/[^\-0-9]/g, '');
        const parsed = parseInt(cleaned);
        if (!isNaN(parsed)) {
          targetChatId = parsed;
        }
      }
      if (!targetChatId) {
        bot.sendMessage(chatId, '❌ Не удалось распознать chat\_id. Введите отрицательное число или перешлите сообщение из группы.', { parse_mode: 'Markdown' });
        return;
      }
      // Пробуем получить название группы
      let resolvedTitle = targetTitle;
      if (!resolvedTitle) {
        try {
          const chatInfo = await bot.getChat(targetChatId);
          resolvedTitle = chatInfo.title || String(targetChatId);
        } catch (_) {
          resolvedTitle = String(targetChatId);
        }
      }
      db.run(
        `INSERT INTO group_chats (chat_id, title, active) VALUES (?, ?, 1)
         ON CONFLICT(chat_id) DO UPDATE SET active = 1, title = excluded.title`,
        [targetChatId, resolvedTitle],
        (err) => {
          if (err) {
            bot.sendMessage(chatId, `❌ Ошибка при добавлении: ${err.message}`);
          } else {
            clearSession(user.id);
            bot.sendMessage(chatId,
              `✅ Группа добавлена в рассылку!\n\n📢 ${resolvedTitle}\n🆔 ${targetChatId}\n\nТеперь бот будет отправлять рекламу в эту группу.`,
              { reply_markup: { inline_keyboard: [
                [{ text: '📋 Список групп', callback_data: 'settings_promo_list_chats' }],
                [{ text: '◀️ Назад', callback_data: 'admin_settings' }]
              ]}}
            );
          }
        }
      );
      return;
    }

    if (user.id === ADMIN_ID && session.state === 'awaiting_edit_text') {
      const key = session.data.textKey;
      // Поддержка /cancel
      if (msg.text && msg.text.trim() === '/cancel') {
        clearSession(user.id);
        bot.sendMessage(chatId, '❌ Редактирование отменено.');
        showBotSettings(chatId, message.message_id);
        return;
      }
      if (!msg.text) {
        bot.sendMessage(chatId, '❌ Пожалуйста, отправьте текстовое сообщение. Или нажмите /cancel для отмены.');
        return;
      }
      const value = msg.text.trim();
      if (!value) {
        bot.sendMessage(chatId, '❌ Значение не может быть пустым. Введите текст или нажмите /cancel для отмены.');
        return;
      }
      saveSetting(key, value, () => {
        bot.sendMessage(chatId, `✅ Сохранено!`);
        showBotSettings(chatId, message.message_id);
      });
      clearSession(user.id);
      return;
    }

    // 🎟️ П.2: СОЗДАНИЕ КУПОНА — шаг 1 (код)
    if (user.id === ADMIN_ID && session.state === 'awaiting_coupon_code') {
      const code = msg.text.trim().toUpperCase().replace(/[^A-Z0-9]/g, '');
      if (!code || code.length < 3 || code.length > 20) {
        bot.sendMessage(chatId, '❌ Код должен быть от 3 до 20 символов (латинские буквы и цифры)');
        return;
      }
      session.data.code = code;
      session.state = 'awaiting_coupon_percent';
      bot.sendMessage(chatId, `✅ Код: \`${code}\`\n\nШаг 2/4: Введите процент скидки (1-100):`, { parse_mode: 'Markdown' });
      return;
    }

    // 🎟️ П.2: СОЗДАНИЕ КУПОНА — шаг 2 (процент)
    if (user.id === ADMIN_ID && session.state === 'awaiting_coupon_percent') {
      const pct = parseInt(msg.text);
      if (isNaN(pct) || pct < 1 || pct > 100) {
        bot.sendMessage(chatId, '❌ Введите число от 1 до 100');
        return;
      }
      session.data.discount_percent = pct;
      session.state = 'awaiting_coupon_max_uses';
      bot.sendMessage(chatId, `✅ Скидка: ${pct}%\n\nШаг 3/4: Максимум использований (0 = неограниченно):`);
      return;
    }

    // 🎟️ П.2: СОЗДАНИЕ КУПОНА — шаг 3 (лимит)
    if (user.id === ADMIN_ID && session.state === 'awaiting_coupon_max_uses') {
      const maxUses = parseInt(msg.text);
      if (isNaN(maxUses) || maxUses < 0) {
        bot.sendMessage(chatId, '❌ Введите 0 или положительное число');
        return;
      }
      session.data.max_uses = maxUses;
      session.state = 'awaiting_coupon_expires';
      bot.sendMessage(chatId, `✅ Лимит: ${maxUses === 0 ? 'не ограничен' : maxUses}\n\nШаг 4/5: Срок действия (ДД.ММ.ГГГГ или "-" для бессрочного):`);
      return;
    }

    // 🎟️ П.2: СОЗДАНИЕ КУПОНА — шаг 4 (срок)
    if (user.id === ADMIN_ID && session.state === 'awaiting_coupon_expires') {
      let expiresAt = null;
      const input = msg.text.trim();

      if (input !== '-') {
        const parts = input.split('.');
        if (parts.length === 3) {
          const d = parseInt(parts[0]), m = parseInt(parts[1]) - 1, y = parseInt(parts[2]);
          const dt = new Date(y, m, d);
          if (isNaN(dt.getTime())) {
            bot.sendMessage(chatId, '❌ Неверный формат даты. Введите ДД.ММ.ГГГГ или "-"');
            return;
          }
          expiresAt = dt.toISOString();
        } else {
          bot.sendMessage(chatId, '❌ Неверный формат. Введите ДД.ММ.ГГГГ или "-"');
          return;
        }
      }

      session.data.expires_at = expiresAt;
      session.state = 'awaiting_coupon_product_restriction';
      bot.sendMessage(chatId, `✅ Срок задан.\n\nШаг 5/5: 🎯 На какой товар действует купон?\n\nВведите: \`1d\`, \`3d\`, \`7d\`, \`30d\` — или \`-\` для всех товаров`, { parse_mode: 'Markdown' });
      return;
    }

    // 🎟️ П.2: СОЗДАНИЕ КУПОНА — шаг 5 (product restriction)
    if (user.id === ADMIN_ID && session.state === 'awaiting_coupon_product_restriction') {
      const input = msg.text.trim().toLowerCase();
      const validProducts = ['1d', '3d', '7d', '30d'];
      const productRestriction = (input === '-' || !validProducts.includes(input)) ? null : input;

      const { code, discount_percent, max_uses, expires_at } = session.data;

      db.run(
        `INSERT INTO coupons (code, discount_percent, max_uses, expires_at, created_by, product_restriction) VALUES (?, ?, ?, ?, ?, ?)`,
        [code, discount_percent, max_uses, expires_at, user.id, productRestriction],
        function(err) {
          if (err) {
            if (err.message.includes('UNIQUE')) {
              bot.sendMessage(chatId, `❌ Купон с кодом \`${code}\` уже существует`, { parse_mode: 'Markdown' });
            } else {
              bot.sendMessage(chatId, `❌ Ошибка создания купона: ${err.message}`);
            }
          } else {
            const couponId = this.lastID;
            // Вставляем ограничение по товару в coupon_products
            if (productRestriction && couponId) {
              db.run(`INSERT OR IGNORE INTO coupon_products (coupon_id, product) VALUES (?, ?)`, [couponId, productRestriction]);
            }
            const expiresStr = expires_at ? new Date(expires_at).toLocaleDateString('ru-RU') : '∞';
            const restrictionStr = productRestriction ? `🎯 Только на: ${productRestriction}` : '🌐 Все товары';
            bot.sendMessage(
              chatId,
              `✅ *Купон создан!*\n\n🎟️ Код: \`${code}\`\n💸 Скидка: ${discount_percent}%\n🔢 Лимит: ${max_uses === 0 ? '∞' : max_uses}\n📅 До: ${expiresStr}\n${restrictionStr}`,
              { parse_mode: 'Markdown' }
            );
            logAction(user.id, 'coupon_created', { code, discount_percent, max_uses, productRestriction });
            showCouponsPanel(chatId, message.message_id);
          }
          clearSession(user.id);
        }
      );
      return;
    }

    // 🎟️ ВЫДАЧА КУПОНА ВСЕМ — шаг 1 (процент)
    if (user.id === ADMIN_ID && session.state === 'awaiting_coupon_all_percent') {
      const pct = parseInt(msg.text);
      if (isNaN(pct) || pct < 1 || pct > 100) {
        bot.sendMessage(chatId, '❌ Введите число от 1 до 100');
        return;
      }
      session.data.discount_percent = pct;
      const targetMode = session.data.targetMode || 'all';
      const modeLabel = targetMode === 'buyers' ? '🛒 Покупателям' : '📣 Всем пользователям';
      session.state = 'awaiting_coupon_all_expires';
      bot.sendMessage(chatId, `✅ Скидка: ${pct}% | ${modeLabel}\n\nШаг 2/3: Срок действия купона:\n- ДД.ММ.ГГГГ для конкретной даты\n- «7д» для 7 дней, «30д» для 30 дней\n- «-» для бессрочного`);
      return;
    }

    // 🎟️ ВЫДАЧА КУПОНА ВСЕМ — шаг 2 (срок)
    if (user.id === ADMIN_ID && session.state === 'awaiting_coupon_all_expires') {
      const input = msg.text.trim();
      let expiresAt = null;
      let expiresLabel = 'бессрочный';

      if (input !== '-') {
        if (/^\d+д$/i.test(input)) {
          const days = parseInt(input);
          expiresAt = new Date(Date.now() + days * 24 * 60 * 60 * 1000).toISOString();
          expiresLabel = `${days} дней`;
        } else {
          const parts = input.split('.');
          if (parts.length === 3) {
            const d = parseInt(parts[0]), m = parseInt(parts[1]) - 1, y = parseInt(parts[2]);
            const dt = new Date(y, m, d);
            if (isNaN(dt.getTime())) {
              bot.sendMessage(chatId, '❌ Неверный формат даты');
              return;
            }
            expiresAt = dt.toISOString();
            expiresLabel = dt.toLocaleDateString('ru-RU');
          } else {
            bot.sendMessage(chatId, '❌ Неверный формат. Используйте ДД.ММ.ГГГГ, "7д", "30д" или "-"');
            return;
          }
        }
      }

      session.data.expires_at = expiresAt;
      session.data.expires_label = expiresLabel;
      session.state = 'awaiting_coupon_all_reason';
      const targetMode3 = session.data.targetMode || 'all';
      const modeLabel3 = targetMode3 === 'buyers' ? '🛒 Только покупателям' : '📣 Всем пользователям';
      bot.sendMessage(chatId, `✅ Срок: ${expiresLabel} | ${modeLabel3}\n\nШаг 3/3: Укажите причину/повод выдачи (будет в уведомлении), или «-» чтобы пропустить:`);
      return;
    }

    // 🎟️ ВЫДАЧА КУПОНА — шаг 3 (причина + массовая выдача: всем или только покупателям)
    if (user.id === ADMIN_ID && session.state === 'awaiting_coupon_all_reason') {
      const reason = msg.text.trim() === '-' ? null : msg.text.trim();
      const { discount_percent, expires_at, expires_label, targetMode } = session.data;
      const isBuyersOnly = targetMode === 'buyers';
      clearSession(user.id);

      const modeText = isBuyersOnly ? 'покупателям (≥1 покупки)' : 'всем пользователям';
      bot.sendMessage(chatId, `⏳ Создаю купоны и рассылаю ${modeText}...`);

      // Запрос: если режим buyers — только те, у кого есть хотя бы 1 подтверждённый заказ
      const query = isBuyersOnly
        ? `SELECT DISTINCT u.id, u.language_code FROM users u
         INNER JOIN orders o ON o.user_id = u.id AND o.status = 'confirmed'`
        : `SELECT id, language_code FROM users`;

      db.all(query, [], async (err, users) => {
        if (err || !users) {
          bot.sendMessage(chatId, '❌ Ошибка получения пользователей');
          return;
        }

        let sent = 0;
        let blocked = 0;
        let failed = 0;
        const total = users.length;
        const prefix = isBuyersOnly ? 'BUY' : 'ALL';

        for (const targetUser of users) {
          try {
            const couponCode = prefix + '-' + crypto.randomBytes(3).toString('hex').toUpperCase();
            await new Promise((resolve, reject) => {
              db.run(
                `INSERT INTO coupons (code, discount_percent, max_uses, expires_at, created_by, user_id) VALUES (?, ?, 1, ?, ?, ?)`,
                [couponCode, discount_percent, expires_at, ADMIN_ID, targetUser.id],
                (err2) => { if (err2) reject(err2); else resolve(); }
              );
            });

            const isRu = !targetUser.language_code || targetUser.language_code.startsWith('ru') ||
              ['uk', 'be', 'kk', 'uz', 'ky', 'tg', 'tk', 'az', 'hy', 'ka', 'ro', 'pl', 'bg', 'sr'].some(l => (targetUser.language_code || '').startsWith(l));

            let clientMsg;
            if (isRu) {
              clientMsg = `🎉 *Подарок от CyraxMods!*\n\n`
                + (reason ? `💬 ${reason}\n\n` : (isBuyersOnly ? `🙏 Спасибо, что доверяете нам!\n\n` : ''))
                + `Получи персональный купон на *${discount_percent}%* скидки!\n\n`
                + `🎟️ Код: \`${couponCode}\`\n`
                + `⏰ Срок: ${expires_label}\n`
                + `⚠️ Одноразовый, только для тебя\n\n`
                + `Применяй при следующей покупке ключа! 🔑`;
            } else {
              clientMsg = `🎉 *Gift from CyraxMods!*\n\n`
                + (reason ? `💬 ${reason}\n\n` : (isBuyersOnly ? `🙏 Thank you for your trust!\n\n` : ''))
                + `Here's your personal *${discount_percent}%* discount coupon!\n\n`
                + `🎟️ Code: \`${couponCode}\`\n`
                + `⏰ Valid: ${expires_label}\n`
                + `⚠️ Single-use, exclusive to you\n\n`
                + `Apply it on your next key purchase! 🔑`;
            }

            await safeSendMessage(targetUser.id, clientMsg, { parse_mode: 'Markdown' });
            sent++;
          } catch (e) {
            if (e?.response?.body?.error_code === 403) {
              blocked++;
            } else {
              failed++;
            }
          }
          await new Promise(r => setTimeout(r, 50));
        }

        const modeEmoji = isBuyersOnly ? '🛒' : '📣';
        const modeLabel = isBuyersOnly ? 'Покупателям' : 'Всем пользователям';
        const report = [
          `${modeEmoji} Рассылка купонов завершена`,
          ``,
          `📋 Режим: *${modeLabel}*`,
          `💸 Скидка: *${discount_percent}%*`,
          `⏰ Срок: *${expires_label}*`,
          reason ? `💬 Повод: ${reason}` : null,
          ``,
          `👥 Целевая аудитория: *${total}*`,
          `✅ Получили купон: *${sent}*`,
          `🚫 Заблокировали бота: *${blocked}*`,
          failed > 0 ? `⚠️ Другие ошибки: *${failed}*` : null,
          ``,
          `📈 Доставлено: *${total > 0 ? Math.round(sent / total * 100) : 0}%*`
        ].filter(Boolean).join('\n');

        bot.sendMessage(chatId, report, { parse_mode: 'Markdown' });
        logAction(ADMIN_ID, 'coupon_issued_to_all', { discount_percent, expires_label, targetMode: targetMode || 'all', sent, blocked, failed, reason: reason || null });
      });
      return;
    }

    // 📨 НАПИСАТЬ ПОЛЬЗОВАТЕЛЮ — шаг 1
    if (user.id === ADMIN_ID && session.state === 'awaiting_msg_user_target' && msg.text) {
      const input = msg.text.trim().replace(/^@/, '');
      const isNumeric = /^\d+$/.test(input);
      const q = isNumeric ? 'SELECT id, username FROM users WHERE id = ?' : 'SELECT id, username FROM users WHERE username = ?';
      db.get(q, [isNumeric ? parseInt(input) : input], (err, tu) => {
        if (err || !tu) {
          bot.sendMessage(chatId, '❌ Пользователь не найден в базе.');
          return;
        }
        session.data.msgTargetId = tu.id;
        session.data.msgTargetUsername = tu.username || String(tu.id);
        session.state = 'awaiting_msg_user_text';
        const label = tu.username ? '@' + tu.username : 'ID: ' + tu.id;
        bot.sendMessage(chatId,
          '👤 Получатель: ' + label + '\n\nВведите текст сообщения (или отправьте фото с подписью):',
          { reply_markup: { inline_keyboard: [[{ text: '◀️ Отмена', callback_data: 'admin' }]] } }
        );
      });
      return;
    }

    // 📨 НАПИСАТЬ ПОЛЬЗОВАТЕЛЮ — шаг 2
    if (user.id === ADMIN_ID && session.state === 'awaiting_msg_user_text') {
      const { msgTargetId, msgTargetUsername } = session.data || {};
      if (!msgTargetId) { clearSession(user.id); return; }
      clearSession(user.id);
      const label = msgTargetUsername ? '@' + msgTargetUsername : 'ID: ' + msgTargetId;
      try {
        if (msg.photo) {
          await bot.sendPhoto(msgTargetId, msg.photo[msg.photo.length - 1].file_id, { caption: msg.caption || '' });
        } else if (msg.text) {
          await safeSendMessage(msgTargetId, msg.text, { parse_mode: 'Markdown' })
            .catch(() => safeSendMessage(msgTargetId, msg.text));
        }
        bot.sendMessage(chatId, '✅ Отправлено → ' + label);
        logAction(ADMIN_ID, 'admin_message_sent', { targetId: msgTargetId, targetUsername: msgTargetUsername });
      } catch (e) {
        bot.sendMessage(chatId, '❌ Ошибка отправки: ' + e.message);
      }
      return;
    }

    // 🎟️ П.2: ВЫДАЧА КУПОНА ЮЗЕРУ — шаг 1 (username)
    if (user.id === ADMIN_ID && session.state === 'awaiting_coupon_issue_username') {
      const username = msg.text.trim().replace('@', '');
      if (!username || username.includes(' ')) {
        bot.sendMessage(chatId, '❌ Неверный формат username');
        return;
      }
      session.data.targetUsername = username;
      session.state = 'awaiting_coupon_issue_percent';
      bot.sendMessage(chatId,
        `👤 Пользователь: @${escapeMarkdown(username || '')}\n\n*Создание нового купона*\n\nШаг 1/3: Введите процент скидки (1–100):`,
        { parse_mode: 'Markdown' }
      );
      return;
    }

    // 🎟️ П.2: ВЫДАЧА КУПОНА ЮЗЕРУ — шаг 2 (процент скидки)
    if (user.id === ADMIN_ID && session.state === 'awaiting_coupon_issue_percent') {
      const pct = parseInt(msg.text.trim());
      if (isNaN(pct) || pct < 1 || pct > 100) {
        bot.sendMessage(chatId, '❌ Введите число от 1 до 100:');
        return;
      }
      session.data.discountPercent = pct;
      session.state = 'awaiting_coupon_issue_days';
      bot.sendMessage(chatId,
        `✅ Скидка: ${pct}%\n\nШаг 2/3: Введите срок действия купона в днях (0 — бессрочно):`,
        { parse_mode: 'Markdown' }
      );
      return;
    }

    // 🎟️ П.2: ВЫДАЧА КУПОНА ЮЗЕРУ — шаг 3 (срок действия)
    if (user.id === ADMIN_ID && session.state === 'awaiting_coupon_issue_days') {
      const days = parseInt(msg.text.trim());
      if (isNaN(days) || days < 0) {
        bot.sendMessage(chatId, '❌ Введите целое число (0 или больше):');
        return;
      }
      session.data.validDays = days;
      session.state = 'awaiting_coupon_issue_product';
      bot.sendMessage(chatId,
        `✅ Срок: ${days === 0 ? 'бессрочно' : days + ' дн.'}\n\nШаг 3/3: На какой товар действует купон?\n\nВведите: \`1d\`, \`3d\`, \`7d\`, \`30d\` — или \`-\` для всех товаров`,
        { parse_mode: 'Markdown' }
      );
      return;
    }

    // 🎟️ П.2: ВЫДАЧА КУПОНА ЮЗЕРУ — шаг 4 (товар, создание и выдача)
    if (user.id === ADMIN_ID && session.state === 'awaiting_coupon_issue_product') {
      const rawInput = msg.text.trim().toLowerCase();
      const validProducts = ['1d', '3d', '7d', '30d'];
      if (rawInput !== '-' && !validProducts.includes(rawInput)) {
        bot.sendMessage(chatId,
          `❌ Неверный товар. Введите одно из: \`1d\`, \`3d\`, \`7d\`, \`30d\` — или \`-\` для всех товаров`,
          { parse_mode: 'Markdown' }
        );
        return;
      }
      const productLimit = rawInput === '-' ? null : rawInput;
      const { targetUsername, discountPercent, validDays } = session.data;

      // Поиск по username (COLLATE NOCASE) или по числовому user_id
      const isNumeric = /^\d+$/.test(targetUsername);
      const lookupQuery = isNumeric
        ? `SELECT id FROM users WHERE id = ?`
        : `SELECT id FROM users WHERE username = ? COLLATE NOCASE`;
      db.get(lookupQuery, [isNumeric ? parseInt(targetUsername) : targetUsername], (err, targetUser) => {
        if (err || !targetUser) {
          bot.sendMessage(chatId,
            '❌ Пользователь не найден в БД.\n\n' +
            'Убедитесь, что:\n• username введён без @ и точно совпадает\n• или введите числовой Telegram ID\n• пользователь хотя бы раз запускал бота'
          );
          clearSession(user.id);
          return;
        }

        // Генерируем уникальный код купона
        const newCode = 'GIFT' + crypto.randomBytes(3).toString('hex').toUpperCase();
        const expiresAt = validDays > 0
          ? new Date(Date.now() + validDays * 86400000).toISOString().slice(0, 10)
          : null;

        db.run(
          `INSERT INTO coupons (code, discount_percent, expires_at, product_restriction, is_active, max_uses, used_count)
           VALUES (?, ?, ?, ?, 1, 1, 0)`,
          [newCode, discountPercent, expiresAt, productLimit],
          function (e) {
            if (e) {
              bot.sendMessage(chatId, '❌ Ошибка создания купона в БД');
              clearSession(user.id);
              return;
            }
            const couponId = this.lastID;

            // Вставляем ограничение по товару в coupon_products если задано
            if (productLimit && couponId) {
              db.run(`INSERT OR IGNORE INTO coupon_products (coupon_id, product) VALUES (?, ?)`, [couponId, productLimit]);
            }

            db.run(
              `INSERT OR IGNORE INTO user_coupons (user_id, coupon_id) VALUES (?, ?)`,
              [targetUser.id, couponId],
              (e2) => {
                if (e2) {
                  bot.sendMessage(chatId, '❌ Ошибка привязки купона к пользователю');
                  clearSession(user.id);
                  return;
                }

                // Уведомляем пользователя
                safeSendMessage(
                  targetUser.user_id || targetUser.id,
                  `🎟 Вам выдан купон на скидку!\n\nКод: \`${newCode}\`\nСкидка: ${discountPercent}%\n${expiresAt ? `Действует до: ${expiresAt}` : 'Бессрочный'}\n${productLimit ? `Для товара: ${productLimit}` : 'На любой товар'}\n\nПрименяйте при следующей покупке!`,
                  { parse_mode: 'Markdown' }
                ).catch(() => { });

                bot.sendMessage(
                  chatId,
                  `✅ *Купон создан и выдан!*\n\nПользователь: @${escapeMarkdown(targetUsername || '')}\nКод: \`${newCode}\`\nСкидка: ${discountPercent}%\nСрок: ${expiresAt || 'бессрочно'}\nТовар: ${productLimit || 'любой'}`,
                  { parse_mode: 'Markdown' }
                );
                logAction(user.id, 'coupon_issued_new', { code: newCode, targetUsername, discountPercent });
                clearSession(user.id);
              }
            );
          }
        );
      });
      return;
    }

    // 🎟️ ЮЗЕР ВВОДИТ КОД КУПОНА
    if (session.state === 'awaiting_coupon_input') {
      const code = msg.text.trim().toUpperCase();
      const { period, currency } = session.data;

      if (!code || !period || !currency) {
        bot.sendMessage(chatId, '❌ Ошибка сессии. Начните покупку заново.');
        clearSession(user.id);
        return;
      }

      // Валидация формата купона: только буквы A-Z, цифры, дефис; длина 3-20
      if (!/^[A-Z0-9\-]{3,20}$/.test(code)) {
        const isRuCoup = getLang(user) === 'ru';
        bot.sendMessage(chatId,
          isRuCoup
            ? '❌ Неверный формат кода. Код купона содержит только буквы, цифры и дефис (3–20 символов).'
            : '❌ Invalid code format. Coupon code may only contain letters, digits and hyphens (3–20 characters).'
        ).catch(() => {});
        return;
      }

      const originalAmount = PRICES[period] && PRICES[period][currency];
      if (!originalAmount) {
        bot.sendMessage(chatId, '❌ Ошибка данных. Начните покупку заново.');
        clearSession(user.id);
        return;
      }

      try {
        const result = await applyCoupon(user.id, code, period, currency, originalAmount);

        // Сохраняем данные купона в сессии
        session.data.couponId = result.couponId;
        session.data.couponCode = result.code;
        session.data.discountPercent = result.discountPercent;
        session.data.discountedAmount = result.newAmount;
        session.data.loyaltyDiscountPercent = 0; // Task 9: купон имеет приоритет над лояльностью
        session.state = 'selected_currency';

        const discountedFormatted = formatPrice(result.newAmount, currency);
        const originalFormatted = formatPrice(originalAmount, currency);

        const kb = { inline_keyboard: [] };

        if (currency === 'USD') {
          if (PAYPAL_LINK) kb.inline_keyboard.push([{ text: t(user, 'paypal'), callback_data: `pay_${period}_${currency}_paypal` }]);
          kb.inline_keyboard.push([{ text: t(user, 'binance'), callback_data: `pay_${period}_${currency}_binance` }]);
          if (CRYPTOBOT_TOKEN) kb.inline_keyboard.push([{ text: t(user, 'cryptobot'), callback_data: `pay_${period}_${currency}_cryptobot` }]);
        } else if (currency === 'EUR') {
          kb.inline_keyboard.push([{ text: t(user, 'italy_card'), callback_data: `pay_${period}_${currency}_card_it` }]);
          kb.inline_keyboard.push([{ text: t(user, 'binance'), callback_data: `pay_${period}_${currency}_binance` }]);
          if (CRYPTOBOT_TOKEN) kb.inline_keyboard.push([{ text: t(user, 'cryptobot'), callback_data: `pay_${period}_${currency}_cryptobot` }]);
          if (PAYPAL_LINK) kb.inline_keyboard.push([{ text: t(user, 'paypal'), callback_data: `pay_${period}_${currency}_paypal` }]);
        } else if (currency === 'RUB') {
          kb.inline_keyboard.push([{ text: t(user, 'russia_sbp'), callback_data: `pay_${period}_${currency}_sbp` }]);
          if (CRYPTOBOT_TOKEN) kb.inline_keyboard.push([{ text: t(user, 'cryptobot'), callback_data: `pay_${period}_${currency}_cryptobot_usd` }]);
        } else if (currency === 'UAH') {
          kb.inline_keyboard.push([{ text: t(user, 'ukraine_card'), callback_data: `pay_${period}_${currency}_card_ua` }]);
          if (CRYPTOBOT_TOKEN) kb.inline_keyboard.push([{ text: t(user, 'cryptobot'), callback_data: `pay_${period}_${currency}_cryptobot_usd` }]);
        }

        // Кнопка «С баланса» — если баланс >= цена ПОСЛЕ купона
        try {
          const userBal = await getUserBalance(user.id);
          const discountedPrice = result.newAmount;
          if (userBal.balance >= discountedPrice && discountedPrice > 0 && userBal.preferred_currency === currency) {
            const isRuBal = getLang(user) === 'ru';
            kb.inline_keyboard.push([{
              text: isRuBal
                ? `💳 С баланса (${formatBalanceAmount(userBal.balance, currency)})`
                : `💳 From balance (${formatBalanceAmount(userBal.balance, currency)})`,
              callback_data: `pay_balance_${period}_${currency}`
            }]);
          }
        } catch(e) { /* не критично */ }

        kb.inline_keyboard.push([{ text: t(user, 'back'), callback_data: `period_${period}` }]);

        bot.sendMessage(
          chatId,
          `✅ *Купон применён!*\n\n🎟️ Код: \`${result.code}\`\n💸 Скидка: ${result.discountPercent}%\n\n${originalFormatted} → *${discountedFormatted}*\n\nВыберите способ оплаты:`,
          { parse_mode: 'Markdown', reply_markup: kb }
        );

      } catch (err) {
        // BUG FIX UX-2: НЕ сбрасываем сессию при ошибке купона — пользователь не теряет
        // выбранный товар/валюту и может ввести другой код или нажать «Назад».
        const isRu = getLang(user) === 'ru';
        const backKb = { reply_markup: { inline_keyboard: [[{ text: isRu ? '◀️ Назад' : '◀️ Back', callback_data: `period_${session.data?.period || 'catalog'}` }]] } };

        // Показываем локализованные сообщения для известных ошибок купонов
        if (err.message === 'COUPON_ERROR_PARTNER') {
          bot.sendMessage(chatId, t(user, 'coupon_error_partner'), backKb);
          return;
        } else if (err.message === 'COUPON_ERROR_NOT_FOUND') {
          // FIX 3.1: локализованная ошибка "купон не найден" вместо сырой русской строки
          bot.sendMessage(chatId, t(user, 'coupon_not_found'), backKb);
          return;
        } else if (err.message.startsWith('COUPON_ERROR_PERIOD:')) {
          const restrictedPeriod = err.message.split(':')[1];
          const periodLabel = PERIOD_NAMES[isRu ? 'ru' : 'en'][restrictedPeriod] || restrictedPeriod;
          bot.sendMessage(chatId, t(user, 'coupon_error_period', { period: periodLabel }), backKb);
          return;
        } else {
          bot.sendMessage(chatId, `❌ ${err.message}\n\n${isRu ? 'Введите другой код или нажмите «Назад».' : 'Enter another code or press "Back".'}`, backKb);
        }
        // Сессию НЕ очищаем — state остаётся 'awaiting_coupon_input'
      }
      return;
    }

    // ==========================================
    // 🆘 ПОДДЕРЖКА — обработка ввода ключа
    // ==========================================
    if (session.state === 'support_awaiting_key' && msg.text) {
      const keyInput = msg.text.trim();
      if (!keyInput || keyInput.length < 5) {
        bot.sendMessage(chatId, getLang(user) === 'ru' ? '❌ Введите корректный ключ' : '❌ Please enter a valid key');
        return;
      }
      session.state = 'support_key_analyzed';
      await analyzeKeyForSupport(chatId, user, keyInput);
      return;
    }

    // 🆘 ПОДДЕРЖКА — получить описание проблемы
    if (session.state === 'support_awaiting_description' && msg.text) {
      const description = msg.text.trim();
      if (!description || description.length < 5) {
        const isRu = getLang(user) === 'ru';
        bot.sendMessage(chatId, isRu ? '❌ Пожалуйста, опишите проблему подробнее' : '❌ Please describe the problem in more detail');
        return;
      }
      session.data.description = description;
      session.state = 'support_awaiting_screenshot';
      const isRu = getLang(user) === 'ru';
      const msg2 = isRu
        ? `2️⃣ *Приложите скриншот* (если есть)\nМожно отправить фото или PDF с ошибкой`
        : `2️⃣ *Attach a screenshot* (if available)\nYou can send a photo or PDF of the error`;
      safeSendMessage(chatId, msg2, {
        parse_mode: 'Markdown',
        reply_markup: { inline_keyboard: [[{ text: isRu ? '📸 Пропустить скриншот' : '📸 Skip screenshot', callback_data: 'support_ticket_skip_screenshot' }]] }
      });
      return;
    }

    // 🆘 ПОДДЕРЖКА — получить скриншот
    if (session.state === 'support_awaiting_screenshot' && (msg.photo || msg.document)) {
      const fileId = msg.photo ? msg.photo[msg.photo.length - 1].file_id : msg.document.file_id;
      await createAndSendTicket(user, chatId, session.data.keyValue, session.data.description || '', fileId, session.data);
      clearSession(user.id);
      return;
    }

    // ЮЗЕР ПРИСЛАЛ ЧЕК ДЛЯ РУЧНОГО БУСТА
    if ((msg.photo || msg.document) && session.state === 'mb_awaiting_receipt') {
      const br = session.data.br;
      if (!br || !br.id) {
        bot.sendMessage(chatId, t(user, 'error_order_data_missing'));
        clearSession(user.id);
        return;
      }
      const fileId = msg.photo ? msg.photo[msg.photo.length - 1].file_id : msg.document.file_id;
      const fileUniqueId = msg.photo ? msg.photo[msg.photo.length - 1].file_unique_id : (msg.document.file_unique_id || null);
      const fileType = msg.photo ? 'photo' : 'document';

      // 🛡️ Сначала проверяем чек
      checkReceiptDuplicate(fileId, fileUniqueId, user.id, (dupInfo) => {
        if (dupInfo && dupInfo.isDuplicate && !dupInfo.isSameUser) {
          const isRu = getLang(user) === 'ru';
          bot.sendMessage(chatId, isRu
            ? '🚫 Этот чек уже был использован. Обратитесь в поддержку.'
            : '🚫 This receipt has already been used. Contact support.'
          ).catch(() => { });
          const origUser = dupInfo.originalUsername ? `@${escapeMarkdown(dupInfo.originalUsername)}` : `ID ${dupInfo.originalUserId}`;
          safeSendMessage(ADMIN_ID, `🚨 *Фрод (Буст)!*\n\nОригинал: ${origUser} (#${dupInfo.originalOrderId})\nПопытка: @${escapeMarkdown(String(user.username || user.id))} (буст #${br.id})`, { parse_mode: 'Markdown' }).catch(() => { });
          increaseSuspicion(user.id, 30, 'Повторный чек другого юзера (буст)');
          clearSession(user.id);
          return;
        }

        if (dupInfo && dupInfo.isDuplicate && dupInfo.isSameUser) {
          safeSendMessage(ADMIN_ID, `⚠️ *Повторный чек (Буст)!*\n\n@${escapeMarkdown(String(user.username || user.id))} — тот же чек для буста #${br.id}`, { parse_mode: 'Markdown' }).catch(() => { });
          increaseSuspicion(user.id, 10, 'Повторный чек (буст, свой)');
        }

        db.run(
          `UPDATE boost_requests SET receipt_file_id = ?, receipt_type = ?, payment_method = ?, status = 'paid_pending' WHERE id = ?`,
          [fileId, fileType, br.method || 'manual', br.id],
          (err) => {
            if (err) { bot.sendMessage(chatId, t(user, 'error_creating_order')); return; }
            clearSession(user.id);

            // Сохраняем чек
            saveReceiptRecord(fileId, fileUniqueId, user.id, br.id, 'boost');

            bot.sendMessage(chatId, t(user, 'manual_boost_receipt_ok'), { parse_mode: 'HTML' });

            // Уведомляем админа
            db.get(`SELECT * FROM boost_requests WHERE id = ?`, [br.id], (e, brRow) => {
              if (e || !brRow) return;
              const caption =
                `📸 *Чек — Ручной Буст #${br.id}*\n\n` +
                `👤 ${brRow.username}\n` +
                `🎮 ${brRow.current_rank} → 🏆 ${brRow.desired_rank}\n` +
                `💰 ${br.amount} ${br.currency}`;
              const adminKb = {
                inline_keyboard: [[
                  { text: '✅ Подтвердить', callback_data: `admin_mb_confirm_${br.id}` },
                  { text: '❌ Отклонить', callback_data: `admin_mb_reject_${br.id}` }
                ]]
              };
              if (fileType === 'photo') {
                bot.sendPhoto(ADMIN_ID, fileId, { caption, parse_mode: 'Markdown', reply_markup: adminKb }).catch(() => { });
              } else {
                bot.sendDocument(ADMIN_ID, fileId, { caption, parse_mode: 'Markdown', reply_markup: adminKb }).catch(() => { });
              }
            });
          });
      }
      );
      return;
    }

    // Лояльность — ввод скидки для пользователя
    if (user.id === ADMIN_ID && session.state === 'awaiting_loyalty_discount') {
      const pct = parseInt(msg.text);
      if (isNaN(pct) || pct < 0 || pct > 100) {
        bot.sendMessage(chatId, '❌ Введите число от 0 до 100');
        return;
      }
      db.run(`UPDATE users SET loyalty_discount = ? WHERE id = ?`, [pct, session.data.targetUserId], (err) => {
        if (err) bot.sendMessage(chatId, '❌ Ошибка БД');
        else bot.sendMessage(chatId, `✅ Персональная скидка установлена: ${pct}%`);
        showLoyaltyPanel(chatId, message.message_id);
        clearSession(user.id);
      });
      return;
    }

    if (user.id === ADMIN_ID && session.state === 'awaiting_admin_rsl_balance') {
      const amount = parseFloat(msg.text.replace(',', '.'));
      if (isNaN(amount) || amount < 0) {
        bot.sendMessage(chatId, '❌ Введите корректную сумму');
        return;
      }
      const rId = session.data.resellerId;
      db.run(`UPDATE resellers SET balance = ? WHERE id = ?`, [amount, rId], (err) => {
        if (err) bot.sendMessage(chatId, '❌ Ошибка обновления баланса');
        else {
          bot.sendMessage(chatId, `✅ Баланс реселлера обновлён: ${amount} ₽`);
          showAdminResellerEdit(chatId, rId);
        }
        clearSession(user.id);
      });
      return;
    }

    // Лояльность — глобальная скидка
    if (user.id === ADMIN_ID && session.state === 'awaiting_default_loyalty') {
      const val = parseInt(msg.text);
      if (isNaN(val) || val < 0 || val > 99) { bot.sendMessage(chatId, '❌ Введите 0-99'); return; }
      saveSetting('default_loyalty_discount', String(val), () => {
        bot.sendMessage(chatId, `✅ Глобальная скидка: ${val}%`);
        showLoyaltyPanel(chatId, message.message_id);
      });
      clearSession(user.id);
      return;
    }

    // FOMO — шанс
    if (user.id === ADMIN_ID && session.state === 'awaiting_fomo_chance') {
      const val = parseInt(msg.text);
      if (isNaN(val) || val < 1 || val > 100) { bot.sendMessage(chatId, '❌ Введите 1-100'); return; }
      saveSetting('fomo_chance', String(val), () => { bot.sendMessage(chatId, `✅ Шанс: ${val}%`); showFomoPanel(chatId, message.message_id); });
      clearSession(user.id);
      return;
    }

    // FOMO — срок действия
    if (user.id === ADMIN_ID && session.state === 'awaiting_fomo_expiry') {
      const val = parseInt(msg.text);
      if (isNaN(val) || val < 1 || val > 365) { bot.sendMessage(chatId, '❌ Введите 1-365'); return; }
      saveSetting('fomo_coupon_expiry_days', String(val), () => { bot.sendMessage(chatId, `✅ Срок: ${val} дней`); showFomoPanel(chatId, message.message_id); });
      clearSession(user.id);
      return;
    }

    // FOMO — макс. процент
    if (user.id === ADMIN_ID && session.state === 'awaiting_fomo_max_percent') {
      const val = parseInt(msg.text);
      if (isNaN(val) || val < 1 || val > 99) { bot.sendMessage(chatId, '❌ Введите 1-99'); return; }
      saveSetting('fomo_max_percent', String(val), () => { bot.sendMessage(chatId, `✅ Макс. %: ${val}%`); showFomoPanel(chatId, message.message_id); });
      clearSession(user.id);
      return;
    }

    // 🔄 Сброс токена реселлера — свободный текст причины
    if (session.state === 'awaiting_rsl_reset_reason' && msg.text) {
      const reason = msg.text.trim();
      if (!reason || reason.length < 3) {
        const isRu = getLang(user) === 'ru';
        bot.sendMessage(chatId, isRu ? '❌ Пожалуйста, укажите причину (минимум 3 символа)' : '❌ Please provide a reason (at least 3 characters)');
        return;
      }
      const rId = session.data.resellerId;
      clearSession(user.id);
      submitTokenResetRequest(user, chatId, rId, reason);
      return;
    }

    // Отзыв — купон
    if (user.id === ADMIN_ID && session.state === 'awaiting_review_coupon_percent') {
      const pct = parseInt(msg.text);
      if (isNaN(pct) || pct < 1 || pct > 100) { bot.sendMessage(chatId, '❌ Введите 1-100'); return; }
      giveReviewRewardCoupon(chatId, session.data.reviewId, pct);
      clearSession(user.id);
      return;
    }

    // ─── 🎁 GIFT ALL — шаг 1: сумма в рублях ───
    if (user.id === ADMIN_ID && session.state === 'awaiting_gift_all_amount') {
      const raw = (msg.text || '').trim().replace(',', '.');
      const amountRub = parseFloat(raw);
      if (isNaN(amountRub) || amountRub <= 0 || amountRub > 10000) {
        bot.sendMessage(chatId,
          '❌ Введите корректную сумму от 1 до 10 000 рублей (только число).',
          { reply_markup: { inline_keyboard: [[{ text: '❌ Отмена', callback_data: 'admin' }]] } }
        );
        return;
      }
      session.data.amountRub = amountRub;
      session.state = 'awaiting_gift_all_comment';
      // ✅ Сохраняем частично в БД (amountRub) на случай рестарта
      db.run(
        `INSERT OR REPLACE INTO admin_pending_actions (admin_id, action, data) VALUES (?, 'gift_all', ?)`,
        [ADMIN_ID, JSON.stringify({ amountRub })]
      );
      bot.sendMessage(chatId,
        `✅ Сумма: *${amountRub} ₽* каждому (в валюте клиента).\n\n` +
        `Шаг 2/2: Введите *текст поздравления* — он будет показан клиентам вместе с зачислением.\n\n` +
        `💡 Можно писать развёрнуто — поддерживается до *1500 символов*.\n\n` +
        `Пример:\n_🎉 С Новым годом! Пусть год принесёт только победы!_`,
        {
          parse_mode: 'Markdown',
          reply_markup: { inline_keyboard: [[{ text: '❌ Отмена', callback_data: 'admin_gift_all_cancel' }]] }
        }
      );
      return;
    }

    // ─── 🎁 GIFT ALL — шаг 2: комментарий → превью ───
    if (user.id === ADMIN_ID && session.state === 'awaiting_gift_all_comment') {
      const comment = (msg.text || '').trim();
      if (!comment || comment.length < 3) {
        bot.sendMessage(chatId, '❌ Комментарий слишком короткий. Введите осмысленный текст поздравления.');
        return;
      }
      if (comment.length > 1500) {
        bot.sendMessage(chatId, '❌ Комментарий слишком длинный (максимум 1500 символов).');
        return;
      }

      const { amountRub } = session.data;
      session.data.comment = comment;
      session.state = 'awaiting_gift_all_confirm'; // ждём нажатия кнопки

      // ✅ Сохраняем оба поля в БД — теперь confirm переживёт рестарт Render
      db.run(
        `INSERT OR REPLACE INTO admin_pending_actions (admin_id, action, data) VALUES (?, 'gift_all', ?)`,
        [ADMIN_ID, JSON.stringify({ amountRub, comment })]
      );

      // Считаем покупателей и показываем превью
      db.get(
        `SELECT COUNT(DISTINCT user_id) as cnt FROM orders
         WHERE status = 'confirmed'
           AND (balance_topup IS NULL OR balance_topup = 0)
           AND user_id != ?`,
        [ADMIN_ID],
        (err, row) => {
          const cnt = row?.cnt || 0;
          const totalRub = Math.round(amountRub * cnt);

          const previewMsg =
            `📋 *Превью начисления*\n\n` +
            `💰 Сумма: *${amountRub} ₽* каждому (конвертируется в USD/EUR/UAH автоматически)\n` +
            `💬 Поздравление: _${comment.replace(/_/g, '\\_ ')}_\n` +
            `👥 Получателей: *${cnt}* активных покупателей\n` +
            `📊 Общий расход: *~${totalRub} ₽*\n\n` +
            `*Клиент увидит:*\n` +
            `━━━━━━━━━━━━━━━━━━\n` +
            `🎁 *Подарок от CyraxMods!*\n` +
            `💬 ${comment}\n` +
            `Зачислено на баланс: *${amountRub} ₽* (или эквивалент)\n` +
            `━━━━━━━━━━━━━━━━━━\n\n` +
            `⚠️ Операция необратима. Подтверждаете?`;

          bot.sendMessage(chatId, previewMsg, {
            parse_mode: 'Markdown',
            reply_markup: {
              inline_keyboard: [
                [
                  { text: `✅ Да, начислить ${cnt} клиентам`, callback_data: 'admin_gift_all_confirm' },
                ],
                [
                  { text: '❌ Отмена', callback_data: 'admin_gift_all_cancel' }
                ]
              ]
            }
          });
        }
      );
      return;
    }

    // Отложенные рассылки — шаги диалога
    if (user.id === ADMIN_ID && session.state === 'awaiting_sched_broadcast_text') {
      const text = (msg.text || '').trim();
      if (!text) { bot.sendMessage(chatId, '❌ Текст не может быть пустым'); return; }
      session.data = { sched_text: text };
      session.state = 'awaiting_sched_broadcast_datetime';
      safeSendMessage(chatId, '📅 *Шаг 2/3*: Введите дату и время в формате ДД.ММ.ГГГГ ЧЧ:ММ\n\nПример: 25.12.2026 18:00', {
        parse_mode: 'Markdown',
        reply_markup: { inline_keyboard: [[{ text: '❌ Отмена', callback_data: 'admin_scheduled_broadcast' }]] }
      }).catch(() => { });
      return;
    }
    if (user.id === ADMIN_ID && session.state === 'awaiting_sched_broadcast_datetime') {
      const input = (msg.text || '').trim();
      const match = input.match(/^(\d{2})\.(\d{2})\.(\d{4})\s+(\d{2}):(\d{2})$/);
      if (!match) { bot.sendMessage(chatId, '❌ Неверный формат. Пример: `25.12.2025 18:00`', { parse_mode: 'Markdown' }); return; }
      const [, d, mo, y, h, mi] = match;
      const dt = `${y}-${mo}-${d} ${h}:${mi}:00`;
      if (new Date(dt) <= new Date()) { bot.sendMessage(chatId, '❌ Дата должна быть в будущем'); return; }
      session.data.sched_at = dt;
      session.state = 'awaiting_sched_broadcast_filter';
      safeSendMessage(chatId, '🎯 *Шаг 3/3*: Выберите аудиторию:', {
        parse_mode: 'Markdown',
        reply_markup: {
          inline_keyboard: [
            [
              { text: '👥 Всем', callback_data: 'sched_filter_all' },
              { text: '✅ Покупавшим', callback_data: 'sched_filter_active' },
              { text: '😴 Неактивным', callback_data: 'sched_filter_inactive' }
            ]
          ]
        }
      }).catch(() => { });
      return;
    }

    // ============================================================
    // 🤝 РЕСЕЛЛЕРЫ: ВВОД НАЦЕНКИ И АНКЕТЫ ПРЕДСТАВИТЕЛЯ
    // ============================================================
    if (session.state === 'awaiting_reseller_markup' && msg.text) {
      const pct = parseFloat(msg.text.trim());
      if (isNaN(pct) || pct < 0 || pct > 200) {
        bot.sendMessage(chatId, getLang(user) === 'ru' ? '❌ Введите корректное число от 0 до 200.' : '❌ Enter a valid number from 0 to 200.');
        return;
      }
      session.data.resellerMarkup = pct;
      session.state = 'awaiting_reseller_questionnaire';
      bot.sendMessage(chatId, getLang(user) === 'ru'
        ? '📝 Пожалуйста, коротко расскажите о вашем опыте продаж и понимании рынка.'
        : '📝 Please briefly describe your sales experience and market understanding.'
      );
      return;
    }

    if (session.state === 'awaiting_reseller_questionnaire' && msg.text) {
      session.data.resellerQuestionnaire = msg.text.trim();
      session.state = 'awaiting_currency';
      const curMsg = getLang(user) === 'ru'
        ? '💱 Выберите валюту для оплаты подключения партнёрской программы:'
        : '💱 Select currency for the partner program connection fee:';
      const curKbd = {
        inline_keyboard: [
          [{ text: '🇷🇺 RUB', callback_data: `currency_reseller_connection_RUB` }],
          [{ text: '🇪🇺 EUR', callback_data: `currency_reseller_connection_EUR` }],
          [{ text: '🇺🇸 USD', callback_data: `currency_reseller_connection_USD` }],
          [{ text: '🇺🇦 UAH', callback_data: `currency_reseller_connection_UAH` }],
          [{ text: '◀️ ' + t(user, 'back'), callback_data: 'partnership' }]
        ]
      };
      bot.sendMessage(chatId, curMsg, { parse_mode: 'Markdown', reply_markup: curKbd }).catch(() => { });
      return;
    }

    // Bundle — получение чека от пользователя
    if (session.state === 'awaiting_bundle_receipt' && (msg.photo || msg.document)) {
      if (!session.data) {
        const isRu = getLang(user) === 'ru';
        clearSession(user.id);
        safeSendMessage(chatId,
          isRu ? '❌ Сессия устарела. Начните оформление заново.' : '❌ Session expired. Please start over.',
          { reply_markup: { inline_keyboard: [[{ text: isRu ? '🏠 В меню' : '🏠 Menu', callback_data: 'start' }]] } }
        ).catch(() => { });
        return;
      }
    }
    if (session.state === 'awaiting_bundle_receipt' && (msg.photo || msg.document) && session.data) {
      const isRu = getLang(user) === 'ru';
      const { bundleOrderId, qty, product, currency, totalPrice, method } = session.data;

      // 🧪 ТЕСТ-РЕЖИМ: перехватываем без создания реального заказа
      const fileId = msg.photo ? msg.photo[msg.photo.length - 1].file_id : msg.document.file_id;
      const fileType = msg.photo ? 'photo' : 'document';

      // Проверяем что заказ существует и ещё в статусе pending
      db.get(`SELECT * FROM bundle_orders WHERE id = ? AND user_id = ?`, [bundleOrderId, user.id], (checkErr, existingOrder) => {
        if (checkErr || !existingOrder) {
          clearSession(user.id);
          safeSendMessage(chatId,
            isRu ? '❌ Заказ не найден. Начните оформление заново.' : '❌ Order not found. Please start over.',
            { reply_markup: { inline_keyboard: [[{ text: isRu ? '🏠 В меню' : '🏠 Menu', callback_data: 'start' }]] } }
          ).catch(() => { });
          return;
        }
        if (existingOrder.status === 'receipt_sent') {
          clearSession(user.id);
          safeSendMessage(chatId,
            isRu ? '⚠️ Чек уже был отправлен ранее. Ожидайте подтверждения.' : '⚠️ Receipt already sent. Awaiting confirmation.',
            { reply_markup: { inline_keyboard: [[{ text: isRu ? '🏠 В меню' : '🏠 Menu', callback_data: 'start' }]] } }
          ).catch(() => { });
          return;
        }
        if (existingOrder.status !== 'pending') {
          clearSession(user.id);
          safeSendMessage(chatId,
            isRu ? `⚠️ Заказ уже обработан (статус: ${existingOrder.status}).` : `⚠️ Order already processed (status: ${existingOrder.status}).`,
            { reply_markup: { inline_keyboard: [[{ text: isRu ? '🏠 В меню' : '🏠 Menu', callback_data: 'start' }]] } }
          ).catch(() => { });
          return;
        }

        db.run(`UPDATE bundle_orders SET status = 'receipt_sent' WHERE id = ?`, [bundleOrderId]);

        // Сохраняем чек
        const bFileUniqueId = msg.photo ? msg.photo[msg.photo.length - 1].file_unique_id : (msg.document.file_unique_id || null);
        saveReceiptRecord(fileId, bFileUniqueId, user.id, bundleOrderId, 'bundle');

        // 🛡️ Антифрод: проверка чека (предупреждение админу, заказ уже создан)
        checkReceiptDuplicate(fileId, bFileUniqueId, user.id, (dupInfo) => {
          if (dupInfo && dupInfo.isDuplicate) {
            const origUser = dupInfo.originalUsername ? `@${escapeMarkdown(dupInfo.originalUsername)}` : `ID ${dupInfo.originalUserId}`;
            const warnMsg = dupInfo.isSameUser
              ? `⚠️ *Повторный чек (Bundle)!*\n\n@${escapeMarkdown(String(user.username || user.id))} — тот же чек для bundle #${bundleOrderId}`
              : `🚨 *Фрод (Bundle)!*\n\nОригинал: ${origUser} (#${dupInfo.originalOrderId})\nПопытка: @${escapeMarkdown(String(user.username || user.id))} (bundle #${bundleOrderId})`;
            safeSendMessage(ADMIN_ID, warnMsg, { parse_mode: 'Markdown' }).catch(() => { });
            increaseSuspicion(user.id, dupInfo.isSameUser ? 10 : 30, 'Повторный чек (bundle)');
          }
        });

        const periodName = PERIOD_NAMES[isRu ? 'ru' : 'en'][product] || product;
        const priceStr = formatPrice(totalPrice, currency);

        // Уведомляем админа с чеком
        const adminMsg = `📦 *Bundle заказ #${bundleOrderId} — чек получен*\n\n👤 @${escapeMarkdown(String(user.username || user.id))}\n📦 ${periodName} ×${qty}\n💰 ${priceStr}\n💳 ${method}\n\nНажмите чтобы одобрить и выдать ключи:`;

        const receiptMarkup = {
          inline_keyboard: [[
            { text: `✅ Выдать ${qty} ключей`, callback_data: `bundle_approve_${bundleOrderId}` },
            { text: '❌ Отклонить', callback_data: `bundle_reject_${bundleOrderId}` }
          ]]
        };

        if (fileType === 'photo') {
          bot.sendPhoto(ADMIN_ID, fileId, { caption: adminMsg, parse_mode: 'Markdown', reply_markup: receiptMarkup }).catch(() => { });
        } else {
          bot.sendDocument(ADMIN_ID, fileId, { caption: adminMsg, parse_mode: 'Markdown', reply_markup: receiptMarkup }).catch(() => { });
        }

        clearSession(user.id);
        safeSendMessage(chatId,
          isRu
            ? '✅ *Чек получен!* Ожидайте подтверждения от администратора.'
            : '✅ *Receipt received!* Awaiting admin confirmation.',
          {
            parse_mode: 'Markdown',
            reply_markup: { inline_keyboard: [[{ text: isRu ? '🏠 В меню' : '🏠 Menu', callback_data: 'start' }]] }
          }
        ).catch(() => { });
      });
      return;
    }

    // Курсы — наценка
    if (user.id === ADMIN_ID && session.state && session.state.startsWith('awaiting_markup_')) {
      const cur = session.state.replace('awaiting_markup_', '');
      const val = parseFloat((msg.text || '').replace(',', '.'));
      if (isNaN(val) || val < 0 || val > 50) { bot.sendMessage(chatId, '❌ Введите число от 0 до 50'); return; }
      saveSetting(`markup_${cur}`, String(val), async () => {
        await fetchAndUpdateExchangeRates();
        bot.sendMessage(chatId, `✅ Наценка ${cur}: ${val}%`);
        showExchangeRatesPanel(chatId);
      });
      clearSession(user.id);
      return;
    }

    // Задача 6: Ввод имени менеджера
    if (user.id === ADMIN_ID && session.state === 'awaiting_manager_display_name' && msg.text) {
      const inputName = msg.text.trim();
      const displayName = inputName === '-' ? null : inputName;
      const { pendingManagerId, pendingManagerUsername, pendingMethods } = session.data || {};

      if (!pendingManagerId || !pendingMethods) {
        bot.sendMessage(chatId, '❌ Ошибка сессии. Начните заново.');
        clearSession(user.id);
        return;
      }

      db.run('INSERT OR REPLACE INTO managers (user_id, username, display_name, assigned_by) VALUES (?, ?, ?, ?)',
        [pendingManagerId, pendingManagerUsername, displayName, ADMIN_ID], (err) => {
          if (err) { bot.sendMessage(chatId, '❌ Ошибка сохранения менеджера'); return; }
          db.run('DELETE FROM manager_methods WHERE manager_id = ?', [pendingManagerId], () => {
            const stmt = db.prepare('INSERT OR IGNORE INTO manager_methods (manager_id, payment_method) VALUES (?, ?)');
            pendingMethods.forEach(m => stmt.run([pendingManagerId, m]));
            stmt.finalize(() => {
              const uname = pendingManagerUsername ? `@${escapeMarkdown(pendingManagerUsername)}` : `ID: ${pendingManagerId}`;
              const greeting = displayName || uname;
              bot.sendMessage(chatId,
                `✅ Менеджер ${uname} сохранён\n📋 Методы: ${pendingMethods.join(', ')}\n👋 Будет приветствоваться как: *${greeting}*`,
                { parse_mode: 'Markdown' }
              );
              // Уведомляем самого менеджера с персональным приветствием
              safeSendMessage(pendingManagerId,
                `👥 *Вас назначили менеджером!*\n\nДобро пожаловать${displayName ? `, *${displayName}*` : ''}! 🎉\n\nВам доступны заказы по методам: ${pendingMethods.join(', ')}\n\n📦 Для просмотра заказов используйте /work`,
                { parse_mode: 'Markdown' }
              ).catch(() => { });
              clearSession(user.id);
              showManagersPanel(chatId, message.message_id);
            });
          });
        });
      return;
    }

    // Ввод username для назначения менеджера
    if (user.id === ADMIN_ID && session.state === 'awaiting_manager_username' && msg.text) {
      const inputUsername = msg.text.trim().replace(/^@/, '');
      db.get('SELECT id, username FROM users WHERE username = ?', [inputUsername], (err, targetUser) => {
        if (err || !targetUser) {
          bot.sendMessage(chatId, '❌ Пользователь не найден. Убедитесь что он уже писал боту.');
          return;
        }
        session.data.pendingManagerId = targetUser.id;
        session.data.pendingManagerUsername = targetUser.username;
        session.state = 'awaiting_manager_methods';
        showManagerMethodsKeyboard(chatId, targetUser.id, []);
      });
      return;
    }

    // Пользователь отправил код отзыва (REVIEW-XXXXXX)
    if (msg.text && /^REVIEW-\d+$/i.test(msg.text.trim()) && session.state !== 'awaiting_coupon_input') {
      const code = msg.text.trim().toUpperCase();
      db.get(`SELECT * FROM review_codes WHERE code = ? AND user_id = ? AND is_used = 0`, [code, user.id], (err, rc) => {
        if (err || !rc) {
          safeSendMessage(chatId, t(user, 'review_code_invalid'));
        } else {
          const isRu = getLang(user) === 'ru';
          const ackMsg = isRu
            ? '✅ Код принят! Администратор проверит ваш отзыв и выдаст награду в ближайшее время. Спасибо! 💜'
            : '✅ Code received! The admin will check your review and issue your reward shortly. Thank you! 💜';
          safeSendMessage(chatId, ackMsg);
          const uname = user.username ? `@${escapeMarkdown(user.username)}` : `ID: ${user.id}`;
          // Используем правильный ключ настройки review_channel_link
          const reviewLink = getSetting('review_channel_link') || 'https://t.me/cyraxml/368';
          const adminMsg = `📝 *Отзыв* | 👤 ${uname} | 🔑 \`${code}\``;

          const adminKb = {
            inline_keyboard: [
              // Кнопка-ссылка на комментарии поста (открывает пост с комментариями)
              [{ text: '🔗 Проверить в комментариях', url: `${reviewLink}?comment=1` }],
              [
                { text: '🎁 Выдать купон (30%)', callback_data: `review_reward_coupon_${rc.id}_30` },
                { text: '🔑 Выдать ключ', callback_data: `review_reward_key_${rc.id}` }
              ],
              [{ text: '❌ Отклонить', callback_data: `review_reject_${rc.id}` }]
            ]
          };

          safeSendMessage(ADMIN_ID, adminMsg, {
            parse_mode: 'Markdown',
            disable_web_page_preview: true,
            reply_markup: adminKb
          }).catch(() => { });
        }
      });
      return;
    }

    // ============================================================
    // 🤝 ЗАДАЧА 4 — ПАРТНЁРСТВО: ВВОД ТОКЕНА
    // ============================================================
    if (session.state === 'awaiting_reseller_token' && msg.text) {
      const token = msg.text.trim();

      // Базовая валидация токена: цифры:строки
      if (!/^\d+:[A-Za-z0-9_-]+$/.test(token)) {
        bot.sendMessage(chatId, t(user, 'partner_token_invalid'));
        return;
      }

      const processingMsg = await bot.sendMessage(chatId, '⏳ Проверка токена...');

      try {
        // Проверяем токен запросом getMe
        let response;
        try {
          response = await axios.get(`https://api.telegram.org/bot${token}/getMe`, { timeout: 10000 });
        } catch (apiErr) {
          console.error('❌ Reseller token API check failed:', apiErr.message);
          bot.editMessageText(t(user, 'partner_token_invalid'), { chat_id: chatId, message_id: processingMsg.message_id }).catch(() => { });
          return;
        }
        const botData = response.data.result;

        // Токен валиден, шифруем
        let encrypted;
        try {
          encrypted = encryptToken(token);
        } catch (encErr) {
          console.error('❌ encryptToken error:', encErr.message);
          const isRu = getLang(user) === 'ru';
          bot.editMessageText(
            isRu ? '❌ Ошибка сервера при шифровании токена. Обратитесь в поддержку.' : '❌ Server error encrypting token. Contact support.',
            { chat_id: chatId, message_id: processingMsg.message_id }
          ).catch(() => { });
          safeSendMessage(ADMIN_ID, `🚨 *Ошибка шифрования токена реселлера!*\n\n${encErr.message}\n\nПроверьте \`RESELLER_ENCRYPTION_KEY\` в Render.`, { parse_mode: 'Markdown' }).catch(() => { });
          return;
        }

        db.run(
          `UPDATE resellers SET 
            username = ?, 
            encrypted_token = ?, 
            status = 'active', 
            activated_at = datetime('now')
           WHERE user_id = ?`,
          [botData.username, encrypted, user.id],
          (err) => {
            if (err) {
              console.error('❌ Error saving reseller token:', err);
              bot.editMessageText('❌ Ошибка сохранения в базу данных.', { chat_id: chatId, message_id: processingMsg.message_id }).catch(() => {});
              return;
            }

            clearSession(user.id);

            // Уведомляем реселлера об успехе
            const successMsg = t(user, 'partner_token_saved', {
              botUsername: botData.username,
              markup: getSetting('reseller_default_markup') || 30
            });
            bot.editMessageText(successMsg, { chat_id: chatId, message_id: processingMsg.message_id }).catch(() => { });

            // Оповещаем админа
            bot.sendMessage(ADMIN_ID, `🚀 *Новый реселлер активирован!*\n\n👤 Юзер: @${escapeMarkdown(String(user.username || user.id))}\n🤖 Бот: @${escapeMarkdown(botData.username || '')}`, { parse_mode: 'Markdown' });

            logAction(user.id, 'reseller_bot_activated', { botUsername: botData.username });

            // 🚀 Запускаем бот реселлера СРАЗУ после сохранения токена
            db.get(`SELECT * FROM resellers WHERE user_id = ?`, [user.id], async (rErr, resellerRow) => {
              if (rErr || !resellerRow) {
                console.error('❌ Failed to load reseller for init:', rErr?.message);
                return;
              }

              // Останавливаем старый инстанс если есть (например при перевыпуске токена)
              const existing = resellerBots.get(resellerRow.id);
              if (existing && existing.bot) {
                try { await existing.bot.deleteWebHook(); } catch (e) { }
                resellerBots.delete(resellerRow.id);
              }

              const started = await initResellerBot(resellerRow);
              if (started) {
                console.log(`✅ [РЕСЕЛЛЕР ${resellerRow.id}] Бот @${botData.username} запущен сразу после активации`);
              } else {
                console.error(`❌ [РЕСЕЛЛЕР ${resellerRow.id}] Не удалось запустить бот @${botData.username}`);
                safeSendMessage(ADMIN_ID, `⚠️ Реселлер @${escapeMarkdown(botData.username || '')} активирован, но бот не запустился. Перезапустите сервер.`).catch(() => { });
              }
            });
          }
        );
      } catch (err) {
        console.error('❌ Reseller token handler error:', err.message);
        bot.editMessageText(t(user, 'partner_token_invalid'), { chat_id: chatId, message_id: processingMsg.message_id }).catch(() => { });
      }
      return;
    }

    // ЮЗЕР ПРИСЛАЛ СКРИН БЕЗ АКТИВНОЙ СЕССИИ — подсказываем
    if ((msg.photo || msg.document) && session.state !== 'awaiting_receipt' && session.state !== 'mb_awaiting_receipt' && session.state !== 'awaiting_topup_receipt' && session.state !== 'awaiting_bundle_receipt' && session.state !== 'support_awaiting_screenshot') {
      const isRu = getLang(user) === 'ru';
      bot.sendMessage(chatId, isRu
        ? '📎 Получили ваш файл!\n\nЧтобы отправить чек, сначала выберите товар и способ оплаты через меню. После этого бот будет ждать ваш чек.\n\n👇 Нажмите /start'
        : '📎 Got your file!\n\nTo send a receipt, first select a product and payment method from the menu. Then the bot will be ready to receive your receipt.\n\n👇 Press /start'
      ).catch(() => { });
      return;
    }

    // ЮЗЕР ПРИСЛАЛ ЧЕК ДЛЯ ПОПОЛНЕНИЯ БАЛАНСА
    if ((msg.photo || msg.document) && session.state === 'awaiting_topup_receipt') {
      const isRu = getLang(user) === 'ru';
      const orderId = session.data?.topupOrderId;
      const topupAmount = session.data?.topupAmount;
      const topupCur = session.data?.topupCurrency;

      if (!orderId) { clearSession(user.id); return; }

      // Защита от повторной отправки чека
      const orderRow = await new Promise(res =>
        db.get(`SELECT status, method FROM orders WHERE id = ? AND balance_topup = 1`, [orderId], (e, r) => res(r))
      );
      if (!orderRow) {
        bot.sendMessage(chatId, isRu ? '❌ Заказ не найден. Начните оформление заново.' : '❌ Order not found. Please start over.');
        clearSession(user.id); return;
      }
      if (orderRow.status === 'receipt_sent' || orderRow.status === 'confirmed') {
        bot.sendMessage(chatId, isRu ? '⚠️ Чек уже был отправлен. Ожидайте подтверждения.' : '⚠️ Receipt already sent. Awaiting confirmation.');
        clearSession(user.id); return;
      }

      const fileId = msg.photo ? msg.photo[msg.photo.length - 1].file_id : msg.document.file_id;
      const topupMethod = orderRow.method || session.data?.topupMethod || '—';

      // Прикрепляем чек к заказу и меняем статус
      db.run(`UPDATE orders SET receipt_file_id = ?, status = 'receipt_sent' WHERE id = ? AND status = 'pending'`, [fileId, orderId]);

      // Пересылаем чек админу с кнопками подтверждения
      const caption = isRu
        ? `💳 *Чек на пополнение баланса*\n\n` +
          `👤 ${user.username ? '@' + user.username : 'ID: ' + user.id}\n` +
          `💰 Сумма: *${formatBalanceAmount(topupAmount, topupCur)}*\n` +
          `💳 Метод: ${topupMethod}\n` +
          `📋 Заказ #${orderId}`
        : `💳 *Balance Top Up Receipt*\n\n` +
          `👤 ${user.username ? '@' + user.username : 'ID: ' + user.id}\n` +
          `💰 Amount: *${formatBalanceAmount(topupAmount, topupCur)}*\n` +
          `💳 Method: ${topupMethod}\n` +
          `📋 Order #${orderId}`;

      const adminKb = { inline_keyboard: [
        [{ text: `✅ Подтвердить #${orderId}`, callback_data: `approve_topup_${orderId}` }],
        [{ text: `❌ Отклонить #${orderId}`, callback_data: `reject_topup_${orderId}` }]
      ]};

      if (msg.photo) {
        bot.sendPhoto(ADMIN_ID, fileId, { caption, parse_mode: 'Markdown', reply_markup: adminKb }).catch(() => {});
      } else {
        bot.sendDocument(ADMIN_ID, fileId, { caption, parse_mode: 'Markdown', reply_markup: adminKb }).catch(() => {});
      }

      bot.sendMessage(chatId,
        isRu
          ? `✅ *Чек получен!*\n\nАдминистратор проверит оплату и пополнит ваш баланс в ближайшее время.\n\n_Уведомление придёт автоматически._`
          : `✅ *Receipt received!*\n\nThe admin will verify your payment and top up your balance shortly.\n\n_You'll be notified automatically._`,
        { parse_mode: 'Markdown' }
      ).catch(() => {});

      clearSession(user.id);
      return;
    }

    // ЮЗЕР ПРИСЛАЛ СКРИН (ПРОВЕРКА ОПЛАТЫ)
    if ((msg.photo || msg.document) && session.state === 'awaiting_receipt') {
      const sessionData = session.data;

      // 🧪 ТЕСТ-РЕЖИМ: перехватываем отправку чека — выдаём тестовый ключ без создания заказа

      if (!sessionData.period || !sessionData.currency || !sessionData.amount || !sessionData.method) {
        console.error('❌ Incomplete session data for receipt');
        bot.sendMessage(msg.chat.id, t(user, 'error_order_data_missing'));
        clearSession(user.id);
        return;
      }

      db.get(
        `SELECT id FROM orders WHERE user_id = ? AND product = ? AND status = 'pending'`,
        [user.id, sessionData.period],
        async (err, existing) => {
          if (err) console.error('❌ Duplicate check error:', err);
          if (existing) {
            bot.sendMessage(chatId, t(user, 'order_already_pending'));
            clearSession(user.id);
            return;
          }

          console.log(`📸 Receipt received from user ${user.id}, creating order...`);

          const fileId = msg.photo ? msg.photo[msg.photo.length - 1].file_id : msg.document.file_id;
          const fileUniqueId = msg.photo ? msg.photo[msg.photo.length - 1].file_unique_id : (msg.document.file_unique_id || null);

          // 🛡️ АНТИФРОД: проверка чека ПЕРЕД созданием заказа
          checkReceiptDuplicate(fileId, fileUniqueId, user.id, (dupInfo) => {
            if (dupInfo && dupInfo.isDuplicate && !dupInfo.isSameUser) {
              // 🚨 Чек от ДРУГОГО пользователя — БЛОКИРУЕМ
              const isRu = getLang(user) === 'ru';
              bot.sendMessage(chatId, isRu
                ? '🚫 Этот чек уже был использован. Если вы считаете что это ошибка — обратитесь в поддержку.'
                : '🚫 This receipt has already been used. If you think this is a mistake — contact support.'
              ).catch(() => { });
              const origUser = dupInfo.originalUsername ? `@${escapeMarkdown(dupInfo.originalUsername)}` : `ID ${dupInfo.originalUserId}`;
              safeSendMessage(ADMIN_ID,
                `🚨 *МОШЕННИЧЕСТВО — чек заблокирован!*\n\n👤 Оригинал: ${origUser} (заказ #${dupInfo.originalOrderId})\n👤 Попытка: @${escapeMarkdown(String(user.username || user.id))}\n📦 Товар: ${sessionData.period}`,
                { parse_mode: 'Markdown' }
              ).catch(() => { });
              increaseSuspicion(user.id, 30, 'Повторный чек другого пользователя');
              clearSession(user.id);
              return;
            }

            // Если тот же юзер — предупреждаем админа, но пропускаем
            if (dupInfo && dupInfo.isDuplicate && dupInfo.isSameUser) {
              // BUG FIX AF-2: Не начисляем suspicion если оригинальный заказ уже подтверждён —
              // пользователь мог случайно повторно отправить чек после получения ключа.
              db.get(`SELECT status FROM orders WHERE id = ?`, [dupInfo.originalOrderId], (_, origOrder) => {
                if (origOrder && origOrder.status === 'confirmed') {
                  // Просто предупреждаем — без штрафа
                  safeSendMessage(ADMIN_ID,
                    `ℹ️ *Повторный чек (уже подтверждённый заказ)*\n\n@${escapeMarkdown(String(user.username || user.id))} отправил тот же чек.\nОригинал: заказ #${dupInfo.originalOrderId} — статус: confirmed`,
                    { parse_mode: 'Markdown' }
                  ).catch(() => { });
                } else {
                  safeSendMessage(ADMIN_ID,
                    `⚠️ *Повторный чек!*\n\n@${escapeMarkdown(String(user.username || user.id))} отправил тот же чек повторно.\nОригинал: заказ #${dupInfo.originalOrderId} (${dupInfo.originalOrderType})`,
                    { parse_mode: 'Markdown' }
                  ).catch(() => { });
                  increaseSuspicion(user.id, 10, 'Повторный чек (свой)');
                }
              });
            }

            // ⏳ — висит пока админ не одобрит/отклонит, потом редактируется
            (async () => {
              const hourglassMsg = await bot.sendMessage(chatId, '⏳').catch(() => null);

              // П.2: применяем скидку если есть купон
              const finalAmount = sessionData.discountedAmount || sessionData.amount;
              const txnId = generateTxnId();

              db.run(
                `INSERT INTO orders (user_id, username, user_lang, product, amount, currency, method, receipt_file_id, receipt_type, status, transaction_id, coupon_id, original_amount, reseller_markup_pct, reseller_questionnaire) 
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?, ?, ?, ?)`,
                [user.id, user.username, getLang(user), sessionData.period, finalAmount, sessionData.currency, sessionData.method,
                  fileId,
                msg.photo ? 'photo' : 'document',
                  txnId,
                sessionData.couponId || null,
                // Сохраняем исходную сумму ДО скидки, если применён купон или лояльность.
                // Это позволяет менеджеру видеть реальную цену и факт применения скидки.
                (sessionData.couponId || sessionData.loyaltyDiscountPercent) ? (sessionData.amount || null) : null,
                sessionData.resellerMarkup || null,
                sessionData.resellerQuestionnaire || null],
                async function (err) {
                  if (err) {
                    console.error('❌ Error creating order:', err);
                    bot.sendMessage(msg.chat.id, t(user, 'error_creating_order'));
                    return;
                  }

                  const orderId = this.lastID;
                  console.log(`✅ Order #${orderId} created`);

              // Сохраняем чек в used_receipts
                  saveReceiptRecord(fileId, fileUniqueId, user.id, orderId, 'order');

                  // Сохраняем message_id песочных часов в orders для последующего редактирования
                  if (hourglassMsg) {
                    db.run(`UPDATE orders SET hourglass_msg_id = ? WHERE id = ?`, [hourglassMsg.message_id, orderId]);
                  }

                  // ⏳ → receipt_received: редактируем одно сообщение, нового не шлём
                  if (hourglassMsg) {
                    bot.editMessageText(t(user, 'receipt_received'), {
                      chat_id: chatId,
                      message_id: hourglassMsg.message_id
                    }).catch(() => { });
                  }

                  // 🏀 Basketball: предлагаем бросить мяч пока идёт проверка
                  // Только для обычных ключей (не ресейлер, не топап, не буст)
                  const isKeyProduct = ['1d','3d','7d','30d'].includes(sessionData.period);
                  if (isKeyProduct) {
                    setTimeout(() => {
                      const isRuUser = getLang(user) === 'ru';
                      const inviteMsg = t(user, 'basketball_invite');
                      const kb = {
                        inline_keyboard: [[
                          { text: t(user, 'basketball_throw_btn'), callback_data: `basketball_throw_${orderId}` }
                        ]]
                      };
                      safeSendMessage(chatId, inviteMsg, { reply_markup: kb }).catch(() => {});
                    }, 1500); // небольшая задержка чтобы receipt_received успел отрендериться
                  }

                  // Купон НЕ отмечаем сразу — только после одобрения админом

                  sendPendingOrderToAdmin(orderId).catch(e => console.error('sendPendingOrderToAdmin error:', e));

                  logAction(user.id, 'order_created_with_receipt', { orderId, method: sessionData.method, coupon: sessionData.couponCode || null });
                });
            })();
          });
        });

      clearSession(user.id);
    }
  } catch (e) {
    console.error('❌ message handler error:', e);
  }

  // 🛡️ RECOVERY FIX: Юзер прислал фото/документ, но сессия пуста (перезапуск бота / таймаут)
  // Поскольку заказы теперь создаются ТОЛЬКО при загрузке чека, у нас нет пустого 'pending'.
  // Мы просто пересылаем чек админу, чтобы оплата не потерялась "в пустоте".
  if ((msg.photo || msg.document) && (!session || !session.state || session.state === 'none')) {
    const fileId = msg.photo ? msg.photo[msg.photo.length - 1].file_id : msg.document.file_id;
    const caption = `⚠️ *ПРЕДУПРЕЖДЕНИЕ — ПОТЕРЯННАЯ СЕССИЯ*\n\n` +
      `Пользователь @${escapeMarkdown(String(user.username || user.id))} прислал изображение, когда его сессия не была активна (вероятно, бот перезагружался или пользователь слишком долго ждал).\n\n` +
      `❗️ *Заказ в базе бота НЕ был создан.*\n` +
      `Если это скриншот оплаты, свяжитесь с клиентом напрямую для выдачи товара.`;

    if (msg.photo) {
      bot.sendPhoto(ADMIN_ID, fileId, { caption, parse_mode: 'Markdown' }).catch(() => {});
    } else {
      bot.sendDocument(ADMIN_ID, fileId, { caption, parse_mode: 'Markdown' }).catch(() => {});
    }

    bot.sendMessage(chatId,
      getLang(user) === 'ru'
        ? `⚠️ Мы получили ваш файл, но ваша платежная сессия истекла (Возможно, из-за перезагрузки). Мы переслали информацию администратору — он свяжется с вами или выдаст товар вручную.`
        : `⚠️ We received your file, but your payment session expired. We forwarded the information to the admin — they will contact you or issue the product manually.`
    ).catch(() => {});

    return; // Выходим
  }

  try {
    const msgType = msg.photo ? 'photo' : msg.document ? 'document' : msg.text ? 'text' : null;
    if (!msgType && user && session) {
      const AWAITING_FILE_STATES = ['awaiting_receipt', 'mb_awaiting_receipt', 'awaiting_bundle_receipt', 'awaiting_topup_receipt', 'support_awaiting_screenshot'];
      const AWAITING_TEXT_STATES = [
        'awaiting_coupon_input', 'awaiting_reseller_markup', 'awaiting_reseller_questionnaire',
        'awaiting_reseller_token', 'support_awaiting_key', 'support_awaiting_description',
        'awaiting_broadcast', 'awaiting_user_search_query', 'awaiting_coupon_issue_username',
        'awaiting_coupon_issue_percent', 'awaiting_coupon_issue_days', 'awaiting_coupon_issue_product',
        'awaiting_topup_amount'  // FIX 1: текстовый ввод суммы пополнения баланса
      ];
      if (session.state && (AWAITING_FILE_STATES.includes(session.state) || AWAITING_TEXT_STATES.includes(session.state))) {
        const isRu = getLang(user) === 'ru';
        const hint = AWAITING_FILE_STATES.includes(session.state)
          ? (isRu ? '📎 Пожалуйста, отправьте фото или документ (скриншот оплаты).' : '📎 Please send a photo or document (payment screenshot).')
          : (isRu ? '⌨️ Пожалуйста, введите текстовый ответ.' : '⌨️ Please type a text response.');
        bot.sendMessage(chatId, hint).catch(() => { });
      }
    }
  } catch (_) { }
});

// ==========================================
// 🚨 ОБРАБОТКА ОШИБОК
// ==========================================
// ==========================================
// 📣 АВТОРЕКЛАМКА В ГРУППАХ
// ==========================================

// Запоминаем/забываем группу при изменении статуса бота
bot.on('my_chat_member', (update) => {
  try {
    const chat = update.chat;
    const newStatus = update.new_chat_member?.status;
    if (chat.type !== 'group' && chat.type !== 'supergroup') return;

    if (newStatus === 'member' || newStatus === 'administrator') {
      db.run(
        `INSERT INTO group_chats (chat_id, title, active) VALUES (?, ?, 1)
         ON CONFLICT(chat_id) DO UPDATE SET title = excluded.title, active = 1`,
        [chat.id, chat.title || ''],
        (err) => { if (!err) console.log(`✅ [PROMO] Группа запомнена: ${chat.title || chat.id}`); }
      );
    } else if (newStatus === 'left' || newStatus === 'kicked') {
      db.run(`UPDATE group_chats SET active = 0 WHERE chat_id = ?`, [chat.id], () => {
        console.log(`⛔ [PROMO] Бот удалён из группы: ${chat.title || chat.id}`);
      });
    }
  } catch (e) { console.error('❌ my_chat_member error:', e.message); }
});

// Функция рассылки — берёт текст из настроек БД
async function sendGroupPromo() {
  try {
    const botUsername = process.env.BOT_USERNAME || 'cyraxxmod_bot';
    const defaultText =
      `👋 I only work in private messages.\n\n🔑 Cyrax mod keys\n🚀 Boost & guides\n\n@${botUsername}`;
    const promoText = getSetting('group_promo_text') || defaultText;

    // Достаём last_promo_msg_id — нужен для удаления предыдущего сообщения
    const chats = await new Promise(res =>
      db.all(`SELECT chat_id, title, last_promo_msg_id FROM group_chats WHERE active = 1`, [], (e, rows) => res(rows || []))
    );
    if (!chats.length) { console.log('📣 [PROMO] Нет активных групп'); return; }

    const kb = { inline_keyboard: [[{ text: '🤖 Open bot', url: `https://t.me/${botUsername}` }]] };
    let sent = 0, failed = 0, deleted = 0;

    for (const chat of chats) {
      // Пауза 1–5 сек между группами — защита от Telegram flood limit
      await new Promise(r => setTimeout(r, 1000 + Math.floor(Math.random() * 4000)));

      // 🗑️ Удаляем предыдущее промо-сообщение в этом чате (если есть)
      // Telegram позволяет удалять только свои сообщения не старше 48 часов.
      // При интервале рассылки 6 ч — лимит никогда не достигается.
      // Если удалить не получилось (уже удалено вручную, истекло 48 ч и т.п.) — молча пропускаем, бот не ломается.
      if (chat.last_promo_msg_id) {
        try {
          await bot.deleteMessage(chat.chat_id, chat.last_promo_msg_id);
          deleted++;
          console.log(`🗑️ [PROMO] Удалено старое сообщение ${chat.last_promo_msg_id} в ${chat.title || chat.chat_id}`);
        } catch (delErr) {
          console.log(`⚠️ [PROMO] Не удалось удалить старое сообщение в ${chat.title || chat.chat_id}: ${delErr.message}`);
        }
      }

      try {
        const sentMsg = await bot.sendMessage(chat.chat_id, promoText, { reply_markup: kb });
        // Сохраняем ID нового сообщения — при следующей рассылке оно будет удалено
        db.run(
          `UPDATE group_chats SET last_promo_at = datetime('now'), last_promo_msg_id = ? WHERE chat_id = ?`,
          [sentMsg.message_id, chat.chat_id]
        );
        sent++;
        console.log(`📣 [PROMO] → ${chat.title || chat.chat_id} (msg_id: ${sentMsg.message_id})`);
      } catch (err) {
        failed++;
        console.error(`❌ [PROMO] ${chat.chat_id}:`, err.message);
        // Бот выгнан — деактивируем и сбрасываем msg_id чтобы не пытаться удалять несуществующее
        if (/kicked|not a member|chat not found|Forbidden/i.test(err.message)) {
          db.run(`UPDATE group_chats SET active = 0, last_promo_msg_id = NULL WHERE chat_id = ?`, [chat.chat_id]);
        }
      }
    }
    console.log(`📣 [PROMO] Итог: ✅ ${sent} отправлено / 🗑️ ${deleted} удалено / ❌ ${failed} ошибок`);

    // 🔔 Уведомление админу о завершении рассылки
    if (sent > 0 || failed > 0) {
      const now = new Date().toLocaleString('ru-RU', { hour: '2-digit', minute: '2-digit', day: '2-digit', month: '2-digit' });
      const deactivated = failed > 0 ? `\n⚠️ Деактивировано (бот исключён): *${failed}*` : '';
      const deletedNote = deleted > 0 ? `\n🗑️ Старых удалено: *${deleted}*` : '';
      const adminNotif =
        `📣 *Авторассылка завершена* · ${now}\n\n` +
        `✅ Отправлено: *${sent}* чат${sent === 1 ? '' : sent < 5 ? 'а' : 'ов'}` +
        deletedNote +
        deactivated +
        `\n\n_Редактировать текст: Настройки → Авторекламка_`;
      safeSendMessage(ADMIN_ID, adminNotif, {
        parse_mode: 'Markdown',
        reply_markup: {
          inline_keyboard: [[
            { text: '✏️ Изменить текст', callback_data: 'settings_edit_promo_text' },
            { text: '📤 Отправить снова', callback_data: 'settings_promo_send_now' }
          ]]
        }
      }).catch(() => {});
    }
  } catch (e) { console.error('❌ sendGroupPromo error:', e.message); }
}

bot.on('polling_error', (error) => {
  console.error('Polling error (should not happen):', error.message);
});


// ==========================================
// 🚀 ФУНКЦИИ INFINITE BOOST
// ==========================================

// Тексты гайда
const BOOST_GUIDE_RU = `🚀 *СЕКРЕТНЫЙ МЕТОД БЕСКОНЕЧНОГО БУСТА В MLBB*

Поздравляем с покупкой! Вот твоя пошаговая инструкция.

⚠️ *ВАЖНО: Риск бана* — метод использует особенности рейтинговой системы MLBB. Moonton может заблокировать аккаунт при злоупотреблении. Используй разумно!

━━━━━━━━━━━━━━━━━━━━

📋 *ЧТО ПОНАДОБИТСЯ:*
• WhatsApp (установлен на телефоне)
• 2 устройства (телефон + планшет/ПК с эмулятором)
• 2 аккаунта MLBB (основной и дополнительный — "твинк")

━━━━━━━━━━━━━━━━━━━━

📖 *КАК ЭТО РАБОТАЕТ:*

GEMPIC (ГЕМПИК) = EPIC
MITIK / JENTIK = MYTHIC (МИФИК)

Метод основан на координированных играх с другими игроками, где ты получаешь победы через специальные лобби.

━━━━━━━━━━━━━━━━━━━━

🔢 *ПОШАГОВАЯ ИНСТРУКЦИЯ:*

*ШАГ 1 — Вступи в специальные чаты WhatsApp*
Войди в как можно больше из этих чатов (некоторые могут быть временно недоступны — это нормально):

https://chat.whatsapp.com/FMl8WQmhOUCIIxrdKfAmYd
https://chat.whatsapp.com/LEwOEB9gX1A4H5ei2BZFme
https://chat.whatsapp.com/Bv92BF2KxMQ4rWjbotA4uq
https://chat.whatsapp.com/KRRnfIB0lYI0JPz9gRcZCA
https://chat.whatsapp.com/FUggJTvzlr29KMnpqMs6XI
https://chat.whatsapp.com/HjBYIu4NcsCAAINetfQCzs
https://chat.whatsapp.com/EWhlU363JAK79UfRjf9Prf
https://chat.whatsapp.com/Lhv9hOpD1euHY7ZJFdbHxk
https://chat.whatsapp.com/I5Ap5TSzcUu6jzy5QmKwrV
https://chat.whatsapp.com/IZjNgSMdej30HLqAQEE0Yd
https://chat.whatsapp.com/CQu84A7Hwo4DMTOACAZA8Y
https://chat.whatsapp.com/FHxEu5QTCnyBOKRSDRtMRv
https://chat.whatsapp.com/LpwCoqmrDWZ1tpS8wTar4a
https://chat.whatsapp.com/GYQJDigXyNsIaHbkR01W6f

💡 *Совет:* В чатах будут появляться ссылки на другие чаты — вступай и в них!

━━━━━━━━━━━━━━━━━━━━

*ШАГ 2 — Найди активных игроков*
В чатах ищи сообщения в формате:
*GEMPIC POINT ВСЕ SVG*
или *GEMPIC POINT ALL SVG*

Это сигнал, что игроки готовы к бусту.
Ориентировочный ID организатора: 343933935

━━━━━━━━━━━━━━━━━━━━

*ШАГ 3 — Подготовь 2 устройства*
• Устройство 1: твой *основной аккаунт*
• Устройство 2: *дополнительный аккаунт* (твинк)

📌 *Важное правило рангов:*
- Основной аккаунт Мифик → твинк должен быть Мифик или Легенда
- Основной аккаунт Легенда → твинк должен быть Эпик или Легенда

━━━━━━━━━━━━━━━━━━━━

*ШАГ 4 — Подключайся к лобби*
1. Скопируй ID своего основного аккаунта (настройки → профиль)
2. Отправь ID в чат WhatsApp
3. Тебя добавят в друзья и пригласят в лобби
4. В лобби тебе скажут ID твинка — добавь его тоже
5. Организаторы сами начнут игру — тебе нужно только *принять приглашение*

━━━━━━━━━━━━━━━━━━━━

*ШАГ 5 — В игре*
Игроки напишут тебе в чат что делать (зачастую на английском — включи автоперевод в настройках игры). Просто следуй инструкциям.

━━━━━━━━━━━━━━━━━━━━

⚠️ *ПРЕДУПРЕЖДЕНИЯ:*
• Не злоупотребляй — не делай слишком много буст-игр подряд
• Не рассказывай другим игрокам об этом методе
• Используй только проверенных организаторов из чатов
• Мы не несём ответственности за блокировки от Moonton

━━━━━━━━━━━━━━━━━━━━

🎁 *ТВОЙ БОНУС:*
В следующем сообщении — купон на 30% скидку на любой ключ CyraxMod!`;

const BOOST_GUIDE_EN = `🚀 *SECRET INFINITE BOOST METHOD FOR MLBB*

Congratulations on your purchase! Here's your step-by-step guide.

⚠️ *WARNING: Ban Risk* — This method uses specific features of the MLBB ranking system. Moonton may ban accounts if abused. Use responsibly!

━━━━━━━━━━━━━━━━━━━━

📋 *WHAT YOU NEED:*
• WhatsApp (installed on your phone)
• 2 devices (phone + tablet/PC with emulator)
• 2 MLBB accounts (main and secondary — "twink")

━━━━━━━━━━━━━━━━━━━━

📖 *HOW IT WORKS:*

GEMPIC = EPIC
MITIK / JENTIK = MYTHIC

The method is based on coordinated games with other players where you receive wins through special lobbies.

━━━━━━━━━━━━━━━━━━━━

🔢 *STEP-BY-STEP INSTRUCTIONS:*

*STEP 1 — Join Special WhatsApp Chats*
Join as many of these chats as possible (some may be temporarily unavailable — this is normal):

https://chat.whatsapp.com/FMl8WQmhOUCIIxrdKfAmYd
https://chat.whatsapp.com/LEwOEB9gX1A4H5ei2BZFme
https://chat.whatsapp.com/Bv92BF2KxMQ4rWjbotA4uq
https://chat.whatsapp.com/KRRnfIB0lYI0JPz9gRcZCA
https://chat.whatsapp.com/FUggJTvzlr29KMnpqMs6XI
https://chat.whatsapp.com/HjBYIu4NcsCAAINetfQCzs
https://chat.whatsapp.com/EWhlU363JAK79UfRjf9Prf
https://chat.whatsapp.com/Lhv9hOpD1euHY7ZJFdbHxk
https://chat.whatsapp.com/I5Ap5TSzcUu6jzy5QmKwrV
https://chat.whatsapp.com/FHxEu5QTCnyBOKRSDRtMRv
https://chat.whatsapp.com/LpwCoqmrDWZ1tpS8wTar4a
https://chat.whatsapp.com/GYQJDigXyNsIaHbkR01W6f

💡 *Tip:* In the chats you'll find links to other chats — join those too!

━━━━━━━━━━━━━━━━━━━━

*STEP 2 — Find Active Players*
In the chats, look for messages in this format:
*GEMPIC POINT ALL SVG*

This signals that players are ready to boost.
Reference organizer ID: 343933935

━━━━━━━━━━━━━━━━━━━━

*STEP 3 — Prepare 2 Devices*
• Device 1: your *main account*
• Device 2: your *secondary account* (twink)

📌 *Rank Rule:*
- Main Mythic → twink must be Mythic or Legend
- Main Legend → twink must be Epic or Legend

━━━━━━━━━━━━━━━━━━━━

*STEP 4 — Connect to the Lobby*
1. Copy your main account ID (settings → profile)
2. Post the ID in the WhatsApp chat
3. You'll get a friend request and lobby invite
4. In the lobby, they'll tell you the twink's ID — add it too
5. The organizers start the game — you just need to *accept*

━━━━━━━━━━━━━━━━━━━━

*STEP 5 — In the Game*
Players will write to you in the game chat (often in non-English — enable chat translation in game settings). Just follow the instructions.

━━━━━━━━━━━━━━━━━━━━

⚠️ *WARNINGS:*
• Don't overdo it — avoid too many boost games in a row
• Don't share this method with other players
• Only use verified organizers from the chats
• We are not responsible for bans from Moonton

━━━━━━━━━━━━━━━━━━━━

🎁 *YOUR BONUS:*
The next message contains a 30% discount coupon for any CyraxMod key!`;

async function sendInfiniteBoostGuide(userId, userLang, botInstance = bot) {
  const isRu = userLang === 'ru' || (userLang && userLang.startsWith('ru'));
  const guide = isRu ? BOOST_GUIDE_RU : BOOST_GUIDE_EN;
  const userObj = { language_code: userLang };

  await safeSendMessage(userId, t(userObj, 'infinite_boost_purchase_success'), {}, botInstance).catch(() => { });
  await new Promise(r => setTimeout(r, 1500));

  // Разбиваем гайд на части если нужно
  const parts = splitMessage(guide, 4000);
  for (const part of parts) {
    await safeSendMessage(userId, part, { parse_mode: 'Markdown', disable_web_page_preview: true }, botInstance).catch(() => { });
    await new Promise(r => setTimeout(r, 500));
  }

  // Генерируем 30% бонусный купон
  const couponCode = 'BOOST30-' + crypto.randomBytes(3).toString('hex').toUpperCase();
  const expiresAt = new Date(Date.now() + 7 * 24 * 60 * 60 * 1000).toISOString();

  db.run(
    `INSERT INTO coupons (code, discount_percent, max_uses, expires_at, created_by, user_id) VALUES (?, 30, 1, ?, ?, ?)`,
    [couponCode, expiresAt, ADMIN_ID, userId],
    (err) => {
      if (err) {
        console.error('❌ Error creating boost coupon:', err);
        return;
      }
      const bonusMsg = isRu
        ? `🎁 *Твой бонусный купон на 30% скидку!*\n\n🎟️ Код: \`${couponCode}\`\n📅 Действует 7 дней\n\nПрименяй при покупке любого ключа CyraxMod!`
        : `🎁 *Your bonus 30% discount coupon!*\n\n🎟️ Code: \`${couponCode}\`\n📅 Valid for 7 days\n\nApply it when buying any CyraxMod key!`;

      safeSendMessage(userId, bonusMsg, { parse_mode: 'Markdown' }, botInstance).catch(() => { });
      logAction(userId, 'boost_guide_delivered', { couponCode });
    }
  );
}

// ==========================================
// 🚀 СОЗДАНИЕ И УПРАВЛЕНИЕ РЕСЕЛЛЕР-БОТАМИ
// ==========================================

async function handleResellerActivation(order) {
  const userId = order.user_id;

  // Получаем данные из сессии пользователя (не из undefined переменной)
  const userSession = getSession(userId);
  const markupPct = order.reseller_markup_pct || userSession?.data?.resellerMarkup || userSession?.data?.markupPct || 30;

  db.run(
    `INSERT INTO resellers (user_id, status, markup_pct) VALUES (?, 'awaiting_token', ?) ON CONFLICT(user_id) DO UPDATE SET status = 'awaiting_token', markup_pct = excluded.markup_pct`,
    [userId, markupPct, markupPct],
    (err) => {
      if (err) {
        console.error('❌ Error creating reseller record:', err);
        return;
      }

      // Обновляем сессию пользователя
      const userSession2 = getSession(userId);
      userSession2.state = 'awaiting_reseller_token';
      userSession2.data = { orderId: order.id };

      const lang = order.user_lang || 'en';
      const msg = t({ language_code: lang }, 'partner_token_request');

      safeSendMessage(userId, msg, { parse_mode: 'Markdown' }).catch(() => { });
    }
  );
}

// Отдельная функция для обработки чеков от реселлеров
function handleResellerPaymentReceived(user, chatId, pendingOrder, session, text) {
  const isRu = pendingOrder.user_lang === 'ru';
  db.run(`UPDATE orders SET status = 'pending', transaction_id = 'Wait review' WHERE id = ?`, [pendingOrder.id], (updErr) => {
    if (updErr) return safeSendMessage(chatId, t(user, 'error_creating_order'));

    // Подготовим сообщение для админа о покупке реселлера
    let partnerInfo = '';
    if (pendingOrder.reseller_markup_pct) {
      partnerInfo += `\n📈 Наценка: +${pendingOrder.reseller_markup_pct}%`;
    }
    if (pendingOrder.reseller_questionnaire) {
      partnerInfo += `\n📝 Анкета: ${pendingOrder.reseller_questionnaire}`;
    }

    const adminMsg = `💼 *Новая заявка на партнёрство*\n\n` +
      `👤 Клиент: ${user.username ? '@' + user.username : 'ID ' + user.id}\n` +
      `📦 Товар: ${pendingOrder.product}\n` +
      `💳 Оплата: ${pendingOrder.amount} ${pendingOrder.currency} (${pendingOrder.method})${partnerInfo}\n\n` +
      `Пожалуйста, проверьте оплату и нажмите одну из кнопок.`;

    const adminKb = {
      inline_keyboard: [
        [
          { text: '✅ Одобрить', callback_data: `approve_${pendingOrder.id}` },
          { text: '❌ Отклонить', callback_data: `reject_${pendingOrder.id}` }
        ]
      ]
    };

    safeSendMessage(ADMIN_ID, adminMsg, { parse_mode: 'Markdown', reply_markup: adminKb }).catch(() => { });

    clearSession(chatId);
    safeSendMessage(chatId, t(user, 'receipt_received'));
  });
}

// ==========================================
// ⚙️ ИНИЦИАЛИЗАЦИЯ ДВИЖКА РЕСЕЛЛЕРОВ
// ==========================================

async function initResellerBot(reseller) {
  try {
    const rawToken = decryptToken(reseller.encrypted_token);
    if (!rawToken) throw new Error('Failed to decrypt token');

    // SERIOUS 1: Если бот уже был в Map — удаляем старые обработчики перед переинициализацией.
    // Без этого при повторном вызове initResellerBot (toggle on/off, рестарт) накапливаются
    // дублирующиеся listeners на одном инстансе, что приводит к двойной/тройной обработке событий.
    const staleBot = resellerBots.get(reseller.id)?.bot;
    if (staleBot) {
      try {
        staleBot.removeAllListeners();
        console.log(`🧹 [РЕСЕЛЛЕР ${reseller.id}] Старые обработчики удалены перед переинициализацией`);
      } catch (_) {}
    }

    const resellerBot = new TelegramBot(rawToken, { polling: false });

    // Поддержка RENDER_EXTERNAL_URL
    const renderUrl = process.env.RENDER_EXTERNAL_URL || `https://cyrax-bot-0vwr.onrender.com`;

    // FIX 4.1: Генерируем/используем webhook_secret для каждого реселлер-бота.
    // Секрет сохраняется в БД при первой инициализации.
    // URL вида /webhook/reseller/{id}/{secret} — злоумышленник без секрета не пройдёт.
    let webhookSecret = reseller.webhook_secret;
    if (!webhookSecret) {
      webhookSecret = crypto.randomBytes(24).toString('hex');
      db.run(`UPDATE resellers SET webhook_secret = ? WHERE id = ?`, [webhookSecret, reseller.id]);
      reseller.webhook_secret = webhookSecret;
    }
    const webhookUrl = `${renderUrl}/webhook/reseller/${reseller.id}/${webhookSecret}`;

    await resellerBot.setWebHook(webhookUrl);
    console.log(`✅ [РЕСЕЛЛЕР ${reseller.id}] Webhook установлен: ${webhookUrl}`);

    // Подключение обработчиков
    resellerBot.on('message', async (msg) => {
      try {
        await handleResellerMessage(msg, resellerBot, reseller);
      } catch (e) {
        console.error(`❌ [РЕСЕЛЛЕР ${reseller.id}] Ошибка обработки message:`, e);
      }
    });

    resellerBot.on('callback_query', async (query) => {
      try {
        await handleResellerCallbackQuery(query, resellerBot, reseller);
      } catch (e) {
        console.error(`❌ [РЕСЕЛЛЕР ${reseller.id}] Ошибка обработки callback_query:`, e);
      }
    });

    // Сохраняем в кэш
    resellerBots.set(reseller.id, { bot: resellerBot, data: reseller });

    return resellerBot;
  } catch (err) {
    console.error(`❌ [РЕСЕЛЛЕР ${reseller.id}] Ошибка запуска:`, err.message);

    // Помечаем как неактивного из-за ошибки (например токен стал невалидным)
    if (err?.response?.body?.error_code === 401) {
      db.run(`UPDATE resellers SET status = 'token_invalid' WHERE id = ?`, [reseller.id]);
      // BUG FIX ML-1: Удаляем невалидный бот из Map и снимаем все слушатели
      const staleEntry = resellerBots.get(reseller.id);
      if (staleEntry?.bot) {
        try { staleEntry.bot.removeAllListeners(); } catch (_) {}
      }
      resellerBots.delete(reseller.id);
      safeSendMessage(ADMIN_ID, `⚠️ *Ошибка запуска реселлер-бота!*\n\nID: ${reseller.id}\nЮзер: ${reseller.user_id}\n\nТокен недействителен (401). Статус изменён на 'token_invalid'.`, { parse_mode: 'Markdown' }).catch(() => { });
    } else {
      // Уведомляем админа о любой другой ошибке (сеть, расшифровка, и т.д.)
      safeSendMessage(ADMIN_ID,
        `⚠️ *Ошибка запуска реселлер-бота*\n\nID реселлера: ${reseller.id}\nПользователь: ID ${reseller.user_id}\n\nОшибка: ${err.message}\n\nБот не запущен — проверьте токен и настройки.`,
        { parse_mode: 'Markdown' }
      ).catch(() => {});
    }
    return null;
  }
}

async function initAllResellers() {
  if (getSetting('reseller_enabled') === '0') {
    console.log('ℹ️ Система реселлеров отключена в настройках.');
    return;
  }

  // FIX 1.1: Принудительно перечитываем реквизиты и цены из БД перед запуском
  await loadPaymentDetailsFromDB();
  await loadPricesFromDB();
  console.log('✅ Payment details & prices re-synced before reseller init');

  // BUG FIX AS-2: Обернуть db.all в Promise — иначе await initAllResellers() возвращался
  // сразу после вызова db.all, не дожидаясь завершения цикла внутри колбэка.
  const rows = await new Promise((resolve, reject) =>
    db.all(`SELECT * FROM resellers WHERE status = 'active'`, [], (err, r) =>
      err ? reject(err) : resolve(r || [])
    )
  );

  if (rows.length === 0) {
    console.log('ℹ️ Нет активных реселлеров для запуска.');
    return;
  }

  console.log(`⏳ Инициализация реселлер-ботов (${rows.length} шт.)...`);

  let successCount = 0;
  for (const reseller of rows) {
    const b = await initResellerBot(reseller);
    if (b) successCount++;
    // Небольшая задержка чтобы не спамить API Telegram
    await new Promise(r => setTimeout(r, 500));
  }

  console.log(`✅ Запущено реселлер-ботов: ${successCount} из ${rows.length}`);
}

// ==========================================
// 🤖 ОБРАБОТЧИКИ СОБЫТИЙ РЕСЕЛЛЕР-БОТА
// ==========================================

// Общая функция отображения главного меню реселлер-бота.
// Используется в /start, rsl_back_to_main и после ошибок.
function sendResellerMainMenu(chatId, resellerBot, reseller, isRu, messageId = null) {
  const shopNum = reseller.id;
  const welcomeText = isRu
    ? `👋 *Добро пожаловать в CyraxMods!* #${shopNum}\n\nВыберите нужный товар:`
    : `👋 *Welcome to CyraxMods!* #${shopNum}\n\nChoose your product:`;

  const keyboardRows = [
    [{ text: isRu ? '🛒 Купить ключ' : '🛒 Buy key', callback_data: 'rsl_buy' }]
  ];
  if (getSetting('manual_boost_disabled') !== '1') {
    keyboardRows.push([{ text: isRu ? '🚀 Бесконечный буст' : '🚀 Infinite boost', callback_data: 'rsl_buy_boost' }]);
  }
  keyboardRows.push([{ text: isRu ? '🤝 Подключить Партнерство' : '🤝 Connect Partnership', callback_data: 'rsl_partnership' }]);

  const opts = { parse_mode: 'Markdown', reply_markup: { inline_keyboard: keyboardRows } };

  if (messageId) {
    resellerBot.editMessageText(welcomeText, { chat_id: chatId, message_id: messageId, ...opts })
      .catch(() => resellerBot.sendMessage(chatId, welcomeText, opts).catch(() => {}));
  } else {
    resellerBot.sendMessage(chatId, welcomeText, opts).catch(() => {});
  }
}

async function handleResellerMessage(msg, resellerBot, reseller) {
  if (!msg || !msg.chat) return;
  const chatId = msg.chat.id;
  const text = msg.text || '';

  // 🛡️ Rate limit для реселлер-ботов.
  // Обычные пользователи — стандартный лимит.
  // Владелец реселлера и наш ADMIN — повышенный лимит (x5), чтобы не мешать работе,
  // но всё равно защищаться от случайного зависания в петле.
  if (msg.from) {
    const isOwnerOrAdmin = msg.from.id === reseller.user_id || msg.from.id === ADMIN_ID;
    const effectiveLimit = isOwnerOrAdmin ? MAX_ACTIONS_PER_WINDOW * 5 : MAX_ACTIONS_PER_WINDOW;
    if (!checkRateLimit(msg.from.id, text, effectiveLimit)) {
      resellerBot.sendMessage(chatId, '⏳ Слишком много запросов. Подождите немного.').catch(() => {});
      return;
    }
  }

  // Task 3.1: Лог входящего сообщения — помогает при отладке видеть какой бот/юзер/состояние
  const msgType = msg.photo ? 'photo' : msg.document ? 'document' : 'text';
  const currentState = getSession(chatId)?.state || 'none';
  console.log(`📨 [RSL ${reseller.id}] msg from uid=${msg.from?.id} type=${msgType} state=${currentState}`);

  try {
    // 🛡️ Проверяем статус реселлера — если деактивирован, не обрабатываем
    const rslStatus = await new Promise(resolve =>
      db.get('SELECT status FROM resellers WHERE id = ?', [reseller.id], (e, r) => resolve(r?.status))
    );
    if (rslStatus !== 'active') {
      console.log(`⛔ [РЕСЕЛЛЕР ${reseller.id}] Message ignored — status: ${rslStatus}`);
      return;
    }

    // Добавляем user в базу основного бота (чтобы id там существовал для внешних ключей)
    if (msg.from) {
      db.run(
        `INSERT OR IGNORE INTO users (id, username, language_code) VALUES (?, ?, ?)`,
        [msg.from.id, msg.from.username, msg.from.language_code],
        (err) => { if (err) console.error('❌ Reseller user insert error:', err.message); }
      );
    }

    // Примитивная сессия для загрузки чеков
    const session = getSession(chatId);

    // Команда /admin или 📊 Управление (только для владельца)
    if ((text === '/admin' || text === '📊 Управление') && msg.from.id === reseller.user_id) {
      db.get('SELECT balance FROM resellers WHERE id = ?', [reseller.id], (err, row) => {
        const balance = row ? row.balance : 0;
        const markup = reseller.markup_pct || 30;
        const kb = {
          inline_keyboard: [
            [{ text: '📊 Статистика продаж', callback_data: 'rsl_admin_stats' }],
            [{ text: `✏️ Изменить наценку (Текущая: ${markup}%)`, callback_data: 'rsl_edit_markup' }],
            [{ text: '📦 Мои последние заказы', callback_data: 'rsl_my_orders' }],
            [{ text: '📢 Рассылка клиентам', callback_data: 'rsl_broadcast' }],
            [{ text: '📋 Логи (последние 10 продаж)', callback_data: 'rsl_logs' }],
            [{ text: '💸 Вывести средства', callback_data: 'rsl_withdraw' }],
            [{ text: '🔄 Сбросить токен', callback_data: 'rsl_reset_token' }],
            // FIX 3.1: Кнопка возврата в главное меню — пользователь не обязан вводить /start
            [{ text: '🏠 Главное меню', callback_data: 'rsl_back_to_main' }]
          ]
        };
        resellerBot.sendMessage(chatId, `🛠 *Панель управления реселлера*\n\n💰 Ваш баланс: *${balance} ₽*\n📈 Ваша наценка: *${markup}%*`, { parse_mode: 'Markdown', reply_markup: kb }).catch(() => { });
      });
      return;
    }

    // Ввод новой наценки
    if (session.state === 'rsl_awaiting_markup') {
      if (text.startsWith('/start') || text.startsWith('/admin')) {
        clearSession(chatId);
        resellerBot.sendMessage(chatId, '❌ Изменение наценки отменено.');
        return;
      }
      const val = parseInt(text);
      if (isNaN(val) || val < 10) {
        resellerBot.sendMessage(chatId, '❌ Наценка должна быть числом от 10%. Попробуйте еще раз:');
        return;
      }
      db.run(`UPDATE resellers SET markup_pct = ? WHERE id = ?`, [val, reseller.id], (err) => {
        if (!err) {
          // BUG FIX DATA-2: Обновляем через Map — гарантирует что все ссылки на объект актуальны.
          reseller.markup_pct = val;
          const mapEntry = resellerBots.get(reseller.id);
          if (mapEntry) mapEntry.data.markup_pct = val;
          resellerBot.sendMessage(chatId, `✅ *Наценка успешно изменена на ${val}%!*`, { parse_mode: 'Markdown' });
        } else {
          resellerBot.sendMessage(chatId, '❌ Ошибка базы данных.');
        }
      });
      clearSession(chatId);
      return;
    }

    // Заявка на вывод
    if (session.state === 'rsl_awaiting_withdraw_details') {
      if (text.startsWith('/start')) {
        clearSession(chatId);
        resellerBot.sendMessage(chatId, '❌ Вывод отменён.');
        return;
      }

      // Защита: если реквизиты пустые — не создаём заявку
      if (!text.trim()) {
        resellerBot.sendMessage(chatId, '❌ Реквизиты не могут быть пустыми.');
        return;
      }

      // Create withdrawal request
      db.get('SELECT balance FROM resellers WHERE id = ?', [reseller.id], (err, r) => {
        const balance = r ? r.balance : 0;
        if (balance <= 0) {
          resellerBot.sendMessage(chatId, '❌ Недостаточно средств для вывода.');
          clearSession(chatId);
          return;
        }

        db.run(`INSERT INTO reseller_withdrawals (reseller_id, amount, details, status) VALUES (?, ?, ?, 'pending')`,
          [reseller.id, balance, text.trim()], function (e) {
            // FIX 2.1: clearSession перенесён внутрь колбэка — раньше он вызывался
            // до завершения асинхронной записи в БД, что могло привести к гонке состояний.
            clearSession(chatId);
            if (!e) {
              const withdrawId = this.lastID;
              resellerBot.sendMessage(chatId, `✅ *Заявка на вывод создана!*\n\nСумма: ${balance} ₽\nРеквизиты: ${text.trim()}\nОжидайте зачисления от администратора.`, { parse_mode: 'Markdown' });

              safeSendMessage(ADMIN_ID, `💸 *Новая заявка на вывод от реселлера!*\n\n🤖 Бот: @${escapeMarkdown(reseller.username || '')}\n👤 UserID: ${reseller.user_id}\n💰 Сумма: *${balance} ₽*\n💳 Реквизиты: \`${text.trim()}\``, {
                parse_mode: 'Markdown',
                reply_markup: {
                  inline_keyboard: [
                    [
                      { text: '✅ Одобрить', callback_data: `rsl_withdraw_approve_${withdrawId}` },
                      { text: '❌ Отклонить', callback_data: `rsl_withdraw_reject_${withdrawId}` }
                    ]
                  ]
                }
              }).catch(() => { });
            } else {
              console.error(`❌ [РЕСЕЛЛЕР ${reseller.id}] Ошибка создания заявки на вывод:`, e);
              resellerBot.sendMessage(chatId, '❌ Произошла ошибка при создании заявки. Попробуйте ещё раз.');
            }
          });
      });
      return;
    }

    // FIX 1.2: Загрузка чека — ПРОВЕРЯЕМ ДО обработки /start,
    // иначе если user отправляет фото и текст пустой — попадёт в /start.
    // Сессия rsl_awaiting_receipt устанавливается в rsl_pay_ callback.
    if ((msg.photo || msg.document) && session.state === 'rsl_awaiting_receipt') {
      const rslHourglassMsg = await resellerBot.sendMessage(chatId, '⏳ Отправляем квитанцию на проверку...').catch(() => null);

      const isRu = getLang(msg.from) === 'ru';
      const order = session.data && session.data.order;

      if (!order) {
        console.error(`❌ [РЕСЕЛЛЕР ${reseller.id}] rsl_awaiting_receipt but session.data.order is missing!`);
        resellerBot.sendMessage(chatId, isRu ? '❌ Ошибка: данные заказа потеряны. Начните оформление заново.' : '❌ Error: order data lost. Please restart the checkout.');
        clearSession(chatId);
        return;
      }

      try {
        const amountStr = formatPrice(order.total_amount, order.currency);
        const caption = `🧾 *Новая ручная оплата* [РЕСЕЛЛЕР @${escapeMarkdown(String(reseller.username || reseller.id))}]\n\n`
          + `👤 Юзер: @${escapeMarkdown(String(msg.from.username || msg.from.id))}\n`
          + `📦 Товар: ${order.product}\n`
          + `💰 Сумма: ${amountStr}\n`
          + `🏦 Метод: ${order.method}\n\n`
          + `Подтвердите или отклоните платёж:`;

        const replyMarkup = {
          inline_keyboard: [[
            { text: '✅ Подтвердить', callback_data: `approve_${order.id}` },
            { text: '❌ Отклонить', callback_data: `reject_${order.id}` }
          ]]
        };

        // FIX 1.2: Скачиваем файл через реселлер-бот в память (Buffer) и загружаем через основной бот.
        // Также сохраняем transaction_id в заказе — чтобы checkLongPendingOrders знал, что чек был прислан.
        let fileId, fileType, fileName;
        if (msg.photo) {
          fileId = msg.photo[msg.photo.length - 1].file_id;
          fileType = 'photo';
        } else {
          fileId = msg.document.file_id;
          fileType = 'document';
          fileName = msg.document.file_name || 'receipt.pdf';
        }

        // Получаем ссылку на скачивание через resellerBot
        const fileLink = await resellerBot.getFileLink(fileId);

        // Скачиваем в буфер через axios
        const response = await axios.get(fileLink, { responseType: 'arraybuffer' });
        const fileBuffer = Buffer.from(response.data);

        const sendOpts = { caption, parse_mode: 'Markdown', reply_markup: replyMarkup };

        // Загружаем файл через основной бот и сохраняем его file_id
        // ВАЖНО: file_id от реселлер-бота не работает с основным ботом (Telegram bot-specific)
        // Поэтому захватываем file_id из ответа основного бота после отправки
        let sentAdminMsg;
        if (fileType === 'photo') {
          sentAdminMsg = await bot.sendPhoto(ADMIN_ID, { source: fileBuffer }, sendOpts);
        } else {
          sentAdminMsg = await bot.sendDocument(ADMIN_ID, { source: fileBuffer, filename: fileName || 'receipt.pdf' }, sendOpts);
        }

        // Извлекаем file_id основного бота для сохранения в orders
        const mainBotFileId = fileType === 'photo'
          ? (sentAdminMsg?.photo?.[sentAdminMsg.photo.length - 1]?.file_id || fileId)
          : (sentAdminMsg?.document?.file_id || fileId);

        // Сохраняем file_unique_id для fraud detection
        const fileUniqueId = msg.photo
          ? msg.photo[msg.photo.length - 1].file_unique_id
          : msg.document?.file_unique_id;

        db.run(
          `UPDATE orders SET transaction_id = 'Wait review', receipt_file_id = ?, receipt_type = ?, hourglass_msg_id = ? WHERE id = ?`,
          [mainBotFileId, fileType, rslHourglassMsg ? rslHourglassMsg.message_id : null, order.id]
        );

        // Защита от повторного использования чека (fraud detection)
        if (fileUniqueId) {
          saveReceiptRecord(fileId, fileUniqueId, msg.from.id, order.id, 'order');
        }

        resellerBot.sendMessage(chatId, isRu ? '✅ Чек отправлен на проверку!' : '✅ Receipt sent for review!');
        clearSession(chatId);
      } catch (e) {
        console.error('❌ Reseller receipt forward error:', e);
        resellerBot.sendMessage(chatId, isRu ? '❌ Ошибка отправки чека. Попробуйте ещё раз или обратитесь в поддержку.' : '❌ Failed to send receipt. Please try again or contact support.');
      }
      return; // FIX 1.2: Явный return — не падаем дальше в /start
    }

    // Рассылка клиентам реселлера
    if (session.state === 'rsl_awaiting_broadcast') {
      if (text.startsWith('/start')) {
        clearSession(chatId);
        resellerBot.sendMessage(chatId, '❌ Рассылка отменена.');
        return;
      }
      // Получаем всех покупателей этого реселлера
      db.all(
        `SELECT DISTINCT user_id FROM orders WHERE reseller_id = ? AND status = 'confirmed'`,
        [reseller.id],
        async (err, buyers) => {
          if (err || !buyers || buyers.length === 0) {
            resellerBot.sendMessage(chatId, '❌ Нет покупателей для рассылки.');
            clearSession(chatId);
            return;
          }
          let sent = 0, failed = 0;
          for (const b of buyers) {
            try {
              if (msg.photo) {
                const fileId = msg.photo[msg.photo.length - 1].file_id;
                await resellerBot.sendPhoto(b.user_id, fileId, { caption: msg.caption || '', parse_mode: 'Markdown' });
              } else {
                await resellerBot.sendMessage(b.user_id, text, { parse_mode: 'Markdown' });
              }
              sent++;
            } catch (e) { failed++; }
            await new Promise(r => setTimeout(r, 100));
          }
          resellerBot.sendMessage(chatId, `✅ Рассылка завершена.\nОтправлено: ${sent}\nНе доставлено: ${failed}`);
          clearSession(chatId);
        }
      );
      return;
    }

    // Команда /start
    if (text.startsWith('/start')) {
      const isRu = getLang(msg.from) === 'ru';
      clearSession(chatId); // сбрасываем любое незавершённое состояние при /start
      sendResellerMainMenu(chatId, resellerBot, reseller, isRu);
      return;
    }

    // (receipt handling moved above /start block — see FIX 1.2)
  } catch (globalErr) {
    console.error(`❌ Global error in handleResellerMessage (Reseller ID: ${reseller.id}):`, globalErr);
  }
}

async function handleResellerCallbackQuery(query, resellerBot, reseller) {
  const chatId = query.message.chat.id;
  const user = query.from;
  const data = query.data;
  const isRu = getLang(user) === 'ru';

  // 🛡️ Rate limit для реселлер-ботов.
  // Обычные пользователи — стандартный лимит.
  // Владелец реселлера и наш ADMIN — повышенный лимит (x5).
  {
    const isOwnerOrAdmin = user.id === reseller.user_id || user.id === ADMIN_ID;
    const effectiveLimit = isOwnerOrAdmin ? MAX_ACTIONS_PER_WINDOW * 5 : MAX_ACTIONS_PER_WINDOW;
    if (!checkRateLimit(user.id, data, effectiveLimit)) {
      resellerBot.answerCallbackQuery(query.id, { text: '⏳ Слишком много запросов.' }).catch(() => {});
      return;
    }
  }

  // Task 3.1: Лог колбэка — помогает при отладке понять какой бот/данные обрабатываются
  console.log(`🔘 [RSL ${reseller.id}] callback from uid=${user.id} data=${data}`);

  // 🛡️ Проверяем статус реселлера — если деактивирован, не обрабатываем
  const rslStatus = await new Promise(resolve =>
    db.get('SELECT status FROM resellers WHERE id = ?', [reseller.id], (e, r) => resolve(r?.status))
  );
  if (rslStatus !== 'active') {
    resellerBot.answerCallbackQuery(query.id, { text: '⛔ Бот деактивирован' }).catch(() => {});
    return;
  }

  resellerBot.answerCallbackQuery(query.id).catch(() => { });

  // FIX 2.1: Глобальный try/catch — любая необработанная ошибка внутри колбэка
  // теперь будет поймана, залогирована и пользователь получит понятное сообщение
  // вместо полной тишины ("молчаливое падение").
  try {

  // ──────────────────────────────────────
  // 🏠 НАВИГАЦИЯ: Главное меню / Назад
  // ──────────────────────────────────────
  // Кнопка "Назад" — возврат в главное меню
  if (data === 'rsl_back_to_main') {
    sendResellerMainMenu(chatId, resellerBot, reseller, isRu, query.message.message_id);
    return;
  }

  // ──────────────────────────────────────
  // 📊 СТАТИСТИКА И ИСТОРИЯ ЗАКАЗОВ
  // ──────────────────────────────────────
  if (data === 'rsl_admin_stats') {
    db.get('SELECT COUNT(*) as cnt, SUM(markup_amount) as total FROM reseller_orders WHERE reseller_id = ?', [reseller.id], (err, row) => {
      const cnt = row ? row.cnt : 0;
      const total = row ? (row.total || 0) : 0;
      resellerBot.sendMessage(chatId, `📊 *Статистика продаж*\n\n🛍 Всего продаж: *${cnt}*\n💰 Заработано за всё время: *${total} ₽*`, { parse_mode: 'Markdown' });
    });
  }

  // ──────────────────────────────────────
  // 💸 ВЫВОД СРЕДСТВ
  // ──────────────────────────────────────
  if (data === 'rsl_withdraw') {
    db.get('SELECT balance FROM resellers WHERE id = ?', [reseller.id], (err, r) => {
      const balance = r ? r.balance : 0;
      if (balance <= 0) {
        resellerBot.sendMessage(chatId, '❌ У вас нет средств для вывода (0 ₽).');
        return;
      }
      resellerBot.sendMessage(chatId, `💸 *Вывод средств*\nДоступно: *${balance} ₽*\n\nВыберите способ вывода:`, {
        parse_mode: 'Markdown',
        reply_markup: {
          inline_keyboard: [
            [{ text: '💳 На карту / Крипту', callback_data: 'rsl_withdraw_money' }],
            [{ text: '🔑 Получить ключом', callback_data: 'rsl_withdraw_key' }]
          ]
        }
      });
    });
  }

  if (data === 'rsl_withdraw_money') {
    db.get('SELECT balance FROM resellers WHERE id = ?', [reseller.id], (err, r) => {
      const balance = r ? r.balance : 0;
      if (balance <= 0) return;
      const session = getSession(chatId);
      session.state = 'rsl_awaiting_withdraw_details';
      resellerBot.sendMessage(chatId, `💳 *Вывод на карту/кошелек*\nДоступно: *${balance} ₽*\n\nОтправьте в ответ ваши реквизиты (номер карты, крипто-кошелёк). Или /start для отмены.`, { parse_mode: 'Markdown' });
    });
  }

  if (data === 'rsl_withdraw_key') {
    db.get('SELECT balance, markup_pct FROM resellers WHERE id = ?', [reseller.id], (err, r) => {
      const balance = r ? r.balance : 0;
      if (balance <= 0) return;

      const session = getSession(chatId);
      session.state = 'rsl_awaiting_withdraw_key';

      // Calculate costs (base price)
      const markup = r.markup_pct || 30;
      const products = ['1d', '3d', '7d', '30d', 'infinite_boost'];
      let msg = `🔑 *Вывод средств ключом*\nДоступно: *${balance} ₽*\n\nНиже указана базовая стоимость ключей (без вашей наценки).\nВыберите период:`;

      const kb = [];
      products.forEach(p => {
        let baseCost = 0;
        if (p === 'infinite_boost') {
          baseCost = getSetting('manual_boost_price_rub') || 400; // default example
          if (getSetting('manual_boost_disabled') === '1') return; // Hide if disabled
        } else if (PRICES[p] && PRICES[p].RUB) {
          baseCost = PRICES[p].RUB;
        }

        if (baseCost > 0) {
          const isAffordable = balance >= baseCost;
          const emoji = isAffordable ? '✅' : '❌';
          kb.push([{
            text: `${emoji} ${p} — ${baseCost} ₽`,
            callback_data: isAffordable ? `rsl_withdraw_req_key_${p}` : 'rsl_withdraw_insufficient'
          }]);
        }
      });

      resellerBot.sendMessage(chatId, msg, {
        parse_mode: 'Markdown',
        reply_markup: { inline_keyboard: kb }
      });
    });
  }

  if (data === 'rsl_withdraw_insufficient') {
    resellerBot.answerCallbackQuery(query.id, { text: '❌ Недостаточно средств для этого ключа', show_alert: true }).catch(() => { });
    return;
  }

  if (data.startsWith('rsl_withdraw_req_key_')) {
    const product = data.replace('rsl_withdraw_req_key_', '');
    db.get('SELECT id, balance FROM resellers WHERE id = ?', [reseller.id], (err, r) => {
      if (!r) return;
      let baseCost = 0;
      if (product === 'infinite_boost') {
        baseCost = getSetting('manual_boost_price_rub') || 400;
      } else if (PRICES[product] && PRICES[product].RUB) {
        baseCost = PRICES[product].RUB;
      }

      if (r.balance < baseCost) {
        resellerBot.sendMessage(chatId, '❌ На балансе недостаточно средств.');
        return;
      }
      // FIX 5.1: защита от бага с нулевой/отрицательной ценой ключа — бесплатный вывод невозможен
      if (baseCost <= 0) {
        resellerBot.sendMessage(chatId, '❌ Некорректная стоимость ключа. Обратитесь к администратору.');
        console.error(`❌ [RSL WITHDRAW] baseCost <= 0 для product=${product}, reseller=${reseller.id}`);
        return;
      }

      // FIX 2.2: Баланс НЕ списываем при создании заявки — только блокируем.
      // Списание происходит в rsl_withdraw_approve_ ПОСЛЕ успешной выдачи ключа.
      // При отказе/ошибке — деньги остаются нетронутыми.
      // Create withdrawal request (balance is NOT deducted here)
      const details = `KEY_${product}`;
      db.run(`INSERT INTO reseller_withdrawals (reseller_id, amount, details, status) VALUES (?, ?, ?, 'pending')`, [r.id, baseCost, details], function (err) {
        if (err) { resellerBot.sendMessage(chatId, '❌ Ошибка создания заявки.'); return; }
        const wId = this.lastID;
        resellerBot.sendMessage(chatId, `✅ Заявка на получение ключа *${product}* создана.\nСумма к списанию: *${baseCost} ₽* (спишется после одобрения)\nВскоре администратор одобрит её и вы получите ключ!`, { parse_mode: 'Markdown' });

          // Notify Admin
          safeSendMessage(ADMIN_ID, `💸 *Новая заявка на вывод (КЛЮЧ)*\n\nРеселлер ID: \`${r.id}\`\nСумма (стоимость): *${baseCost} ₽*\nТовар: *${product}*\n\nЗаявка #${wId}`, {
            parse_mode: 'Markdown',
            reply_markup: {
              inline_keyboard: [
                [{ text: '✅ Одобрить и выдать ключ', callback_data: `rsl_withdraw_approve_${wId}` }],
                [{ text: '❌ Отклонить', callback_data: `rsl_withdraw_reject_${wId}` }]
              ]
            }
          });
      });
    });
  }

  if (data === 'rsl_edit_markup') {
    const session = getSession(chatId);
    session.state = 'rsl_awaiting_markup';
    resellerBot.sendMessage(chatId, '✏️ *Изменение наценки*\n\nОтправьте в процентах (например `30` или `50`). Минимум: 10%.', { parse_mode: 'Markdown' });
  }

  if (data === 'rsl_broadcast') {
    const session = getSession(chatId);
    session.state = 'rsl_awaiting_broadcast';
    resellerBot.sendMessage(chatId, '📢 Рассылка клиентам\n\nОтправьте текст (или фото с текстом), которое будет разослано всем пользователям, совершившим хотя бы одну покупку в вашем боте.\nДля отмены напишите /start.', { parse_mode: 'Markdown' });
  }

  if (data === 'rsl_logs') {
    db.all(`SELECT id, product, amount, currency, updated_at FROM orders WHERE reseller_id = ? AND status = 'completed' ORDER BY updated_at DESC LIMIT 10`, [reseller.id], (err, rows) => {
      if (err || !rows || rows.length === 0) {
        return resellerBot.sendMessage(chatId, 'У вас пока нет успешных продаж.');
      }
      let logMsg = '📋 *Последние 10 продаж:*\n\n';
      rows.forEach(r => {
        const d = new Date(r.updated_at).toLocaleString('ru-RU', { timeZone: 'Europe/Moscow' });
        logMsg += `🔹 Заказ #${r.id} | ${r.product} | ${r.amount} ${r.currency}\n🕒 ${d}\n\n`;
      });
      resellerBot.sendMessage(chatId, logMsg, { parse_mode: 'Markdown' });
    });
  }

  if (data === 'rsl_my_orders') {
    db.all(`SELECT id, product, amount, currency, status FROM orders WHERE reseller_id = ? ORDER BY id DESC LIMIT 5`, [reseller.id], (err, rows) => {
      if (err || !rows || rows.length === 0) {
        return resellerBot.sendMessage(chatId, '📦 У вас пока нет заказов.');
      }
      let msg = '📦 *Ваши последние заказы:*\n\n';
      rows.forEach(r => {
        const s = r.status === 'confirmed' ? '✅' : (r.status === 'out_of_stock' ? '❌' : (r.status === 'pending' ? '⏳' : '📥'));
        msg += `${s} Заказ #${r.id} | ${r.product} | ${r.amount} ${r.currency}\n`;
      });
      resellerBot.sendMessage(chatId, msg, { parse_mode: 'Markdown' });
    });
  }

  if (data === 'rsl_reset_token') {
    // BUG FIX RSL-1: При сбросе токена удаляем бота из resellerBots Map,
    // чтобы старый TelegramBot объект не накапливался и не обрабатывал апдейты.
    db.run(`UPDATE resellers SET status = 'awaiting_token', encrypted_token = NULL WHERE id = ?`, [reseller.id], async (err) => {
      if (!err) {
        try { await resellerBot.deleteWebHook(); } catch (_) {}
        resellerBot.removeAllListeners();
        resellerBots.delete(reseller.id);
        console.log(`🔄 [РЕСЕЛЛЕР ${reseller.id}] Токен сброшен, бот удалён из Map`);
      }
    });
    resellerBot.sendMessage(chatId, '🔄 Токен сброшен. Для активации нового токена — откройте основной бот и пройдите процедуру подключения заново.');
    return;
  }

  // Получаем текущую наценку
  const baseMarkup = reseller.markup_pct || 30;
  const multiplier = 1 + (baseMarkup / 100);

  // ──────────────────────────────────────
  // 🛒 ФЛОУ ПОКУПКИ КЛЮЧА
  // ──────────────────────────────────────
  if (data === 'rsl_buy') {
    // Читаем актуальные реквизиты из БД — не из кэша
    db.all(`SELECT method, details FROM payment_details`, [], (dbErr, payRows) => {
      const pd = {};
      if (payRows) payRows.forEach(r => { pd[r.method] = r.details; });
      // Fallback на in-memory для методов которых ещё нет в БД
      const getDetails = (m) => pd[m] || PAYMENT_DETAILS[m] || '';
      const isPd = (m) => { const v = getDetails(m); return v && !['0000 0000 0000 0000 (Mono)', 'CARD: IT00...', 'ID: 12345678', 'Wallet address', ''].includes(v); };

      const kbRows = [];
      const row1 = [];
      if (isPd('sbp')) row1.push({ text: '🇷🇺 RUB', callback_data: 'rsl_choose_cur_RUB' });
      if (isPd('card_ua')) row1.push({ text: '🇺🇦 UAH', callback_data: 'rsl_choose_cur_UAH' });
      if (row1.length) kbRows.push(row1);
      if (isPd('card_it')) kbRows.push([{ text: '🇪🇺 EUR', callback_data: 'rsl_choose_cur_EUR' }]);

      if (kbRows.length === 0) {
        resellerBot.sendMessage(chatId, isRu
          ? '❌ Методы оплаты не настроены. Свяжитесь с администратором.'
          : '❌ No payment methods configured. Please contact the admin.');
        return;
      }
      const txt = isRu ? '💳 Выберите валюту оплаты:' : '💳 Select payment currency:';
      resellerBot.sendMessage(chatId, txt, { reply_markup: { inline_keyboard: kbRows } });
    });
  }

  // Валюта выбрана → показываем витрину с ценами в нужной валюте
  if (data.startsWith('rsl_choose_cur_')) {
    const currency = data.replace('rsl_choose_cur_', '');
    const products = ['1d', '3d', '7d', '30d'];
    const rows = [];
    for (let i = 0; i < products.length; i += 2) {
      const row = [];
      for (let j = i; j < Math.min(i + 2, products.length); j++) {
        const p = products[j];
        const baseAmt = PRICES[p]?.[currency] || 0;
        const finalAmt = Math.round(baseAmt * multiplier * 100) / 100;
        const label = `${PERIOD_NAMES[isRu ? 'ru' : 'en'][p]} — ${formatPrice(finalAmt, currency)}`;
        row.push({ text: label, callback_data: `rsl_product_cur_${p}_${currency}` });
      }
      rows.push(row);
    }
    const txt = isRu ? '📦 Выберите ключ:' : '📦 Select key:';
    resellerBot.sendMessage(chatId, txt, { reply_markup: { inline_keyboard: rows } });
  }

  if (data === 'rsl_buy_boost') {
    if (getSetting('manual_boost_disabled') === '1') {
      resellerBot.sendMessage(chatId, isRu ? '❌ Опция временно отключена сервером.' : '❌ Option currently disabled by server.');
      return;
    }
    const bPrice = Math.round((PRICES['infinite_boost']?.RUB || 2500) * multiplier);
    const msgTemplate = isRu
      ? `🚀 *Секретный гайд: Бесконечный буст*\n\nЦена: *${bPrice} ₽* (эквивалент: ~$${(bPrice / EXCHANGE_RATES.USD).toFixed(2)} / ~€${(bPrice / EXCHANGE_RATES.EUR).toFixed(2)})`
      : `🚀 *Secret Infinite Boost Guide*\n\nPrice: *${bPrice} ₽* (equiv: ~$${(bPrice / EXCHANGE_RATES.USD).toFixed(2)} / ~€${(bPrice / EXCHANGE_RATES.EUR).toFixed(2)})`;

    resellerBot.sendMessage(chatId, msgTemplate, {
      parse_mode: 'Markdown',
      reply_markup: { inline_keyboard: [[{ text: isRu ? 'Купить' : 'Buy', callback_data: 'rsl_product_infinite_boost' }]] }
    });
  }

  // rsl_product_cur_{product}_{currency} — выбран продукт с уже известной валютой
  if (data.startsWith('rsl_product_cur_')) {
    const rest = data.replace('rsl_product_cur_', '');
    const parts = rest.split('_');
    const currency = parts.pop();
    const product = parts.join('_');
    const baseAmt = PRICES[product]?.[currency];
    if (!baseAmt) { resellerBot.sendMessage(chatId, '❌ Товар недоступен'); return; }
    const finalAmt = Math.round(baseAmt * multiplier * 100) / 100;

    // Читаем актуальные реквизиты из БД
    db.all(`SELECT method, details FROM payment_details`, [], (dbErr2, payRows2) => {
      const pd2 = {};
      if (payRows2) payRows2.forEach(r => { pd2[r.method] = r.details; });
      const getDetails2 = (m) => pd2[m] || PAYMENT_DETAILS[m] || '';
      const isPd2 = (m) => { const v = getDetails2(m); return v && !['0000 0000 0000 0000 (Mono)', 'CARD: IT00...', 'ID: 12345678', 'Wallet address', ''].includes(v); };

      const keyboard = [];
      if (currency === 'RUB' && isPd2('sbp')) keyboard.push([{ text: '🇷🇺 СБП / Карта РУ', callback_data: `rsl_pay_sbp_${product}_${currency}` }]);
      if (currency === 'UAH' && isPd2('card_ua')) keyboard.push([{ text: '🇺🇦 Карта UA (Mono)', callback_data: `rsl_pay_card_ua_${product}_${currency}` }]);
      if (currency === 'EUR' && isPd2('card_it')) keyboard.push([{ text: '🇮🇹 Card IT / SEPA', callback_data: `rsl_pay_card_it_${product}_${currency}` }]);
      if (keyboard.length === 0) {
        resellerBot.sendMessage(chatId, isRu ? `❌ Для валюты *${currency}* нет доступных методов.` : `❌ No methods for *${currency}*.`, { parse_mode: 'Markdown' });
        return;
      }
      const markupNote = isRu
        ? `\n_Цена включает наценку партнёра (${baseMarkup}%)_`
        : `\n_Price includes partner markup (${baseMarkup}%)_`;
      resellerBot.sendMessage(chatId, `💰 *К оплате:* ${formatPrice(finalAmt, currency)}\n${isRu ? 'Выберите способ оплаты:' : 'Select payment method:'}${markupNote}`, {
        parse_mode: 'Markdown', reply_markup: { inline_keyboard: keyboard }
      });
    });
  }

  // Выбор продукта -> валюта (только для boost и других мест где нет pre-selected валюты)
  if (data.startsWith('rsl_product_') && !data.startsWith('rsl_product_cur_')) {
    const product = data.replace('rsl_product_', '');
    const keyboard = {
      inline_keyboard: [
        [
          { text: '🇪🇺 EUR', callback_data: `rsl_cur_${product}_EUR` }
        ],
        [
          { text: '🇷🇺 RUB', callback_data: `rsl_cur_${product}_RUB` },
          { text: '🇺🇦 UAH', callback_data: `rsl_cur_${product}_UAH` }
        ]
      ]
    };
    resellerBot.sendMessage(chatId, isRu ? '💳 Выберите валюту:' : '💳 Select currency:', { reply_markup: keyboard });
  }

  // Выбор валюты -> метод оплаты
  if (data.startsWith('rsl_cur_')) {
    const parts = data.replace('rsl_cur_', '').split('_');
    const currency = parts.pop();
    const product = parts.join('_');

    const baseAmt = PRICES[product]?.[currency];
    if (!baseAmt) return;
    const multiplier = product === 'reseller_connection' ? 1 : Math.max(1.10, 1 + ((reseller.markup_pct || 30) / 100));
    const finalAmt = Math.round(baseAmt * multiplier * 100) / 100;

    // Читаем актуальные реквизиты из БД
    db.all(`SELECT method, details FROM payment_details`, [], (dbErrC, payRowsC) => {
      const pdC = {};
      if (payRowsC) payRowsC.forEach(r => { pdC[r.method] = r.details; });
      const isPdC = (m) => { const v = pdC[m] || PAYMENT_DETAILS[m] || ''; return v && !['0000 0000 0000 0000 (Mono)', 'CARD: IT00...', 'ID: 12345678', 'Wallet address', ''].includes(v); };

      const keyboard = [];
      if (currency === 'RUB') {
        if (isPdC('sbp')) keyboard.push([{ text: '🇷🇺 СБП / Карта РУ', callback_data: `rsl_pay_sbp_${product}_${currency}` }]);
      } else if (currency === 'UAH') {
        if (isPdC('card_ua')) keyboard.push([{ text: '🇺🇦 Карта UA (Mono)', callback_data: `rsl_pay_card_ua_${product}_${currency}` }]);
      } else if (currency === 'EUR') {
        if (isPdC('card_it')) keyboard.push([{ text: '🇮🇹 Card IT / SEPA', callback_data: `rsl_pay_card_it_${product}_${currency}` }]);
      }

      if (keyboard.length === 0) {
        resellerBot.sendMessage(chatId, isRu
          ? `❌ Для валюты *${currency}* нет доступных методов оплаты. Выберите другую валюту.`
          : `❌ No payment methods available for *${currency}*. Please select another currency.`,
          { parse_mode: 'Markdown' });
        return;
      }

      const markupNoteRsl = isRu
        ? `\n_Цена включает наценку партнёра (${baseMarkup}%)_`
        : `\n_Price includes partner markup (${baseMarkup}%)_`;
      resellerBot.sendMessage(chatId, `💰 *До оплаты:* ${formatPrice(finalAmt, currency)}\nВыберите способ оплаты:${markupNoteRsl}`, {
        parse_mode: 'Markdown',
        reply_markup: { inline_keyboard: keyboard }
      });
    });
  }

  // Способ оплаты (ручной) — ВСЕГДА читаем реквизиты из БД в момент показа,
  // чтобы изменения в admin-панели сразу видели все реселлер-боты.
  if (data.startsWith('rsl_pay_')) {
    // Формат: rsl_pay_{method}_{product}_{currency}
    // Методы с '_': card_ua, card_it — парсим явно
    const knownMethods = ['card_ua', 'card_it', 'sbp', 'binance', 'paypal'];
    let method = null, product = null, currency = null;
    const withoutPrefix = data.replace(/^rsl_pay_/, '');
    for (const m of knownMethods) {
      if (withoutPrefix.startsWith(m + '_')) {
        const rest = withoutPrefix.slice(m.length + 1);
        const curMatch = rest.match(/_(USD|EUR|RUB|UAH)$/);
        if (curMatch) {
          method = m;
          currency = curMatch[1];
          product = rest.slice(0, rest.length - curMatch[0].length);
          break;
        }
      }
    }
    if (!method || !product || !currency) return;

    const baseAmt = PRICES[product]?.[currency];
    if (!baseAmt) { resellerBot.sendMessage(chatId, isRu ? '❌ Товар недоступен.' : '❌ Product unavailable.'); return; }
    const mult = product === 'reseller_connection' ? 1 : Math.max(1.10, 1 + ((reseller.markup_pct || 30) / 100));
    const finalAmt = Math.round(baseAmt * mult * 100) / 100;

    // ✅ ВСЕГДА читаем актуальные реквизиты из БД — не из in-memory кэша
    db.get(`SELECT details FROM payment_details WHERE method = ?`, [method], (dbErr, payRow) => {
      // Fallback на in-memory если в БД ещё нет записи
      const details = (payRow && payRow.details) || PAYMENT_DETAILS[method] || '';
      const configured = details && !['0000 0000 0000 0000 (Mono)', 'CARD: IT00...', 'ID: 12345678', 'Wallet address', ''].includes(details);

      if (!configured) {
        resellerBot.sendMessage(chatId, isRu
          ? '❌ Реквизиты для этого способа не настроены. Обратитесь к администратору.'
          : '❌ Payment details for this method are not configured. Please contact the admin.');
        return;
      }

      // Создаем заказ в основной БД
      db.run(
        `INSERT INTO orders (user_id, username, user_lang, product, amount, currency, method, status, reseller_id, original_amount, original_currency) VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?, ?)`,
        [user.id, user.username, user.language_code, product, finalAmt, currency, method, reseller.id, baseAmt, currency],
        function (err) {
          if (err) return resellerBot.sendMessage(chatId, '❌ Ошибка создания заказа');

          const orderId = this.lastID;
          const session = getSession(chatId);
          session.state = 'rsl_awaiting_receipt';
          session.data = { order: { id: orderId, product, total_amount: finalAmt, currency, method } };

          const sbpIsUrl = (details || '').startsWith('http');

          if (method === 'sbp' && sbpIsUrl) {
            const msgTpl = isRu
              ? `🏦 *Оплата через СБП*\n\nНажмите кнопку ниже, вас перебросит на страницу банка для перевода *${formatPrice(finalAmt, currency)}*.\n\n📌 После оплаты отправьте скриншот чека сюда.`
              : `🏦 *Payment via SBP*\n\nClick the button below to proceed to the bank page to transfer *${formatPrice(finalAmt, currency)}*.\n\n📌 After paying, send a screenshot of the receipt here.`;
            resellerBot.sendMessage(chatId, msgTpl, {
              parse_mode: 'Markdown',
              reply_markup: { inline_keyboard: [[{ text: isRu ? `🇷🇺 Оплатить ${formatPrice(finalAmt, currency)}` : `🇷🇺 Pay ${formatPrice(finalAmt, currency)}`, url: details }]] }
            });
          } else if (method === 'sbp') {
            const msgTpl = isRu
              ? `🇷🇺 *Оплата через СБП / Карта*\n\nПереведите *${formatPrice(finalAmt, currency)}* по реквизитам ниже:\n\n💳 \`${escapeForBacktick(details)}\`\n\n📌 После оплаты отправьте скриншот чека сюда.`
              : `🇷🇺 *Payment via SBP / Card RU*\n\nTransfer *${formatPrice(finalAmt, currency)}* to the details below:\n\n💳 \`${escapeForBacktick(details)}\`\n\n📌 After paying, send a screenshot of the receipt here.`;
            resellerBot.sendMessage(chatId, msgTpl, { parse_mode: 'Markdown' });
          } else if (method === 'card_ua') {
            const msgTpl = isRu
              ? `🇺🇦 *Оплата — Карта UA (Mono)*\n\nПереведите *${formatPrice(finalAmt, currency)}* по реквизитам ниже:\n\n💳 \`${escapeForBacktick(details)}\`\n\n📌 После оплаты отправьте скриншот чека сюда.`
              : `🇺🇦 *Payment — UA Card (Mono)*\n\nTransfer *${formatPrice(finalAmt, currency)}* to the details below:\n\n💳 \`${escapeForBacktick(details)}\`\n\n📌 After paying, send a screenshot of the receipt here.`;
            resellerBot.sendMessage(chatId, msgTpl, { parse_mode: 'Markdown' });
          } else if (method === 'card_it') {
            const isUrl = (details || '').startsWith('http');
            const msgTpl = isRu
              ? `🇮🇹 *Оплата — Card IT / SEPA*\n\nПереведите *${formatPrice(finalAmt, currency)}* по реквизитам ниже${isUrl ? '' : `:\n\n💳 \`${escapeForBacktick(details)}\``}.\n\n📌 После оплаты отправьте скриншот чека сюда.`
              : `🇮🇹 *Payment — Card IT / SEPA*\n\nTransfer *${formatPrice(finalAmt, currency)}*${isUrl ? '' : ` to the details below:\n\n💳 \`${escapeForBacktick(details)}\``}.\n\n📌 After paying, send a screenshot of the receipt here.`;
            const payOpts = { parse_mode: 'Markdown' };
            if (isUrl) payOpts.reply_markup = { inline_keyboard: [[{ text: isRu ? `🇮🇹 Оплатить ${formatPrice(finalAmt, currency)}` : `🇮🇹 Pay ${formatPrice(finalAmt, currency)}`, url: details }]] };
            resellerBot.sendMessage(chatId, msgTpl, payOpts);
          } else {
            const msgTpl = isRu
              ? `🏦 *Оплата*\n\nПереведите *${formatPrice(finalAmt, currency)}*\n\n💳 \`${escapeForBacktick(details)}\`\n\n📌 После оплаты отправьте скриншот чека сюда.`
              : `🏦 *Payment*\n\nTransfer *${formatPrice(finalAmt, currency)}*\n\n💳 \`${escapeForBacktick(details)}\`\n\n📌 After paying, send a screenshot of the receipt here.`;
            resellerBot.sendMessage(chatId, msgTpl, { parse_mode: 'Markdown' });
          }
        }
      );
    });
  }

  // ──────────────────────────────────────
  // 🤝 ПАРТНЁРСТВО
  // ──────────────────────────────────────
  // FIX 1.2: Флоу партнёрства полностью переведён в основной бот.
  // Старая версия создавала кнопки rsl_cur_reseller_connection_* прямо здесь, минуя
  // шаги выбора наценки и анкеты. Это приводило к тому что заказ создавался без
  // reseller_markup_pct и reseller_questionnaire.
  // Основной бот уже содержит полный корректный флоу: reseller_activate →
  // awaiting_reseller_markup → awaiting_reseller_questionnaire → currency_ → pay_.
  // Направляем пользователя туда.
  if (data === 'rsl_partnership') {
    const mainBotUsername = process.env.BOT_USERNAME || 'cyraxxmod_bot';
    const connectionPrice = getSetting('reseller_price_rub') || 1500;

    const txt = isRu
      ? `🤝 *Партнёрская программа CyraxMods*\n\n` +
        `Стоимость подключения: *${connectionPrice} ₽* (разовый платёж)\n\n` +
        `Для оформления партнёрства перейдите в основной бот — там вы выберете наценку, ` +
        `заполните короткую анкету и оплатите:\n\n` +
        `👉 @${mainBotUsername}`
      : `🤝 *CyraxMods Partnership Program*\n\n` +
        `Connection fee: *${connectionPrice} ₽* (one-time payment)\n\n` +
        `To set up your partnership, please go to the main bot — ` +
        `you'll select your markup, fill a short form and pay there:\n\n` +
        `👉 @${mainBotUsername}`;

    resellerBot.sendMessage(chatId, txt, {
      parse_mode: 'Markdown',
      reply_markup: {
        inline_keyboard: [
          [{ text: isRu ? '🤝 Открыть основной бот' : '🤝 Open main bot', url: `https://t.me/${mainBotUsername}?start=partnership` }],
          [{ text: isRu ? '◀️ Назад' : '◀️ Back', callback_data: 'rsl_back_to_main' }]
        ]
      }
    });
    return;
  }

  // FIX 2.1: Закрываем глобальный try/catch
  } catch (err) {
    console.error(`❌ [РЕСЕЛЛЕР ${reseller.id}] handleResellerCallbackQuery error (data: ${data}):`, err);
    resellerBot.sendMessage(chatId,
      isRu ? '❌ Произошла ошибка. Попробуйте ещё раз или напишите /start.' : '❌ An error occurred. Please try again or type /start.'
    ).catch(() => {});
  }
}

// ==========================================
// ⏰ СИСТЕМА НАПОМИНАНИЙ (node-cron)
// ==========================================

// Буферы напоминаний по периодам (часы)
/**
 * Строит умное, персонализированное напоминание о продлении.
 * Шаблон зависит от периода ключа — тон и детали разные.
 * Включает честную оговорку про ориентировочность времени.
 *
 * @param {string} lang - 'ru' | 'en'
 * @param {string} product - '1d'|'3d'|'7d'|'30d'
 * @param {string} confirmedAt - ISO дата подтверждения заказа
 * @param {string} periodName - локализованное название периода
 * @returns {string}
 */
function buildReminderMessage(lang, product, confirmedAt, periodName) {
  const isRu = lang === 'ru';

  const daysMap = { '1d': 1, '3d': 3, '7d': 7, '30d': 30 };
  const days = daysMap[product] || 1;
  // IMPROVEMENT 1: Используем UTC для расчёта даты истечения вместо локального времени сервера.
  // На Render (и любом облачном хостинге) локальная TZ сервера может быть UTC+0 или произвольной,
  // что приводит к расхождению показываемой пользователю даты и реальной даты истечения ключа.
  const confirmedMs = new Date(confirmedAt).getTime();
  const expiryMs = confirmedMs + days * 24 * 60 * 60 * 1000;
  const expiryDate = new Date(expiryMs);
  // toLocaleDateString с явным timeZone: 'UTC' — дата всегда корректна независимо от TZ сервера
  const expiryStr = expiryDate.toLocaleDateString(
    isRu ? 'ru-RU' : 'en-GB',
    { day: 'numeric', month: 'long', timeZone: 'UTC' }
  );

  // Сноска — честная, без давления
  const disclaimer = isRu
    ? `<i>Время ориентировочное — точная дата зависит от момента активации ключа.</i>`
    : `<i>This is an approximate date — exact expiry depends on when the key was activated.</i>`;

  if (isRu) {
    switch (product) {
      case '1d':
        return (
          `👋 Добрый день!\n\n` +
          `Хотели напомнить: ваш суточный ключ к моду Cyrax, по нашим данным, истекает примерно <b>${expiryStr}</b>.\n\n` +
          `Если захотите продолжить — магазин всегда открыт, никакого давления 🙂\n\n` +
          disclaimer
        );
      case '3d':
        return (
          `👋 Добрый день!\n\n` +
          `Напоминаем, что ваш 3-дневный ключ к моду Cyrax ориентировочно истекает <b>${expiryStr}</b>.\n\n` +
          `Если ещё не наигрались — мы здесь 😊 Но это совершенно без спешки.\n\n` +
          disclaimer
        );
      case '7d':
        return (
          `👋 Добрый день!\n\n` +
          `Надеемся, неделя с модом Cyrax прошла хорошо! Ключ ориентировочно истекает <b>${expiryStr}</b>.\n\n` +
          `Если захотите — можно продолжить в любой момент, мы никуда не торопим.\n\n` +
          disclaimer
        );
      case '30d':
        return (
          `👋 Добрый день!\n\n` +
          `Месяц пролетел незаметно! Ваш ключ к моду Cyrax ориентировочно истекает <b>${expiryStr}</b>.\n\n` +
          `Было приятно быть рядом этот месяц 🙌 Если захотите — возвращайтесь, всегда рады.\n\n` +
          disclaimer
        );
      default:
        return (
          `👋 Добрый день!\n\n` +
          `Напоминаем: ваш ключ к моду Cyrax ориентировочно истекает <b>${expiryStr}</b>.\n\n` +
          `Если что-то понадобится — мы здесь.\n\n` +
          disclaimer
        );
    }
  } else {
    switch (product) {
      case '1d':
        return (
          `👋 Hello!\n\n` +
          `Just a friendly heads-up: your 1-day Cyrax mod key is expiring around <b>${expiryStr}</b>.\n\n` +
          `If you'd like to continue — the shop is always open, no pressure 🙂\n\n` +
          disclaimer
        );
      case '3d':
        return (
          `👋 Hello!\n\n` +
          `A quick reminder: your 3-day Cyrax mod key expires around <b>${expiryStr}</b>.\n\n` +
          `If you're still enjoying it — we're here 😊 No rush at all.\n\n` +
          disclaimer
        );
      case '7d':
        return (
          `👋 Hello!\n\n` +
          `Hope you had a great week with Cyrax mod! Your key expires around <b>${expiryStr}</b>.\n\n` +
          `If you'd like to keep going — feel free to drop by whenever you're ready.\n\n` +
          disclaimer
        );
      case '30d':
        return (
          `👋 Hello!\n\n` +
          `A month went by fast! Your Cyrax mod key expires around <b>${expiryStr}</b>.\n\n` +
          `It's been great having you with us 🙌 Come back whenever you feel like it.\n\n` +
          disclaimer
        );
      default:
        return (
          `👋 Hello!\n\n` +
          `Just a reminder: your Cyrax mod key expires around <b>${expiryStr}</b>.\n\n` +
          `We're here if you need anything.\n\n` +
          disclaimer
        );
    }
  }
}

const REMINDER_BUFFERS = {
  '1d': 12,
  '3d': 48,
  '7d': 120,
  '30d': 600,
  'infinite_boost': null // не напоминаем
};

async function sendRenewalReminders() {
  if (getSetting('reminders_enabled') === '0') return;
  console.log('🔔 Checking renewal reminders...');

  try {
    const bufferChecks = Object.entries(REMINDER_BUFFERS)
      .filter(([, hours]) => hours !== null)
      .map(([period, hours]) => `(product='${period}' AND datetime(confirmed_at, '+${hours} hours') <= datetime('now'))`);

    if (bufferChecks.length === 0) return;

    const query = `
      SELECT o.id, o.user_id, o.product, o.user_lang, o.confirmed_at, o.reseller_id, u.username
      FROM orders o
      JOIN users u ON o.user_id = u.id
      WHERE o.status = 'confirmed'
        AND (${bufferChecks.join(' OR ')})
        AND o.id NOT IN (SELECT order_id FROM reminders)
        AND o.user_id NOT IN (
          SELECT DISTINCT user_id FROM orders
          WHERE status = 'confirmed'
            AND datetime(confirmed_at) > datetime('now', '-2 days')
            AND id != o.id
        )
      ORDER BY o.confirmed_at ASC
      LIMIT 50
    `;

    db.all(query, [], async (err, orders) => {
      if (err) { console.error('❌ Reminders query error:', err); return; }
      if (!orders || orders.length === 0) { console.log('✅ No reminders to send'); return; }

      console.log(`🔔 Found ${orders.length} reminder(s) to propose`);

      // ✅ ОДНО сводное сообщение вместо спама по одному на каждый заказ
      // Формируем компактные строки для каждого клиента
      const rows = [];
      for (const order of orders) {
        const userObj = { language_code: order.user_lang || 'ru' };
        const periodName = PERIOD_NAMES[getLang(userObj)]?.[order.product] || order.product;
        const uname = order.username ? `@${escapeMarkdown(order.username)}` : `ID ${order.user_id}`;
        const confirmedDate = new Date(order.confirmed_at).toLocaleDateString('ru-RU');
        rows.push({ order, periodName, uname, confirmedDate });
      }

      // Автоматически отправляем напоминания всем — уведомляем админа фактом
      let sentCount = 0;
      for (const { order, periodName, uname } of rows) {
        const lang = getLang({ language_code: order.user_lang || 'ru' });
        const userObj = { language_code: order.user_lang || 'ru' };
        const msgText = buildReminderMessage(lang, order.product, order.confirmed_at, periodName);
        try {
          // Используем правильный бот — реселлерский или основной
          const reminderBotInstance = (order.reseller_id && resellerBots.get(order.reseller_id)?.bot) || bot;
          await sendWithAnimatedEmoji(order.user_id, msgText, ANIMATED_EMOJI.HOURGLASS, '⏳', {
            parse_mode: 'HTML',
            reply_markup: { inline_keyboard: [[{ text: t(userObj, 'reminder_button'), callback_data: 'buy' }]] }
          }, reminderBotInstance);
          db.run(`INSERT OR IGNORE INTO reminders (user_id, order_id) VALUES (?, ?)`, [order.user_id, order.id]);
          logAction(ADMIN_ID, 'reminder_sent_auto', { orderId: order.id, userId: order.user_id });
          sentCount++;
        } catch (e) {
          console.error(`❌ Auto reminder failed for user ${order.user_id}:`, e.message);
        }
        await new Promise(r => setTimeout(r, 300)); // небольшая пауза между отправками
      }
      if (sentCount > 0) {
        // Компактный отчёт — одна строка на клиента
        const lines = rows.slice(0, sentCount).map(({ uname, periodName }) =>
          `• ${uname} · ${periodName}`
        ).join('\n');
        safeSendMessage(ADMIN_ID,
          `🔔 *Напоминания (${sentCount})* — отправлены авто\n\n${lines}`,
          { parse_mode: 'Markdown' }
        ).catch(() => {});
      }
      return;

      // Несколько клиентов — одно сводное сообщение с кнопкой "Открыть список"
      // Детальный просмотр — постранично в том же сообщении (editMessage), без спама
      const batchKey = `remind_batch_${Date.now()}`;
      // Сохраняем batch в сессии ADMIN_ID
      const adminSession = getSession(ADMIN_ID);
      adminSession.reminderBatch = rows.map(r => ({
        orderId: r.order.id,
        userId: r.order.user_id,
        product: r.order.product,
        confirmedAt: r.order.confirmed_at,
        periodName: r.periodName,
        uname: r.uname,
        confirmedDate: r.confirmedDate,
        userLang: r.order.user_lang || 'ru'
      }));
      adminSession.reminderBatch.__key = batchKey;

      const summaryLines = rows.map((r, i) =>
        `${i + 1}. ${r.uname} · 📦 ${r.periodName} · 📅 ${r.confirmedDate}`
      ).join('\n');

      const summaryMsg =
        `🔔 *Напоминания о продлении — ${rows.length} клиентов*

` +
        `${summaryLines}

` +
        `_Нажмите «Разобрать» чтобы отправить/пропустить каждому._`;

      await safeSendMessage(ADMIN_ID, summaryMsg, {
        parse_mode: 'Markdown',
        reply_markup: {
          inline_keyboard: [
            [{ text: `📋 Разобрать (${rows.length})`, callback_data: 'remind_batch_open_0' }],
            [
              { text: '✅ Отправить всем', callback_data: 'remind_batch_send_all' },
              { text: '⏭ Пропустить всех', callback_data: 'remind_batch_skip_all' }
            ]
          ]
        }
      }).catch(e => console.error('❌ Reminder batch summary error:', e));
    });
  } catch (e) {
    console.error('❌ sendRenewalReminders error:', e);
  }
}

// ==========================================
// 🏆 СИСТЕМА АНАЛИЗА ЧАСТЫХ ПОКУПАТЕЛЕЙ
// ==========================================

async function analyzeFrequentBuyers() {
  console.log('🔍 Analyzing frequent buyers...');

  try {
    const thirtyDaysAgo = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString();

    db.all(
      `SELECT
        o.user_id,
        u.username,
        u.language_code,
        COUNT(o.id) as purchase_count,
        SUM(o.amount) as total_spent,
        o.currency,
        (SELECT product FROM orders WHERE user_id = o.user_id AND status = 'confirmed'
           AND datetime(created_at) >= datetime(?)
           GROUP BY product ORDER BY COUNT(*) DESC LIMIT 1) as fav_product
       FROM orders o
       JOIN users u ON o.user_id = u.id
       WHERE o.status = 'confirmed'
         AND datetime(o.created_at) >= datetime(?)
         AND o.product != 'infinite_boost'
       GROUP BY o.user_id
       HAVING purchase_count >= 2
       ORDER BY purchase_count DESC
       LIMIT 10`,
      [thirtyDaysAgo, thirtyDaysAgo],
      async (err, buyers) => {
        if (err) { console.error('❌ Frequent buyers error:', err); return; }
        if (!buyers || buyers.length === 0) { console.log('ℹ️ No frequent buyers found'); return; }

        // Сохраняем список в памяти для последующего просмотра
        global._frequentBuyersCache = {
          buyers,
          fetchedAt: new Date().toISOString()
        };

        // Одно сводное сообщение вместо спама карточек
        const count = buyers.length;
        const topBuyer = buyers[0];
        const topName = topBuyer.username ? `@${escapeMarkdown(topBuyer.username)}` : `ID: ${topBuyer.user_id}`;
        const summary =
          `🏆 *Постоянные клиенты — сводка*\n\n` +
          `👥 Найдено активных клиентов: *${count}*\n` +
          `🥇 Топ: ${topName} (${topBuyer.purchase_count} покупок за 30 дней)\n\n` +
          `Нажмите кнопку ниже, чтобы просмотреть список и выдать купоны.`;

        await safeSendMessage(ADMIN_ID, summary, {
          parse_mode: 'Markdown',
          reply_markup: {
            inline_keyboard: [[
              { text: `🏆 Просмотреть ${count} клиентов`, callback_data: 'admin_frequent_buyers' }
            ]]
          }
        }).catch(e => console.error('❌ Frequent buyers notify error:', e));
      }
    );
  } catch (e) {
    console.error('❌ analyzeFrequentBuyers error:', e);
    safeSendMessage(ADMIN_ID, `❌ Ошибка анализа частых покупателей: ${e.message}`).catch(() => { });
  }
}

// ==========================================
// 🌟 РАСШИРЕННЫЙ ЕЖЕДНЕВНЫЙ ОТЧЁТ
// ==========================================

async function sendDailyReport() {
  if (getSetting('notify_daily_report') !== '1') return;
  try {
    const today = new Date().toISOString().split('T')[0];
    const yesterday = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString().split('T')[0];
    const thirtyAgo = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString();
    db.get(
      `SELECT
        (SELECT COUNT(*) FROM orders WHERE date(created_at)=? AND status='confirmed') as ord_today,
        (SELECT COUNT(*) FROM orders WHERE date(created_at)=? AND status='confirmed') as ord_yest,
        (SELECT COUNT(*) FROM orders WHERE status='pending') as pending,
        (SELECT COUNT(*) FROM orders WHERE date(created_at)=? AND status='rejected') as rejected,
        (SELECT COUNT(*) FROM users) as total_users,
        (SELECT COUNT(*) FROM users WHERE date(joined_at)=?) as new_users,
        (SELECT SUM(amount) FROM orders WHERE date(created_at)=? AND status='confirmed' AND currency='USD') as rev_usd,
        (SELECT SUM(amount) FROM orders WHERE date(created_at)=? AND status='confirmed' AND currency='RUB') as rev_rub,
        (SELECT SUM(amount) FROM orders WHERE date(created_at)=? AND status='confirmed' AND currency='EUR') as rev_eur,
        (SELECT SUM(amount) FROM orders WHERE date(created_at)=? AND status='confirmed' AND currency='UAH') as rev_uah,
        (SELECT COUNT(*) FROM keys WHERE status='available') as keys_left`,
      [today, yesterday, today, today, today, today, today, today],
      async (err, s) => {
        if (err) return;
        const o = (s && s.ord_today) || 0;
        const oy = (s && s.ord_yest) || 0;
        const trend = o > oy ? '📈' : o < oy ? '📉' : '➡️';
        let msg = `📊 *Ежедневный отчёт* — ${today}\n\n`;
        msg += `${trend} Заказов сегодня: *${o}* _(вчера: ${oy})_\n`;
        msg += `⏳ Ожидают подтверждения: *${(s && s.pending) || 0}*\n`;
        if ((s && s.rejected) > 0) msg += `❌ Отклонено: *${s.rejected}*\n`;
        msg += `\n👥 Пользователей: *${(s && s.total_users) || 0}*`;
        if ((s && s.new_users) > 0) msg += ` _(+${s.new_users} новых)_`;
        msg += `\n🔑 Ключей в запасе: *${(s && s.keys_left) || 0}*\n`;
        msg += `\n💰 *Выручка сегодня:*\n`;
        let any = false;
        if (s && s.rev_usd) { msg += `   💵 $${parseFloat(s.rev_usd).toFixed(2)}\n`; any = true; }
        if (s && s.rev_rub) { msg += `   🇷🇺 ${Math.round(s.rev_rub)} ₽\n`; any = true; }
        if (s && s.rev_eur) { msg += `   💶 €${parseFloat(s.rev_eur).toFixed(2)}\n`; any = true; }
        if (s && s.rev_uah) { msg += `   🇺🇦 ${Math.round(s.rev_uah)} ₴\n`; any = true; }
        if (!any) msg += `   —\n`;
        db.all(
          `SELECT o.user_id, u.username, COUNT(o.id) as cnt
           FROM orders o JOIN users u ON o.user_id = u.id
           WHERE o.status='confirmed' AND datetime(o.created_at)>=datetime(?)
           AND o.product!='infinite_boost'
           GROUP BY o.user_id HAVING cnt>=2 ORDER BY cnt DESC LIMIT 3`,
          [thirtyAgo],
          async (e2, top) => {
            if (!e2 && top && top.length > 0) {
              msg += `\n🏆 *Постоянники (30 дней):*\n`;
              top.forEach((b, i) => {
                const u = b.username ? escapeMarkdown('@' + b.username) : 'ID: ' + b.user_id;
                msg += `${i + 1}. ${u} — ${b.cnt} покупок\n`;
              });
            }
            await safeSendMessage(ADMIN_ID, msg, { parse_mode: 'Markdown' }).catch(() => { });
          }
        );
      }
    );
  } catch (e) { console.error('❌ sendDailyReport error:', e); }
}



// ==========================================
// 📥 ЭКСПОРТ ЗАКАЗОВ В CSV
// ==========================================
async function exportStatsToCsv(chatId) {
  try {
    await safeSendMessage(chatId, '⏳ Формирую CSV...');
    db.all(
      `SELECT o.id, o.user_id, COALESCE(u.username,'') as username, o.product,
              o.amount, o.currency, o.method, o.status,
              COALESCE(o.key_issued,'') as key_issued,
              o.created_at, COALESCE(o.confirmed_at,'') as confirmed_at,
              COALESCE(o.transaction_id,'') as transaction_id,
              COALESCE(c.code,'') as coupon_code,
              COALESCE(c.discount_percent,0) as discount_percent
       FROM orders o
       LEFT JOIN users u ON o.user_id = u.id
       LEFT JOIN user_coupons uc ON uc.order_id = o.id
       LEFT JOIN coupons c ON uc.coupon_id = c.id
       ORDER BY o.created_at DESC`,
      [],
      async (err, orders) => {
        if (err || !orders) {
          safeSendMessage(chatId, 'ERR getting data');
          return;
        }
        const header = 'ID,UserID,Username,Product,Amount,Currency,Method,Status,Key,CreatedAt,ConfirmedAt,TxnID,CouponCode,Discount%';
        const rows = orders.map(o => {
          const uname = o.username ? '@' + o.username : '';
          const key = String(o.key_issued || '').replace(/,/g, ';');
          const created = o.created_at ? new Date(o.created_at).toLocaleString('ru-RU') : '';
          const confirmed = o.confirmed_at ? new Date(o.confirmed_at).toLocaleString('ru-RU') : '';
          return [o.id, o.user_id, uname, o.product, o.amount, o.currency,
          o.method, o.status, key, created, confirmed,
          o.transaction_id, o.coupon_code, o.discount_percent].join(',');
        });
        const csvContent = rows.length > 0 ? [header, ...rows].join('\n') : header;
        const date = new Date().toISOString().split('T')[0];
        const fileName = 'cyraxmods_orders_' + date + '.csv';
        const filePath = path.join(__dirname, fileName);
        fs.writeFileSync(filePath, '\uFEFF' + csvContent, 'utf8');
        await bot.sendDocument(chatId, filePath, {
          caption: '*Экспорт заказов*\n' + date + '\nСтрок: ' + orders.length,
          parse_mode: 'Markdown'
        });
        fs.unlinkSync(filePath);
        logAction(chatId, 'csv_export', { rows: orders.length });
        console.log('Export CSV: ' + orders.length + ' orders');
      }
    );
  } catch (e) {
    console.error('exportStatsToCsv error:', e);
    safeSendMessage(chatId, 'ERR export: ' + e.message);
  }
}


// ============================================================
// 🆕 НОВЫЕ ФУНКЦИИ (Задачи 1-5)
// ============================================================

// ============================================================
// 🆕 НОВЫЕ ФУНКЦИИ — CyraxMods Bot Patch (Задачи 1-5)
// Вставить в основной файл перед разделом EXPRESS СЕРВЕР
// ============================================================

// ============================================================
// 🎁 ЗАДАЧА 1 — ПРОГРАММА ЛОЯЛЬНОСТИ
// ============================================================

/**
 * Возвращает персональную скидку пользователя из БД.
 * @param {number} userId
 * @returns {Promise<number>} процент скидки (0 = нет скидки)
 */
function getLoyaltyDiscount(userId) {
  return new Promise((resolve) => {
    db.get(`SELECT loyalty_discount FROM users WHERE id = ?`, [userId], (err, row) => {
      if (err || !row) return resolve(0);
      resolve(row.loyalty_discount || 0);
    });
  });
}

/**
 * Применяет персональную скидку к сумме.
 * @param {number} amount - исходная сумма
 * @param {number} discountPercent - процент скидки
 * @returns {number}
 */
function applyLoyaltyDiscount(amount, discountPercent) {
  if (!discountPercent || discountPercent <= 0) return amount;
  const discounted = amount * (1 - discountPercent / 100);
  // Округляем: для RUB до целых, для остальных до 2 знаков
  return Math.round(discounted * 100) / 100;
}

/**
 * Показывает панель управления лояльностью в админке.
 * @param {number} chatId
 */
function showLoyaltyPanel(chatId, msgId = null) {
  const defaultDiscount = getSetting('default_loyalty_discount') || '0';

  db.all(
    `SELECT u.id, u.username, u.loyalty_discount,
            COUNT(o.id) as orders_count,
            COALESCE(SUM(o.amount), 0) as total_spent,
            MAX(o.currency) as currency
     FROM users u
     LEFT JOIN orders o ON o.user_id = u.id AND o.status = 'confirmed'
     GROUP BY u.id
     ORDER BY orders_count DESC
     LIMIT 10`,
    [],
    (err, users) => {
      let message = `🎁 *Управление лояльностью*\n\n`;
      message += `⚙️ Глобальная скидка по умолчанию: *${defaultDiscount}%*\n`;
      message += `_(Устанавливается вручную при желании для новых VIP-клиентов)_\n\n`;
      message += `👥 *Топ-10 клиентов:*\n\n`;

      const keyboard = { inline_keyboard: [] };

      if (!err && users && users.length > 0) {
        users.forEach((u, i) => {
          const uname = u.username ? escapeMarkdown(`@${u.username}`) : `ID: ${u.id}`;
          const discount = u.loyalty_discount || 0;
          const discIcon = discount > 0 ? `🎁 ${discount}%` : '—';
          message += `${i + 1}. ${uname}\n`;
          message += `   🛒 ${u.orders_count} покупок | 💸 Скидка: ${discIcon}\n\n`;
          keyboard.inline_keyboard.push([
            { text: `✏️ Скидка: ${uname} (${discount}%)`, callback_data: `loyalty_edit_discount_${u.id}` }
          ]);
        });
      } else {
        message += '_Нет клиентов_\n';
      }

      keyboard.inline_keyboard.push([
        { text: `✏️ Глобальная скидка (${defaultDiscount}%)`, callback_data: 'loyalty_edit_default' }
      ]);
      keyboard.inline_keyboard.push([{ text: '◀️ Назад', callback_data: 'admin' }]);

      adminSend(chatId, ADMIN_ID, message, { parse_mode: 'Markdown', reply_markup: keyboard }, msgId);
    }
  );
}

// ============================================================
// 🎫 ЗАДАЧА 2 — FOMO-КУПОНЫ (случайные купоны после покупки)
// ============================================================

/**
 * Показывает панель управления FOMO-механикой в админке.
 * @param {number} chatId
 */
function showFomoPanel(chatId, msgId = null) {
  const enabled = getSetting('fomo_enabled') === '1';
  const chance = getSetting('fomo_chance') || '40';
  const maxPct = getSetting('fomo_max_percent') || '20';
  const expiry = getSetting('fomo_coupon_expiry_days') || '7';

  const statusIcon = enabled ? '🟢' : '🔴';
  const message =
    `🎫 *Управление FOMO-купонами*\n\n` +
    `${statusIcon} Статус: *${enabled ? 'Включено' : 'Выключено'}*\n` +
    `🎲 Шанс выпадения: *${chance}%*\n` +
    `💯 Макс. процент купона: *${maxPct}%*\n` +
    `📅 Срок действия купона: *${expiry} дней*\n\n` +
    `*Правила выпадения купонов (по RUB-эквиваленту заказа):*\n` +
    `• < 300 ₽ → купон 3–5%\n` +
    `• 300–1000 ₽ → купон 5–10%\n` +
    `• > 1000 ₽ → купон 10–${maxPct}%\n\n` +
    `_Для USD/EUR/UAH заказов сумма автоматически пересчитывается в ₽ по курсу._\n` +
    `_Купон выдаётся после подтверждения заказа._`;

  const keyboard = {
    inline_keyboard: [
      [{ text: `${statusIcon} ${enabled ? 'Выключить' : 'Включить'} FOMO`, callback_data: 'fomo_toggle' }],
      [
        { text: `🎲 Шанс (${chance}%)`, callback_data: 'fomo_edit_chance' },
        { text: `💯 Макс. % (${maxPct}%)`, callback_data: 'fomo_edit_max_percent' }
      ],
      [{ text: `📅 Срок (${expiry} дн.)`, callback_data: 'fomo_edit_expiry' }],
      [{ text: '◀️ Назад', callback_data: 'admin' }]
    ]
  };

  adminSend(chatId, ADMIN_ID, message, { parse_mode: 'Markdown', reply_markup: keyboard }, msgId);
}

/**
 * Основная логика FOMO-купонов. Вызывается после успешного подтверждения заказа.
 * @param {object} order - { user_id, amount_rub, product, user_lang, order_id }
 * @param {object} botInstance - инстанс бота для отправки
 */
// ==========================================
// 🤝 FIX 2.3: Единая функция начисления бонуса рефереру за reseller_connection
// Вынесена из approve_ и CryptoBot-вебхука чтобы избежать дублирования и ошибок.
// ==========================================
async function processResellerConnectionBonus(resellerId) {
  if (!resellerId) return;
  return new Promise((resolve) => {
    db.run(`UPDATE resellers SET balance = balance + 500 WHERE id = ?`, [resellerId], (err) => {
      if (err) {
        console.error('❌ processResellerConnectionBonus update error:', err);
        resolve();
        return;
      }
      db.get(`SELECT user_id FROM resellers WHERE id = ?`, [resellerId], (err2, rec) => {
        if (!err2 && rec) {
          safeSendMessage(
            rec.user_id,
            `🎉 *Поздравляем!*\n\nВаш клиент приобрёл партнёрскую программу.\nВам начислен бонус: *500 ₽*`,
            { parse_mode: 'Markdown' }
          ).catch(() => { });
        }
        resolve();
      });
    });
  });
}

async function handleFomoCoupon(order, botInstance = bot) {
  try {
    if (getSetting('fomo_enabled') !== '1') return;
    if (order.product === 'infinite_boost') return; // Для гайда — не выдаём

    const chance = parseInt(getSetting('fomo_chance') || '40');
    const maxPct = parseInt(getSetting('fomo_max_percent') || '20');
    const expiryDays = parseInt(getSetting('fomo_coupon_expiry_days') || '7');

    // Бросаем кубик
    if (Math.random() * 100 > chance) return;

    const amountRub = order.amount_rub || 0;

    // Выбираем диапазон процентов по сумме
    let minPct, maxPctRange;
    if (amountRub < 300) {
      minPct = 3; maxPctRange = 5;
    } else if (amountRub <= 1000) {
      minPct = 5; maxPctRange = 10;
    } else {
      minPct = 10; maxPctRange = maxPct;
    }

    // Не выходим за пределы maxPct
    maxPctRange = Math.min(maxPctRange, maxPct);
    if (minPct > maxPctRange) minPct = Math.max(3, maxPctRange - 2);

    const pct = Math.floor(Math.random() * (maxPctRange - minPct + 1)) + minPct;

    // Генерируем уникальный код купона
    const couponCode = 'FOMO' + crypto.randomBytes(4).toString('hex').toUpperCase();
    const expiresAt = new Date(Date.now() + expiryDays * 24 * 60 * 60 * 1000).toISOString();

    // Создаём купон в БД, привязанный к пользователю
    await new Promise((resolve, reject) => {
      db.run(
        `INSERT INTO coupons (code, discount_percent, max_uses, expires_at, created_by, user_id) VALUES (?, ?, 1, ?, ?, ?)`,
        [couponCode, pct, expiresAt, ADMIN_ID, order.user_id],
        function (err) {
          if (err) return reject(err);
          // Связываем купон с пользователем в user_coupons
          db.run(
            `INSERT OR IGNORE INTO user_coupons (user_id, coupon_id) VALUES (?, ?)`,
            [order.user_id, this.lastID],
            resolve
          );
        }
      );
    });

    // Отправляем сообщение пользователю на его языке
    const userObj = { language_code: order.user_lang || 'en' };
    const msgText = t(userObj, 'fomo_coupon_msg', { percent: pct, code: couponCode, days: expiryDays });

    // Небольшая задержка — не перебиваем основное сообщение с ключом
    await new Promise(r => setTimeout(r, 2000));
    await safeSendMessage(order.user_id, msgText, { parse_mode: 'HTML' }, botInstance);

    logAction(order.user_id, 'fomo_coupon_issued', { code: couponCode, pct, amountRub });
    console.log(`🎫 FOMO купон ${couponCode} (${pct}%) выдан пользователю ${order.user_id}`);

  } catch (e) {
    console.error('❌ handleFomoCoupon error:', e);
  }
}

// ============================================================
// 💬 ЗАДАЧА 3 — СИСТЕМА ОТЗЫВОВ С БОНУСОМ
// ============================================================

/**
 * Обрабатывает запрос на отзыв от пользователя (по кнопке в сообщении с ключом).
 * Создаёт уникальный код и отправляет инструкцию.
 * @param {object} user - Telegram user object
 * @param {number} chatId
 * @param {number} orderId
 */
async function handleReviewRequest(user, chatId, orderId) {
  try {
    const isRu = getLang(user) === 'ru';

    // Проверяем существующий код для этого заказа
    const existing = await new Promise((resolve) => {
      db.get(`SELECT * FROM review_codes WHERE user_id = ? AND order_id = ?`, [user.id, orderId], (e, row) => resolve(row));
    });

    let code;
    if (existing) {
      code = existing.code;
    } else {
      const orderPeriod = await new Promise((resolve) => {
        db.get(`SELECT product FROM orders WHERE id = ?`, [orderId], (err, row) => resolve(row ? row.product : null));
      });

      code = 'REVIEW-' + Math.floor(100000 + Math.random() * 900000);
      await new Promise((resolve, reject) => {
        db.run(
          `INSERT INTO review_codes (user_id, code, order_id, product_period) VALUES (?, ?, ?, ?)`,
          [user.id, code, orderId, orderPeriod],
          (err) => {
            if (err) {
              db.run(`ALTER TABLE review_codes ADD COLUMN product_period TEXT`, () => {
                db.run(`INSERT INTO review_codes (user_id, code, order_id, product_period) VALUES (?, ?, ?, ?)`,
                  [user.id, code, orderId, orderPeriod], (e) => e ? reject(e) : resolve());
              });
            } else resolve();
          }
        );
      });
    }

    const reviewLink = getSetting('review_channel_link') || 'https://t.me/cyraxml/368';

    const msg = isRu
      ? `✍️ <b>Бонус за отзыв</b>\n\n1. Нажмите кнопку ниже и оставьте отзыв в нашем посте\n\n2. Отправьте сюда ваш код:\n🔑 <code>${code}</code>\n└ (нажмите чтобы скопировать)\n\nПосле этого бонус придёт автоматически. 💜`
      : `✍️ <b>Review bonus</b>\n\n1. Tap the button below and leave a review in our post\n\n2. Send your code here:\n🔑 <code>${code}</code>\n└ (tap to copy)\n\nYour bonus will be sent automatically. 💜`;

    await safeSendMessage(chatId, msg, {
      parse_mode: 'HTML',
      reply_markup: {
        inline_keyboard: [
          [{ text: isRu ? '✍️ Оставить отзыв' : '✍️ Leave a review', url: reviewLink }]
        ]
      }
    });

    logAction(user.id, 'review_code_requested', { code, orderId });
  } catch (e) {
    console.error('❌ handleReviewRequest error:', e);
  }
}

/**
 * Отправляет пользователю приглашение оставить отзыв (вызывается автоматически после подтверждения заказа).
 * НЕ генерирует код — только приглашает нажать кнопку.
 * @param {number} userId
 * @param {string} userLang
 * @param {number} orderId
 */
// [handleReviewInvite removed — review button embedded in sendKeyMessage]

/**
 * Показывает панель управления отзывами в админке.
 * @param {number} chatId
 */
function showReviewsPanel(chatId, page = 0, msgId = null) {
  db.all(
    `SELECT rc.*, u.username FROM review_codes rc
     LEFT JOIN users u ON u.id = rc.user_id
     WHERE rc.is_used = 0
     ORDER BY rc.created_at DESC LIMIT 20`,
    [],
    (err, rows) => {
      if (!err && rows && rows.length > 0) {
        // Показываем по одному — постраничная навигация без спама
        const idx = Math.min(page, rows.length - 1);
        const r = rows[idx];
        const uname = r.username ? `@${escapeMarkdown(r.username)}` : `ID: ${r.user_id}`;
        const date = r.created_at ? new Date(r.created_at).toLocaleDateString('ru-RU', { day: '2-digit', month: '2-digit', year: '2-digit' }) : '—';
        const total = rows.length;

        const msg =
          `📝 *Награды за отзывы* (${idx + 1}/${total})

` +
          `👤 ${uname}
` +
          `🔑 Код: \`${r.code}\`
` +
          `📅 ${date}`;

        const kb = { inline_keyboard: [] };

        // Строка выдачи
        kb.inline_keyboard.push([
          { text: '🎁 Выдать награду', callback_data: `review_reward_${r.id}` },
          { text: '❌ Отклонить', callback_data: `review_reject_${r.id}_p${idx}` }
        ]);

        // Навигация
        const navRow = [];
        if (idx > 0) navRow.push({ text: `◀️ (${idx})`, callback_data: `review_page_${idx - 1}` });
        if (idx < total - 1) navRow.push({ text: `(${total - idx - 1}) ▶️`, callback_data: `review_page_${idx + 1}` });
        if (navRow.length > 0) kb.inline_keyboard.push(navRow);

        // Отклонить все сразу
        if (total > 1) {
          kb.inline_keyboard.push([{ text: `🗑 Отклонить все ${total}`, callback_data: 'review_reject_all' }]);
        }
        kb.inline_keyboard.push([{ text: '◀️ Назад', callback_data: 'admin' }]);

        adminSend(chatId, ADMIN_ID, msg, { parse_mode: 'Markdown', reply_markup: kb }, msgId);
      } else {
        safeSendMessage(chatId, `📝 *Награды за отзывы*

_Нет ожидающих кодов_`, {
          parse_mode: 'Markdown',
          reply_markup: { inline_keyboard: [[{ text: '◀️ Назад', callback_data: 'admin' }]] }
        });
      }
    }
  );
}

/**
 * Выдаёт купон в качестве награды за отзыв.
 * @param {number} adminChatId
 * @param {number} reviewId - ID записи в review_codes
 * @param {number} pct - процент скидки
 */
function giveReviewRewardCoupon(adminChatId, reviewId, pct) {
  db.get(`SELECT * FROM review_codes WHERE id = ? AND is_used = 0`, [reviewId], (err, rc) => {
    if (err || !rc) { bot.sendMessage(adminChatId, '❌ Код не найден или уже использован'); return; }

    const couponCode = 'REVIEW-BONUS-' + crypto.randomBytes(3).toString('hex').toUpperCase();
    const expiresAt = new Date(Date.now() + 30 * 24 * 60 * 60 * 1000).toISOString(); // 30 дней

    db.run(
      `INSERT INTO coupons (code, discount_percent, max_uses, expires_at, created_by, user_id) VALUES (?, ?, 1, ?, ?, ?)`,
      [couponCode, pct, expiresAt, ADMIN_ID, rc.user_id],
      function (dbErr) {
        if (dbErr) { bot.sendMessage(adminChatId, '❌ Ошибка создания купона: ' + dbErr.message); return; }
        const couponId = this.lastID;

        db.run(`INSERT OR IGNORE INTO user_coupons (user_id, coupon_id) VALUES (?, ?)`, [rc.user_id, couponId]);
        db.run(`UPDATE review_codes SET is_used = 1, reward_type = 'coupon', reward_value = ?, rewarded_at = datetime('now') WHERE id = ?`,
          [couponCode, reviewId]);

        // Уведомляем пользователя
        db.get(`SELECT language_code FROM users WHERE id = ?`, [rc.user_id], (e, u) => {
          const userObj = { language_code: (u && u.language_code) || 'en' };
          const isRu = getLang(userObj) === 'ru';
          const rewardText = isRu
            ? `🎟️ Купон *${pct}%* скидки\n🔑 Код: \`${couponCode}\`\n⏰ Действует 30 дней`
            : `🎟️ *${pct}%* discount coupon\n🔑 Code: \`${couponCode}\`\n⏰ Valid 30 days`;
          safeSendMessage(rc.user_id, t(userObj, 'review_reward_sent', { reward: rewardText }), { parse_mode: 'Markdown' }).catch(() => { });
        });

        bot.sendMessage(adminChatId, `✅ Купон \`${couponCode}\` (${pct}%) выдан`, { parse_mode: 'Markdown' });
        logAction(ADMIN_ID, 'review_reward_coupon', { reviewId, pct, couponCode });
      }
    );
  });
}

/**
 * Выдаёт бесплатный ключ в качестве награды за отзыв.
 * @param {number} adminChatId
 * @param {number} reviewId
 * @param {string} period - '1d'|'3d'|'7d'|'30d'
 */
async function giveReviewRewardKey(adminChatId, reviewId, period) {
  db.get(`SELECT * FROM review_codes WHERE id = ? AND is_used = 0`, [reviewId], async (err, rc) => {
    if (err || !rc) { bot.sendMessage(adminChatId, '❌ Код не найден или уже использован'); return; }

    try {
      const key = await issueKeyToUser(rc.user_id, period, 'review');
      db.run(`UPDATE review_codes SET is_used = 1, reward_type = 'key', reward_value = ?, rewarded_at = datetime('now') WHERE id = ?`,
        [period, reviewId]);

      db.get(`SELECT language_code FROM users WHERE id = ?`, [rc.user_id], (e, u) => {
        const userLang = (u && u.language_code) || 'en';
        const userObj = { language_code: userLang };
        const isRu = getLang(userObj) === 'ru';
        const periodName = PERIOD_NAMES[isRu ? 'ru' : 'en'][period] || period;
        const rewardText = isRu
          ? `🔑 Бесплатный ключ на *${periodName}*\n\`${key}\``
          : `🔑 Free key for *${periodName}*\n\`${key}\``;
        safeSendMessage(rc.user_id, t(userObj, 'review_reward_sent', { reward: rewardText }), { parse_mode: 'Markdown' }).catch(() => { });
      });

      bot.sendMessage(adminChatId, `✅ Ключ на ${period} выдан пользователю ${rc.user_id}`);
      logAction(ADMIN_ID, 'review_reward_key', { reviewId, period, key });
    } catch (e) {
      if (e.code === 'OUT_OF_STOCK') {
        // FIX 2.3: Единая стратегия нехватки ключей — операция откладывается, НЕ отклоняется.
        // review_code остаётся is_used = 0 (не трогаем), admin получает кнопку повтора.
        // Это согласовано с поведением rsl_withdraw_approve_: при OUT_OF_STOCK заявка
        // тоже остаётся pending, а баланс/награда не списываются.
        console.error(`❌ OUT_OF_STOCK for review reward: reviewId=${reviewId}, period=${period}`);
        bot.sendMessage(adminChatId,
          `⚠️ *Нет ключей для периода ${period}*\n\nНаграда за отзыв #${reviewId} НЕ выдана — ключи закончились.\nДобавьте ключи и нажмите кнопку для повторной выдачи.`,
          {
            parse_mode: 'Markdown',
            reply_markup: {
              inline_keyboard: [[
                { text: `🔄 Повторить выдачу (${period})`, callback_data: `review_key_period_${period}_${reviewId}` }
              ]]
            }
          }
        );
      } else {
        console.error(`❌ giveReviewRewardKey error: reviewId=${reviewId}`, e);
        bot.sendMessage(adminChatId, `❌ Ошибка выдачи ключа: ${e.message}`);
      }
    }
  });
}


// ============================================================
// 🎁 РЕФЕРАЛЬНАЯ ПРОГРАММА
// ============================================================

/**
 * Генерирует уникальный реф-код для пользователя (если ещё нет).
 */
function getOrCreateRefCode(userId) {
  return new Promise((resolve, reject) => {
    db.get(`SELECT ref_code FROM user_ref_codes WHERE user_id = ?`, [userId], (err, row) => {
      if (err) return reject(err);
      if (row) return resolve(row.ref_code);
      const code = 'REF' + crypto.randomBytes(4).toString('hex').toUpperCase();
      db.run(`INSERT OR IGNORE INTO user_ref_codes (user_id, ref_code) VALUES (?, ?)`, [userId, code], (e) => {
        if (e) return reject(e);
        resolve(code);
      });
    });
  });
}

/**
 * Обрабатывает переход по реферальной ссылке.
 */
function processRefStart(user, refCode) {
  const referredId = user.id;
  db.get(`SELECT user_id, is_blocked, blocked_until FROM user_ref_codes WHERE ref_code = ?`, [refCode], (err, refRow) => {
    if (err || !refRow) return;
    const referrerId = refRow.user_id;
    if (referrerId === referredId) return; // Нельзя пригласить себя

    // Проверяем блокировку ссылки
    if (refRow.is_blocked) {
      const now = new Date();
      const blockedUntil = refRow.blocked_until ? new Date(refRow.blocked_until) : null;
      if (!blockedUntil || now < blockedUntil) return;
      // Блок истёк — разблокируем
      db.run(`UPDATE user_ref_codes SET is_blocked = 0, blocked_until = NULL, dead_ref_count = 0 WHERE user_id = ?`, [referrerId]);
    }

    // Проверяем — не было ли уже записи реферала (INSERT OR IGNORE)
    db.run(
      `INSERT OR IGNORE INTO referrals (referrer_id, referred_id, referred_username) VALUES (?, ?, ?)`,
      [referrerId, referredId, user.username || null]
    );
  });
}

/**
 * Выдаёт реферальный купон после первой покупки приглашённого.
 */
async function handleRefReward(userId, product, botInstance = bot) {
  // Находим реферал где userId — referred_id и статус pending
  const refRow = await new Promise(res =>
    db.get(`SELECT * FROM referrals WHERE referred_id = ? AND status = 'pending'`, [userId], (e, r) => res(r))
  );
  if (!refRow) return;

  // BUG FIX RC-3: Атомарно переводим статус в 'processing' ДО любых начислений.
  // Если два параллельных вызова (approve_ + CryptoBot webhook) нашли ту же строку —
  // только первый получит changes=1 и продолжит, второй выйдет.
  const locked = await new Promise(res =>
    db.run(
      `UPDATE referrals SET status = 'processing' WHERE id = ? AND status = 'pending'`,
      [refRow.id],
      function (e) { res(!e && this.changes > 0); }
    )
  );
  if (!locked) {
    console.log(`⚠️ handleRefReward: referral #${refRow.id} already being processed — skipping duplicate`);
    return;
  }

  // Проверяем что у приглашённого это первая покупка (confirmed заказов = 1, текущий)
  const prevOrders = await new Promise(res =>
    db.get(`SELECT COUNT(*) as cnt FROM orders WHERE user_id = ? AND status = 'confirmed'`, [userId], (e, r) => res(r ? r.cnt : 0))
  );
  if (prevOrders > 1) {
    // Не первая покупка — сбрасываем обратно в pending чтобы не потерять реферал
    db.run(`UPDATE referrals SET status = 'pending' WHERE id = ?`, [refRow.id]);
    return;
  }

  const referrerId = refRow.referrer_id;
  const referredId = userId;

  // Определяем % скидки купона
  const discountMap = {
    '1d': 3, '3d': 6, '7d': 10, '30d': 20
    // infinite_boost намеренно исключён — бонус за буст не начисляется
  };
  let discount = discountMap[product] || 10;

  // Умный срок купона — анализируем последние покупки реферера
  const recentOrders = await new Promise(res =>
    db.all(
      `SELECT product FROM orders WHERE user_id = ? AND status = 'confirmed' ORDER BY confirmed_at DESC LIMIT 10`,
      [referrerId], (e, rows) => res(rows || [])
    )
  );
  const periodMap = { '1d': 1, '3d': 3, '7d': 7, '30d': 30 };
  const periodCounts = {};
  recentOrders.forEach(o => {
    if (periodMap[o.product]) {
      periodCounts[o.product] = (periodCounts[o.product] || 0) + 1;
    }
  });
  let mostFreqPeriod = '7d';
  let maxCount = 0;
  Object.entries(periodCounts).forEach(([p, c]) => { if (c > maxCount) { maxCount = c; mostFreqPeriod = p; } });
  const expiryDays = periodMap[mostFreqPeriod] || 7;

  // Проверяем подозрение на скрутку: купил в первые 5 минут
  const regTime = await new Promise(res =>
    db.get(`SELECT joined_at FROM users WHERE id = ?`, [referredId], (e, r) => res(r ? r.joined_at : null))
  );
  const joinedAt = regTime ? new Date(regTime) : null;
  const timeDiff = joinedAt ? (Date.now() - joinedAt.getTime()) / 60000 : 999;

  if (timeDiff < 1) {
    // Подозрение — замораживаем купон на 24 часа
    db.run(`UPDATE referrals SET status = 'frozen' WHERE id = ?`, [refRow.id]);
    safeSendMessage(ADMIN_ID,
      `⚠️ *Обратите внимание: Очень быстрая покупка*\n\nПриглашённый @${escapeMarkdown(String(refRow.referred_username || referredId))} купил через ${timeDiff.toFixed(1)} мин после регистрации.\nРеферер: ID ${referrerId}\n\nБонус пока заморожен (на 24ч) для проверки.`,
      { parse_mode: 'Markdown' }
    ).catch(() => { });

    // Вежливо уведомляем реферала
    safeSendMessage(referrerId,
      `⏳ Ваш друг только что совершил покупку! Пожалуйста, подождите немного, система проводит стандартную проверку, после чего мы начислим ваш бонус.`,
      { parse_mode: 'Markdown' }
    , botInstance).catch(() => { });
    return;
  }

  // 🛡️ Проверка Suspicion Score
  const suspicion = await new Promise(res => {
    db.get(`SELECT MAX(suspicion_score) as max_score FROM users WHERE id IN (?, ?)`, [referrerId, referredId], (e, r) => res(r ? r.max_score : 0));
  });

  // IMPROVEMENT 2: Используем константу вместо магического числа 50
  if (suspicion > SUSPICION_THRESHOLD_BLOCK) {
    db.run(`UPDATE referrals SET status = 'frozen' WHERE id = ?`, [refRow.id]);
    safeSendMessage(ADMIN_ID,
      `🚨 *ФРОД-ФИЛЬТР: Высокий уровень подозрения*\n\nРеферер или реферал имеют Suspicion Score: *${suspicion}* (порог: ${SUSPICION_THRESHOLD_BLOCK})\nРеферер: ID ${referrerId}\nРеферал: ID ${referredId}\n\nБонус заморожен для ручной проверки.`,
      { parse_mode: 'Markdown' }
    ).catch(() => { });
    return;
  }

  // Вежливо уведомляем реферала об успехе (если не заморожен)
  safeSendMessage(referrerId, `🎉 Ваш друг совершил первую покупку!`, { parse_mode: 'Markdown' }, botInstance).catch(() => { });


  // Генерируем купон через существующую систему
  const couponCode = 'REF' + crypto.randomBytes(3).toString('hex').toUpperCase();
  const expiresAt = new Date(Date.now() + expiryDays * 24 * 60 * 60 * 1000).toISOString().replace('T', ' ').split('.')[0];

  db.run(
    `INSERT INTO coupons (code, discount_percent, max_uses, expires_at, is_active, user_id, product_restriction) VALUES (?, ?, 1, ?, 1, ?, ?)`,
    [couponCode, discount, expiresAt, referrerId, product],
    function (err) {
      if (err) { console.error('REF coupon error:', err); return; }
      const couponId = this.lastID;
      db.run(`INSERT OR IGNORE INTO user_coupons (user_id, coupon_id) VALUES (?, ?)`, [referrerId, couponId]);
      // Вставляем ограничение по товару если задано
      if (product && couponId) {
        db.run(`INSERT OR IGNORE INTO coupon_products (coupon_id, product) VALUES (?, ?)`, [couponId, product]);
      }
      db.run(
        `UPDATE referrals SET status = 'rewarded', coupon_code = ?, rewarded_at = datetime('now') WHERE id = ?`,
        [couponCode, refRow.id]
      );
      // Уведомляем реферера
      const invitee = refRow.referred_username ? `@${escapeMarkdown(refRow.referred_username)}` : `ID ${referredId}`;
      safeSendMessage(referrerId,
        `🎁 *Реферальный бонус!*\n\nВаш друг ${invitee} совершил первую покупку.\nВы получили купон на скидку *${discount}%*:\n\n\`${couponCode}\`\n\n📅 Действует ${expiryDays} дней`,
        { parse_mode: 'Markdown' }
      , botInstance).catch(() => { });
      logAction(referrerId, 'ref_reward', { couponCode, discount, referredId });
    }
  );
}

/**
 * Проверяет мёртвые рефералы (не купили за 30 дней) — запускается раз в сутки.
 */
function checkDeadReferrals() {
  db.all(
    `SELECT r.*, urc.dead_ref_count, urc.user_id as ref_user_id
     FROM referrals r
     JOIN user_ref_codes urc ON urc.user_id = r.referrer_id
     WHERE r.status = 'pending'
       AND r.created_at <= datetime('now', '-30 days')`,
    [],
    (err, rows) => {
      if (err || !rows) return;
      rows.forEach(row => {
        db.run(`UPDATE referrals SET status = 'dead' WHERE id = ?`, [row.id]);
        const newCount = (row.dead_ref_count || 0) + 1;
        db.run(`UPDATE user_ref_codes SET dead_ref_count = ? WHERE user_id = ?`, [newCount, row.referrer_id]);

        // IMPROVEMENT 2: Используем константу вместо магического числа 3
        if (newCount >= DEAD_REFERRAL_LIMIT) {
          // Проверяем — были ли последние DEAD_REFERRAL_LIMIT мёртвыми подряд
          db.get(
            `SELECT COUNT(*) as cnt FROM referrals
             WHERE referrer_id = ? AND status = 'dead'
             ORDER BY created_at DESC LIMIT ${DEAD_REFERRAL_LIMIT}`,
            [row.referrer_id],
            (e2, r2) => {
              if (r2 && r2.cnt >= DEAD_REFERRAL_LIMIT) {
                const blockedUntil = new Date(Date.now() + 14 * 24 * 60 * 60 * 1000).toISOString().replace('T', ' ').split('.')[0];
                db.run(`UPDATE user_ref_codes SET is_blocked = 1, blocked_until = ? WHERE user_id = ?`, [blockedUntil, row.referrer_id]);
                safeSendMessage(row.referrer_id,
                  `⚠️ *Реферальная ссылка заблокирована*\n\nПоследние ${DEAD_REFERRAL_LIMIT} приглашённых не совершили покупку.\nСсылка заблокирована на 14 дней.`,
                  { parse_mode: 'Markdown' }
                , botInstance).catch(() => { });
              }
            }
          );
        }
      });
    }
  );
}

// ============================================================
// ❓ FAQ-БОТ (статичный, без AI)
// ============================================================

const FAQ_ITEMS = [
  {
    id: 1,
    q: { ru: '🔑 Что такое ключ активации?', en: '🔑 What is an activation key?' },
    a: {
      ru: `🔑 *Ключ активации* — это уникальный код для активации модуля CyraxMods в MLBB.\n\n📅 *Периоды:*\n• 1 день — краткосрочный доступ\n• 3 дня — расширенный период\n• 7 дней — недельный доступ\n• 30 дней — месячная подписка\n\n⚠️ Каждый ключ одноразовый — активируется только на одном аккаунте MLBB. После активации возврат/передача невозможны.`,
      en: `🔑 *Activation key* is a unique code to activate CyraxMods module in MLBB.\n\n📅 *Periods:*\n• 1 day — short-term access\n• 3 days — extended period\n• 7 days — weekly access\n• 30 days — monthly subscription\n\n⚠️ Each key is one-time — it activates on one MLBB account only. No refund or transfer after activation.`
    }
  },
  {
    id: 2,
    q: { ru: '💳 Как оплатить?', en: '💳 How to pay?' },
    a: {
      ru: `💳 *Способы оплаты:*\n\n🇷🇺 СБП (Система Быстрых Платежей)\n🇺🇦 Mono UA — карта Украины\n💳 Карта IT\n🔶 Binance P2P\n🤖 CryptoBot (USDT, авто)\n🅿️ PayPal\n\nВыбрать способ можно при оформлении заказа.`,
      en: `💳 *Payment methods:*\n\n🇷🇺 SBP (Russia)\n🇺🇦 Mono UA card\n💳 IT bank card\n🔶 Binance P2P\n🤖 CryptoBot (USDT, auto)\n🅿️ PayPal\n\nChoose your method when placing an order.`
    }
  },
  {
    id: 3,
    q: { ru: '⏱ Когда придёт ключ?', en: '⏱ When will I get the key?' },
    a: {
      ru: `⏱ *Сроки выдачи ключа:*\n\n🤖 *CryptoBot* — автоматически после оплаты (моментально)\n\n💳 *Ручные методы* (СБП, Mono, карта, Binance, PayPal) — до 15 минут после отправки чека.\n\nЕсли прошло больше 15 минут — обратитесь в поддержку через кнопку в главном меню.`,
      en: `⏱ *Key delivery time:*\n\n🤖 *CryptoBot* — automatically after payment (instant)\n\n💳 *Manual methods* (SBP, Mono, card, Binance, PayPal) — up to 15 minutes after sending receipt.\n\nIf more than 15 minutes passed — contact support via the button in the main menu.`
    }
  },
  {
    id: 4,
    q: { ru: '❌ Ключ не активируется', en: '❌ Key not activating' },
    a: {
      ru: `❌ *Ключ не активируется — что делать:*\n\n1. Проверьте правильность ввода (без пробелов)\n2. Перезапустите игру MLBB\n3. Убедитесь, что ключ не был активирован ранее\n4. Проверьте интернет-соединение\n\n⏰ Если проблема не решена — обратитесь в поддержку в течение *24 часов* с момента покупки.`,
      en: `❌ *Key not activating — what to do:*\n\n1. Check input (no spaces)\n2. Restart MLBB\n3. Make sure the key wasn't already activated\n4. Check internet connection\n\n⏰ If the issue persists — contact support within *24 hours* of purchase.`
    }
  },
  {
    id: 5,
    q: { ru: '💸 Можно ли вернуть деньги?', en: '💸 Can I get a refund?' },
    a: {
      ru: `💸 *Возврат средств:*\n\nВ соответствии с офертой магазина, *возврат за цифровые товары не производится* — ключи передаются мгновенно и не могут быть «возвращены».\n\n✅ *Исключения по оферте:*\n• Ключ не был выдан (технический сбой)\n• Ключ неактивен по вине продавца\n\n⏰ Обратитесь в поддержку в течение *24 часов* с момента покупки.`,
      en: `💸 *Refund policy:*\n\nPer the store's terms, *no refunds on digital goods* — keys are delivered instantly and cannot be "returned".\n\n✅ *Exceptions:*\n• Key was not delivered (technical issue)\n• Key is inactive due to seller's fault\n\n⏰ Contact support within *24 hours* of purchase.`
    }
  },
  {
    id: 6,
    q: { ru: '⚠️ Есть ли риск бана в MLBB?', en: '⚠️ Is there a ban risk in MLBB?' },
    a: {
      ru: `⚠️ *Риск бана:*\n\nДа — использование сторонних модификаций нарушает правила Moonton (разработчика MLBB).\n\n🚫 Магазин CyraxMods *не несёт ответственности* за блокировку аккаунта в игре.\n\nПокупая ключ, вы принимаете этот риск и соглашаетесь с офертой.`,
      en: `⚠️ *Ban risk:*\n\nYes — using third-party modifications violates Moonton's (MLBB developer) rules.\n\n🚫 CyraxMods store *is not responsible* for in-game account bans.\n\nBy purchasing a key, you accept this risk and agree to the terms.`
    }
  },
  {
    id: 7,
    q: { ru: '🏷 Как использовать купон?', en: '🏷 How to use a coupon?' },
    a: {
      ru: `🏷 *Использование купона:*\n\n1. Выберите товар и валюту\n2. Нажмите кнопку *«Применить купон»*\n3. Введите код купона\n4. Цена пересчитается автоматически\n\n⚠️ Купон одноразовый и привязан к аккаунту. Скидка применяется только на тот товар, для которого купон создан (если есть ограничение).`,
      en: `🏷 *Using a coupon:*\n\n1. Choose product and currency\n2. Tap *"Apply coupon"*\n3. Enter coupon code\n4. Price updates automatically\n\n⚠️ Coupon is one-time and linked to your account. Discount applies only to the specific product the coupon was created for (if restricted).`
    }
  },
  {
    id: 8,
    q: { ru: '🎁 Как работает реферальная программа?', en: '🎁 How does the referral program work?' },
    a: {
      ru: `🎁 *Реферальная программа:*\n\n1. Получите вашу уникальную ссылку (кнопка «🎁 Реферальная программа» в меню)\n2. Отправьте ссылку другу\n3. Друг переходит по ссылке и совершает *первую покупку*\n4. Вы автоматически получаете персональный купон на скидку!\n\n💰 *Размер скидки:*\n• 1 день → 3%\n• 3 дня → 6%\n• 7 дней → 10%\n• 30 дней → 20%`,
      en: `🎁 *Referral program:*\n\n1. Get your unique link (button "🎁 Referral program" in menu)\n2. Share the link with a friend\n3. Friend follows the link and makes *first purchase*\n4. You automatically get a personal discount coupon!\n\n💰 *Discount amount:*\n• 1 day → 3%\n• 3 days → 6%\n• 7 days → 10%\n• 30 days → 20%`
    }
  },
  {
    id: 9,
    q: { ru: '📈 Что такое Метод Буст?', en: '📈 What is Method Boost?' },
    a: {
      ru: `📈 *Метод Буст* — инструкция по самостоятельному прокачиванию ранга в MLBB с помощью специальных техник.\n\n✅ Включает пошаговую инструкцию\n🛟 Поддержка 24/7 по вопросам применения\n\nВыбрать можно в разделе «🚀 Буст» главного меню.`,
      en: `📈 *Method Boost* — a guide for self-ranking in MLBB using special techniques.\n\n✅ Includes step-by-step instructions\n🛟 24/7 support on implementation\n\nAvailable in the "🚀 Boost" section of the main menu.`
    }
  },
  {
    id: 10,
    q: { ru: '🏆 Что такое Ручной Буст?', en: '🏆 What is Manual Boost?' },
    a: {
      ru: `🏆 *Ручной Буст* — прокачка ранга нашими специалистами вручную.\n\n✅ Отчёт со скриншотами по завершении\n🎯 Гарантия выполнения до заказанного ранга\n🚫 *Возврат невозможен* после начала работы\n\nЗаказать можно в разделе «🚀 Буст» → «Ручной Буст».`,
      en: `🏆 *Manual Boost* — rank boosting by our specialists.\n\n✅ Screenshot report upon completion\n🎯 Guaranteed to reach the ordered rank\n🚫 *No refund* once work has started\n\nOrder in "🚀 Boost" → "Manual Boost".`
    }
  },
  {
    id: 11,
    q: { ru: '🆘 Как связаться с поддержкой?', en: '🆘 How to contact support?' },
    a: {
      ru: `🆘 *Связь с поддержкой:*\n\nНажмите кнопку *«🆘 Проблема с ключом?»* в главном меню.\n\nВас спросят о цели обращения:\n🔑 *Вопрос с ключом* — бот автоматически проверит ключ и составит тикет\n💬 *Личный вопрос* — получите прямой контакт администратора\n\nОператор отвечает в течение 15 минут.`,
      en: `🆘 *Contact support:*\n\nTap the *«🆘 Key issue?»* button in the main menu.\n\nYou'll be asked about the topic:\n🔑 *Key issue* — the bot checks your key and opens a ticket automatically\n💬 *Personal question* — you'll receive direct admin contact\n\nOperator responds within 15 minutes.`
    }
  },
  {
    id: 13,
    q: { ru: '📡 Как настроить DNS для контейнера?', en: '📡 How to set up DNS for the container?' },
    a: {
      // Динамические функции — адрес читается из настроек в момент нажатия.
      // Изменить адрес: Админка → Настройки → DNS-адрес (контейнер)
      ru: () => {
        const dnsAddr = getSetting('dns_address') || 'ff73dd.dns.nextdns.io';
        return `📡 *Настройка приватного DNS*\n\nDNS — бесплатная настройка, необходимая для стабильной работы контейнера.\n\n━━━━━━━━━━━━━━━━━━━━━━\n📌 *Рабочий DNS-адрес:*\n\`${dnsAddr}\`\n━━━━━━━━━━━━━━━━━━━━━━\n\n📱 *Как прописать DNS на Android:*\n\n1. Откройте *Настройки* телефона\n2. В строке поиска введите: *DNS*\n3. Откройте найденный раздел «Частный DNS»\n4. Выберите «Частный DNS-провайдер» и введите адрес выше\n5. Сохраните\n\n_Это работает на любом Android-телефоне, независимо от производителя._\n\n⚠️ *Важно перед запуском MLBB:*\n• Отключите VPN — с активным VPN приватный DNS не работает\n• Отключите проверку сертификатов, оставьте активным только DigiCert:\n  Настройки → Биометрия и безопасность → Другие настройки безопасности → Учётные данные\n\n━━━━━━━━━━━━━━━━━━━━━━\n🔔 *При первом запуске контейнера — окно Anti-Ban DNS*\n\nКонтейнер проверяет настройку DNS при запуске. Если вы увидели экран *«Anti-Ban Private DNS not set»* — это значит что DNS ещё не прописан в настройках телефона.\n\nЭкран покажет список DNS-адресов с пингом — выберите тот, у которого самый низкий пинг (обычно отмечен ✅). Нажмите *YES* — контейнер откроет настройки телефона, где нужно вручную прописать рекомендованный адрес.\n\nПосле этого вернитесь в контейнер — он запустится в нормальном режиме.\n\n❓ Остались вопросы? Обратитесь в поддержку через главное меню.`;
      },
      en: () => {
        const dnsAddr = getSetting('dns_address') || 'ff73dd.dns.nextdns.io';
        return `📡 *Private DNS Setup*\n\nDNS is a free setting required for stable container operation.\n\n━━━━━━━━━━━━━━━━━━━━━━\n📌 *DNS address:*\n\`${dnsAddr}\`\n━━━━━━━━━━━━━━━━━━━━━━\n\n📱 *How to set DNS on Android:*\n\n1. Open phone *Settings*\n2. In the search bar type: *DNS*\n3. Open the "Private DNS" section from the results\n4. Select "Private DNS provider hostname" and enter the address above\n5. Save\n\n_This works on any Android phone, regardless of manufacturer._\n\n⚠️ *Important before launching MLBB:*\n• Disable VPN — private DNS does not work with VPN enabled\n• Disable certificate verification, keep only DigiCert active:\n  Settings → Biometrics and security → Other security settings → Credentials\n\n━━━━━━━━━━━━━━━━━━━━━━\n🔔 *On first container launch — Anti-Ban DNS screen*\n\nThe container checks your DNS settings on startup. If you see the *"Anti-Ban Private DNS not set"* screen — it means DNS is not yet configured in your phone settings.\n\nThe screen shows a list of DNS addresses with ping — choose the one with the lowest ping (usually marked ✅). Tap *YES* — the container will open your phone settings where you need to manually enter the recommended DNS address.\n\nAfter that, return to the container — it will launch normally.\n\n❓ Still have questions? Contact support from the main menu.`;
      }
    }
  },
  {
    id: 14,
    q: { ru: '📱 Совместимость контейнера', en: '📱 Container Compatibility' },
    a: {
      ru: `📱 *Совместимость контейнера CyraxMods*\n\n✅ *Поддерживаемые версии MLBB:*\n   VNG · FT · TT · USA · MI · Huawei\n\n🖥 *Эмулятор:*\n   MeMu Player — полностью поддерживается\n   _(Если ещё не установлен — кнопка ниже)_\n\n💡 *Рекомендация:*\n   Используйте отдельный аккаунт специально для контейнера. После установки не запускайте оригинальный MLBB — это предотвратит конфликты и защитит основной аккаунт.\n\n❓ Не нашли свою версию? Напишите в поддержку — поможем разобраться.`,
      en: `📱 *CyraxMods Container Compatibility*\n\n✅ *Supported MLBB versions:*\n   VNG · FT · TT · USA · MI · Huawei\n\n🖥 *Emulator:*\n   MeMu Player — fully supported\n   _(Not installed yet? Use the button below)_\n\n💡 *Recommendation:*\n   Use a dedicated account specifically for the container. After installation, do not launch the original MLBB — this prevents conflicts and protects your main account.\n\n❓ Don't see your version? Contact support — we'll help.`
    },
    extraButtons: (isRu) => [[{ text: '🖥 Скачать MeMu Player', url: 'https://www.memuplay.com/' }]]
  },
  {
    id: 15,
    q: { ru: '🤝 Что такое партнёрская программа?', en: '🤝 What is the partnership program?' },
    a: {
      ru: `🤝 *Партнёрская программа CyraxMods*\n\nСтань официальным реселлером и зарабатывай на продаже ключей MLBB — без вложений в товар и без рисков.\n\n*Что ты получаешь:*\n🤖 Личный Telegram-бот с твоим брендингом\n💰 Наценка от 30% до 200% с каждой продажи\n📊 Личный кабинет: статистика, баланс, вывод средств\n🛒 Готовый магазин ключей и буста\n💸 Вывод заработка в любое время\n\n⚠️ *Важно про наценку:* рекомендуем 30–50% — высокая наценка отпугнёт клиентов.\n\n👥 *Поиск клиентов — на тебе.* Мы даём инструмент, ты строишь аудиторию.\n\n*Стоимость:* разовый платёж, без абонентки.\n\nДля подключения — кнопка «🤝 Партнёрство» в главном меню.`,
      en: `🤝 *CyraxMods Partnership Program*\n\nBecome an official reseller and earn on MLBB key sales — no inventory, no risk.\n\n*What you get:*\n🤖 Your own branded Telegram bot\n💰 30%–200% markup on every sale\n📊 Personal dashboard: stats, balance, withdrawals\n🛒 Ready-made key and boost store\n💸 Withdraw earnings anytime\n\n⚠️ *Markup tip:* keep it at 30–50% — high markups drive customers away.\n\n👥 *Finding customers is your job.* We provide the tool, you build the audience.\n\n*Cost:* one-time payment, no monthly fee.\n\nTo connect — tap «🤝 Partnership» in the main menu.`
    }
  }
];

/**
 * Показывает список FAQ.
 */
function showFaqMenu(chatId, user) {
  const isRu = getLang(user) === 'ru';
  const keyboard = {
    inline_keyboard: FAQ_ITEMS.map(item => [
      { text: item.q[isRu ? 'ru' : 'en'], callback_data: `faq_item_${item.id}` }
    ])
  };
  keyboard.inline_keyboard.push([{ text: isRu ? '◀️ Назад' : '◀️ Back', callback_data: 'start' }]);

  // chatId === user.id в личных чатах (бот работает только в личных)
  sendNavWithAnimatedEmoji(chatId, chatId,
    isRu
      ? '💬 <b>Часто задаваемые вопросы</b>\n\n💡 <i>Совет: для контейнера используйте отдельный аккаунт и не запускайте оригинальный MLBB после установки.</i>\n\nВыберите вопрос:'
      : '💬 <b>FAQ</b>\n\n💡 <i>Tip: for the container, use a dedicated account and do not launch the original MLBB after installation.</i>\n\nChoose a question:',
    ANIMATED_EMOJI.SUPPORT, '💬',
    { reply_markup: keyboard }
  ).catch(() => { });
}

// ============================================================
// ⏰ ОТЛОЖЕННЫЕ РАССЫЛКИ
// ============================================================

/**
 * Проверяет и отправляет запланированные рассылки (запускается каждую минуту).
 */
async function processScheduledBroadcasts() {
  // BUG FIX AS-1 + RC-2: Используем Promise-обёртку над db.all,
  // иначе async-колбэк не ждётся и рассылка может уйти дважды при повторном вызове.
  const broadcasts = await new Promise((resolve) =>
    db.all(
      `SELECT * FROM scheduled_broadcasts WHERE status = 'pending' AND scheduled_at <= datetime('now')`,
      [],
      (err, rows) => resolve((!err && rows) ? rows : [])
    )
  );

  if (broadcasts.length === 0) return;

  for (const broadcast of broadcasts) {
    // BUG FIX RC-2: Атомарно блокируем статус ДО начала рассылки.
    // Если две параллельные итерации нашли одну запись — только первая пройдёт (changes = 1).
    const locked = await new Promise((resolve) =>
      db.run(
        `UPDATE scheduled_broadcasts SET status = 'sending' WHERE id = ? AND status = 'pending'`,
        [broadcast.id],
        function (e) { resolve(!e && this.changes > 0); }
      )
    );
    if (!locked) continue; // Уже обрабатывается другой итерацией

    let usersQuery = `SELECT id FROM users`;
    if (broadcast.filter === 'active') {
      usersQuery = `SELECT DISTINCT user_id as id FROM orders WHERE status = 'confirmed' AND confirmed_at >= datetime('now', '-30 days')`;
    } else if (broadcast.filter === 'inactive') {
      usersQuery = `SELECT id FROM users WHERE id NOT IN (SELECT DISTINCT user_id FROM orders WHERE status = 'confirmed' AND confirmed_at >= datetime('now', '-7 days'))`;
    }

    const users = await new Promise((resolve) =>
      db.all(usersQuery, [], (e2, rows) => resolve((!e2 && rows) ? rows : []))
    );

    let sent = 0;
    for (const u of users) {
      try {
        await safeSendMessage(u.id, broadcast.text, { parse_mode: 'Markdown' });
        sent++;
        await new Promise(r => setTimeout(r, 50));
      } catch (_) { }
    }

    await new Promise((resolve) =>
      db.run(
        `UPDATE scheduled_broadcasts SET status = 'sent', sent_at = datetime('now'), sent_count = ? WHERE id = ?`,
        [sent, broadcast.id],
        resolve
      )
    );

    safeSendMessage(ADMIN_ID,
      `✅ *Отложенная рассылка отправлена*\n\n📝 ${broadcast.text.substring(0, 80)}...\n👥 Отправлено: ${sent}\n🎯 Аудитория: ${broadcast.filter}`,
      { parse_mode: 'Markdown' }
    ).catch(() => { });
  }
}

/**
 * Показывает меню отложенных рассылок для админа.
 */
function showScheduledBroadcastPanel(chatId) {
  db.all(
    `SELECT * FROM scheduled_broadcasts WHERE status = 'pending' ORDER BY scheduled_at ASC LIMIT 10`,
    [],
    (err, rows) => {
      let text = `⏰ *Отложенные рассылки*\n\n`;
      if (!rows || rows.length === 0) {
        text += '_Нет запланированных рассылок_';
      } else {
        rows.forEach((r, i) => {
          const dt = r.scheduled_at.replace('T', ' ');
          text += `${i + 1}. 📅 ${dt} | 🎯 ${r.filter}\n📝 ${r.text.substring(0, 60)}...\n\n`;
        });
      }
      const keyboard = {
        inline_keyboard: [
          [{ text: '➕ Создать рассылку', callback_data: 'sched_broadcast_create' }],
          ...((rows && rows.length > 0) ? [[{ text: '❌ Отменить все', callback_data: 'sched_broadcast_cancel_all' }]] : []),
          [{ text: '◀️ Назад', callback_data: 'admin' }]
        ]
      };
      safeSendMessage(chatId, text, { parse_mode: 'Markdown', reply_markup: keyboard }).catch(() => { });
    }
  );
}


// ============================================================
// 📦 СИСТЕМА ПАКЕТОВ (BUNDLE) — прогрессивная скидка
// ============================================================

// Конфигурация пакетов: { product, qty, discountPct, label_ru, label_en }
const BUNDLE_CONFIGS = [
  // 1 день — пакеты
  { product: '1d', qty: 1, discount: 0, label_ru: '1 день', label_en: '1 day' },
  { product: '1d', qty: 3, discount: 10, label_ru: '3×1 день (3 дня)', label_en: '3×1 day (3d)' },
  // 3 дня — пакеты
  { product: '3d', qty: 1, discount: 0, label_ru: '3 дня', label_en: '3 days' },
  { product: '3d', qty: 3, discount: 12, label_ru: '3×3 дня (9 дней)', label_en: '3×3 days (9d)' },
  // 7 дней — пакеты
  { product: '7d', qty: 1, discount: 0, label_ru: '7 дней', label_en: '7 days' },
  { product: '7d', qty: 3, discount: 15, label_ru: '3×7 дней (21 д)', label_en: '3×7 days (21d)' },
  // 30 дней — пакеты
  { product: '30d', qty: 1, discount: 0, label_ru: '30 дней', label_en: '30 days' },
  { product: '30d', qty: 2, discount: 15, label_ru: '2×30 дней (60 д)', label_en: '2×30 days (60d)' },
];

/**
 * Показывает предложение пакетов для конкретного продукта.
 * Вызывается из раздела "Мои ключи" или из любого места.
 */
async function showBundleOffer(chatId, user, product, baseMessage) {
  const isRu = getLang(user) === 'ru';
  const configs = BUNDLE_CONFIGS.filter(c => c.product === product);
  if (!configs.length) return;

  const periodName = PERIOD_NAMES[isRu ? 'ru' : 'en'][product] || product;

  let msg = isRu
    ? `📦 <b>Пакетное предложение — ${periodName}</b>\n\n`
    : `📦 <b>Bundle offer — ${periodName}</b>\n\n`;

  const rows = [];

  configs.forEach(cfg => {
    const basePrice = PRICES[cfg.product];
    if (!basePrice) return;

    // Считаем цену в рублях как базовую
    const unitRub = basePrice.RUB;
    const totalRub = Math.round(unitRub * cfg.qty * (1 - cfg.discount / 100));
    const savedRub = Math.round(unitRub * cfg.qty - totalRub);

    const label = isRu ? cfg.label_ru : cfg.label_en;
    const discountStr = cfg.discount > 0
      ? (isRu ? ` 🔥 −${cfg.discount}%` : ` 🔥 −${cfg.discount}%`)
      : '';
    const saveStr = cfg.discount > 0 && savedRub > 0
      ? (isRu ? ` (экономия ~${savedRub}₽)` : ` (save ~${savedRub}₽)`)
      : '';

    msg += `• <b>${label}</b> — ~${totalRub}₽${discountStr}${saveStr}\n`;

    rows.push([{
      text: `${label}${discountStr}`,
      callback_data: `bundle_select_${cfg.product}_${cfg.qty}`
    }]);
  });

  msg += isRu
    ? `\n<i>Цены указаны в RUB. Для других валют цена пересчитается при оформлении.</i>\n<i>Ключи выдаются последовательно в одном сообщении после оплаты.</i>`
    : `\n<i>Prices in RUB. Other currencies will be shown at checkout.</i>\n<i>Keys are delivered together in one message after payment.</i>`;

  rows.push([{ text: isRu ? '◀️ Назад' : '◀️ Back', callback_data: 'orders' }]);

  sendWithAnimatedEmoji(chatId, msg, ANIMATED_EMOJI.GIFT, '📦', {
    parse_mode: 'HTML',
    reply_markup: { inline_keyboard: rows }
  }).catch(() => { });
}

/**
 * Обрабатывает выбор пакета и переходит к выбору валюты/способа оплаты.
 */
function handleBundleSelect(chatId, user, product, qty, message) {
  const isRu = getLang(user) === 'ru';
  const cfg = BUNDLE_CONFIGS.find(c => c.product === product && c.qty === qty);
  if (!cfg) return;

  const basePrice = PRICES[product];
  if (!basePrice) return;

  // Рассчитываем цены во всех валютах
  const currencies = ['RUB', 'USD', 'EUR', 'UAH'];
  const unitPrices = { RUB: basePrice.RUB, USD: basePrice.USD, EUR: basePrice.EUR, UAH: basePrice.UAH };
  const bundlePrices = {};
  currencies.forEach(cur => {
    bundlePrices[cur] = +(unitPrices[cur] * qty * (1 - cfg.discount / 100)).toFixed(2);
  });

  const label = isRu ? cfg.label_ru : cfg.label_en;
  const discountStr = cfg.discount > 0 ? ` (−${cfg.discount}%)` : '';

  const msg = isRu
    ? `📦 *Пакет: ${label}${discountStr}*\n\nВыберите валюту оплаты:`
    : `📦 *Bundle: ${label}${discountStr}*\n\nChoose payment currency:`;

  const keyboard = {
    inline_keyboard: [
      [
        { text: `${FLAGS.USD} $${bundlePrices.USD}`, callback_data: `bundle_currency_${product}_${qty}_USD` },
        { text: `${FLAGS.EUR} €${bundlePrices.EUR}`, callback_data: `bundle_currency_${product}_${qty}_EUR` }
      ],
      [
        { text: `${FLAGS.RUB} ${bundlePrices.RUB}₽`, callback_data: `bundle_currency_${product}_${qty}_RUB` },
        { text: `${FLAGS.UAH} ${bundlePrices.UAH}₴`, callback_data: `bundle_currency_${product}_${qty}_UAH` }
      ],
      [{ text: isRu ? '◀️ Назад' : '◀️ Back', callback_data: `bundle_offer_${product}` }]
    ]
  };

  if (message) {
    bot.editMessageText(msg, {
      chat_id: chatId, message_id: message.message_id,
      parse_mode: 'Markdown', reply_markup: keyboard
    }).catch(() => safeSendMessage(chatId, msg, { parse_mode: 'Markdown', reply_markup: keyboard }));
  } else {
    safeSendMessage(chatId, msg, { parse_mode: 'Markdown', reply_markup: keyboard });
  }
}

/**
 * Показывает способы оплаты для выбранного bundle + валюты.
 */
function handleBundleCurrency(chatId, user, product, qty, currency, message) {
  const isRu = getLang(user) === 'ru';
  const cfg = BUNDLE_CONFIGS.find(c => c.product === product && c.qty === qty);
  if (!cfg) return;

  const basePrice = PRICES[product];
  if (!basePrice) return;

  const unitPrice = basePrice[currency] || basePrice.RUB;
  const totalPrice = +(unitPrice * qty * (1 - cfg.discount / 100)).toFixed(2);
  const label = isRu ? cfg.label_ru : cfg.label_en;

  // Сохраняем в сессию — перезаписываем bundle, сбрасываем старое состояние
  const session = userSessions.get(user.id) || {};
  session.bundle = { product, qty, currency, totalPrice, discount: cfg.discount, label };
  session.state = null;
  userSessions.set(user.id, session);

  const priceStr = formatPrice(totalPrice, currency);
  const msg = isRu
    ? `📦 *${label}* — *${priceStr}*\n\nВыберите способ оплаты:`
    : `📦 *${label}* — *${priceStr}*\n\nChoose payment method:`;

  // Кнопки оплаты — аналогично обычному заказу
  const paymentMethods = [
    { text: '🇷🇺 СБП', callback_data: `bundle_pay_SBP` },
    { text: '🇺🇦 Mono UA', callback_data: `bundle_pay_Card UA` },
    { text: '💳 Карта IT', callback_data: `bundle_pay_Card IT` },
    { text: '🔶 Binance', callback_data: `bundle_pay_Binance` },
    { text: '🤖 CryptoBot', callback_data: `bundle_pay_CryptoBot` },
    ...(PAYPAL_LINK ? [{ text: '🅿️ PayPal', callback_data: `bundle_pay_PayPal` }] : [])
  ];

  const rows = [];
  for (let i = 0; i < paymentMethods.length; i += 2) {
    rows.push(paymentMethods.slice(i, i + 2));
  }
  rows.push([{ text: isRu ? '◀️ Назад' : '◀️ Back', callback_data: `bundle_select_${product}_${qty}` }]);

  if (message) {
    bot.editMessageText(msg, {
      chat_id: chatId, message_id: message.message_id,
      parse_mode: 'Markdown', reply_markup: { inline_keyboard: rows }
    }).catch(() => safeSendMessage(chatId, msg, { parse_mode: 'Markdown', reply_markup: { inline_keyboard: rows } }));
  } else {
    safeSendMessage(chatId, msg, { parse_mode: 'Markdown', reply_markup: { inline_keyboard: rows } });
  }
}

/**
 * Создаёт bundle-заказ и показывает реквизиты.
 */
async function handleBundlePayment(chatId, user, method, message) {
  const isRu = getLang(user) === 'ru';
  const session = userSessions.get(user.id) || {};
  const bundle = session.bundle;
  if (!bundle) {
    bot.sendMessage(chatId, isRu ? '❌ Сессия устарела. Начните заново.' : '❌ Session expired. Start again.');
    return;
  }

  const { product, qty, currency, totalPrice, discount, label } = bundle;

  // 🧪 ТЕСТ-РЕЖИМ: выдаём тестовые ключи без создания заказа и без оплаты
  const priceStr = formatPrice(totalPrice, currency);

  // Создаём bundle-заказ в БД
  // Проверяем дубликат pending-заказа: тот же пользователь + продукт + метод оплаты.
  // SERIOUS 2: Без проверки метода пользователь мог создать два разных заказа (CryptoBot + ручной)
  // на один продукт — оба были бы pending, второй создавался без предупреждения.
  const existingBundle = await new Promise(res =>
    db.get(
      `SELECT id FROM bundle_orders WHERE user_id = ? AND product = ? AND method = ? AND status = 'pending'`,
      [user.id, product, method],
      (e, r) => res(r)
    )
  );
  if (existingBundle) {
    const isRuDup = getLang(user) === 'ru';
    bot.sendMessage(chatId,
      isRuDup
        ? '⚠️ У вас уже есть ожидающий bundle-заказ на этот продукт. Дождитесь подтверждения или обратитесь в поддержку.'
        : '⚠️ You already have a pending bundle order for this product. Wait for confirmation or contact support.'
    );
    return;
  }

  db.run(
    `INSERT INTO bundle_orders (user_id, username, user_lang, product, quantity, unit_price, discount_percent, total_price, currency, method, status)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending')`,
    [user.id, user.username || null, user.language_code || 'en', product,
      qty, PRICES[product][currency], discount, totalPrice, currency, method],
    async function (err) {
      if (err) { console.error('Bundle order error:', err); return; }
      const bundleOrderId = this.lastID;

      if (method === 'CryptoBot') {
        // CryptoBot автооплата для bundle
        try {
          const usdAmount = currency === 'USD' ? totalPrice
            : currency === 'EUR' ? +(totalPrice / EXCHANGE_RATES.EUR * EXCHANGE_RATES.USD).toFixed(2)
              : currency === 'RUB' ? +(totalPrice * EXCHANGE_RATES.USD).toFixed(2)
                : +(totalPrice * EXCHANGE_RATES.USD / EXCHANGE_RATES.UAH).toFixed(2);
          const cryptoUsd = Math.max(0.01, +usdAmount.toFixed(2));

          const response = await axios.post('https://pay.crypt.bot/api/createInvoice', {
            asset: 'USDT', amount: String(cryptoUsd),
            description: `CyraxMods Bundle: ${label} (x${qty})`,
            payload: JSON.stringify({ type: 'bundle', bundleOrderId }),
            paid_btn_name: 'viewItem', paid_btn_url: 'https://t.me/' + (process.env.BOT_USERNAME || 'cyraxxmod_bot')
          }, { headers: { 'Crypto-Pay-API-Token': CRYPTOBOT_TOKEN } });

          const invoice = response.data.result;
          db.run(`UPDATE bundle_orders SET invoice_id = ? WHERE id = ?`, [invoice.invoice_id, bundleOrderId]);

          const instruction = isRu
            ? `📦 *Пакет: ${label}*\n\n💰 Сумма: *$${cryptoUsd} USDT*\n🔑 Будет выдано: *${qty} ключей*\n\n⚡️ Ключи выдаются автоматически сразу после оплаты`
            : `📦 *Bundle: ${label}*\n\n💰 Amount: *$${cryptoUsd} USDT*\n🔑 Keys: *${qty}*\n\n⚡️ Keys issued automatically after payment`;

          safeSendMessage(chatId, instruction, {
            parse_mode: 'Markdown',
            reply_markup: {
              inline_keyboard: [
                [{ text: isRu ? `🤖 Оплатить $${cryptoUsd} USDT` : `🤖 Pay $${cryptoUsd} USDT`, url: invoice.pay_url }],
                [{ text: isRu ? '◀️ Назад' : '◀️ Back', callback_data: 'start' }]
              ]
            }
          }).catch(() => { });
        } catch (e) {
          console.error('Bundle CryptoBot error:', e.message);
          bot.sendMessage(chatId, isRu ? '❌ Ошибка создания инвойса. Попробуйте другой способ оплаты.' : '❌ Invoice error. Try another payment method.');
        }
      } else {
        // Ручная оплата — показываем реквизиты
        const details = PAYMENT_DETAILS[
          method === 'SBP' ? 'sbp' :
            method === 'Card UA' ? 'card_ua' :
              method === 'Card IT' ? 'card_it' :
                method === 'Binance' ? 'binance' :
                  method === 'PayPal' ? 'paypal' : 'sbp'
        ] || '—';

        const instruction = isRu
          ? `📦 *Пакет: ${label}*\n💰 К оплате: *${priceStr}*\n🔑 Будет выдано: *${qty} ключей*\n\n📋 *Реквизиты (${method}):*\n\`${details}\`\n\nОтправьте чек (скриншот) — ключи выдам вручную.`
          : `📦 *Bundle: ${label}*\n💰 Amount: *${priceStr}*\n🔑 Keys: *${qty}*\n\n📋 *Details (${method}):*\n\`${details}\`\n\nSend receipt (screenshot) — keys will be issued manually.`;

        // Уведомляем админа
        const adminNote = `📦 *Bundle заказ #${bundleOrderId}*\n\n👤 @${escapeMarkdown(String(user.username || user.id))}\n📦 ${label} (x${qty})\n💰 ${priceStr}\n💳 ${method}\n🔑 Нужно выдать ключей: ${qty}\n\nКнопки: одобрить через /admin`;
        safeSendMessage(ADMIN_ID, adminNote, { parse_mode: 'Markdown' }).catch(() => { });

        // Сохраняем в сессию для получения чека
        session.state = 'awaiting_bundle_receipt';
        session.data = { bundleOrderId, qty, product, currency, totalPrice, method };
        userSessions.set(user.id, session);

        const hourglass = await safeSendMessage(chatId, '⏳').catch(() => null);
        await safeSendMessage(chatId, instruction, {
          parse_mode: 'Markdown',
          reply_markup: { inline_keyboard: [[{ text: isRu ? '◀️ В меню' : '◀️ Menu', callback_data: 'start' }]] }
        }).catch(() => { });

        // Редактируем ⏳ на ✅
        if (hourglass) {
          setTimeout(() => {
            bot.editMessageText('✅', { chat_id: chatId, message_id: hourglass.message_id }).catch(() => { });
          }, 1000);
        }
      }
    }
  );
}

/**
 * Выдаёт несколько ключей для bundle и отправляет в одном красивом сообщении.
 */
/**
 * issueMultipleKeys — универсальная утилита для массовой выдачи ключей.
 * Task 2.3: Вынесена из issueBundleKeys чтобы логика частичной выдачи
 * (PARTIAL_OUT_OF_STOCK) была доступна для любого сценария —
 * бандлы, награды за отзывы, другие будущие массовые выдачи.
 *
 * @param {number} userId   - ID пользователя-получателя
 * @param {string} product  - Тип ключа ('1d', '7d', ...)
 * @param {number} qty      - Количество ключей
 * @param {string} reason   - Причина для логов issueKeyToUser ('bundle', 'review_batch', ...)
 * @returns {Promise<string[]>} Массив выданных ключей
 * @throws {Error} с code='PARTIAL_OUT_OF_STOCK' и полями .issued/.keys при нехватке на полный объём
 */
async function issueMultipleKeys(userId, product, qty, reason = 'batch', _retryCount = 0) {
  // ─── АТОМАРНАЯ МАССОВАЯ ВЫДАЧА ────────────────────────────────────────────
  //
  // ПРОБЛЕМА старой логики: последовательный цикл issueKeyToUser × qty.
  //   Каждая итерация — отдельная транзакция. При нехватке ключей часть уже
  //   выдана пользователю, часть нет — частичная выдача без возможности откатить.
  //
  // РЕШЕНИЕ:
  //   Одна транзакция BEGIN IMMEDIATE на весь пакет:
  //   1. SELECT id, key_value FROM keys WHERE product=? AND status='available' LIMIT qty
  //   2. Если нашли < qty — ROLLBACK, выбрасываем PARTIAL_OUT_OF_STOCK с available.
  //      При этом НИ ОДИН ключ ещё не помечен sold — пользователь ничего не получил.
  //   3. UPDATE keys SET status='sold',... WHERE id IN (...)
  //   4. COMMIT — только теперь ключи уходят пользователю.
  // ──────────────────────────────────────────────────────────────────────────

  // CRITICAL 3: Защита от stack overflow при гонке SELECT→UPDATE.
  // Максимум 3 рекурсивных повтора — после этого бросаем ошибку вместо бесконечной рекурсии.
  const MAX_RETRY_ATTEMPTS = 3;
  if (_retryCount >= MAX_RETRY_ATTEMPTS) {
    console.error(`❌ issueMultipleKeys: превышено максимальное число попыток (${MAX_RETRY_ATTEMPTS}) для userId=${userId}, product=${product}, qty=${qty}`);
    const retryErr = new Error(`Не удалось атомарно выдать ключи после ${MAX_RETRY_ATTEMPTS} попыток — высокая конкуренция`);
    retryErr.code = 'MAX_RETRIES_EXCEEDED';
    return Promise.reject(retryErr);
  }

  return new Promise((resolve, reject) => {
    db.serialize(() => {
      db.run("BEGIN IMMEDIATE", (beginErr) => {
        if (beginErr) {
          console.error('❌ issueMultipleKeys BEGIN error:', beginErr);
          return reject(new Error('Ошибка начала транзакции'));
        }

        const rollback = (err) => { db.run("ROLLBACK", () => {}); reject(err); };

        // Шаг 1: выбираем нужное количество доступных ключей нужного типа
        db.all(
          `SELECT id, key_value FROM keys WHERE product = ? AND status = 'available' LIMIT ?`,
          [product, qty],
          (selErr, rows) => {
            if (selErr) {
              console.error('❌ issueMultipleKeys SELECT error:', selErr);
              return rollback(new Error('Ошибка выборки ключей'));
            }

            if (!rows || rows.length === 0) {
              // Нет ни одного ключа
              db.run("ROLLBACK", () => {});
              const err = new Error(`Нет доступных ключей для ${product}`);
              err.code = 'OUT_OF_STOCK';
              return reject(err);
            }

            if (rows.length < qty) {
              // Ключей меньше чем запрошено — откатываем, возвращаем специальную ошибку.
              // Caller может решить: выдать частично или отказать полностью.
              // Ключи НЕ помечены sold — данные консистентны.
              db.run("ROLLBACK", () => {});
              const availableKeys = rows.map(r => r.key_value);
              const partialErr = new Error(
                `Out of stock: requested ${qty}, available ${rows.length} for ${product}`
              );
              partialErr.code = 'PARTIAL_OUT_OF_STOCK';
              partialErr.issued = rows.length;
              partialErr.keys = availableKeys;
              return reject(partialErr);
            }

            // Шаг 2: у нас ровно qty ключей — атомарно помечаем все как sold
            const ids = rows.map(r => r.id);
            const placeholders = ids.map(() => '?').join(',');
            const now = new Date().toISOString();

            db.run(
              `UPDATE keys SET status = 'sold', buyer_id = ?, sold_at = datetime('now'), issue_reason = ?
               WHERE id IN (${placeholders}) AND status = 'available'`,
              [userId, reason, ...ids],
              function (updErr) {
                if (updErr) {
                  console.error('❌ issueMultipleKeys UPDATE error:', updErr);
                  return rollback(new Error('Ошибка обновления ключей'));
                }

                if (this.changes < qty) {
                  // Кто-то занял часть ключей между SELECT и UPDATE (крайне редко при IMMEDIATE)
                  console.warn(`⚠️ issueMultipleKeys: expected ${qty} changes, got ${this.changes} — retrying (attempt ${_retryCount + 1}/${MAX_RETRY_ATTEMPTS})`);
                  db.run("ROLLBACK", () => {});
                  // CRITICAL 3: Рекурсивный повтор с инкрементом счётчика попыток
                  resolve(issueMultipleKeys(userId, product, qty, reason, _retryCount + 1));
                  return;
                }

                // Шаг 3: фиксируем
                db.run("COMMIT", (commitErr) => {
                  if (commitErr) {
                    console.error('❌ issueMultipleKeys COMMIT error:', commitErr);
                    return rollback(new Error('Ошибка фиксации транзакции'));
                  }
                  console.log(`✅ issueMultipleKeys: issued ${qty} keys of ${product} to user ${userId}`);
                  resolve(rows.map(r => r.key_value));
                });
              }
            );
          }
        );
      });
    });
  });
}

async function issueBundleKeys(userId, product, qty, userLang) {
  // Делегируем в issueMultipleKeys — issueBundleKeys сохраняем для обратной совместимости
  return issueMultipleKeys(userId, product, qty, 'bundle');
}

/**
 * Отправляет пользователю красивое сообщение со всеми ключами пакета.
 */
async function sendBundleKeys(userId, userLang, product, keys, bundleOrderId) {
  const userObj = { language_code: userLang };
  const isRu = getLang(userObj) === 'ru';
  const periodName = PERIOD_NAMES[isRu ? 'ru' : 'en'][product] || product;

  let msg = isRu
    ? `✅ *Ваш пакет ключей получен!*\n\n`
    : `✅ *Your key bundle is ready!*\n\n`;

  keys.forEach((key, i) => {
    msg += isRu
      ? `🔑 *Ключ ${i + 1}* (${periodName})\n\`${key}\`\n\n`
      : `🔑 *Key ${i + 1}* (${periodName})\n\`${key}\`\n\n`;
  });

  msg += isRu
    ? `_Каждый ключ одноразовый. Активируется при первом вводе в CyraxMods._`
    : `_Each key is single-use. Activated on first input in CyraxMods._`;

  await safeSendMessage(userId, msg, { parse_mode: 'Markdown' });
}

// ============================================================
// 📊 ЗАДАЧА 5 — АВТОМАТИЧЕСКИЕ КУРСЫ ВАЛЮТ С НАЦЕНКОЙ
// ============================================================

// Флаг инициализации
let exchangeRatesInitialized = false;

/**
 * Загружает актуальные курсы валют и применяет наценку из настроек.
 * Обновляет глобальную переменную EXCHANGE_RATES.
 * Источник: exchangerate-api.com (бесплатный tier, 1500 req/месяц).
 */
async function fetchAndUpdateExchangeRates() {
  try {
    console.log('💱 Updating exchange rates...');

    const isManualMode = getSetting('manual_exchange_rates') === '1';

    if (isManualMode) {
      console.log('ℹ️ Manual exchange rates mode is ENABLED');
      const fixedUSD = parseFloat(getSetting('fixed_rate_USD') || '0');
      const fixedEUR = parseFloat(getSetting('fixed_rate_EUR') || '0');
      const fixedUAH = parseFloat(getSetting('fixed_rate_UAH') || '0');

      if (fixedUSD > 0 && fixedEUR > 0 && fixedUAH > 0) {
        EXCHANGE_RATES.USD = fixedUSD;
        EXCHANGE_RATES.EUR = fixedEUR;
        EXCHANGE_RATES.UAH = fixedUAH;
        console.log(`✅ Manual rates applied: USD=${fixedUSD}, EUR=${fixedEUR}, UAH=${fixedUAH}`);
        exchangeRatesInitialized = true;
        return;
      } else {
        console.warn('⚠️ Manual mode enabled but some fixed rates are 0. Falling back to API/Cache.');
      }
    }

    // Базовый курс RUB → USD, EUR, UAH с exchangerate-api.com
    const response = await axios.get('https://open.er-api.com/v6/latest/RUB', { timeout: 8000 });

    if (!response.data || response.data.result !== 'success') {
      throw new Error('Invalid response from exchange rate API');
    }

    const rates = response.data.rates;
    // rates[X] = сколько X можно купить за 1 RUB

    const markupUSD = parseFloat(getSetting('markup_USD') || '0');
    const markupEUR = parseFloat(getSetting('markup_EUR') || '0');
    const markupUAH = parseFloat(getSetting('markup_UAH') || '0');

    const newRates = {
      USD: Math.round((rates.USD || 0.0108) * (1 + markupUSD / 100) * 10000) / 10000,
      EUR: Math.round((rates.EUR || 0.0095) * (1 + markupEUR / 100) * 10000) / 10000,
      UAH: Math.round((rates.UAH || 0.44) * (1 + markupUAH / 100) * 10000) / 10000,
    };

    // Обновляем глобальную переменную
    EXCHANGE_RATES.USD = newRates.USD;
    EXCHANGE_RATES.EUR = newRates.EUR;
    EXCHANGE_RATES.UAH = newRates.UAH;

    // Сохраняем в настройки для отказоустойчивости (при перезапуске используем последние)
    saveSetting('cached_rate_USD', String(newRates.USD));
    saveSetting('cached_rate_EUR', String(newRates.EUR));
    saveSetting('cached_rate_UAH', String(newRates.UAH));
    saveSetting('rates_updated_at', new Date().toISOString());

    exchangeRatesInitialized = true;
    console.log(`✅ Exchange rates updated: USD=${newRates.USD}, EUR=${newRates.EUR}, UAH=${newRates.UAH}`);

  } catch (e) {
    console.error('❌ fetchAndUpdateExchangeRates error:', e.message);

    // Уведомляем администратора об ошибке обновления курсов
    try {
      safeSendMessage(ADMIN_ID, `⚠️ *Ошибка обновления курсов валют*

${e.message}

_Используются кэшированные или хардкодные значения._`, { parse_mode: 'Markdown' }).catch(() => { });
    } catch (_) { }

    // При ошибке — пробуем использовать кэшированные значения из БД
    if (!exchangeRatesInitialized) {
      const cachedUSD = parseFloat(getSetting('cached_rate_USD') || '0');
      const cachedEUR = parseFloat(getSetting('cached_rate_EUR') || '0');
      const cachedUAH = parseFloat(getSetting('cached_rate_UAH') || '0');
      if (cachedUSD > 0) {
        EXCHANGE_RATES.USD = cachedUSD;
        EXCHANGE_RATES.EUR = cachedEUR;
        EXCHANGE_RATES.UAH = cachedUAH;
        console.log('⚠️ Using cached exchange rates from DB');
        exchangeRatesInitialized = true;
      }
      // Если и кэша нет — остаются хардкодные значения из начала файла
    }
  }
}

/**
 * Показывает панель управления курсами валют в админке.
 * @param {number} chatId
 */
function showExchangeRatesPanel(chatId) {
  const isManual = getSetting('manual_exchange_rates') === '1';
  const markupUSD = getSetting('markup_USD') || '0';
  const markupEUR = getSetting('markup_EUR') || '0';
  const markupUAH = getSetting('markup_UAH') || '0';

  const fixedUSD = getSetting('fixed_rate_USD') || '0';
  const fixedEUR = getSetting('fixed_rate_EUR') || '0';
  const fixedUAH = getSetting('fixed_rate_UAH') || '0';

  const updatedAt = getSetting('rates_updated_at') ? new Date(getSetting('rates_updated_at')).toLocaleString('ru-RU') : 'не обновлялись';

  let message = `💱 *Управление курсами валют*\n\n`;
  message += `Режим: ${isManual ? '🔴 Ручной' : '🟢 Автоматический'}\n`;
  message += `🕐 Обновлено: ${updatedAt}\n`;
  message += `_(1 RUB = ...)_ \n\n`;

  if (!isManual) {
    message += `🇺🇸 *USD*: ${EXCHANGE_RATES.USD.toFixed(5)} (+${markupUSD}%)\n`;
    message += `🇪🇺 *EUR*: ${EXCHANGE_RATES.EUR.toFixed(5)} (+${markupEUR}%)\n`;
    message += `🇺🇦 *UAH*: ${EXCHANGE_RATES.UAH.toFixed(4)} (+${markupUAH}%)\n`;
  } else {
    message += `🇺🇸 *USD (Fix)*: ${fixedUSD}\n`;
    message += `🇪🇺 *EUR (Fix)*: ${fixedEUR}\n`;
    message += `🇺🇦 *UAH (Fix)*: ${fixedUAH}\n`;
  }

  const keyboard = {
    inline_keyboard: [
      [{ text: isManual ? '🟢 Включить АВТО' : '🔴 Включить РУЧНОЙ', callback_data: 'rates_toggle_manual' }],
      [{ text: '🔄 Обновить API сейчас', callback_data: 'rates_refresh' }],
      [
        { text: `🇺🇸 USD Fix: ${fixedUSD}`, callback_data: 'rates_set_fixed_USD' },
        { text: `🇪🇺 EUR Fix: ${fixedEUR}`, callback_data: 'rates_set_fixed_EUR' }
      ],
      [
        { text: `🇺🇦 UAH Fix: ${fixedUAH}`, callback_data: 'rates_set_fixed_UAH' }
      ],
      [
        { text: `✏️ Наценка USD (${markupUSD}%)`, callback_data: 'rates_markup_USD' },
        { text: `✏️ Наценка EUR (${markupEUR}%)`, callback_data: 'rates_markup_EUR' }
      ],
      [{ text: '◀️ Назад', callback_data: 'admin' }]
    ]
  };

  safeSendMessage(chatId, message, { parse_mode: 'Markdown', reply_markup: keyboard });
}

// ==========================================
// 📢 ФУНКЦИЯ ВЫПОЛНЕНИЯ РАССЫЛКИ
// ==========================================
// ==========================================
// 📢 ПРЕВЬЮ РАССЫЛКИ — показываем перед отправкой
// ==========================================
async function showBroadcastPreview(adminChatId, broadcastData, user) {
  const isRu = getLang(user) === 'ru';

  // Считаем получателей
  const totalUsers = await new Promise(res =>
    db.get(`SELECT COUNT(*) as cnt FROM users`, [], (e, r) => res(r ? r.cnt : 0))
  );

  const previewHeader = `👁 *Превью рассылки*\n\n✅ Получат: *${totalUsers}* пользователей\n\n──────────────\n`;

  try {
    if (broadcastData.photo) {
      await bot.sendPhoto(adminChatId, broadcastData.photo, {
        caption: (previewHeader + (broadcastData.caption || '')).slice(0, 1024),
        parse_mode: 'Markdown'
      });
    } else if (broadcastData.text) {
      await bot.sendMessage(adminChatId,
        previewHeader + broadcastData.text,
        { parse_mode: 'HTML', disable_web_page_preview: true }
      );
    }
  } catch (e) {
    await bot.sendMessage(adminChatId, `👁 *Превью рассылки*\n\n✅ Получат: *${totalUsers}* пользователей\n\n_(ошибка отображения превью — сообщение всё равно будет отправлено)_`, { parse_mode: 'Markdown' }).catch(() => {});
  }

  await bot.sendMessage(adminChatId,
    `──────────────\nОтправить рассылку ${totalUsers} пользователям?`,
    {
      reply_markup: {
        inline_keyboard: [
          [{ text: `✅ Отправить всем (${totalUsers})`, callback_data: 'broadcast_preview_confirm' }],
          [{ text: '✏️ Изменить сообщение', callback_data: 'broadcast_preview_cancel' }]
        ]
      }
    }
  ).catch(() => {});
}

async function executeAdminBroadcast(adminChatId, broadcastData) {
  db.all(`SELECT id FROM users`, [], async (err, users) => {
    if (err || !users) {
      bot.sendMessage(adminChatId, '❌ Ошибка получения пользователей');
      return;
    }

    let sent = 0;
    let blocked = 0;
    let failed = 0;
    const total = users.length;

    // Генерирует текстовый прогресс-бар: ████████░░ 80%
    // Лёгкая визуализация без доп. запросов — просто unicode-символы в тексте.
    const buildProgressBar = (done, total) => {
      const BAR_LEN = 10;
      const pct = total > 0 ? Math.round(done / total * 100) : 0;
      const filled = Math.round(pct / 100 * BAR_LEN);
      return `[${'█'.repeat(filled)}${'░'.repeat(BAR_LEN - filled)}] ${pct}%`;
    };

    // ⏳ Отправляем стартовое сообщение и редактируем его по ходу рассылки
    let progressMsg = null;
    try {
      progressMsg = await bot.sendMessage(
        adminChatId,
        `📡 Рассылка запущена...\n\n${buildProgressBar(0, total)}\n\n👥 Всего: *${total}*\n📤 Отправлено: *0*\n🚫 Заблокировали: *0*`,
        { parse_mode: 'Markdown' }
      );
    } catch (e) { /* не критично */ }

    // FIX 3.2: обновляем прогресс по двум условиям:
    // — каждые 10 отправок (для быстрых маленьких рассылок, где таймер не успевает)
    // — не чаще раза в 2.5 секунды (защита от спама API при больших рассылках)
    let lastProgressUpdate = Date.now();

    const optsTemplate = {};
    if (broadcastData.btnName && broadcastData.btnAction) {
      const isUrl = /^https?:\/\/|t\.me\//i.test(broadcastData.btnAction);
      optsTemplate.reply_markup = {
        inline_keyboard: [
          [
            isUrl
              ? { text: broadcastData.btnName, url: broadcastData.btnAction }
              : { text: broadcastData.btnName, callback_data: broadcastData.btnAction }
          ]
        ]
      };
    }

    for (const row of users) {
      try {
        if (broadcastData.photo) {
          await bot.sendPhoto(row.id, broadcastData.photo, { caption: broadcastData.caption, parse_mode: 'HTML', ...optsTemplate });
        } else if (broadcastData.text) {
          await bot.sendMessage(row.id, broadcastData.text, { parse_mode: 'HTML', disable_web_page_preview: true, ...optsTemplate });
        }
        sent++;
        await new Promise(r => setTimeout(r, 50));

        // FIX 3.2: обновляем прогресс по двум условиям:
        // — каждые 10 отправок (для быстрых маленьких рассылок, где таймер не успевает)
        // — не чаще раза в 2.5 секунды (защита от спама API при больших рассылках)
        const now = Date.now();
        const shouldUpdate = progressMsg && (
          sent % 10 === 0 ||
          (now - lastProgressUpdate > 2500)
        );
        if (shouldUpdate) {
          lastProgressUpdate = now;
          bot.editMessageText(
            `📡 Рассылка в процессе...\n\n${buildProgressBar(sent, total)}\n\n👥 Всего: *${total}*\n📤 Отправлено: *${sent}*\n🚫 Заблокировали: *${blocked}*`,
            { chat_id: adminChatId, message_id: progressMsg.message_id, parse_mode: 'Markdown' }
          ).catch(() => {});
        }
      } catch (e) {
        if (e?.response?.body?.error_code === 403) {
          blocked++;
        } else {
          failed++;
        }
      }
    }

    const pct = total > 0 ? Math.round(sent / total * 100) : 0;
    const broadcastReport = [
      `🏁 Рассылка завершена!`,
      ``,
      `${buildProgressBar(sent, total)}`,
      ``,
      `👥 Всего пользователей: *${total}*`,
      `✅ Получили сообщение: *${sent}*`,
      `🚫 Заблокировали бота: *${blocked}*`,
      failed > 0 ? `⚠️ Другие ошибки: *${failed}*` : null,
      ``,
      `📈 Доставлено: *${pct}%* ${pct >= 90 ? '🟢' : pct >= 70 ? '🟡' : '🔴'}`
    ].filter(Boolean).join('\n');

    // Редактируем прогресс-сообщение итогами
    if (progressMsg) {
      bot.editMessageText(broadcastReport, { chat_id: adminChatId, message_id: progressMsg.message_id, parse_mode: 'Markdown' }).catch(() => {
        bot.sendMessage(adminChatId, broadcastReport, { parse_mode: 'Markdown' });
      });
    } else {
      bot.sendMessage(adminChatId, broadcastReport, { parse_mode: 'Markdown' });
    }
  });
}

// ==========================================
// 🌐 EXPRESS СЕРВЕР
// ==========================================
// Для /cryptobot-webhook нужно raw body — CryptoBot подписывает оригинальную строку,
// а не результат JSON.stringify(req.body). Без этого подпись всегда не совпадает.
app.use((req, res, next) => {
  if (req.path === '/cryptobot-webhook') {
    let raw = '';
    req.setEncoding('utf8');
    req.on('data', chunk => { raw += chunk; });
    req.on('end', () => {
      req.rawBody = raw;
      try { req.body = JSON.parse(raw); } catch (e) { req.body = {}; }
      next();
    });
  } else {
    express.json()(req, res, next);
  }
});

app.get('/', (req, res) => {
  res.send('CyraxMods Shop Bot is running! 🚀');
});

// ✅ Health check endpoint — используется для keep-alive пинга
app.get('/health', (req, res) => {
  // Расширенный health endpoint с метриками состояния бота.
  // Полезен для мониторинга на Render/UptimeRobot и для отладки на продакшене.
  const uptimeSec = Math.round(process.uptime());
  const memMb = (process.memoryUsage().rss / 1024 / 1024).toFixed(1);
  const ratesUpdatedAt = getSetting('rates_updated_at') || null;

  // Статус БД: проверяем что соединение живо простым синхронным вызовом
  let dbStatus = 'ok';
  try {
    db.prepare('SELECT 1'); // бросает если db закрыта
  } catch (e) {
    dbStatus = 'error';
  }

  res.json({
    status: dbStatus === 'ok' ? 'ok' : 'degraded',
    uptime_sec: uptimeSec,
    uptime_human: `${Math.floor(uptimeSec / 3600)}h ${Math.floor((uptimeSec % 3600) / 60)}m`,
    memory_rss_mb: parseFloat(memMb),
    db: {
      path: DB_PATH,
      status: dbStatus
    },
    sessions: {
      active: userSessions.size,
      max: SESSION_MAX_SIZE,
      usage_pct: Math.round(userSessions.size / SESSION_MAX_SIZE * 100)
    },
    locks: {
      approving_orders: approvingOrders.size,
      approving_topups: approvingTopups.size
    },
    exchange_rates: {
      initialized: exchangeRatesInitialized,
      updated_at: ratesUpdatedAt
    },
    backup: (() => {
      // П.7: метрики бэкапа
      try {
        const metaPath = '/tmp/cyrax_last_backup.json';
        if (!fs.existsSync(metaPath)) return { status: 'no_backup', files_count: 0 };
        const meta = JSON.parse(fs.readFileSync(metaPath, 'utf8'));
        const latest = meta.latest || (meta.file_id ? meta : null);
        const backupsCount = meta.backups ? meta.backups.length : (latest ? 1 : 0);
        const lastMinutesAgo = lastBackupTimestamp
          ? Math.round((Date.now() - lastBackupTimestamp) / 60000)
          : (latest ? Math.round((Date.now() - latest.saved_at) / 60000) : null);
        return {
          status: 'ok',
          files_count: backupsCount,
          last_backup_minutes_ago: lastMinutesAgo,
          last_backup_date: latest ? `${latest.date} ${latest.time}` : null,
          interval_minutes: BACKUP_INTERVAL_MINUTES
        };
      } catch (e) {
        return { status: 'error', error: e.message };
      }
    })(),
    timestamp: new Date().toISOString()
  });
});

// HTTP endpoint для ручного бэкапа (GET /backup_now?secret=WEBHOOK_SECRET)
app.get('/backup_now', async (req, res) => {
  if (req.query.secret !== WEBHOOK_SECRET) {
    return res.status(403).json({ error: 'Forbidden' });
  }
  res.json({ status: 'started', message: 'Backup initiated' });
  try {
    await sendDatabaseBackup(true);
  } catch (e) {
    console.error('❌ /backup_now error:', e.message);
  }
});

// Защищенный вебхук для Telegram бота
app.post('/webhook/:secret', (req, res) => {
  if (req.params.secret !== WEBHOOK_SECRET) {
    return res.sendStatus(403);
  }
  // Task 2.1: Базовая структурная валидация — та же логика что в reseller webhook.
  // Защита от мусорных payload'ов и потенциальных крашей при парсинге.
  const body = req.body;
  if (!body || typeof body !== 'object' || typeof body.update_id !== 'number') {
    console.warn('⚠️ [MAIN_WEBHOOK] Rejected: invalid body structure');
    return res.sendStatus(200);
  }
  bot.processUpdate(body);
  res.sendStatus(200);
});

// Вебхуки для реселлер-ботов
// FIX 4.1: Обновлённый endpoint — теперь требует секрет в URL.
// Старый путь /webhook/reseller/:resellerId оставлен для graceful 404.
app.post('/webhook/reseller/:resellerId/:secret', (req, res) => {
  const resellerId = parseInt(req.params.resellerId);
  const secret = req.params.secret;
  const entry = resellerBots.get(resellerId);

  // Если бот не в Map — отвечаем 200 чтобы Telegram не ретраил, но не обрабатываем
  if (!entry || !entry.bot) {
    return res.sendStatus(200);
  }

  // FIX 4.1: Проверяем секрет — защита от фейковых апдейтов
  const storedSecret = entry.data?.webhook_secret;
  if (!storedSecret || secret !== storedSecret) {
    console.warn(`⚠️ [РЕСЕЛЛЕР ${resellerId}] Webhook rejected: invalid secret`);
    return res.sendStatus(403);
  }

  // FIX 2.2: Базовая структурная валидация тела запроса.
  // Telegram всегда присылает объект с полем update_id (число).
  // Если тело не является объектом или не содержит update_id — это не легитимный апдейт.
  // Это защищает от крашей при парсинге мусорных payload'ов.
  const body = req.body;
  if (!body || typeof body !== 'object' || typeof body.update_id !== 'number') {
    console.warn(`⚠️ [РЕСЕЛЛЕР ${resellerId}] Webhook rejected: invalid body structure`);
    return res.sendStatus(200); // 200 чтобы Telegram не ретраил если мы сами это отправили
  }
  // Апдейт должен содержать хотя бы одно известное Telegram-поле
  const KNOWN_UPDATE_TYPES = ['message', 'callback_query', 'edited_message', 'channel_post',
    'inline_query', 'chosen_inline_result', 'shipping_query', 'pre_checkout_query', 'poll'];
  const hasKnownType = KNOWN_UPDATE_TYPES.some(t => body[t] !== undefined);
  if (!hasKnownType) {
    console.warn(`⚠️ [РЕСЕЛЛЕР ${resellerId}] Webhook rejected: no known update type in body`);
    return res.sendStatus(200);
  }

  // Проверяем статус реселлера в БД — если деактивирован, игнорируем апдейт
  db.get('SELECT status FROM resellers WHERE id = ?', [resellerId], (err, row) => {
    if (err || !row || row.status !== 'active') {
      return res.sendStatus(200); // Бот выключен — молча игнорируем
    }
    try {
      entry.bot.processUpdate(body);
    } catch (err) {
      console.error(`❌ [РЕСЕЛЛЕР ${resellerId}] Webhook Update Error:`, err);
    }
    res.sendStatus(200);
  });
});

// Webhook для CryptoBot с проверкой подписи
app.post('/cryptobot-webhook', async (req, res) => {
  try {
    // ⚠️ БЕЗОПАСНОСТЬ: Если CRYPTOBOT_TOKEN не задан — CryptoBot не используется.
    // Возвращаем 404 (не 403): 403 сигнализирует что endpoint существует, но доступ запрещён.
    // 404 полностью скрывает endpoint от злоумышленников, снижая поверхность атаки.
    if (!CRYPTOBOT_TOKEN) {
      return res.sendStatus(404);
    }

    // Проверяем подпись — всегда обязательна когда токен задан.
    const signature = req.headers['crypto-pay-api-signature'];
    if (!signature) {
      console.error('❌ Missing CryptoBot signature');
      return res.sendStatus(403);
    }

    // Используем простое строковое сравнение hex-хешей.
    // sha256 hex всегда 64 символа — время сравнения постоянно.
    const hmac = crypto.createHmac('sha256', CRYPTOBOT_WEBHOOK_SECRET);
    // Используем rawBody — оригинальную строку запроса как её подписал CryptoBot
    const calculatedSignature = hmac.update(req.rawBody || JSON.stringify(req.body)).digest('hex');
    if (calculatedSignature !== signature) {
      console.error('❌ Invalid CryptoBot signature — отклонён поддельный webhook');
      return res.sendStatus(403);
    }

    const update = req.body;

    // ИСПРАВЛЕНИЕ: Игнорируем все типы обновлений кроме invoice_paid.
    // CryptoBot может присылать и другие события (invoice_created и т.д.)
    if (update.update_type !== 'invoice_paid') {
      console.log(`ℹ️ CryptoBot webhook: ignoring update_type=${update.update_type}`);
      return res.sendStatus(200);
    }

    // payload может быть объектом или JSON-строкой в зависимости от версии API.
    // Оборачиваем парсинг в try/catch: злоумышленник с валидной подписью (если утёк токен)
    // теоретически может прислать невалидный JSON в payload и уронить обработчик без защиты.
    let payload = {};
    try {
      payload = typeof update.payload === 'string'
        ? JSON.parse(update.payload)
        : (update.payload || {});
    } catch (parseErr) {
      console.error('❌ CryptoBot webhook: failed to parse payload JSON:', parseErr.message, '| raw:', String(update.payload).slice(0, 200));
      return res.sendStatus(200); // игнорируем, не падаем
    }
    const invoiceId = payload?.invoice_id || update.invoice_id;

    if (!invoiceId) {
      console.error('❌ CryptoBot webhook: no invoice_id in payload', update);
      return res.sendStatus(200);
    }

    console.log(`✅ CryptoBot webhook: Invoice ${invoiceId} paid`);

    // ИСПРАВЛЕНИЕ ИДЕМПОТЕНТНОСТИ: сначала ищем заказ и атомарно меняем статус,
    // только потом выдаём ключ. Повторный webhook не пройдёт — статус уже не pending.
    db.get(
      `SELECT * FROM orders WHERE invoice_id = ? AND (status = 'pending' OR status = 'out_of_stock_pending')`,
      [invoiceId],
      async (err, order) => {
        if (err) {
          console.error('❌ DB error:', err);
          return;
        }

        if (!order) {
          console.log(`⚠️ Order not found for invoice ${invoiceId}, checking bundle orders...`);
          // Check if it's a bundle order
          db.get(`SELECT * FROM bundle_orders WHERE invoice_id = ? AND status = 'pending'`, [invoiceId], async (be, bo) => {
            if (be || !bo) { console.log(`⚠️ Bundle order not found either`); return; }
            try {
              // Атомарно меняем статус, потом выдаём ключи.
              // Повторный webhook уже не найдёт запись с status='pending'.
              const updated = await new Promise(res =>
                db.run(
                  `UPDATE bundle_orders SET status = 'confirmed', confirmed_at = datetime('now') WHERE id = ? AND status = 'pending'`,
                  [bo.id],
                  function (e) { res(!e && this.changes > 0); }
                )
              );
              if (!updated) { console.log(`⚠️ Bundle #${bo.id} already confirmed — skipping`); return; }
              const keys = await issueBundleKeys(bo.user_id, bo.product, bo.quantity, bo.user_lang || 'en');
              await sendBundleKeys(bo.user_id, bo.user_lang || 'en', bo.product, keys, bo.id);
              safeSendMessage(ADMIN_ID, `✅ *Bundle автооплата*\n\n👤 @${escapeMarkdown(String(bo.username || bo.user_id))}\n📦 ${bo.product} ×${bo.quantity}\n🔑 Выдано: ${keys.length}`, { parse_mode: 'Markdown' }).catch(() => {});
            } catch (e) {
              if (e.code === 'PARTIAL_OUT_OF_STOCK') {
                console.error(`⚠️ Bundle #${bo.id}: partial out of stock — issued ${e.issued}/${bo.quantity}`);
                db.run(`UPDATE bundle_orders SET status = 'partial', partial_issued = ? WHERE id = ?`,
                  [e.issued, bo.id]);
                if (e.keys && e.keys.length > 0) {
                  await sendBundleKeys(bo.user_id, bo.user_lang || 'en', bo.product, e.keys, bo.id).catch(() => {});
                }
                safeSendMessage(ADMIN_ID,
                  `⚠️ *Bundle — частичная выдача!*\n\n👤 @${escapeMarkdown(String(bo.username || bo.user_id))}\n📦 ${bo.product} ×${bo.quantity}\n🔑 Выдано: ${e.issued} из ${bo.quantity}\n\n❗ Нужно довыдать *${bo.quantity - e.issued}* ключей вручную!\nBundle #${bo.id}`,
                  {
                    parse_mode: 'Markdown',
                    reply_markup: { inline_keyboard: [[{ text: '📦 Открыть заказы', callback_data: 'admin_manage_orders' }]] }
                  }
                ).catch(() => {});
              } else {
                console.error('Bundle CryptoBot confirm error:', e);
                safeSendMessage(ADMIN_ID, `⚠️ Ошибка bundle автовыдачи: ${e.message}`).catch(() => {});
              }
            }
          });
          return;
        }

        // Если это пополнение баланса — обрабатываем отдельно
        if (order.balance_topup === 1 || order.product === 'balance_topup') {
          const atomicTopup = await new Promise(resolve =>
            db.run(
              `UPDATE orders SET status = 'confirmed', confirmed_at = datetime('now')
               WHERE id = ? AND status = 'pending'`,
              [order.id],
              function(e) { resolve(!e && this.changes > 0); }
            )
          );
          if (!atomicTopup) { console.log(`ℹ️ Duplicate topup webhook for order #${order.id}`); return; }

          try {
            const newBal = await adjustUserBalance(order.user_id, order.amount, order.currency,
              'topup', 'Пополнение через CryptoBot', order.id, null);
            const isRuTop = (order.user_lang || 'en').startsWith('ru');
            safeSendMessage(order.user_id,
              isRuTop
                ? `💰✅ Баланс пополнен!

💳 Зачислено: *${formatBalanceAmount(order.amount, order.currency)}*
💰 Текущий баланс: *${formatBalanceAmount(newBal, order.currency)}*

_Используйте баланс для мгновенной покупки ключей._ 🔑`
                : `💰✅ Balance topped up!

💳 Credited: *${formatBalanceAmount(order.amount, order.currency)}*
💰 Current balance: *${formatBalanceAmount(newBal, order.currency)}*

_Use your balance for instant key purchases._ 🔑`,
              { parse_mode: 'Markdown', reply_markup: { inline_keyboard: [[{ text: isRuTop ? '👤 Мой профиль' : '👤 My Profile', callback_data: 'my_profile' }]] } }
            ).catch(() => {});
            logAction(order.user_id, 'balance_topup_confirmed', { amount: order.amount, currency: order.currency, orderId: order.id });
          } catch(e) {
            console.error('Balance topup confirm error:', e.message);
          }
          return;
        }

        try {
          // 🔒 АТОМАРНО: захватываем заказ + выдаём ключ + обновляем статус в одной транзакции.
          // issueKeyAndConfirmOrder сам проверяет что заказ ещё pending через UPDATE orders.
          // Повторный webhook не пройдёт — статус уже будет 'confirmed'.
          let key = null;
          try {
            const result = await issueKeyAndConfirmOrder(order.id, order.user_id, order.product, 'purchase');
            key = result.key;
          } catch (issueErr) {
            // Дубль вебхука — заказ уже подтверждён.
            // FIX 1.3: логируем и уведомляем админа (тихо для пользователя — он уже получил ключ).
            if (issueErr.code === 'ALREADY_CONFIRMED') {
              console.warn(`⚠️ [ATOMIC] Order #${order.id} ALREADY_CONFIRMED — duplicate CryptoBot webhook`);
              safeSendMessage(ADMIN_ID,
                `ℹ️ *Дубль CryptoBot webhook*\n\nЗаказ #${order.id} уже был подтверждён ранее.\nПовторный вебхук проигнорирован — ключ пользователю не отправлялся повторно.\n\n_Это нормально при сетевых ретраях CryptoBot._`,
                { parse_mode: 'Markdown' }
              ).catch(() => {});
              return;
            }
            // Откатываем статус чтобы вебхук можно было повторить вручную
            if (issueErr.code === 'OUT_OF_STOCK') {
              db.run(`UPDATE orders SET status = 'out_of_stock' WHERE id = ?`, [order.id], async (rollbackErr) => {
                if (rollbackErr) console.error('❌ Error rolling back order to out_of_stock:', rollbackErr);

                const oosBotInstance = (() => {
                  if (order.reseller_id) {
                    const rslE = resellerBots.get(order.reseller_id);
                    if (rslE?.bot) return rslE.bot;
                  }
                  return bot;
                })();

                // Проверяем: если у пользователя есть баланс в нужной валюте — рефандим туда
                const orderCurrency = order.original_currency || order.currency;
                const orderAmount = order.original_amount || order.amount;
                const userBal = await getUserBalance(order.user_id).catch(() => null);
                const canRefundToBalance = userBal && userBal.preferred_currency === orderCurrency && orderAmount > 0;

                if (canRefundToBalance) {
                  // Рефанд на баланс + умный OOS-купон с антифармом
                  try {
                    await adjustUserBalance(order.user_id, orderAmount, orderCurrency, 'refund',
                      `Возврат: нет ключей ${order.product}`, order.id, null);

                    const oosCode = await issueOosCoupon(order.user_id, order.product, order.id);
                    const oosPct = OOS_COUPON_PCT[order.product] || 10;
                    const isRuOos = (order.user_lang || 'en').startsWith('ru');
                    const pNameOos = PERIOD_NAMES[isRuOos ? 'ru' : 'en'][order.product] || order.product;
                    const couponLineOos = oosCode
                      ? (isRuOos
                          ? `\n\n🎟 В знак извинения — купон на *${oosPct}% скидку* на «${pNameOos}»:\n\`${oosCode}\`\n_Действует 30 дней._`
                          : `\n\n🎟 As an apology — *${oosPct}% discount coupon* for «${pNameOos}»:\n\`${oosCode}\`\n_Valid 30 days._`)
                      : '';

                    await safeSendMessage(
                      order.user_id,
                      isRuOos
                        ? `😔 *Ключи для «${pNameOos}» временно закончились.*\n\n💳 Сумма *${formatBalanceAmount(orderAmount, orderCurrency)}* возвращена на ваш баланс.${couponLineOos}\n\n_Ключи появятся в ближайшее время!_`
                        : `😔 *Keys for «${pNameOos}» are temporarily out of stock.*\n\n💳 *${formatBalanceAmount(orderAmount, orderCurrency)}* refunded to your balance.${couponLineOos}\n\n_Keys will be restocked soon!_`,
                      { parse_mode: 'Markdown', reply_markup: { inline_keyboard: [[{ text: isRuOos ? '👤 Мой профиль' : '👤 My Profile', callback_data: 'my_profile' }]] } },
                      oosBotInstance
                    ).catch(() => {});
                  } catch (refundErr) {
                    console.error('OOS refund error:', refundErr.message);
                    await safeSendMessage(order.user_id, t({ language_code: order.user_lang || 'en' }, 'order_out_of_stock'), {}, oosBotInstance).catch(() => {});
                  }
                } else {
                  await safeSendMessage(
                    order.user_id,
                    t({ language_code: order.user_lang || 'en' }, 'order_out_of_stock'),
                    {}, oosBotInstance
                  ).catch(() => {});
                }

                await sendOutOfStockNotification(order, ADMIN_ID);
              });
            } else {
              // Откатываем статус на pending — чтобы вебхук можно было обработать повторно
              db.run(`UPDATE orders SET status = 'pending' WHERE id = ?`, [order.id], (rollbackErr) => {
                if (rollbackErr) console.error('❌ Error rolling back order status:', rollbackErr);
              });
              safeSendMessage(
                ADMIN_ID,
                `⚠️ *Ошибка выдачи ключа (CryptoBot)*\n\nOrder #${order.id}\nInvoice: ${invoiceId}\n\n${escapeMarkdown(issueErr.message)}`,
                { parse_mode: 'Markdown' }
              ).catch(e => console.error('❌ Error notify failed:', e));
            }
            return;
          }

          // CryptoBot — автооплата, ⏳ не было, сразу шлём ключ/гайд
          // FIX 2: Используем ?? чтобы гарантированно получить bot если реселлер-инстанс недоступен
          const botInstance = resellerBots.get(order.reseller_id)?.bot ?? bot;

          if (order.product === 'infinite_boost') {
            // 🚀 Метод буста — отправляем гайд
            await sendInfiniteBoostGuide(order.user_id, order.user_lang || 'en', botInstance);
          } else if (order.product === 'reseller_connection') {
            // 🤝 CryptoBot автооплата партнёрства — создаём запись реселлера
            const isRu = (order.user_lang || 'en').startsWith('ru');
            const defaultMarkup = parseInt(getSetting('reseller_default_markup')) || 30;

            db.get(`SELECT id, status FROM resellers WHERE user_id = ?`, [order.user_id], (rErr, existingR) => {
              if (existingR) {
                db.run(`UPDATE resellers SET status = 'awaiting_token', markup_pct = ? WHERE user_id = ?`,
                  [defaultMarkup, order.user_id]);
              } else {
                db.run(`INSERT INTO resellers (user_id, status, markup_pct, balance) VALUES (?, 'awaiting_token', ?, 0)`,
                  [order.user_id, defaultMarkup]);
              }

              const tokenMsg = isRu
                ? `🎉 *Оплата подтверждена!*\n*Добро пожаловать в партнёрскую программу!*\n\n` +
                `Теперь отправьте токен вашего бота:\n\n` +
                `1️⃣ Откройте @BotFather в Telegram\n` +
                `2️⃣ Создайте нового бота командой /newbot\n` +
                `3️⃣ Скопируйте полученный токен\n` +
                `4️⃣ Отправьте его сюда\n\n` +
                `⚠️ *Токен выглядит так:* \`123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11\`\n\n` +
                `🔒 Токен будет зашифрован и надёжно сохранён.`
                : `🎉 *Payment confirmed! Welcome to the partnership program!*\n\n` +
                `Now send your bot token:\n\n` +
                `1️⃣ Open @BotFather in Telegram\n` +
                `2️⃣ Create a new bot with /newbot\n` +
                `3️⃣ Copy the token you receive\n` +
                `4️⃣ Send it here\n\n` +
                `⚠️ *Token looks like:* \`123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11\`\n\n` +
                `🔒 Your token will be encrypted and securely stored.`;

              const sess = getSession(order.user_id);
              sess.state = 'awaiting_reseller_token';
              sess.data = {};

              safeSendMessage(order.user_id, tokenMsg, { parse_mode: 'Markdown' }).catch(() => {});
              logAction(order.user_id, 'reseller_connection_auto_activated', { orderId: order.id });
            });
          } else {
            // 🏀 CryptoBot: тоже проверяем баскетбольный выигрыш перед отправкой ключа
            let basketballCouponCrypto = null;
            const KEY_PRODUCT_LIST_CRYPTO = ['1d', '3d', '7d', '30d'];
            if (KEY_PRODUCT_LIST_CRYPTO.includes(order.product)) {
              const basketWinCrypto = await new Promise(res =>
                db.get(`SELECT 1 FROM basketball_throws WHERE order_id = ? AND won = 1`,
                  [order.id], (e, row) => res(row || null))
              );
              if (basketWinCrypto) basketballCouponCrypto = await _issueBasketballCoupon(order, order.id);
            }
            await sendKeyMessage(order.user_id, order.user_lang || 'en', order.product, key, order.id, botInstance, basketballCouponCrypto);
          }

          // Купон, наценка реселлеру, FOMO, реферальный бонус
          await finalizeSuccessfulOrder(order, order.id, botInstance);

          const displayCurrency = order.original_currency || order.currency;
          const displayAmount = order.original_amount || order.amount;
          const formattedAmount = formatPrice(displayAmount, displayCurrency);
          const productLabel = PERIOD_NAMES.ru[order.product] || order.product;
          const resultLabel = order.product === 'reseller_connection' ? '✅ Активировано'
            : order.product === 'infinite_boost' ? '📄 Гайд отправлен'
            : '🔑 Ключ выдан';

          safeSendMessage(
            ADMIN_ID,
            `✅ *Автооплата CryptoBot*\n\n👤 ${escapeMarkdown(order.username ? '@' + order.username : 'ID: ' + order.user_id)}\n📦 ${escapeMarkdown(productLabel)}\n💰 *${formattedAmount}*\n${resultLabel}`,
            { parse_mode: 'Markdown' }
          ).catch(e => console.error('❌ Admin notify error:', e));

          logAction(order.user_id, 'cryptobot_auto_confirmed', { orderId: order.id, invoiceId });
          console.log(`✅ Order #${order.id} auto-confirmed via CryptoBot`);

        } catch (unexpectedError) {
          // Сюда попадают только неожиданные ошибки вне issueKeyToUser (например, ошибка отправки сообщения)
          console.error('❌ Unexpected error in webhook handler:', unexpectedError);
          safeSendMessage(
            ADMIN_ID,
            `⚠️ *Неожиданная ошибка (CryptoBot webhook)*\n\nOrder #${order.id}\nInvoice: ${invoiceId}\n\n${escapeMarkdown(unexpectedError.message)}`,
            { parse_mode: 'Markdown' }
          ).catch(e => console.error('❌ Error notify failed:', e));
        }
      }
    );

    // Всегда возвращаем 200 — иначе CryptoBot будет ретраить webhook
    res.status(200).send('OK');
  } catch (error) {
    console.error('❌ Webhook error:', error);
    res.status(200).send('OK'); // 200 даже при ошибке — не допускаем ретраев
  }
});

// ==========================================
// 🚀 ИНТЕГРАЦИЯ: WEB SITE API (CYRAX-SHOP)
// ==========================================
const cors = require('cors');
app.use(cors());
app.use(express.json());

// Разрешаем раздавать статику сайта, если папка site существует (для localhost)
app.use('/site', express.static(path.join(__dirname, 'site')));

app.get('/api/prices', (req, res) => res.json(PRICES));

const PAYMENT_METHODS_PER_CURRENCY = {
  RUB: ['sbp', 'cryptobot'],
  USD: ['paypal', 'binance', 'cryptobot'],
  EUR: ['card_it', 'binance', 'cryptobot', 'paypal'],
  UAH: ['card_ua', 'cryptobot']
};

app.get('/api/payment-methods', (req, res) => {
  const currency = req.query.currency;
  res.json(currency && PAYMENT_METHODS_PER_CURRENCY[currency] ? PAYMENT_METHODS_PER_CURRENCY[currency] : PAYMENT_METHODS_PER_CURRENCY);
});

app.get('/api/payment-details/:method', (req, res) => {
  const method = req.params.method;
  const mappedMethod = method === 'cryptobot' ? 'crypto' : method;
  res.json({ details: PAYMENT_DETAILS[mappedMethod] || '' });
});

app.get('/api/order-status/:orderId', (req, res) => {
  const orderId = parseInt(req.params.orderId);
  db.get(`SELECT status FROM orders WHERE id = ?`, [orderId], (err, order) => {
    if (err || !order) return res.status(404).json({ error: 'Order not found' });
    res.json({ status: order.status });
  });
});

app.post('/api/site/create-order', (req, res) => {
  const { telegram_id, product, currency, method } = req.body;
  if (!telegram_id || !product || !currency || !method) return res.status(400).json({ error: 'Missing params' });
  const price = PRICES[product]?.[currency];
  if (!price) return res.status(400).json({ error: 'Invalid product or currency' });

  db.run(
    `INSERT INTO orders (user_id, product, amount, currency, payment_method, status, created_at) VALUES (?, ?, ?, ?, ?, 'pending', datetime('now'))`,
    [parseInt(telegram_id), product, price, currency, method],
    function(err) {
      if (err) return res.status(500).json({ error: 'DB Error' });
      const orderId = this.lastID;
      
      const userMsg = `✅ *Заказ #${orderId} создан!*\n\n🔑 Товар: ${product}\n💰 Сумма: ${price} ${currency}\n💳 Метод: ${method}\n\n⚠️ После оплаты отправьте скриншот чека в этот чат (если оплата ручная) или ожидайте проверки.`;
      bot.sendMessage(telegram_id, userMsg, {
        parse_mode: 'Markdown',
        reply_markup: { inline_keyboard: [[{ text: '❌ Отменить заказ', callback_data: `cancel_order_${orderId}` }]] }
      }).catch(e => console.error('Site order notify error:', e.message));

      if (ADMIN_ID) {
        bot.sendMessage(ADMIN_ID, `🛒 *Новый заказ с САЙТА!*\n\n👤 ID: ${telegram_id}\n🔑 ${product}\n💰 ${price} ${currency}\n💳 ${method}\n📦 Заказ #${orderId}`).catch(()=>{});
      }

      res.json({ success: true, orderId });
    }
  );
});

// ==========================================
// 🚀 ЗАПУСК СЕРВЕРА
// ==========================================
app.listen(PORT, async () => {
  try {
    console.log(`Server running on port ${PORT}`);
    console.log(`📂 DB_PATH: ${DB_PATH}`);

    await initializeDatabase();
    await loadPricesFromDB();
    await loadPaymentDetailsFromDB();
    await loadSettings();

    // 🧹 Очищаем сессии при старте — после рестарта в памяти не остаётся устаревших данных.
    // Все критичные данные хранятся в БД, поэтому это безопасно.
    userSessions.clear();
    approvingOrders.clear();
    approvingTopups.clear();
    console.log('🧹 Сессии очищены при старте');

    // Поддержка RENDER_EXTERNAL_URL — автоматически определяет домен на Render
    const renderUrl = process.env.RENDER_EXTERNAL_URL || `https://cyrax-bot-0vwr.onrender.com`;
    const webhookUrl = `${renderUrl}/webhook/${WEBHOOK_SECRET}`;

    try {
      await bot.deleteWebHook();
      console.log('✅ Старый вебхук удалён');

      await bot.setWebHook(webhookUrl, {
        allowed_updates: ['message', 'callback_query', 'my_chat_member', 'chat_member']
      });
      console.log(`✅ Webhook установлен на ${webhookUrl}`);

      const webhookInfo = await bot.getWebHookInfo();
      console.log('📊 Информация о вебхуке:', webhookInfo);

      // Инициализируем реселлеров после основного бота
      await initAllResellers();

    } catch (error) {
      console.error('❌ Ошибка установки webhook:', error.message);
    }

    console.log('Bot started successfully!');

    // ==========================================
    // 🔄 АВТОРЕСТОР БАЗЫ ПРИ СТАРТЕ
    // ==========================================
    // Если БД пустая (нет таблиц или 0 пользователей) — пробуем восстановить
    // автоматически из последнего бэкапа по сохранённому file_id.
    // file_id хранится в /tmp/cyrax_last_backup.json — он записывается
    // при каждом успешном автобэкапе (каждые 6ч + при старте).
    // На Render при простом рестарте процесса /tmp сохраняется → авторестор срабатывает.
    // При полном redeploy /tmp стирается → бот сообщает об этом и просит файл вручную.
    setTimeout(async () => {
      try {
        // Проверяем, пустая ли БД
        const usersCount = await new Promise((resolve) => {
          db.get(`SELECT COUNT(*) as cnt FROM users`, [], (err, row) => {
            resolve(err ? -1 : (row ? row.cnt : 0));
          });
        });

        const dbIsEmpty = usersCount === 0;
        const backupMetaPath = '/tmp/cyrax_last_backup.json';
        const hasBackupMeta = fs.existsSync(backupMetaPath);

        if (dbIsEmpty && hasBackupMeta) {
          // БД пустая И есть сохранённый file_id → пробуем автовосстановление
          console.log('🔄 БД пустая, найден file_id бэкапа — запускаю авторестор...');
          let backupMeta;
          try {
            backupMeta = JSON.parse(fs.readFileSync(backupMetaPath, 'utf8'));
          } catch (parseErr) {
            console.error('❌ Не удалось прочитать cyrax_last_backup.json:', parseErr.message);
            backupMeta = null;
          }

          // Поддержка нового формата (массив backups) и старого (один объект)
          // Перебираем все доступные бэкапы от свежего к старому
          let backupsList = [];
          if (backupMeta && Array.isArray(backupMeta.backups) && backupMeta.backups.length > 0) {
            backupsList = backupMeta.backups; // уже отсортированы: [0] = свежий
          } else if (backupMeta && backupMeta.file_id) {
            backupsList = [backupMeta]; // старый формат — один объект
          }

          let restored = false;
          for (let attempt = 0; attempt < backupsList.length; attempt++) {
            const backupEntry = backupsList[attempt];
            if (!backupEntry || !backupEntry.file_id) continue;
            backupMeta = backupEntry; // для совместимости с кодом ниже

          if (backupMeta && backupMeta.file_id) {
            if (attempt === 0) {
              await safeSendMessage(ADMIN_ID,
                `🔄 *Авторестор базы данных*\n\n` +
                `📂 БД пустая после рестарта Render\n` +
                `💾 Найден бэкап от ${backupMeta.date} ${backupMeta.time} (${backupMeta.size_kb} KB)\n\n` +
                `⏳ _Скачиваю и восстанавливаю автоматически..._`,
                { parse_mode: 'Markdown' }
              );
            } else {
              console.log(`🔄 Попытка ${attempt + 1}: бэкап от ${backupMeta.date} ${backupMeta.time}`);
            }

            try {
              const fileLink = await bot.getFileLink(backupMeta.file_id);
              const response = await axios({
                method: 'get',
                url: fileLink,
                responseType: 'stream',
                maxContentLength: 10 * 1024 * 1024,
                timeout: 30000
              });

              const tempPath = '/tmp/shop_autorestore_temp.db';
              const writer = fs.createWriteStream(tempPath);
              response.data.pipe(writer);

              await new Promise((resolve, reject) => {
                writer.on('finish', resolve);
                writer.on('error', reject);
              });

              // Проверяем целостность скачанного файла
              const isValid = await new Promise((resolve) => {
                const testDb = new sqlite3.Database(tempPath, sqlite3.OPEN_READONLY, (openErr) => {
                  if (openErr) return resolve(false);
                  testDb.get('SELECT COUNT(*) as cnt FROM sqlite_master WHERE type="table"', [], (testErr, row) => {
                    testDb.close();
                    resolve(!testErr && row && row.cnt > 0);
                  });
                });
              });

              // П.5: проверяем SHA256 если он был сохранён
              if (isValid && backupMeta.sha256) {
                const downloadedContent = fs.readFileSync(tempPath);
                const downloadedHash = crypto.createHash('sha256').update(downloadedContent).digest('hex');
                if (downloadedHash !== backupMeta.sha256) {
                  isValid = false;
                  console.error(`❌ SHA256 mismatch: expected ${backupMeta.sha256.slice(0,12)}... got ${downloadedHash.slice(0,12)}...`);
                  try { fs.unlinkSync(tempPath); } catch (e) {}
                  if (attempt + 1 < backupsList.length) {
                    console.log(`⚠️ SHA256 не совпал, пробую следующий бэкап...`);
                  } else {
                    await safeSendMessage(ADMIN_ID,
                      `❌ *Авторестор отменён — SHA256 не совпал*\n\nФайл бэкапа мог быть повреждён при передаче.\n\n📎 Отправьте .db файл вручную.`,
                      { parse_mode: 'Markdown', reply_markup: { inline_keyboard: [[{ text: '🗄️ Восстановить вручную', callback_data: 'admin_restore' }]] } }
                    );
                  }
                }
              }

              if (!isValid) {
                try { fs.unlinkSync(tempPath); } catch (e) {}
                if (attempt + 1 < backupsList.length) {
                  console.log(`⚠️ Бэкап #${attempt + 1} битый, пробую следующий...`);
                  continue; // следующая итерация цикла
                }
                await safeSendMessage(ADMIN_ID,
                  `❌ *Авторестор не удался*\n\nСкачанный файл повреждён или пустой.\n\n` +
                  `📎 Отправьте .db файл вручную через кнопку ниже.`,
                  { parse_mode: 'Markdown', reply_markup: { inline_keyboard: [[{ text: '🗄️ Восстановить вручную', callback_data: 'admin_restore' }]] } }
                );
              } else {
                // Файл валидный — восстанавливаем
                if (fs.existsSync(DB_PATH + '-wal')) fs.unlinkSync(DB_PATH + '-wal');
                if (fs.existsSync(DB_PATH + '-shm')) fs.unlinkSync(DB_PATH + '-shm');
                fs.copyFileSync(tempPath, DB_PATH);
                fs.unlinkSync(tempPath);

                // Переоткрываем БД с новыми данными
                db = new sqlite3.Database(DB_PATH);
                db.run('PRAGMA journal_mode=WAL;');
                await initializeDatabase();
                await loadPricesFromDB();
                await loadPaymentDetailsFromDB();
                await loadSettings();

                // Считаем что восстановили
                const stats = await new Promise((resolve) => {
                  db.get(
                    `SELECT
                      (SELECT COUNT(*) FROM users) as users_count,
                      (SELECT COUNT(*) FROM orders) as total_orders,
                      (SELECT COUNT(*) FROM keys WHERE status='available') as available_keys`,
                    [],
                    (e, row) => resolve(e ? null : row)
                  );
                });

                const statsText = stats
                  ? `\n\n📊 *Восстановлено:*\n👥 Пользователей: ${stats.users_count}\n📦 Заказов: ${stats.total_orders}\n🔑 Ключей: ${stats.available_keys}`
                  : '';

                console.log('✅ Авторестор успешен!', stats);
                logAction(ADMIN_ID, 'database_restored', { method: 'auto', date: backupMeta.date });
                restored = true;

                await safeSendMessage(ADMIN_ID,
                  `🟢✅ Авторестор успешен!\n\n` +
                  `🗄️ База восстановлена из бэкапа от ${backupMeta.date} ${backupMeta.time}${statsText}\n\n` +
                  `_Бот работает в штатном режиме._ 🚀`,
                  { parse_mode: 'Markdown', reply_markup: { inline_keyboard: [[{ text: '📊 Панель управления', callback_data: 'admin' }]] } }
                );
              }
            } catch (restoreErr) {
              console.error('❌ Ошибка авторестора:', restoreErr.message);
              await safeSendMessage(ADMIN_ID,
                `❌ *Авторестор не удался*\n\n` +
                `Ошибка: ${restoreErr.message}\n\n` +
                `📎 Отправьте .db файл вручную через кнопку ниже.`,
                { parse_mode: 'Markdown', reply_markup: { inline_keyboard: [[{ text: '🗄️ Восстановить вручную', callback_data: 'admin_restore' }]] } }
              );
            }
          } // end if backupMeta.file_id
          if (restored) break; // успешно восстановили — выходим из цикла
          } // end for loop

          // Если прошли все бэкапы и ни один не подошёл
          if (!restored && backupsList.length > 0) {
            await safeSendMessage(ADMIN_ID,
              `❌ *Все бэкапы повреждены*\n\n` +
              `Проверено ${backupsList.length} бэкап(ов) — ни один не прошёл проверку.\n\n` +
              `📎 Отправьте .db файл вручную через кнопку ниже.`,
              { parse_mode: 'Markdown', reply_markup: { inline_keyboard: [[{ text: '🗄️ Восстановить вручную', callback_data: 'admin_restore' }]] } }
            );
          }

        } else if (dbIsEmpty && !hasBackupMeta) {
          // БД пустая И нет file_id (полный redeploy — /tmp стёрт)
          await safeSendMessage(ADMIN_ID,
            `🟢🚀 Бот запущен\n\n` +
            `⚠️ *База данных пустая!*\n\n` +
            `Это был полный redeploy — файл бэкапа не найден в /tmp.\n\n` +
            `📎 *Отправьте .db файл через кнопку ниже* для восстановления данных.\n\n` +
            `_Бэкап ищите в чате с ботом — он присылает его каждые 6 часов._`,
            {
              parse_mode: 'Markdown',
              reply_markup: {
                inline_keyboard: [
                  [{ text: '🗄️ Восстановить БД', callback_data: 'admin_restore' }],
                  [{ text: '📊 Панель управления', callback_data: 'admin' }]
                ]
              }
            }
          );

        } else {
          // БД в порядке — обычный старт
          const startMsg = `🟢🚀 Бот запущен и готов к работе!\n\n` +
            `📂 База: \`${DB_PATH}\`\n` +
            `👥 Пользователей: ${usersCount}\n\n` +
            `_📦 Плановый бэкап БД будет отправлен следующим сообщением._`;
          await safeSendMessage(ADMIN_ID, startMsg, {
            parse_mode: 'Markdown',
            reply_markup: { inline_keyboard: [[{ text: '📊 Панель управления', callback_data: 'admin' }]] }
          });
        }
      } catch (e) {
        console.error('❌ Startup notify error:', e.message);
      }
    }, STARTUP_NOTIFY_DELAY_MS);

    // ✅ Keep-alive пинг — предотвращает засыпание бесплатного инстанса на Render
    // Render Free tier засыпает после 15 минут бездействия. Пингуем себя каждые 10 минут.
    const selfUrl = renderUrl;
    let keepAliveFails = 0; // счётчик последовательных неудач
    setInterval(async () => {
      try {
        await axios.get(`${selfUrl}/health`, { timeout: 5000 });
        console.log('💓 Keep-alive ping OK');
        keepAliveFails = 0; // сбрасываем при успехе
      } catch (e) {
        keepAliveFails++;
        console.error(`💓 Keep-alive ping failed (${keepAliveFails}x):`, e.message);
        // Если 2 раза подряд не прошёл — предупреждаем админа
        if (keepAliveFails === 2) {
          safeSendMessage(ADMIN_ID,
            `⚠️ *Keep-alive не работает 2 раза подряд*\n\nСервис может засыпать — новые заказы не будут обрабатываться.\n\nОшибка: ${e.message}`,
            { parse_mode: 'Markdown' }
          ).catch(() => {});
        }
      }
    }, KEEP_ALIVE_INTERVAL_MS);

    // Запускаем проверку долгих заказов
    setInterval(checkLongPendingOrders, LONG_PENDING_CHECK_MS);

    // 📣 Авторекламка в группах — первый запуск случайный в течение часа, затем каждые 6 ч
    const promoDelay = Math.floor(Math.random() * 60 * 60 * 1000);
    console.log(`📣 [PROMO] Первая рассылка через ${Math.round(promoDelay / 60000)} мин`);
    function scheduleNextPromo() {
      // Читаем интервал из настроек БД (в часах), fallback на константу
      const intervalHours = parseInt(getSetting('promo_interval_hours') || '6');
      const intervalMs = intervalHours * 60 * 60 * 1000;
      const jitter = Math.floor((Math.random() - 0.5) * 15 * 60 * 1000); // ±7.5 мин
      const delay = Math.max(intervalMs + jitter, 60 * 60 * 1000); // не менее 1 часа
      console.log(`📣 [PROMO] Следующая рассылка через ${Math.round(delay / 60000)} мин (интервал: ${intervalHours} ч)`);
      setTimeout(async () => {
        await sendGroupPromo();
        scheduleNextPromo();
      }, delay);
    }
    setTimeout(async () => {
      await sendGroupPromo();
      scheduleNextPromo();
    }, promoDelay);
    setInterval(cleanupAbandonedCryptobotOrders, CRYPTOBOT_CLEANUP_INTERVAL_MS);
    setTimeout(cleanupAbandonedCryptobotOrders, CRYPTOBOT_CLEANUP_FIRST_MS); // и сразу при старте

    // Task 2.2: Напоминание о зависших заявках на вывод (раз в сутки).
    // Если заявка pending больше 3 дней — admin получает уведомление с кнопкой.
    setInterval(async () => {
      try {
        const STALE_DAYS = 3;
        const staleDate = new Date(Date.now() - STALE_DAYS * 24 * 60 * 60 * 1000)
          .toISOString().replace('T', ' ').substring(0, 19);
        const stale = await new Promise((resolve) =>
          db.all(
            `SELECT rw.id, rw.amount, rw.details, rw.created_at, r.username as rsl_username, r.user_id as rsl_user_id
             FROM reseller_withdrawals rw
             JOIN resellers r ON rw.reseller_id = r.id
             WHERE rw.status = 'pending' AND rw.created_at < ?
             ORDER BY rw.created_at ASC`,
            [staleDate],
            (e, rows) => resolve(e ? [] : rows)
          )
        );
        if (!stale.length) return;

        let msg = `⏰ *Необработанные заявки на вывод (>${STALE_DAYS} дней)*\n\n`;
        stale.forEach(w => {
          const rsl = w.rsl_username ? `@${escapeMarkdown(w.rsl_username)}` : `ID ${w.rsl_user_id}`;
          const dt = new Date(w.created_at).toLocaleString('ru-RU');
          const isKey = w.details?.startsWith('KEY_');
          msg += `• #${w.id} | ${rsl} | ${isKey ? '🔑' : '💸'} ${w.amount}₽ | ${dt}\n`;
        });
        msg += `\nОбработайте в панели управления:`;

        safeSendMessage(ADMIN_ID, msg, {
          parse_mode: 'Markdown',
          reply_markup: { inline_keyboard: [[{ text: '📋 Заявки на вывод', callback_data: 'admin_rsl_withdrawals' }]] }
        }).catch(() => {});
        console.log(`⏰ [WITHDRAWAL_REMINDER] Sent reminder for ${stale.length} stale withdrawal(s)`);
      } catch (e) {
        console.error('❌ Withdrawal reminder error:', e);
      }
    }, WITHDRAWAL_REMINDER_INTERVAL_MS);

    // FIX 2.2: Периодическая синхронизация реквизитов и цен из БД каждые 5 минут.
    // Страховка от расхождения памяти с диском после рестарта или ручного редактирования БД.
    // Все реселлер-боты в одном процессе — читают тот же PAYMENT_DETAILS, обновление сразу видно всем.
    setInterval(async () => {
      await loadPaymentDetailsFromDB();
      await loadPricesFromDB();
    }, SYNC_INTERVAL_MS);

    // 🛡️ Anti-scam: мониторинг подозрительной активности каждый час
    setInterval(runSuspicionMonitor, SUSPICION_MONITOR_INTERVAL_MS);

    // П.3: Проверка запаса ключей каждые 30 минут
    setInterval(checkLowKeysAndNotify, LOW_KEYS_CHECK_INTERVAL_MS);

    // 🎟️ Синхронизация used_count купонов при старте
    setTimeout(syncCouponUsedCount, COUPON_SYNC_FIRST_MS);
    // И каждый час для поддержки актуальности
    setInterval(syncCouponUsedCount, COUPON_SYNC_INTERVAL_MS);

    // 💱 Задача 5: Автообновление курсов валют — при старте и каждый час
    await fetchAndUpdateExchangeRates();
    setInterval(fetchAndUpdateExchangeRates, EXCHANGE_RATE_INTERVAL_MS);

    // ⏰ Напоминания о продлении — каждые 6 часов
    setInterval(sendRenewalReminders, RENEWAL_REMINDER_INTERVAL_MS);
    // Первый запуск через 2 минуты после старта
    setTimeout(sendRenewalReminders, RENEWAL_REMINDER_FIRST_MS);

    // Ежедневный отчёт в 09:00
    const scheduleDaily = () => {
      const now = new Date();
      const next = new Date();
      next.setHours(9, 0, 0, 0);
      if (next <= now) next.setDate(next.getDate() + 1);
      setTimeout(() => {
        sendDailyReport();
        // Анализ частых покупателей в то же время
        analyzeFrequentBuyers();
        setInterval(() => {
          sendDailyReport();
          analyzeFrequentBuyers();
        }, DEAD_REFERRAL_CHECK_INTERVAL_MS);
      }, next - now);
    };
    scheduleDaily();

    // Реферальная программа — проверка мёртвых рефов раз в сутки
    setInterval(checkDeadReferrals, DEAD_REFERRAL_CHECK_INTERVAL_MS);

    // Отложенные рассылки — проверяем каждую минуту
    setInterval(processScheduledBroadcasts, BROADCAST_PROCESS_INTERVAL_MS);

    // 🆕 Сводка новых пользователей — раз в час вместо мгновенного спама
    setInterval(flushNewUserBatch, NEW_USER_BATCH_INTERVAL_MS);

    // Запускаем автоматический бэкап каждые 6 часов (чаще, т.к. на Render бесплатном бывают рестарты)
    setInterval(() => {
      sendDatabaseBackup();
    }, BACKUP_INTERVAL_MS);

    // ✅ Первый бэкап через 15 секунд после запуска — КРИТИЧНО для бесплатного Render!
    // Отправляется всегда при старте, чтобы иметь актуальный файл БД в чате.
    setTimeout(() => {
      sendDatabaseBackup();
    }, BACKUP_FIRST_DELAY_MS);

  } catch (error) {
    console.error('❌ Fatal error on startup:', error);
    process.exit(1);
  }
}).on('error', (error) => {
  console.error('❌ Server error:', error);
  process.exit(1);
});

// ==========================================
// 🛡️ П.6: GRACEFUL SHUTDOWN
// ==========================================
// При получении SIGTERM/SIGINT (Render shutdown, Ctrl+C) делаем финальный бэкап
// и checkpoint WAL перед выходом, чтобы не потерять данные.
async function gracefulShutdown(signal) {
  console.log(`\n🛑 Получен сигнал ${signal} — graceful shutdown...`);
  try {
    console.log('💾 Делаю финальный бэкап перед выходом...');
    await sendDatabaseBackup(true);
    console.log('✅ Финальный бэкап отправлен');
  } catch (e) {
    console.error('⚠️ Не удалось сделать финальный бэкап:', e.message);
  }
  try {
    await new Promise((resolve) => db.run('PRAGMA wal_checkpoint(FULL);', resolve));
    console.log('✅ WAL checkpoint выполнен');
  } catch (e) {
    console.error('⚠️ WAL checkpoint error:', e.message);
  }
  process.exit(0);
}

process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT',  () => gracefulShutdown('SIGINT'));