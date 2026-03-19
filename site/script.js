// --- Константы и Настройки ---
const RENDER_URL = 'https://cyrax-bot-0vwr.onrender.com';
const API_BASE = window.location.hostname === 'localhost' ? 'http://localhost:3000/api' : '/api';

async function apiFetch(endpoint, options = {}) {
    const url = endpoint.startsWith('http') ? endpoint : `${API_BASE}${endpoint}`;
    try {
        const response = await fetch(url, options);
        if (response.status === 404 && !endpoint.startsWith('http')) {
            // Fallback to Render directly if proxy fails
            return fetch(`${RENDER_URL}${endpoint}`, options).then(r => r.json());
        }
        return response.json();
    } catch (e) {
        if (!endpoint.startsWith('http')) {
            return fetch(`${RENDER_URL}${endpoint}`, options).then(r => r.json());
        }
        throw e;
    }
}

const METHOD_NAMES = {
  'cryptobot': 'CryptoBot ₿',
  'sbp': 'СБП — Россия 🇷🇺',
  'card_ua': 'Карта UA 🇺🇦',
  'card_it': 'Карта IT 🇮🇹',
  'binance': 'Binance P2P 💎',
  'paypal': 'PayPal 💰',
  'crypto': 'CryptoBot ₿'
};

const PERIOD_NAMES = {
  '1d': 'Ключ на 1 день 🔑',
  '3d': 'Ключ на 3 дня 🔑',
  '7d': 'Ключ на 7 дней 🔑',
  '30d': 'Ключ на 30 дней 🔑',
  'infinite_boost': 'Буст Метод 🚀',
  'reseller_connection': 'Партнёрство 🤝'
};

// --- Глобальная защита и статус системы ---
async function checkSystemStatus() {
  try {
    const res = await apiFetch('/status');
    const status = await res.json();
    
    if (status.maintenanceMode && !window.isAdmin) {
      showMaintenance(status.maintenanceReason);
    }
    return status;
  } catch (e) { 
    console.error('Ошибка проверки статуса', e);
    return { maintenanceMode: false };
  }
}

function guardAuth() {
    const user = JSON.parse(sessionStorage.getItem('tgUser'));
    const isLanding = window.location.pathname.endsWith('index.html') || window.location.pathname === '/' || window.location.pathname === '';
    
    // Авторизация больше не обязательна для просмотра магазина
    // Но для админ-панели (если будем делать) можно оставить
  
  if (user) {
    window.authorizedTgId = user.id;
    // Админ-проверка (ID админа из промпта)
    window.isAdmin = (user.id === 5187702657); 
  }
  return user;
}

function showMaintenance(reason) {
  document.body.innerHTML = `
    <div style="height:100vh; display:flex; align-items:center; justify-content:center; text-align:center; padding:20px; background:#0A0C10; color:white;">
      <div class="glass-card" style="max-width:500px; border-radius:24px;">
        <h1 style="color:var(--accent-purple); margin-bottom:20px;">🚧 ТЕХОБСЛУЖИВАНИЕ</h1>
        <p style="font-size:1.2rem; margin-bottom:30px; color:var(--text-secondary);">${reason}</p>
        <div style="font-size:3rem; filter: drop-shadow(0 0 10px var(--accent-purple));">⏳</div>
      </div>
    </div>
  `;
}

// --- Утилиты ---
function formatPrice(amount, currency) {
  if (currency === 'RUB') return amount + ' ₽';
  if (currency === 'UAH') return amount + ' ₴';
  if (currency === 'USD') return '$' + amount;
  if (currency === 'EUR') return '€' + amount;
  return amount + ' ' + currency;
}

// --- Логика магазина (shop.html) ---
async function loadPrices() {
  try {
    const res = await apiFetch('/prices');
    const prices = await res.json();
    
    const keysGrid = document.getElementById('keys-grid');
    const servicesGrid = document.getElementById('services-grid');
    if (!keysGrid || !servicesGrid) return;

    keysGrid.innerHTML = '';
    servicesGrid.innerHTML = '';
    
    const sortedKeys = Object.keys(prices).sort((a, b) => {
      const order = ['1d', '3d', '7d', '30d'];
      const aIndex = order.indexOf(a);
      const bIndex = order.indexOf(b);
      if (aIndex === -1 && bIndex === -1) return a.localeCompare(b);
      if (aIndex === -1) return 1;
      if (bIndex === -1) return -1;
      return aIndex - bIndex;
    });
    
    for (const key of sortedKeys) {
      const currencyPrices = prices[key];
      const name = PERIOD_NAMES[key] || key;
      const isKey = key.endsWith('d');
      
      const card = document.createElement('div');
      card.className = 'product-card glass-card';
      const displayPrice = currencyPrices['RUB'] ? formatPrice(currencyPrices['RUB'], 'RUB') : formatPrice(currencyPrices['USD'] || 0, 'USD');
      const icon = isKey ? '🔑' : '⚡';
      
      card.innerHTML = `
        <div class="product-title">${icon} ${name}</div>
        <div class="product-price">${displayPrice} <span style="font-size: 0.8rem; color: var(--text-secondary); opacity: 0.7;">(RUB)</span></div>
        <button class="btn" style="width: 100%; border-radius: 10px;" onclick="buyProduct('${key}')">Купить</button>
      `;
      if (isKey) keysGrid.appendChild(card);
      else servicesGrid.appendChild(card);
    }
  } catch (e) {
    console.error('Загрузка цен провалена', e);
  }
}

// --- Обработка входа через бота (Plan B) ---
window.buyProduct = function(productKey) {
  sessionStorage.setItem('selectedProduct', productKey);
  window.location.href = 'checkout.html';
};

// --- Логика оформления (checkout.html) ---
let checkoutState = { product: null, currency: null, method: null };

async function setupCheckout() {
  const urlParams = new URLSearchParams(window.location.search);
  const isTopup = urlParams.get('type') === 'topup';
  let product = sessionStorage.getItem('selectedProduct');

  if (isTopup) {
      product = 'topup_balance';
      const amount = sessionStorage.getItem('topupAmount');
      document.getElementById('selected-product-info').innerText = `💳 Пополнение баланса: ${amount} RUB`;
      checkoutState.product = 'topup_balance';
      checkoutState.currency = 'RUB';
      const rubBtn = document.querySelector('[data-currency="RUB"]');
      if (rubBtn) { rubBtn.click(); rubBtn.style.pointerEvents = 'none'; }
  } else {
      if (!product) { window.location.href = 'shop.html'; return; }
      document.getElementById('selected-product-info').innerText = `🛍️ Товар: ${PERIOD_NAMES[product] || product}`;
      checkoutState.product = product;
      // Auto-select RUB by default
      setTimeout(() => {
        const rubBtn = document.querySelector('[data-currency="RUB"]');
        if (rubBtn) rubBtn.click();
      }, 100);
  }

  document.querySelectorAll('.currency-selector .selector-btn').forEach(btn => {
    btn.addEventListener('click', (e) => selectCurrency(e.target.dataset.currency));
  });
  document.getElementById('pay-btn').addEventListener('click', createOrder);
}

async function selectCurrency(currency) {
  checkoutState.currency = currency;
  checkoutState.method = null;
  document.querySelectorAll('.currency-selector .selector-btn').forEach(b => b.classList.remove('active'));
  const target = document.querySelector(`.currency-selector .selector-btn[data-currency="${currency}"]`);
  if (target) target.classList.add('active');
  
  const methodSelector = document.getElementById('method-selector');
  document.getElementById('method-group').classList.remove('hidden');
  methodSelector.innerHTML = '<div class="loader"></div>';
  
  try {
    const res = await apiFetch(`/payment-methods?currency=${currency}`);
    const methods = await res.json();
    methodSelector.innerHTML = '';
    methods.forEach(method => {
      const btn = document.createElement('div');
      btn.className = 'selector-btn';
      btn.innerText = METHOD_NAMES[method] || method;
      btn.onclick = () => selectMethod(method, btn);
      methodSelector.appendChild(btn);
    });
  } catch (e) { methodSelector.innerHTML = 'Ошибка загрузки'; }
}

async function selectMethod(method, btn) {
  checkoutState.method = method;
  document.querySelectorAll('.method-selector .selector-btn').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  
  const paymentDetails = document.getElementById('payment-details');
  document.getElementById('details-group').classList.remove('hidden');
  paymentDetails.innerHTML = '<div class="loader"></div>';

  try {
    const res = await apiFetch(`/payment-details/${method}`);
    const data = await res.json();
    let inst = '';
    if (method === 'cryptobot' || method === 'crypto') {
      inst = '<p>🤖 Инвойс будет отправлен вам в Telegram после нажатия ниже.</p>';
    } else {
      const urlRegex = /(https?:\/\/[^\s]+)/g;
      const htmlDetails = data.details.replace(urlRegex, '<a href="$1" target="_blank" class="btn" style="display:block; margin: 15px 0; text-align:center;">🔗 Открыть страницу оплаты</a>');
      inst = `<div style="background: rgba(0,0,0,0.3); padding: 20px; border-radius:12px; margin: 10px 0;">${htmlDetails}</div>`;
    }
    paymentDetails.innerHTML = inst;
    document.getElementById('submit-group').classList.remove('hidden');
  } catch (e) { paymentDetails.innerHTML = 'Ошибка'; }
}

async function createOrder() {    
    // Вся аутентификация убрана, работаем только с введенным никнеймом
    const guestInput = document.getElementById('guest-username');
    const username = guestInput ? guestInput.value.trim() : "";
    
    if (!username) {
        alert("Пожалуйста, введите ваш Telegram для связи и получения ключа.");
        if (guestInput) guestInput.focus();
        return;
    }
   
  if (!checkoutState.product || !checkoutState.currency || !checkoutState.method) {
    alert("Пожалуйста, выберите валюту и метод оплаты.");
    return;
  }

  const isTopup = checkoutState.product === 'topup_balance';
  const endpoint = isTopup ? '/site/topup-request' : '/site/create-order';
  // IMPORTANT: send telegram_id as string "guest" for guests to avoid JS falsy check on 0
  const body = isTopup 
    ? { telegram_id: 'guest', amount: sessionStorage.getItem('topupAmount'), method: checkoutState.method, username: username }
    : { telegram_id: 'guest', product: checkoutState.product, currency: checkoutState.currency, method: checkoutState.method, username: username };

  const payBtn = document.getElementById('pay-btn');
  payBtn.disabled = true;
  payBtn.innerText = '⌛ Создание...';

  try {
    const res = await apiFetch(endpoint, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body)
    });
    const data = await res.json();
    if (data.success) {
      window.location.href = `success.html?order=${data.orderId}${isTopup ? '&type=topup' : ''}`;
    } else alert(data.error || 'Ошибка');
  } catch (e) { alert('Сервер недоступен'); }
  finally { payBtn.disabled = false; payBtn.innerText = 'Я оплатил'; }
}

// --- Виджет поддержки ---
function initSupportWidget() {
  const user = JSON.parse(sessionStorage.getItem('tgUser'));
  if (!user || document.getElementById('support-bubble')) return;

  const html = `
    <div class="support-bubble" onclick="toggleSupport()">💬</div>
    <div class="support-modal glass-card" id="support-modal">
      <div class="support-header"><span>👨‍💻 ТЕХПОДДЕРЖКА</span><span onclick="toggleSupport()" style="cursor:pointer">✖</span></div>
      <div class="support-body">
        <textarea class="support-input" id="support-text" placeholder="Опишите ваш вопрос..."></textarea>
        <button class="btn" style="width:100%" onclick="sendSupportMessage()">ОТПРАВИТЬ</button>
      </div>
    </div>`;
  document.body.insertAdjacentHTML('beforeend', html);
}

window.toggleSupport = () => {
  const m = document.getElementById('support-modal');
  if (m) m.style.display = m.style.display === 'flex' ? 'none' : 'flex';
};

window.sendSupportMessage = async () => {
  const text = document.getElementById('support-text').value;
  const user = JSON.parse(sessionStorage.getItem('tgUser'));
  if (!text || text.length < 5) return alert('Сообщение слишком короткое');
  
  try {
    const res = await apiFetch('/site/support-message', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ telegram_id: user.id, username: user.username || user.first_name, message: text })
    });
    if (res.ok) { 
      alert('Ваше сообщение отправлено администратору!'); 
      document.getElementById('support-text').value = '';
      toggleSupport(); 
    }
  } catch (e) { alert('Ошибка отправки'); }
};

// --- Админ-панель (упрощенная) ---
async function checkAdminStatus() {
  const user = JSON.parse(sessionStorage.getItem('tgUser'));
  if (!user || !window.isAdmin || document.getElementById('admin-dash-btn')) return;
  const header = document.querySelector('header');
  if (header) {
    const btn = document.createElement('button');
    btn.id = 'admin-dash-btn'; btn.className = 'admin-dash-btn'; btn.innerText = '⚙️ АДМИН';
    btn.onclick = openAdminPanel;
    header.appendChild(btn);
  }
}

async function openAdminPanel() {
  const html = `
    <div class="modal-overlay" onclick="closeAdminPanel()"></div>
    <div id="admin-panel-modal" class="glass-card" style="display:flex; flex-direction:column; position:fixed; top:50%; left:50%; transform:translate(-50%,-50%); width:90%; max-width:800px; height:80vh; z-index:2000;">
      <div class="support-header" style="background:var(--accent-purple);"><span>🔐 ЗАКАЗЫ САЙТА</span><span onclick="closeAdminPanel()" style="cursor:pointer">✖</span></div>
      <div style="padding:20px; overflow-y:auto; flex:1;">
        <table class="admin-table" style="width:100%; border-collapse:collapse;">
          <thead style="text-align:left; color:var(--text-secondary);"><tr><th>ID</th><th>Клиент</th><th>Товар</th><th>Сумма</th><th>Действие</th></tr></thead>
          <tbody id="admin-orders-body"></tbody></table></div>
    </div>`;
  document.body.insertAdjacentHTML('beforeend', html);
  loadAdminOrders();
}

window.closeAdminPanel = () => {
  document.querySelector('.modal-overlay')?.remove();
  document.getElementById('admin-panel-modal')?.remove();
};

async function loadAdminOrders() {
  const u = JSON.parse(sessionStorage.getItem('tgUser'));
  try {
    const res = await apiFetch(`/admin/orders?admin_id=${u.id}`);
    const orders = await res.json();
    document.getElementById('admin-orders-body').innerHTML = orders.map(o => `
      <tr style="border-bottom:1px solid var(--glass-border); height:50px;">
        <td>#${o.id}</td><td>@${o.username || o.user_id}</td><td>${o.product}</td><td>${o.amount} ${o.currency}</td>
        <td><button class="btn" style="padding:5px 10px; font-size:0.8rem;" onclick="approveOrder(${o.id})">ОДОБРИТЬ</button></td>
      </tr>`).join('');
  } catch (e) { }
}

window.approveOrder = async (id) => {
  const u = JSON.parse(sessionStorage.getItem('tgUser'));
  if (!confirm(`Одобрить заказ #${id}?`)) return;
  try {
    const res = await apiFetch('/admin/approve-order', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ admin_id: u.id, order_id: id })
    });
    if (res.ok) { alert('Заказ успешно одобрен!'); loadAdminOrders(); }
  } catch (e) { }
};

// --- Баланс Пользователя ---
async function updateOrderUI() {
    const user = JSON.parse(sessionStorage.getItem('tgUser'));
    const headerProfile = document.getElementById('user-profile-header');
    const headerName = document.getElementById('header-user-name');
    const headerBalance = document.getElementById('header-balance');

    if (user) {
        if (headerProfile) headerProfile.classList.remove('hidden');
        if (headerName) headerName.innerText = user.username ? `@${user.username}` : user.first_name;
        if (headerBalance) headerBalance.innerText = `${user.balance || 0} ₽`;
        
        // На странице оформления скрываем выбор авторизации
        const authOptions = document.getElementById('auth-options');
        const authSuccess = document.getElementById('auth-success');
        const authIdDisplay = document.getElementById('auth-id-display');
        if (authOptions) authOptions.classList.add('hidden');
        if (authSuccess) authSuccess.classList.remove('hidden');
        if (authIdDisplay) authIdDisplay.innerText = user.username ? `@${user.username}` : user.first_name;
    } else {
        // Гостевой режим
        if (headerProfile) {
            headerProfile.classList.remove('hidden');
            headerName.innerHTML = `<a href="index.html" style="color:var(--accent-teal); text-decoration:none; font-size:1rem;">📥 Войти</a>`;
            headerBalance.innerText = "Guest";
        }
    }
}
async function loadUserBalance() {
  const user = JSON.parse(sessionStorage.getItem('tgUser'));
  if (!user) {
    updateOrderUI(); // Update UI for guest if no user
    return;
  }
  try {
    const res = await apiFetch(`/site/user-profile?telegram_id=${user.id}`);
    const data = await res.json();
    
    // Обновляем шапку (если есть)
    const headerProfile = document.getElementById('user-profile-header');
    if (headerProfile) {
        headerProfile.classList.remove('hidden');
        document.getElementById('header-user-name').innerText = `@${data.username || user.username || user.first_name}`;
        document.getElementById('header-balance').innerText = `${data.balance} ₽`;
    }

    // Обновляем профиль на странице оплаты
    const prof = document.getElementById('user-profile-section');
    if (prof) {
        prof.classList.remove('hidden');
        document.getElementById('user-balance-display').innerText = `${data.balance} ₽`;
        document.getElementById('auth-id-display').innerText = `@${data.username || user.username || user.first_name}`;
    }
  } catch (e) { }
}

// --- Инициализация ---
document.addEventListener('DOMContentLoaded', async () => {
    const user = guardAuth();
    await checkSystemStatus();

    if (user) {
        initSupportWidget();
        checkAdminStatus();
        loadUserBalance();
    }

    if (window.location.pathname.includes('checkout.html')) {
        setupCheckout();
        if (!user) updateOrderUI();
    }
    if (window.location.pathname.endsWith('shop.html')) loadPrices();
});
// Rebuild trigger: 2026-03-19 09:25
