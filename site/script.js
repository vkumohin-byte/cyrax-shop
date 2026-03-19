// Замените этот URL на ваш актуальный URL бэкенда (например, от Render)
const API_BASE = window.location.origin.includes('localhost') 
  ? 'http://localhost:5500/api' 
  : 'https://shadows-apps-sites-while.trycloudflare.com/api';

const PERIOD_NAMES = {
  '1d': '1 день',
  '3d': '3 дня',
  '7d': '7 дней',
  '30d': '30 дней',
  'infinite_boost': 'Метод Бесконечного Буста',
  'reseller_connection': 'Партнёрская программа'
};

const METHOD_NAMES = {
  'sbp': 'СБП',
  'card_ua': 'Карта UA',
  'card_it': 'Карта IT',
  'paypal': 'PayPal',
  'binance': 'Binance P2P',
  'cryptobot': 'CryptoBot'
};

// Utilities
function formatPrice(amount, currency) {
  if (currency === 'RUB') return amount + ' ₽';
  if (currency === 'UAH') return amount + ' ₴';
  if (currency === 'USD') return '$' + amount;
  if (currency === 'EUR') return '€' + amount;
  return amount + ' ' + currency;
}

// Shop Page Logic
if (window.location.pathname.endsWith('shop.html')) {
  document.addEventListener('DOMContentLoaded', loadPrices);
}

async function loadPrices() {
  try {
    const res = await fetch(`${API_BASE}/prices`);
    const prices = await res.json();
    
    const keysGrid = document.getElementById('keys-grid');
    const servicesGrid = document.getElementById('services-grid');
    
    keysGrid.innerHTML = '';
    servicesGrid.innerHTML = '';
    
    for (const [key, currencyPrices] of Object.entries(prices)) {
      const isKey = key.endsWith('d');
      const name = PERIOD_NAMES[key] || key;
      
      const card = document.createElement('div');
      card.className = 'product-card glass-card';
      
      const displayPrice = currencyPrices['RUB'] ? formatPrice(currencyPrices['RUB'], 'RUB') : formatPrice(currencyPrices['USD'] || 0, 'USD');
      
      card.innerHTML = `
        <div class="product-title">${name}</div>
        <div class="product-price">${displayPrice} <span style="font-size: 1rem; color: var(--text-secondary);">(RUB)</span></div>
        <button class="btn" onclick="buyProduct('${key}')">Купить</button>
      `;
      
      if (isKey) {
        keysGrid.appendChild(card);
      } else {
        servicesGrid.appendChild(card);
      }
    }
  } catch (e) {
    console.error('Failed to load prices', e);
    document.getElementById('keys-grid').innerHTML = '<p style="color:red">Ошибка загрузки цен</p>';
  }
}

window.buyProduct = function(productKey) {
  sessionStorage.setItem('selectedProduct', productKey);
  window.location.href = 'checkout.html';
};

// Checkout Page Logic
if (window.location.pathname.endsWith('checkout.html')) {
  document.addEventListener('DOMContentLoaded', setupCheckout);
}

let checkoutState = {
  product: null,
  currency: null,
  method: null,
  telegramId: null
};

async function setupCheckout() {
  const product = sessionStorage.getItem('selectedProduct');
  if (!product) {
    window.location.href = 'shop.html';
    return;
  }
  
  checkoutState.product = product;
  document.getElementById('selected-product-info').innerText = `Товар: ${PERIOD_NAMES[product] || product}`;
  
  // Setup Currency Selection
  document.querySelectorAll('.currency-selector .selector-btn').forEach(btn => {
    btn.addEventListener('click', (e) => selectCurrency(e.target.dataset.currency));
  });
  
  document.getElementById('pay-btn').addEventListener('click', createOrder);
}

async function selectCurrency(currency) {
  checkoutState.currency = currency;
  checkoutState.method = null;
  
  // UI update
  document.querySelectorAll('.currency-selector .selector-btn').forEach(b => b.classList.remove('active'));
  document.querySelector(`.currency-selector .selector-btn[data-currency="${currency}"]`).classList.add('active');
  
  document.getElementById('details-group').classList.add('hidden');
  document.getElementById('submit-group').classList.add('hidden');
  
  // Show methods
  const methodGroup = document.getElementById('method-group');
  const methodSelector = document.getElementById('method-selector');
  methodSelector.innerHTML = '<div class="loader" style="display:inline-block"></div> Загрузка...';
  methodGroup.classList.remove('hidden');
  
  try {
    const res = await fetch(`${API_BASE}/payment-methods?currency=${currency}`);
    const methods = await res.json();
    
    methodSelector.innerHTML = '';
    methods.forEach(method => {
      const btn = document.createElement('div');
      btn.className = 'selector-btn';
      btn.innerText = METHOD_NAMES[method] || method;
      btn.dataset.method = method;
      btn.addEventListener('click', () => selectMethod(method, btn));
      methodSelector.appendChild(btn);
    });
  } catch (e) {
    methodSelector.innerHTML = '<p style="color:red">Ошибка загрузки методов</p>';
  }
}

async function selectMethod(method, btnElement) {
  checkoutState.method = method;
  
  // UI update
  document.querySelectorAll('.method-selector .selector-btn').forEach(b => b.classList.remove('active'));
  btnElement.classList.add('active');
  
  const detailsGroup = document.getElementById('details-group');
  const paymentDetails = document.getElementById('payment-details');
  paymentDetails.innerHTML = '<div class="loader" style="display:inline-block"></div> Загрузка...';
  detailsGroup.classList.remove('hidden');
  document.getElementById('submit-group').classList.add('hidden');
  
  try {
    const res = await fetch(`${API_BASE}/payment-details/${method}`);
    const data = await res.json();
    
    let instructions = '';
    if (method === 'cryptobot') {
      instructions = '<p>🤖 <b>Оплата через CryptoBot</b></p><p>Инвойс будет создан и отправлен вам в Telegram после нажатия "Я оплатил".</p>';
    } else {
      instructions = `
        <p>Реквизиты для оплаты (${METHOD_NAMES[method]}):</p>
        <pre>${data.details}</pre>
        <p>⚠️ Переведите точную сумму и <b>обязательно сохраните чек</b>. После оплаты нажмите кнопку ниже.</p>
      `;
    }
    
    paymentDetails.innerHTML = instructions;
    document.getElementById('submit-group').classList.remove('hidden');
  } catch (e) {
    paymentDetails.innerHTML = '<p style="color:red">Ошибка загрузки реквизитов</p>';
  }
}

async function createOrder() {
  const telegramIdInput = document.getElementById('telegram-id').value;
  const errorMsg = document.getElementById('error-message');
  
  if (!telegramIdInput || telegramIdInput.length < 5) {
    errorMsg.innerText = 'Пожалуйста, введите корректный Telegram ID';
    errorMsg.classList.remove('hidden');
    return;
  }
  
  checkoutState.telegramId = telegramIdInput;
  
  const payBtn = document.getElementById('pay-btn');
  payBtn.classList.add('loading');
  payBtn.disabled = true;
  errorMsg.classList.add('hidden');
  
  try {
    const res = await fetch(`${API_BASE}/site/create-order`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        telegram_id: checkoutState.telegramId,
        product: checkoutState.product,
        currency: checkoutState.currency,
        method: checkoutState.method
      })
    });
    
    const data = await res.json();
    
    if (res.ok && data.success) {
      window.location.href = `success.html?order=${data.orderId}`;
    } else {
      errorMsg.innerText = data.error || 'Ошибка при создании заказа';
      errorMsg.classList.remove('hidden');
    }
  } catch (e) {
    errorMsg.innerText = 'Сетевая ошибка';
    errorMsg.classList.remove('hidden');
  } finally {
    payBtn.classList.remove('loading');
    payBtn.disabled = false;
  }
}
