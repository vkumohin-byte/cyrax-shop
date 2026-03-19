const TelegramBot = require('node-telegram-bot-api');
const token = '8076724921:AAEpm_AENCGFXKV0K7oj3DmJjWkYld9-G6c';

const bot = new TelegramBot(token, { polling: true });

bot.on('message', (msg) => {
    console.log('✅ Сообщение пришло!', msg.text);
    bot.sendMessage(msg.chat.id, '✅ Бот работает!');
});

console.log('🤖 Тестовый бот запущен');