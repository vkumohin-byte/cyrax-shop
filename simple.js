const TelegramBot = require('node-telegram-bot-api');
const token = '8076724921:AAGPYwcIhN6crrBivuJBwvNoM7kIMjwXuj0';

console.log('🚀 Запускаю супер-простой тест...');

const bot = new TelegramBot(token, { 
    polling: true,
    polling_options: {
        timeout: 30,
        limit: 100
    }
});

bot.on('message', (msg) => {
    console.log('🔥🔥🔥 СООБЩЕНИЕ ПРИШЛО!', msg.text);
    console.log('От:', msg.from.id);
    console.log('Чат:', msg.chat.id);
    
    bot.sendMessage(msg.chat.id, '✅ БОТ РАБОТАЕТ!')
        .then(() => console.log('✅ Ответ отправлен'))
        .catch(e => console.log('❌ Ошибка отправки:', e.message));
});

bot.on('polling_error', (error) => {
    console.log('❌❌❌ POLLING ERROR:', error.message);
});

console.log('🤖 Тест запущен, жду сообщения...');