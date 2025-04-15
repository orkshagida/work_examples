import logging
from telethon import TelegramClient, events
import re
import asyncio
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import pytz
from datetime import datetime, timedelta

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", 
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Глобальные переменные
signal_count = 0
signal_direction = None
last_signal_time = None
scheduler = None
client = None  # Единый клиент для всего приложения

# Функции калькулятора
def calculate_levels(direction, entry_price, stop_size=50):
    if stop_size == 50:
        tp_percent = 0.0015
        sl_percent = 0.0005
        alarm_percent = 0.00075
    elif stop_size == 75:
        tp_percent = 0.00225
        sl_percent = 0.00075
        alarm_percent = 0.00075
    elif stop_size == 100:
        tp_percent = 0.003
        sl_percent = 0.001
        alarm_percent = 0.00075
    else:
        raise ValueError("Некорректный размер стопа")

    if direction.lower() == "лонг":
        tp = entry_price * (1 + tp_percent)
        sl = entry_price * (1 - sl_percent)
        alarm = entry_price * (1 + alarm_percent)
    else:
        tp = entry_price * (1 - tp_percent)
        sl = entry_price * (1 + sl_percent)
        alarm = entry_price * (1 - alarm_percent)
    return tp, sl, alarm

async def send_bu_message(chat_id):
    await client.send_message(chat_id, "БУ по времени!")

async def process_signal(chat_id, direction):
    global signal_count, signal_direction, last_signal_time
    current_time = asyncio.get_event_loop().time()

    if signal_direction is None:
        signal_direction = direction
    
    if direction != signal_direction:
        signal_count = 0
        signal_direction = direction

    if last_signal_time is None or current_time - last_signal_time <= 900:
        signal_count += 1
        last_signal_time = current_time

        if signal_count >= 8:
            signal_count = 0
            signal_direction = None
            last_signal_time = None
            await client.send_message(chat_id, f"ВАЛИДНЫЙ {direction.upper()}ОВЫЙ СИГНАЛ!")
    else:
        signal_count = 1
        signal_direction = direction
        last_signal_time = current_time

async def calculate(chat_id, message):
    try:
        match = re.match(r"(Лонг|Шорт)\s+(\d+(\.\d+)?)\s*(50|75|100)?", message)
        if match:
            direction = match.group(1)
            price = float(match.group(2))
            stop_size = int(match.group(4) or 50)  # По умолчанию 50
            
            tp, sl, alarm = calculate_levels(direction, price, stop_size)
            response = (
                f"Направление: {direction}\n"
                f"Цена входа: {price}\n"
                f"Размер стопа: {stop_size} пипсов\n"
                f"TP: {tp:.5f}\n"
                f"SL: {sl:.5f}\n"
                f"Будильник: {alarm:.5f}"
            )
            await client.send_message(chat_id, response)

            scheduler.add_job(
                send_bu_message,
                'date',
                run_date=datetime.now(pytz.timezone('Europe/Moscow')) + timedelta(hours=2),
                args=(chat_id,)
            )
            await client.send_message(chat_id, "Будильник установлен!")
        else:
            await client.send_message(chat_id, "Некорректный формат сообщения. Используйте: Лонг/Шорт <цена> [50/75/100]")
    except Exception as e:
        logger.error(f"Ошибка в calculate: {e}")
        await client.send_message(chat_id, f"Ошибка: {e}")

async def start_scheduler():
    global scheduler
    scheduler = AsyncIOScheduler(timezone=pytz.timezone('Europe/Moscow'))
    scheduler.start()

async def main():
    global client
    api_id = 21474335
    api_hash = 'bf6e91368c78cd5c726a2232be6f6ad4'
    
    # Единый клиент для всего приложения
    client = TelegramClient('unique_session_name', api_id, api_hash)

    await start_scheduler()

    @client.on(events.NewMessage)
    async def handler(event):
        chat_id = event.chat_id
        message = event.message.message
        
        if "Шортовая змея полезла!" in message:
            await process_signal(chat_id, "Шорт")
        elif "Лонговая змея полезла!" in message:
            await process_signal(chat_id, "Лонг")
        elif re.match(r"(Лонг|Шорт)\s+(\d+(\.\d+)?)\s*(50|75|100)?", message):
            await calculate(chat_id, message)

    await client.start()
    await client.run_until_disconnected()

async def shutdown():
    if scheduler:
        scheduler.shutdown()
    if client and client.is_connected():
        await client.disconnect()

if __name__ == '__main__':
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        logger.info("Получен сигнал прерывания")
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
    finally:
        loop.run_until_complete(shutdown())
        loop.close()
