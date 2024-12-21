import pandas as pd
import re
import asyncio
from telethon.sync import TelegramClient
from telethon.tl.functions.contacts import ImportContactsRequest
from telethon.tl.types import InputPhoneContact
from quart import Quart, render_template, request, redirect, url_for, session, jsonify, send_from_directory
import os
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from concurrent.futures import ThreadPoolExecutor
import openpyxl
import urllib.parse
import traceback
app = Quart(__name__)

# Устанавливаем secret_key для работы с сессиями
app.secret_key = 'your_unique_secret_key_here'

# Для хранения клиента Telegram
client = None

# Путь к файлам
excel_path = 'output (1).xlsx'
output_excel_path = 'exported_usernames.xlsx'

# Функция для очистки номера телефона
def clean_phone_number(phone):
    return re.sub(r'[^\d+]', '', phone)

# Функция для авторизации в Telegram
async def authorize_telegram(phone_number, api_id, api_hash):
    global client
    session_name = api_hash[:10]  # Используем первые 10 символов api_hash как имя сессии
    session_path = f"{session_name}.session"

    # Если сессия существует, корректно завершаем старую и удаляем файл
    if os.path.exists(session_path):
        try:
            if client:
                await client.disconnect()  # Завершаем предыдущую сессию
            os.remove(session_path)
            print(f"Старая сессия {session_name} успешно удалена.")
            time.sleep(5)
        except Exception as e:
            print(f"Ошибка при удалении старой сессии: {e}")
            return False

    # Создаем новый клиент
    client = TelegramClient(session_name, api_id, api_hash, system_version='4.16.30-vxCUSTOM')

    # Подключение клиента
    try:
        if not client.is_connected():
            await client.connect()

        # Проверка авторизации
        if not await client.is_user_authorized():
            tries = 3  # Количество попыток отправки кода
            for _ in range(tries):
                try:
                    result = await client.send_code_request(phone_number)
                    session['phone_code_hash'] = result.phone_code_hash
                    return True
                except Exception as e:
                    print(f"Ошибка при отправке кода: {e}")
                    break
    except Exception as e:
        print(f"Ошибка при подключении клиента: {e}")
        return False

    return False

# Асинхронная отправка сообщений
async def send_messages(client, user_data, message_text):
    for user in user_data:
        username = user["Username"]
        if username != "Нет username":
            try:
                await client.send_message(username, message_text)
                print(f"Сообщение отправлено пользователю {username}")
            except Exception as e:
                print(f"Не удалось отправить сообщение пользователю {username}: {e}")
        else:
            print("Пропуск пользователя без username")

@app.route('/')
async def index():
    return await render_template('index.html')

@app.route('/enter_code', methods=['GET', 'POST'])
async def enter_code():
    if request.method == 'POST':
        form_data = await request.form
        code = form_data.get('code')
        phone_code_hash = session.get('phone_code_hash')

        if not code or not phone_code_hash:
            return "Ошибка: Код подтверждения или hash не был введен.", 400

        api_id = session.get('api_id')
        api_hash = session.get('api_hash')
        phone_number = session.get('phone_number')

        try:
            global client
            if client is None:
                client = TelegramClient(api_hash[:10], api_id, api_hash, system_version='4.16.30-vxCUSTOM')
                await client.connect()

            await client.sign_in(phone_number, code, phone_code_hash=phone_code_hash)

            if await client.is_user_authorized():
                session['user_authorized'] = True
                return redirect('/profile')

            return "Ошибка: не удалось авторизовать пользователя.", 400

        except Exception as e:
            return f"Ошибка при авторизации: {e}", 400

    return await render_template('enter_code.html')

@app.route('/profile', methods=['GET', 'POST'])
async def profile():
    global client
    if session.get('user_authorized') and client and await client.is_user_authorized():
        user = await client.get_me()

        if request.method == 'POST':
            # Получаем файл из формы
            files = await request.files
            if 'file' in files:
                file = files['file']
                # Сохраняем файл
                file_path = os.path.join('uploads', file.filename)
                os.makedirs('uploads', exist_ok=True)
                await file.save(file_path)

                # Чтение Excel файла
                df = pd.read_excel(file_path)

                # Проверка наличия нужных колонок
                if 'Телефон' not in df.columns:
                    return "В файле отсутствует колонка 'Телефон'.", 400

                df = df.dropna(subset=['Телефон'])

                contacts_to_add = []
                user_data = []

                # Создание списка контактов из Excel
                for index, row in df.iterrows():
                    phone = clean_phone_number(str(row['Телефон']))
                    first_name = row['Название']
                    last_name = row.get('Город', '')

                    if phone.startswith('+') and len(phone) > 10:
                        contact = InputPhoneContact(client_id=index, phone=phone, first_name=first_name, last_name=last_name)
                        contacts_to_add.append(contact)
                    else:
                        print(f"Некорректный номер телефона: {phone}")

                # Добавление контактов в Telegram
                if contacts_to_add:
                    result = await client(ImportContactsRequest(contacts_to_add))
                    print(f"Добавлено контактов: {len(result.users)}")
                    for user in result.users:
                        username = user.username or "Нет username"
                        user_data.append({
                            "Телефон": user.phone,
                            "Имя": user.first_name,
                            "Фамилия": user.last_name or "",
                            "Username": username
                        })

                    # Сохранение информации о пользователях в Excel
                    export_df = pd.DataFrame(user_data)
                    export_df.to_excel(output_excel_path, index=False)
                    print(f"Данные успешно сохранены в {output_excel_path}")

                # Получение текста сообщения из формы
                form = await request.form  # Получаем данные формы асинхронно
                message_text = form.get('message')
                if message_text:
                    # Отправка сообщений
                    await send_messages(client, user_data, message_text)
                    return "Рассылка завершена."

        return await render_template('profile.html', 
                                     username=user.username, 
                                     user_id=user.id, 
                                     phone_number=user.phone)
    return redirect('/')

@app.route('/login', methods=['POST'])
async def login():
    form_data = await request.form
    api_id = form_data['api_id']
    api_hash = form_data['api_hash']
    phone_number = form_data['phone_number']

    try:
        api_id = int(api_id)
    except ValueError:
        return "API ID должен быть числом.", 400

    session['api_id'] = api_id
    session['api_hash'] = api_hash
    session['phone_number'] = phone_number

    if await authorize_telegram(phone_number, api_id, api_hash):
        return redirect('/enter_code')

    return "Ошибка авторизации", 400