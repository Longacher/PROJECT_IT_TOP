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
excel_path = 'output (1).csv'
output_excel_path = 'exported_usernames.csv'

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

@app.route('/parse', methods=['GET', 'POST'])
async def parse():
    if request.method == 'POST':
        form_data = await request.form
        selected_cities = form_data.getlist('cities')  # Получаем список выбранных городов
        poisk = form_data.get('poisk', '')

        if not selected_cities:
            return "Выберите хотя бы один город.", 400

        # Запуск парсинга в фоне
        loop = asyncio.get_event_loop()
        all_results = await loop.run_in_executor(None, lambda: run_parser(poisk, selected_cities))

        # Группируем результаты по городам
        grouped_results = {}
        for result in all_results:
            city = result["Город"]
            grouped_results.setdefault(city, []).append(result)

        # Сохраняем данные для каждого города и формируем ссылки
        download_links = []
        for city, data in grouped_results.items():
            filename = f"output_{city.replace(' ', '_')}.csv"
            file_path = os.path.join('downloads', filename)
            os.makedirs('downloads', exist_ok=True)
            write_to_excel(data, filename=file_path)
            download_links.append(f"/download/{filename}")  # Ссылка на скачивание

        # Возвращаем страницу с ссылками на скачивание
        return await render_template('parse.html', download_links=download_links)

    return await render_template('parse.html')



def write_to_excel(data, filename='output.csv'):
    wb = openpyxl.Workbook()
    sheet = wb.active
    sheet.title = "Companies"
    sheet.append(["Город", "Название", "Телефон", "Адрес", "VK", "Telegram", "Email"])

    for row in data:
        sheet.append([row["Город"], row["Название"], row["Телефонный номер"], row["Адрес"], row["VK"], row["Telegram"], row.get("Email", "Не указано")])

    wb.save(filename)


def run_parser(poisk, cities):
    all_results = []
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(process_city, city, poisk) for city in cities]

        for future in futures:
            all_results.extend(future.result())

    return all_results


def process_city(city, poisk):
    print(f"Начинаем поиск в городе: {city}")

    # Настройка браузера
    options = webdriver.FirefoxOptions()
    options.add_argument("--headless")  # Фоновый режим
    driver = webdriver.Firefox(options=options)

    results = []

    try:
        base_url = f'https://2gis.ru/{city}/search/{urllib.parse.quote(poisk)}'
        page = 1  # Номер текущей страницы
        
        while page <= 5:
            url = f"{base_url}/page/{page}"
            driver.get(url)
            print(f"Открыта страница {page} в городе {city}.")

            try:
                # Ожидаем загрузки карточек компаний
                WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.XPATH, '//div[contains(@class, "_1idnaau")]')))
                companies = driver.find_elements(By.XPATH, '//div[contains(@class, "_1idnaau")]')

                if not companies:
                    print(f"Компаний больше нет на странице {page}. Завершение обработки города {city}.")
                    break

                for company in companies:
                    try:
                        driver.execute_script("arguments[0].click();", company)
                        WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.CSS_SELECTOR, 'h1._cwjbox')))#название
                        time.sleep(2)
                        # Извлечение данных о компании
                        # Название компании
                        try:
                            title_element = driver.find_element(By.CSS_SELECTOR, 'h1._cwjbox')
                            title_text = title_element.text.strip()
                            print('Name company: ', title_text)
                        except Exception:
                            title_text = "Не найдено"
                            print('Name company: ', title_text)
                            print('Error:')
                            traceback.print_exc()

                        # Телефонный номер
                        try:
                            phone_button = WebDriverWait(driver, 10).until(
                                EC.element_to_be_clickable((By.XPATH, '//div[contains(@class, "_b0ke8")]/a'))
                            )
                            driver.execute_script("arguments[0].click();", phone_button)
                            time.sleep(2)

                            phone_element = WebDriverWait(driver, 10).until(
                                EC.presence_of_element_located((By.CSS_SELECTOR, 'a._2lcm958 bdo[dir="ltr"]'))
                            )
                            phone_number = phone_element.text.strip()
                        except Exception:
                            phone_number = "Не указан"

                        # Адрес
                        try:
                            address_element = driver.find_element(By.CLASS_NAME, '_er2xx9')
                            address_text = address_element.text
                            parts = address_text.split("показать вход")

                            # Берем первую часть
                            result_string = parts[0].strip()  # .strip() убирает пробелы в начале и конце
                            print('Adress company: ', address_text)
                            
                        except Exception:
                            address_text = "Не указан"
                            print('Adress company Error:')
                            traceback.print_exc()

                        #Извлечение ссылки VK
                        try:
                            vk_element = driver.find_element(By.CLASS_NAME, '_1cuu3ci')
                            driver.execute_script("arguments[0].click();", vk_element)
                            time.sleep(2)
                            driver.switch_to.window(driver.window_handles[1])
                            social_url = driver.current_url
                            WebDriverWait(driver, 20).until(lambda d: d.current_url != "about:blank")
                            print(f"Соцсеть URL: {social_url}")
                            driver.close()
                            driver.switch_to.window(driver.window_handles[0])
                        except Exception:
                            social_url = "Не указано"

                        # Email
                        try:
                            email_element = driver.find_element(By.CSS_SELECTOR, 'a[href^="mailto:"]')
                            email = email_element.text
                        except Exception:
                            email = "Не указано"

                        #Телеграмм
                        phone_clean = (phone_number)
                        cleaned_number = re.sub(r"[()\s‒-]", "", phone_clean)
                        print(cleaned_number)

                        
                        telegram_link = (f"https://t.me/{cleaned_number}")
                        print (telegram_link)

                        #Сайты
                        try:
                            site_element = driver.find_element(By.CLASS_NAME, '_1rehek')
                            site_text = site_element.text
                        except Exception:
                            site_text = "Не указан"
                        print(site_text)

                        # Сохраняем данные
                        results.append({
                            "Город": city,
                            "Название": title_text,
                            "Телефонный номер": phone_number,
                            "Адрес": address_text,
                            "VK": social_url,
                            "Telegram": telegram_link,
                            "Email": email
                        })

                        driver.back()
                        WebDriverWait(driver, 5).until(
                            EC.presence_of_all_elements_located((By.XPATH, '//div[contains(@class, "_1idnaau")]'))
                        )

                    except Exception as e:
                        print(f"Ошибка при обработке компании: {e}")

            except Exception as e:
                print(f"Ошибка на странице {page} в городе {city}: {e}")
                break

            # Увеличиваем номер страницы
            page += 1
            

    except Exception as e:
        print(f"Ошибка при обработке города {city}: {e}")

    finally:
        driver.quit()

    return results





@app.route('/download/<filename>')
async def download_file(filename):
    # Путь к папке с файлами
    file_directory = os.path.join(os.getcwd(), 'downloads')
    return await send_from_directory(file_directory, filename, as_attachment=True)



@app.route('/favicon.ico')
async def favicon():
    return '', 204  # Пустой ответ с кодом 204 (No Content)

@app.errorhandler(Exception)
async def handle_error(error):
    print(f"Error: {error}")
    return jsonify({"error": str(error)}), 500

if __name__ == "__main__":
    app.run(debug=True)