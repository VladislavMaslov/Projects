import os
from flask import Flask, request, render_template_string
import psycopg2


# Создаем экземпляр приложения Flask
app = Flask(__name__)

# URL подключения к базе данных
DATABASE_URL = os.environ.get('DATABASE_URL')

# Подключение к базе данных
conn = psycopg2.connect(DATABASE_URL)
cursor = conn.cursor()


# Шаблон HTML для главной страницы
index_html = """
<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <title>Web_flask_app</title>
</head>
<body>
    <h1>Введите текст:</h1>
    <form action="/process_str" method="POST">
        <input type="text" name="user_input" placeholder="Ваш текст...">
        <button type="submit">Отправить</button>
    </form>
</body>
</html>
"""



# Маршрут для главной страницы
@app.route('/')
def index():
    return render_template_string(index_html)

# Маршрут для обработки введенного текста
@app.route('/process_str', methods=['POST'])
def process_text():
    # Получаем введенный текст
    user_input = request.form['user_input']

    # Сохраняем значение в базу данных
    cursor.execute("INSERT INTO test_table (value) VALUES (%s)", (user_input,))
    conn.commit()

    # Отображаем результат
    return render_template_string(f"""
<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <title>Web_flask_app</title>
</head>
<body>
    <h1>Введите текст:</h1>
    <form action="/process_str" method="POST">
        <input type="text" name="user_input" placeholder="Ваш текст...">
        <button type="submit">Отправить</button>
    </form>
    <h1>Значение '{user_input}' успешно сохранено в базу данных.</h1>
</body>
</html>
""")


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
