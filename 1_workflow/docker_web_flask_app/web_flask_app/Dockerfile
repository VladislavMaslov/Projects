# Указываем базовый образ, от которого будем наследоваться
FROM python:3.10-slim-buster

# Устанавливаем рабочую директорию внутри контейнера
WORKDIR /app

# Копируем все файлы из текущей директории в контейнер
COPY . .

# Устанавливаем зависимости из файла requirements.txt
RUN pip install -r requirements.txt

# Открываем порт 8080 для доступа к приложению снаружи контейнера
EXPOSE 8080

# Запускаем приложение при старте контейнера
CMD ["python", "./web_app_main.py", "--host=0.0.0.0"]
