# goit-de-hw-06

Потік показів IoT-датчиків з Kafka обробляє Spark Structured Streaming. За ковзним вікном рахується середня температура і вологість, умови алертів читаються з `alerts_conditions.csv`, спрацьовані сповіщення пишуться в окремий топік.

## Запуск

Потрібні Docker, Java 17 і Python 3.11 (PySpark 3.5 на 3.13 не стартує). На Windows Spark ще шукає `winutils.exe`: якщо `HADOOP_HOME` не заданий, скрипт дивиться в `%USERPROFILE%\tools\hadoop`, а `JAVA_HOME` — у `%USERPROFILE%\tools\jdk`.

Якщо брокер з ДЗ 5 уже слухає порт 9092, другий контейнер не піднімайте. Інакше з цієї теки:

```bash
docker compose up -d
py -3.11 -m pip install -r requirements.txt
py -3.11 1_create_topics.py
```

Далі три окремі вікна, у такому порядку:

```bash
py -3.11 3_spark_alerts.py
py -3.11 4_alerts_viewer.py
py -3.11 2_sensor_producer.py hot
py -3.11 2_sensor_producer.py dry
```

Останні два рядки це два датчики. Профілі `hot`, `cold`, `dry`, `wet` лишаються в межах температур 25-45 і вологості 15-85, але звужують випадкові значення, щоб середнє за хвилину перетнуло поріг з CSV. Без профілю діапазон такий самий, як у ДЗ 5:

```bash
py -3.11 2_sensor_producer.py
```

Вікно в режимі `append` закривається приблизно через 70 секунд після першого показу. Зупинка кожного вікна: Ctrl+C.

Повторний запуск Spark з тією самою текою `checkpoints/alerts` не читає старі записи вдруге. Щоб пройти топік знову, зупиніть скрипт і видаліть цю теку.
