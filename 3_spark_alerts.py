import os
import sys

# Пакет читання і запису Kafka для Spark Structured Streaming 3.5.1.
# Змінну треба виставити до створення SparkSession.
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell"
)
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


def _use_dir(path):
    if path and os.path.isdir(path):
        os.environ["PATH"] = path + os.pathsep + os.environ.get("PATH", "")
        return True
    return False


# Локальний запасний шлях, якщо Java або winutils ще не прописані в системі.
# На Linux і на машині з JAVA_HOME / HADOOP_HOME цей блок нічого не міняє.
_tools = os.path.join(os.environ.get("USERPROFILE", ""), "tools")
if not os.environ.get("JAVA_HOME"):
    _jdk_root = os.path.join(_tools, "jdk")
    if os.path.isdir(_jdk_root):
        for _name in sorted(os.listdir(_jdk_root), reverse=True):
            _home = os.path.join(_jdk_root, _name)
            if os.path.isdir(os.path.join(_home, "bin")):
                os.environ["JAVA_HOME"] = _home
                _use_dir(os.path.join(_home, "bin"))
                break
if not os.environ.get("HADOOP_HOME"):
    _hadoop = os.path.join(_tools, "hadoop")
    if os.path.isdir(os.path.join(_hadoop, "bin")):
        os.environ["HADOOP_HOME"] = _hadoop
        _use_dir(os.path.join(_hadoop, "bin"))
if os.environ.get("HADOOP_HOME"):
    os.environ["hadoop.home.dir"] = os.environ["HADOOP_HOME"]

if sys.version_info >= (3, 12):
    sys.exit(
        "PySpark 3.5 з цього завдання розрахований на Python 3.11.\n"
        "Запуск: py -3.11 3_spark_alerts.py"
    )

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    date_format,
    from_json,
    round as spark_round,
    struct,
    to_json,
    to_timestamp,
    window,
)
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from configs import (
    ALERTS,
    BUILDING_SENSORS,
    UNUSED_BOUND,
    setup_console,
    spark_kafka_options,
)

setup_console()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONDITIONS_CSV = os.path.join(BASE_DIR, "alerts_conditions.csv")
CHECKPOINT_DIR = os.path.join(BASE_DIR, "checkpoints", "alerts")

# RawLocalFileSystem не викликає winutils.exe, тож checkpoint на Windows
# працює без окремої установки Hadoop.
spark = (
    SparkSession.builder.appName("SensorAlerts")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.ui.showConsoleProgress", "false")
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

sensor_schema = StructType(
    [
        StructField("id", IntegerType(), True),
        StructField("temperature", IntegerType(), True),
        StructField("humidity", IntegerType(), True),
        StructField("timestamp", StringType(), True),
    ]
)

kafka_options = spark_kafka_options()

raw = (
    spark.readStream.format("kafka")
    .options(**kafka_options)
    .option("subscribe", BUILDING_SENSORS)
    .option("startingOffsets", "earliest")
    .load()
)

events = (
    raw.select(from_json(col("value").cast("string"), sensor_schema).alias("data"))
    .select("data.*")
    .withColumn("event_time", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
    .filter(col("id").isNotNull() & col("event_time").isNotNull())
    .withWatermark("event_time", "10 seconds")
)

# Ковзне вікно 1 хвилина, крок 30 секунд. Середнє рахується окремо для кожного
# датчика: інакше два прилади в одному вікні змішуються в одне число.
averages = events.groupBy(
    window(col("event_time"), "1 minute", "30 seconds"),
    col("id"),
).agg(
    avg("temperature").alias("avg_temperature"),
    avg("humidity").alias("avg_humidity"),
)

conditions = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .csv(CONDITIONS_CSV)
    .withColumnRenamed("id", "condition_id")
)


def bound_matches(value_col, min_col, max_col):
    """Пара -999,-999 вимикає перевірку. Інакше середнє має лежати між межами включно."""
    unused = (col(min_col) == UNUSED_BOUND) & (col(max_col) == UNUSED_BOUND)
    inside = (col(value_col) >= col(min_col)) & (col(value_col) <= col(max_col))
    return unused | inside


alerts = (
    averages.crossJoin(conditions)
    .where(
        bound_matches("avg_humidity", "humidity_min", "humidity_max")
        & bound_matches("avg_temperature", "temperature_min", "temperature_max")
    )
    .select(
        col("id"),
        date_format(col("window.start"), "yyyy-MM-dd HH:mm:ss").alias("window_start"),
        date_format(col("window.end"), "yyyy-MM-dd HH:mm:ss").alias("window_end"),
        spark_round(col("avg_temperature"), 2).alias("avg_temperature"),
        spark_round(col("avg_humidity"), 2).alias("avg_humidity"),
        col("code"),
        col("message"),
    )
)


def publish_alerts(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    payload = batch_df.select(
        col("id").cast("string").alias("key"),
        to_json(
            struct(
                col("id"),
                col("window_start"),
                col("window_end"),
                col("avg_temperature"),
                col("avg_humidity"),
                col("code"),
                col("message"),
            )
        ).alias("value"),
    )

    (
        payload.write.format("kafka")
        .options(**kafka_options)
        .option("topic", ALERTS)
        .save()
    )

    for row in batch_df.collect():
        print(
            f"Відправлено в {ALERTS}: датчик {row['id']}, "
            f"{row['window_start']} - {row['window_end']}, "
            f"t={row['avg_temperature']}, h={row['avg_humidity']}, "
            f"код {row['code']} ({row['message']})",
            flush=True,
        )


query = (
    alerts.writeStream.outputMode("append")
    .trigger(processingTime="10 seconds")
    .option("checkpointLocation", CHECKPOINT_DIR)
    .foreachBatch(publish_alerts)
    .start()
)

print(f"Читаю {BUILDING_SENSORS}, алерти пишу в {ALERTS}.", flush=True)
print("Режим append: вікно з'явиться, коли watermark пройде його кінець (близько 70 с даних).", flush=True)
print("Зупинка: Ctrl+C.", flush=True)
query.awaitTermination()
