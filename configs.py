import sys

# Ідентифікатор у назвах топіків, щоб вони не збігалися з чужими на спільному кластері.
my_name = "kushneruk"

# Локальний брокер з docker-compose.yml (той самий, що в ДЗ 5, порт 9092).
# Для кластера курсу підставте значення з кабінету:
# security_protocol зазвичай SASL_PLAINTEXT, sasl_mechanism — PLAIN,
# username і password — ті, що видає курс.
kafka_config = {
    "bootstrap_servers": ["localhost:9092"],
    "security_protocol": "PLAINTEXT",
    "sasl_mechanism": "PLAIN",
    "username": "",
    "password": "",
}

BUILDING_SENSORS = f"{my_name}_building_sensors"
ALERTS = f"{my_name}_alerts"

# Пара -999,-999 у alerts_conditions.csv означає, що ця межа не бере участі в умові.
UNUSED_BOUND = -999


def setup_console():
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass


def connection_params():
    params = {
        "bootstrap_servers": kafka_config["bootstrap_servers"],
        "security_protocol": kafka_config["security_protocol"],
    }
    if str(kafka_config["security_protocol"]).startswith("SASL"):
        params["sasl_mechanism"] = kafka_config["sasl_mechanism"]
        params["sasl_plain_username"] = kafka_config["username"]
        params["sasl_plain_password"] = kafka_config["password"]
    return params


def spark_kafka_options():
    """Параметри брокера для Spark. Пароль додається лише для SASL."""
    options = {
        "kafka.bootstrap.servers": ",".join(kafka_config["bootstrap_servers"]),
        "kafka.security.protocol": kafka_config["security_protocol"],
    }
    if str(kafka_config["security_protocol"]).startswith("SASL"):
        options["kafka.sasl.mechanism"] = kafka_config["sasl_mechanism"]
        options["kafka.sasl.jaas.config"] = (
            "org.apache.kafka.common.security.plain.PlainLoginModule required "
            f'username="{kafka_config["username"]}" '
            f'password="{kafka_config["password"]}";'
        )
    return options
