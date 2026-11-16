import random
import sys
import time
from datetime import datetime

from kafka import KafkaProducer
import json

from configs import BUILDING_SENSORS, connection_params, setup_console

setup_console()

# Діапазони як у ДЗ 5: температура 25-45, вологість 15-85.
# Профілі звужують діапазон усередині тих самих меж, щоб середнє за хвилину
# стабільно перетинало поріг з alerts_conditions.csv. Без профілю середнє
# двох датчиків лишається біля середини шкали і алерт майже не спрацьовує.
PROFILES = {
    "random": ((25, 45), (15, 85)),
    "hot": ((40, 45), (45, 55)),
    "cold": ((25, 30), (45, 55)),
    "dry": ((31, 39), (15, 35)),
    "wet": ((31, 39), (65, 85)),
}

message_limit = None
profile = "random"
for arg in sys.argv[1:]:
    if arg.isdigit():
        message_limit = int(arg)
    elif arg in PROFILES:
        profile = arg
    else:
        names = ", ".join(PROFILES)
        print(f"Невідомий аргумент '{arg}'. Формат: python 2_sensor_producer.py [кількість] [{names}]")
        sys.exit(1)

temp_range, humidity_range = PROFILES[profile]

# Один запуск = один датчик. Ідентифікатор обирається один раз і не змінюється,
# доки цей процес працює. Наступний запуск отримає інший id.
sensor_id = random.randint(1000, 9999)

producer = KafkaProducer(
    **connection_params(),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda v: str(v).encode("utf-8"),
)

print(f"Датчик {sensor_id} ({profile}) пише в топік '{BUILDING_SENSORS}'.")
print("Зупинка: Ctrl+C. Приклади: python 2_sensor_producer.py hot")
print("         python 2_sensor_producer.py 40 dry")

sent = 0
try:
    while message_limit is None or sent < message_limit:
        data = {
            "id": sensor_id,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "temperature": random.randint(*temp_range),
            "humidity": random.randint(*humidity_range),
        }
        producer.send(BUILDING_SENSORS, key=str(sensor_id), value=data)
        producer.flush()
        sent += 1
        print(f"Sent #{sent} -> {BUILDING_SENSORS}: {data}")
        time.sleep(2)
except KeyboardInterrupt:
    print(f"\nДатчик {sensor_id} зупинено. Відправлено повідомлень: {sent}.")
finally:
    producer.close()
