import json

from kafka import KafkaConsumer

from configs import ALERTS, connection_params, my_name, setup_console

setup_console()

consumer = KafkaConsumer(
    **connection_params(),
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    key_deserializer=lambda v: v.decode("utf-8") if v else None,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id=f"{my_name}_hw06_alerts_viewer",
)

consumer.subscribe([ALERTS])

print(f"Слухаю топік сповіщень: {ALERTS}")

try:
    for message in consumer:
        value = message.value
        print(
            f"[{message.topic}] partition {message.partition}, "
            f"offset {message.offset}, key {message.key}"
        )
        print(
            f"  id={value['id']}  {value['window_start']} - {value['window_end']}  "
            f"t={value['avg_temperature']}  h={value['avg_humidity']}  "
            f"код {value['code']} ({value['message']})"
        )
except KeyboardInterrupt:
    print("\nЧитання сповіщень зупинено.")
finally:
    consumer.close()
