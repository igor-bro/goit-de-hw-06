import time

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

from configs import ALERTS, BUILDING_SENSORS, connection_params, my_name, setup_console

setup_console()

admin_client = KafkaAdminClient(**connection_params())

topic_names = [
    BUILDING_SENSORS,
    ALERTS,
]

num_partitions = 2
replication_factor = 1

for topic_name in topic_names:
    new_topic = NewTopic(
        name=topic_name,
        num_partitions=num_partitions,
        replication_factor=replication_factor,
    )
    try:
        admin_client.create_topics(new_topics=[new_topic], validate_only=False)
        print(f"Topic '{topic_name}' created successfully.")
    except TopicAlreadyExistsError:
        print(f"Topic '{topic_name}' already exists.")
    except Exception as e:
        print(f"An error occurred for '{topic_name}': {e}")

# list_topics() читає кеш метаданих. Одразу після створення там буває неповний
# список, тому чекаємо, поки з'являться обидва топіки, і лише тоді друкуємо.
admin_client.close()
for _ in range(15):
    admin_client = KafkaAdminClient(**connection_params())
    visible = [topic for topic in admin_client.list_topics() if my_name in topic]
    if all(name in visible for name in topic_names):
        break
    admin_client.close()
    time.sleep(0.5)

print("\nТопіки з ідентифікатором:")
[print(topic) for topic in sorted(admin_client.list_topics()) if my_name in topic]

admin_client.close()
