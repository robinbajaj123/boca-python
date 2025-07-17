
from confluent_kafka import Producer
import json, time, random
from faker import Faker

fake = Faker()
p = Producer({'bootstrap.servers': 'localhost:9092'})

def generate_order():
    return {
        "user_id": fake.user_name(),
        "item": random.choice(["laptop", "phone", "book", "shoes"]),
        "amount": round(random.uniform(10, 500), 2),
        "ts": fake.iso8601()
    }

while True:
    order = generate_order()
    p.produce('orders', key=order['user_id'], value=json.dumps(order))
    p.flush()
    print(f"Produced: {order}")
    time.sleep(1)
