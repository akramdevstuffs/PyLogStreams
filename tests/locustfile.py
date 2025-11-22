from locust import User, task, between, events, constant
import time, json, threading
from client.client import Client
from gevent.event import Event
from gevent.lock import Semaphore
import random
import string

TOPIC = "bench"
HOST = "localhost"
PORT = 1234

NUMBER_OF_PRODUCER = 20
CONSUMER_PRODUCER_RATIO = 5
TARGET_MESSAGE_PER_SEC = 13000
INTERVAL = (NUMBER_OF_PRODUCER*(CONSUMER_PRODUCER_RATIO+1))/TARGET_MESSAGE_PER_SEC


consumers_ready = Event()
consumer_counter = 0
consumer_total = NUMBER_OF_PRODUCER*CONSUMER_PRODUCER_RATIO
# Shared start flag so producers wait until consumers ready
lock = Semaphore()

# Counter for producers
producer_counter = 0


class ConsumerUser(User):
    """
    Consumers subscribe and block waiting for messages.
    When messages arrive, record latency using the Locust event system.
    """
    weight = CONSUMER_PRODUCER_RATIO
    wait_time = between(1,1)

    def on_start(self):
        global consumer_counter, consumers_ready, consumer_total
        self.client_obj = Client(HOST, PORT)
        self.client_obj.register()
        with lock:
            producer_id = consumer_counter//CONSUMER_PRODUCER_RATIO
            self.client_obj.subscribe(f"{TOPIC}{producer_id}")
            self.client_obj.reset_offset_latest(f"{TOPIC}{producer_id}")
            print(f"[Consumer-{consumer_counter-1}] Subscribed to {TOPIC}{producer_id}")
            consumer_counter += 1
            if consumer_counter==consumer_total:
                consumers_ready.set()
        consumers_ready.wait()  # wait until all the consumer starts
        threading.Thread(target=self.consume_loop, daemon=True).start()

    def consume_loop(self):
        while True:
            try:
                resp = self.client_obj.consume()
                if not resp:
                    continue
                msg = resp.split(' ',1)[1]
                if not msg:
                    continue
                data = json.loads(msg)
                sent_ts = data.get("ts", None)
                if sent_ts:
                    latency = (time.time() - sent_ts) * 1000
                    events.request.fire(
                        request_type="consume",
                        name="message_latency",
                        response_time=latency,
                        response_length=len(msg),
                        exception=None,
                    )
            except Exception as e:
                events.request.fire(
                    request_type="consume",
                    name="message_latency",
                    response_time=0,
                    response_length=0,
                    exception=e,
                )

    @task
    def idle(self):
        time.sleep(1)  # main Locust loop just stays alive


class ProducerUser(User):
    """
    Producers send messages at a steady rate once consumers are ready.
    """
    weight=1
    wait_time = constant(INTERVAL)

    def on_start(self):
        global producer_counter
        self.client_obj = Client(HOST, PORT)
        self.client_obj.register()
        with lock:
            self.producer_id = producer_counter
            producer_counter += 1 # Increase the producer count
        # Wait until consumers_ready is set before producing
        consumers_ready.wait()
        print(f"[Producer-{self.producer_id}] Consumers ready. Starting production...")

    @task
    def produce_message(self):
        payload = json.dumps({"ts": time.time(), "msg": ''.join(random.choices(string.ascii_letters, k=990))})
        start = time.time()
        try:
            self.client_obj.produce(f"{TOPIC}{self.producer_id}", payload)
            latency = (time.time() - start) * 1000
            events.request.fire(
                request_type="produce",
                name="produce_message",
                response_time=latency,
                response_length=len(payload),
                exception=None,
            )
        except Exception as e:
            events.request.fire(
                request_type="produce",
                name="produce_message",
                response_time=0,
                response_length=0,
                exception=e,
            )
