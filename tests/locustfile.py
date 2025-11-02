from locust import User, task, between, events, constant
import time, json, threading
from client.client import Client
import random
import string

TOPIC = "bench"
HOST = "localhost"
PORT = 1234

PER_PRODUCER_RATE = 100  # msgs/s per producer
INTERVAL = 1.0 / PER_PRODUCER_RATE

# Shared start flag so producers wait until consumers ready
consumers_ready = threading.Event()


class ConsumerUser(User):
    """
    Consumers subscribe and block waiting for messages.
    When messages arrive, record latency using the Locust event system.
    """
    weight = 5
    wait_time = between(1,1)

    def on_start(self):
        self.client_obj = Client(HOST, PORT)
        self.client_obj.register()
        self.client_obj.subscribe(TOPIC)
        self.client_obj.reset_offset_latest(TOPIC)
        print(f"[Consumer] Subscribed to {TOPIC}")
        consumers_ready.set()  # signal that at least one consumer is ready
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
    weight=100
    wait_time = constant(INTERVAL)

    def on_start(self):
        self.client_obj = Client(HOST, PORT)
        self.client_obj.register()
        # Wait until consumers_ready is set before producing
        consumers_ready.wait()
        print("[Producer] Consumers ready. Starting production...")

    @task
    def produce_message(self):
        payload = json.dumps({"ts": time.time(), "msg": ''.join(random.choices(string.ascii_letters, k=990))})
        start = time.time()
        try:
            self.client_obj.produce(TOPIC, payload)
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
