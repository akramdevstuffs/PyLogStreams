import time
import os
import socket
import struct
import pickle
import threading
from multiprocessing import Process
from queue import Queue

HOST, PORT = 'localhost',1234
TOPIC = 'test_topic'

MSG_CNT_PER_PRODUCER = 5000
NUM_PRODUCERS = 200
MSG_SIZE = 1024

FLAG = 'Z'

user_id = {}

pickle_file = 'test_minikafka_pickle.pkl'

def sendall(conn, data):
    conn.sendall(data)
    return
    total_sent = 0
    while total_sent < len(data):
        sent = conn.send(data[total_sent:])
        if sent == 0:
            raise RuntimeError("socket connection broken")
        total_sent += sent

def producer(num_msgs, msg_size, producer_id):
    conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    conn.connect((HOST,PORT))
    _ ='REG'.encode()
    conn.sendall((len(_)).to_bytes(4,'big') + _) # Register
    _ = conn.recv(4) # id_length
    id_len = int.from_bytes(_, 'big')
    _ = conn.recv(id_len)
    id_str = _.decode()
    _ = f'ID {id_str}'.encode()
    sendall(conn,len(_).to_bytes(4,'big') + _)

    send_queue = Queue()

    def beating():
        while True:
            try:
                time.sleep(30)
                ping_msg = 'PING'.encode()
                msg_bytes = len(ping_msg).to_bytes(4,'big') + ping_msg
                send_queue.put(msg_bytes)
            except Exception as e:
                print(f"Exception in beating: {e}")
                break
    beat_thread = threading.Thread(target=beating, daemon=True)
    beat_thread.start()
    LOCAL_TOPIC = f'{TOPIC}{producer_id}'
    msg_batches = []
    BATCH_SIZE = 1
    # Measuring total time to send messages
    start = time.time()
    for i in range(num_msgs):
        if not send_queue.empty():
            msg = send_queue.get()
            sendall(conn,msg)
        msg_bytes = f'PUB {LOCAL_TOPIC} {str(time.time())} '.encode()
        msg_bytes = msg_bytes + (FLAG*(msg_size-len(msg_bytes)-8)).encode()
        msg_bytes = len(msg_bytes).to_bytes(4,'big') + msg_bytes
        if((i+1)%BATCH_SIZE==0):
            sendall(conn, msg_bytes)
    conn.close()
    duration = time.time() - start
    # print(f"Producer {producer_id} sent {num_msgs} msgs in {duration:.2f}s -> {num_msgs/(duration+0.001):.1f} msg/s")
    # print(f"Producer sent {num_msgs} msgs in {duration:.2f}s -> {num_msgs/(duration+0.001):.1f} msg/s")

def recvall(conn, n):
    data = b''
    while len(data) < n:
        packet = conn.recv(n - len(data))
        if not packet or packet==b'':
            return None
        data += packet
    return data

CONSUMER_DELAY = 0 # in seconds

def consumer(id_str,producer_id=0):

    conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    conn.connect((HOST,PORT))

    id_msg = f'ID {id_str}'.encode()
    sendall(conn,len(id_msg).to_bytes(4,'big') + id_msg)


    sub_msg = f"SUB {TOPIC}{producer_id}".encode()
    sendall(conn,len(sub_msg).to_bytes(4,'big') + sub_msg)

    # Offset reset message
    reset_msg = f"SET {TOPIC}{producer_id} 0".encode()
    sendall(conn,len(reset_msg).to_bytes(4,'big') + reset_msg)


    def beating():
        while True:
            try:
                time.sleep(30)
                ping_msg = 'PING'.encode()
                sendall(conn,len(ping_msg).to_bytes(4,'big') + ping_msg)
                print(f"Consumer {producer_id} heartbeat sent")
            except Exception as e:
                print(f"Exception in beating: {e}")
                break
    beat_thread = threading.Thread(target=beating, daemon=True)
    beat_thread.start()

    recv_count = 0
    start = time.time()
    total_latency = 0
    flag = ' '
    max_latency = 0
    while recv_count<MSG_CNT_PER_PRODUCER:
        try:
            len_bytes = conn.recv(4)
            if not len_bytes:
                print(f"Connection closed by server cnt {recv_count}")
                break
            msg_len = int.from_bytes(len_bytes,'big')
            msg = recvall(conn,msg_len).decode()
            t_str = msg.split(' ')[1]
            t = float(t_str)
            latency = time.time() - t - CONSUMER_DELAY
            flag = msg[100]
            total_latency += latency*1000
            max_latency = max(max_latency, latency*1000)
            recv_count += 1
        except Exception as e:
            print(f"Exception in consumer: {e}")
    conn.close()
    avg_latency = total_latency/(recv_count)
    duration = time.time() - start
    print(f"Consumer received {recv_count} msgs in {duration:.2f}s -> {recv_count/(duration+0.001):.1f} msg/s and avg_latency {avg_latency}ms max_latency {max_latency}ms flag {flag}")


# load_topics_log()
# start_threads()

"""
def test_file_speed(num_msgs, msgs_size):
    msg = 'X'*msgs_size
    start = time.time()
    for i in range(num_msgs):
        append_message(TOPIC, msg)
    duration = time.time() - start
    print(f'message {num_msgs} in {duration:.2f}s {num_msgs/duration} msgs/s')
"""

# for i in range(1):
#     t = threading.Thread(target=test_file_speed, args=(1000000, 1*1024))
#     t.start()
#     threads.append(t)

if __name__ == '__main__':
    if os.path.exists(pickle_file):
        with open(pickle_file,'rb') as f:
            user_id = pickle.load(f)

    processes = []

    for idx in range(NUM_PRODUCERS):
        # p = Process(target=consumer, args=(idx,))
        # p.start()
        # processes.append(p)
        conn = None
        while conn is None:
            try:
                conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                conn.connect((HOST,PORT))
            except Exception:
                conn = None
                print("Retrying connection to server...")
        if(user_id.get(idx) is None):
            _ ='REG'.encode()
            conn.sendall((len(_)).to_bytes(4,'big') + _) # Register
            _ = conn.recv(4) # id_length
            id_len = int.from_bytes(_,'big')
            _ = conn.recv(id_len)
            id_str = _.decode()
            user_id[idx] = id_str
        else:
            id_str = user_id[idx]
        conn.close()
        t = Process(target=consumer, args=(id_str,idx,))
        t.start()
        processes.append(t)

    start = time.time()

    for idx in range(NUM_PRODUCERS):
        p = Process(target=producer, args=(MSG_CNT_PER_PRODUCER, MSG_SIZE, idx))
        p.start()
        processes.append(p)

    for p in processes:
        p.join()

    # duration = time.time() - start
    # print(f"All producers finished in {duration:.2f}s")

    processes = []



    for p in processes:
        p.start()

    # Wait for producers to finish
    for p in processes:
        p.join()
    duration = time.time() - start
    total_msgs = NUM_PRODUCERS * MSG_CNT_PER_PRODUCER
    print(f"All producers/consumers sent and receive {total_msgs} msgs in {duration:.2f}s -> {total_msgs/(duration+0.001):.1f} msg/s")

    # processes = []
    # start = time.time()


    # for p in processes:
    #     p.join()
    # duration = time.time() - start
    # print(f"All consumers finished in {duration:.2f}s")


    with open(pickle_file,'wb') as f:
        pickle.dump(user_id,f)
