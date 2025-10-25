import time
import os
import socket
import struct
import pickle
import threading
from multiprocessing import Process

HOST, PORT = 'localhost',1234
TOPIC = 'test_topic'

MSG_CNT_PER_PRODUCER = 10000*10
NUM_PRODUCERS = 50
MSG_SIZE = 1024

user_id = {}

pickle_file = 'test_minikafka_pickle.pkl'

def producer(conn, id_str,num_msgs, msg_size, producer_id):
    msg = ('X'*msg_size).encode()
    LOCAL_TOPIC = f'{TOPIC}{producer_id}'
    msg_batches = []
    curr_batch = b''
    BATCH_SIZE = 1
    for i in range(num_msgs):
        t_str = str(time.time()).encode()
        msg_bytes = f'PUB {LOCAL_TOPIC} {str(time.time())} '.encode()
        msg_bytes = msg_bytes + ('X'*(msg_size-len(msg_bytes)-8)).encode()
        curr_batch += len(msg_bytes).to_bytes(4,'big') + msg_bytes
        if((i+1)%BATCH_SIZE==0):
            msg_batches.append(curr_batch)
            curr_batch=b''
    # Measuring total time to send messages
    if(curr_batch!=b''):
        msg_batches.append(curr_batch)
    start = time.time()
    for batch in msg_batches:
        conn.sendall(batch)
    conn.close()
    duration = time.time() - start
    # print(f"Producer sent {num_msgs} msgs in {duration:.2f}s -> {num_msgs/(duration+0.001):.1f} msg/s")

def recvall(conn, n):
    data = b''
    while len(data) < n:
        packet = conn.recv(n - len(data))
        if not packet:
            return None
        data += packet
    return data

CONSUMER_DELAY = 0 # in seconds

def consumer(conn,id_str,producer_id=0):
    
    sub_msg = f"SUB {TOPIC}{producer_id}".encode()
    conn.sendall(len(sub_msg).to_bytes(4,'big') + sub_msg)

    recv_count = 0
    start = time.time()
    total_latency = 0
    flag = ' '
    while recv_count<MSG_CNT_PER_PRODUCER:
        try:
            len_bytes = recvall(conn,4)
            if not len_bytes:
                continue
            msg_len = int.from_bytes(len_bytes,'big')
            msg = recvall(conn,msg_len).decode()
            t_str = msg.split(' ')[1]
            t = float(t_str)
            latency = time.time() - t - CONSUMER_DELAY
            flag = msg[30]
            total_latency += latency*1000
            recv_count += 1
        except Exception as e:
            print(f"Exception in consumer: {e}")
    conn.close()
    avg_latency = total_latency/(recv_count)
    duration = time.time() - start
    # print(f"Consumer received {recv_count} msgs in {duration:.2f}s -> {recv_count/(duration+0.001):.1f} msg/s and avg_latency {avg_latency}ms")


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
        conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        conn.connect((HOST,PORT))
        _ ='REG'.encode()
        conn.sendall((len(_)).to_bytes(4,'big') + _) # Register
        _ = conn.recv(4) # id_length
        id_len = int.from_bytes(_)
        _ = conn.recv(id_len)
        id_str = _.decode()
        p = Process(target=producer, args=(conn,id_str,MSG_CNT_PER_PRODUCER, MSG_SIZE, idx))
        processes.append(p)
    start = time.time()
    for p in processes:
        p.start()

    for p in processes:
        p.join()
    
    duration = time.time() - start
    print(f"All producers finished in {duration:.2f}s")
    
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
            id_len = int.from_bytes(_)
            _ = conn.recv(id_len)
            id_str = _.decode()
            user_id[idx] = id_str
        else:
            id_str = user_id[idx]
            id_msg = f'ID {id_str}'.encode()
            conn.sendall(len(id_msg).to_bytes(4,'big') + id_msg)
        t = Process(target=consumer, args=(conn,id_str,idx,))
        processes.append(t)

    
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
