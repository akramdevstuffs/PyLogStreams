import socket
import selectors
import uuid
import mmap
import os

from kafka_file_handler import read_message, append_message, start_threads,load_topics_log

sel = selectors.DefaultSelector()

client_offsets = {} # {uuid: {topic1: 0, topic2:1}, ...}

def accept(sock, mask, key):
    conn, addr = sock.accept()
    conn.setblocking(False)
    sel.register(conn, selectors.EVENT_READ, {"callback":read})

def recvall(conn, size):
    data = b''
    while len(data) < size:
        chunk = conn.recv(size - len(data))
        if not chunk:
            # connection closed or broken mid-transfer
            raise ConnectionError("Socket closed before expected bytes received")
        data += chunk
    return data


def read(conn, mask, key):
    len_bytes = conn.recv(4)
    if len_bytes and len_bytes!=b'\x00\x00\x00\x00':
        msg_length = int.from_bytes(len_bytes, 'big')
        msg_bytes = recvall(conn,msg_length)
        msg = msg_bytes.decode()
        if(msg.startswith('REG')):
            id = str(uuid.uuid4())
            id_bytes = id.encode()
            conn.sendall(len(id_bytes).to_bytes(4,'big') + id_bytes)
            client_offsets[id] = {}
            sel.modify(conn, selectors.EVENT_READ, {"id":id, "callback":read})
        elif (msg.startswith('ID')):
            id = msg.split(' ', 1)[1]
            if id and id in client_offsets:
                sel.modify(conn, selectors.EVENT_READ, {"id":id, "callback":read})
            else:
                print(f"ID {id} not found")
        elif(msg.startswith('SUB')):
            parts = msg.split(' ')
            topic = parts[1]
            id = key.data['id']
            if topic not in client_offsets[id]:
                client_offsets[id][topic] = 0
            sel.modify(conn, selectors.EVENT_READ | selectors.EVENT_WRITE, {"id": key.data['id'], "callback":read_write})
        elif(msg.startswith('PUB')):
            parts = msg.split(' ', 2)
            topic = parts[1]
            conv = parts[2]
            append_message(topic, conv)
    else:
        print(f"No data {conn.fileno()}")
        sel.unregister(conn)
        conn.close()

def read_write(conn, mask, key):
    if mask & selectors.EVENT_READ:
        read(conn, mask, key)
        return
    id = key.data['id']
    for topic, offset in client_offsets[id].items():
        msg, new_offset = read_message(topic, offset)
        if msg is not None:
            ansbytes = f'{topic} {msg}'.encode()
            try:
                conn.sendall(len(ansbytes).to_bytes(4,'big') + ansbytes)
                client_offsets[id][topic] = new_offset
            except Exception:
                break
            break
        else:
            # No new message, update offset anyway
            # This can happen if message was expired
            client_offsets[id][topic] = new_offset

def start_server():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(('localhost',1234))
    sock.listen(200)
    sel.register(sock, selectors.EVENT_READ, {"callback":accept})

    load_topics_log()
    start_threads()

    while True:
        events = sel.select()
        for key,mask in events:
            callback = key.data['callback']
            callback(key.fileobj, mask, key)
start_server()