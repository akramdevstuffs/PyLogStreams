import uuid
import asyncio
import time
from log_manager import read_message, append_message, start_threads,load_topics_log, check_message_available
from offsets_manager import update_client_offset, get_client_offsets, load_client_offsets
from concurrent.futures import ThreadPoolExecutor
from functools import partial
import os

if os.name == "posix":
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    print("Using uvloop for asyncio")
else:
    print("Using default asyncio loop")


BATCH_SIZE = 50       # drain after 50 messages
MAX_BUFFERED = 32_000 # or when >64KB of data queued
LINGER_MS = 50    # only wait 50ms before draining
BEAT_MAX_DELAY = 120  # seconds

MESSAGE_CHECKSUM_ENABLE = True # Enables the message integrity checks

pool = ThreadPoolExecutor(max_workers=50)


# client last beat
client_heartbeats = {}

topic_events = {}

clients_task = {}

async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    client_id = None
    while True:
        try:
            len_bytes = await reader.readexactly(4)
            if not len_bytes or len_bytes == b'\x00\x00\x00\x00':
                break
            msg_length = int.from_bytes(len_bytes, 'big')
            msg_bytes = await reader.readexactly(msg_length)
            command = msg_bytes[:3].decode()
        except Exception as e:
            return
        if command == 'REG':
            client_id = str(uuid.uuid4())
            id_bytes = client_id.encode()
            try:
                writer.write(len(id_bytes).to_bytes(4,'big') + id_bytes)
                await writer.drain()
            except Exception:
                return
        elif command == 'CID':
            msg = msg_bytes.decode()
            client_id = msg.split(' ', 1)[1]
            if client_id in clients_task:
                # Writer from previous connection still active, close this one
                task = clients_task[client_id]
                task.cancel()
                clients_task.pop(client_id,None)
            client_heartbeats[client_id] = time.time()
        elif client_id is None:
            # Client must register first
            return
        elif command == 'SUB':
            msg = msg_bytes.decode()
            parts = msg.split(' ')
            topic = parts[1]
            if topic not in get_client_offsets(client_id):
                update_client_offset(client_id, topic, 0)
            if topic not in topic_events:
                topic_events[topic] = asyncio.Event()
            # Start client writer task
            if not clients_task.get(client_id) :
                task = asyncio.create_task(client_writer(writer, client_id))
                task.add_done_callback(lambda t,cid=client_id:
                    clients_task.pop(cid,None))
                clients_task[client_id] = task
        # For setting offsets from clients side
        elif command == 'SET':
            msg = msg_bytes.decode()
            parts = msg.split(' ')
            topic = parts[1]
            offset = int(parts[2])
            update_client_offset(client_id, topic, offset)
        elif command == 'PUB':
            msg = msg_bytes.decode()
            parts = msg.split(' ', 2)
            topic = parts[1]
            conv = parts[2]
            if MESSAGE_CHECKSUM_ENABLE:
                hash = await reader.readexactly(4) # Reads the 4 byte for checksum
            else:
                hash = None
            # loop = asyncio.get_running_loop()
            # await loop.run_in_executor(pool, partial(append_message, topic, conv))
            code = append_message(topic, conv.encode(), hash)
            if code==0:
                if topic in topic_events:
                    topic_events[topic].set()
                    topic_events[topic].clear()
            # Add the ack logic here
            pass

        # Heart beat from client
        elif command=='PNG':
            client_heartbeats[client_id] = time.time()
            print(f"Client {client_id} heartbeat at {time.time()}")

async def client_writer(writer: asyncio.StreamWriter, client_id: str):
    count = 0
    buffered = 0
    timestamp = time.time()
    updated_offsets = {}
    transport = writer.transport
    # Checking the client last beat
    while client_heartbeats.get(client_id,0)+BEAT_MAX_DELAY > time.time():
        # exit if connection closed
        if transport.is_closing():
            print(f"Connection closed for client {client_id}")
            return
        if count == 0:
            timestamp = time.time()
        # Get current offsets
        client_offsets = get_client_offsets(client_id)
        for topic in client_offsets.keys():
            offset = client_offsets[topic]
            if topic in updated_offsets:
                offset = updated_offsets[topic]
            if(not check_message_available(topic, offset)):
                continue
            # loop = asyncio.get_running_loop()
            msg, new_offset = read_message(topic, offset, MESSAGE_CHECKSUM_ENABLE)
            if msg is not None:
                ansbytes = f'{topic} {msg}'.encode()
                try:
                    writer.write(len(ansbytes).to_bytes(4,'big') + ansbytes)
                    buffered += len(ansbytes) + 4
                    count += 1
                    updated_offsets[topic] = new_offset
                    if (count >= BATCH_SIZE or buffered >= MAX_BUFFERED or (count>0 and (time.time()-timestamp)*1000 >= LINGER_MS)):
                        break
                except Exception:
                    print(f"Exception sending to client {client_id}")
                    return
            else:
                # No new message, update offset anyway
                # This can happen if message was expired
                if new_offset != offset:
                    updated_offsets[topic] = new_offset
        if buffered > 0 and (count >= BATCH_SIZE or buffered >= MAX_BUFFERED or (time.time()-timestamp)*1000 >= LINGER_MS):
            try:
                await writer.drain()
            except Exception:
                print(f"Exception draining to client {client_id}")
                return
            for topic, new_offset in updated_offsets.items():
                update_client_offset(client_id, topic, new_offset)
            updated_offsets = {}
            count = 0
            buffered = 0
            timestamp = time.time()
        # Nothing was sent, wait for new messages
        elif count==0 and updated_offsets=={}:
            # Subscribe to all topic events
            tasks = [
                asyncio.create_task(topic_events[tp].wait())
                for tp in client_offsets.keys()
                if tp in topic_events
            ]
            done,pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED,timeout=1)
            for t in pending:
                t.cancel()


async def start_server():
    load_topics_log()
    start_threads()
    load_client_offsets()

    server = await asyncio.start_server(handle_client, 'localhost', 1234)
    addrs = ', '.join(str(sock.getsockname()) for sock in server.sockets)
    print(f'Serving on {addrs}')

    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    asyncio.run(start_server())
