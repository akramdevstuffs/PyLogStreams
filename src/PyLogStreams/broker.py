import uuid
import asyncio
from kafka_file_handler import read_message, append_message, start_threads,load_topics_log, check_message_available
from offsets_manager import update_client_offset, get_client_offsets
from concurrent.futures import ThreadPoolExecutor
from functools import partial
import os

if os.name == "posix":
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    print("Using uvloop for asyncio")
else:
    print("Using default asyncio loop")

pool = ThreadPoolExecutor(max_workers=50)

topic_events = {}

client_subscriptions = {}

async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    client_id = None
    while True:
        try:
            len_bytes = await reader.readexactly(4)
            if not len_bytes or len_bytes == b'\x00\x00\x00\x00':
                break
            msg_length = int.from_bytes(len_bytes, 'big')
            msg_bytes = await reader.readexactly(msg_length)
            msg = msg_bytes.decode()
        except Exception as e:
            return
        if msg.startswith('REG'):
            client_id = str(uuid.uuid4())
            id_bytes = client_id.encode()
            writer.write(len(id_bytes).to_bytes(4,'big') + id_bytes)
            await writer.drain()
        elif msg.startswith('ID'):
            client_id = msg.split(' ', 1)[1]
            client_subscriptions[client_id] = {}
        elif client_id is None:
            # Client must register first
            return
        elif msg.startswith('SUB'):
            parts = msg.split(' ')
            topic = parts[1]
            if topic not in get_client_offsets(client_id):
                update_client_offset(client_id, topic, 0)
            if topic not in topic_events:
                topic_events[topic] = asyncio.Event()
            # Start client writer task 
            if not client_subscriptions.get(client_id):
                client_subscriptions[client_id] = {}
                task = asyncio.create_task(client_writer(writer, client_id))
                task.add_done_callback(lambda t,cid=client_id: 
                    client_subscriptions.pop(cid,None))
            client_subscriptions[client_id][topic] = True

        elif msg.startswith('PUB'):
            parts = msg.split(' ', 2)
            topic = parts[1]
            conv = parts[2]
            # loop = asyncio.get_running_loop()
            # await loop.run_in_executor(pool, partial(append_message, topic, conv))
            append_message(topic, conv)
            if topic in topic_events:
                topic_events[topic].set()
                topic_events[topic].clear()

async def client_writer_topic(writer: asyncio.StreamWriter, client_id: str, topic: str):
    while True:
        client_offsets = get_client_offsets(client_id)
        offset = client_offsets.get(topic, 0)
        if(not check_message_available(topic, offset)):
            await asyncio.sleep(0.5)
            continue
        msg, new_offset = read_message(topic, offset)
        if msg is not None:
            ansbytes = f'{topic} {msg}'.encode()
            try:
                writer.write(len(ansbytes).to_bytes(4,'big') + ansbytes)
                await writer.drain()
                update_client_offset(client_id, topic, new_offset)
            except Exception:
                return
        else:
            # No new message, update offset anyway
            # This can happen if message was expired
            if new_offset != offset:
                update_client_offset(client_id, topic, new_offset)
                # So that we re-check for new messages immediately
                continue
            # Wait for new message event
            if topic in topic_events:
                try:
                    await topic_events[topic].wait(timeout=5)
                except Exception:
                    return
            else:
                await asyncio.sleep(0.5)  # No subscribed topics, sleep briefly

async def client_writer(writer: asyncio.StreamWriter, client_id: str):
    while True:
        client_offsets = get_client_offsets(client_id)
        for topic in client_offsets.keys():
            offset = client_offsets[topic]
            if(not check_message_available(topic, offset)):
                continue
            # loop = asyncio.get_running_loop()
            msg, new_offset = read_message(topic, offset)
            if msg is not None:
                ansbytes = f'{topic} {msg}'.encode()
                try:
                    writer.write(len(ansbytes).to_bytes(4,'big') + ansbytes)
                    await writer.drain()
                    update_client_offset(client_id, topic, new_offset)
                    #Send one message at a time per topic
                    break
                except Exception:
                    return
            else:
                # No new message, update offset anyway
                # This can happen if message was expired
                if new_offset != offset:
                    update_client_offset(client_id, topic, new_offset)
                    # So that we re-check for new messages immediately
                    break
        else:
            # Subscribe to all topic events
            tasks = [
                asyncio.create_task(topic_events[tp].wait())
                for tp in client_offsets.keys() 
                if tp in topic_events
            ]
            done,pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED,timeout=5)
            for t in pending:
                t.cancel()


async def start_server():
    load_topics_log()
    start_threads()
    
    server = await asyncio.start_server(handle_client, 'localhost', 1234)
    addrs = ', '.join(str(sock.getsockname()) for sock in server.sockets)
    print(f'Serving on {addrs}')

    async with server:
        await server.serve_forever()
    
if __name__ == "__main__":
    asyncio.run(start_server())