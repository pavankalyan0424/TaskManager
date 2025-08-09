import socket
import json
import hashlib
import os
import sys
import threading
import time
import random

# --- Task functions ---
def count_words(file_path):
    try:
        with open(file_path, 'r') as f:
            return len(f.read().split())
    except FileNotFoundError:
        return 0

def sha256_checksum(file_path):
    try:
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()
    except FileNotFoundError:
        return "File not found"

def line_count(file_path):
    try:
        with open(file_path, 'r') as f:
            return sum(1 for _ in f)
    except FileNotFoundError:
        return 0

def file_count(directory_path):
    try:
        if not os.path.isdir(directory_path):
            return "Path is not a directory"
        return len(os.listdir(directory_path))
    except FileNotFoundError:
        return 0

# --- P2P State ---
MY_ADDR = None
FILE_UPLOAD_DIR = './peer_uploads/'
peer_list = set()
peer_list_lock = threading.Lock()

# --- RPC Communication ---
def send_rpc_call(host, port, request, timeout=5):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(timeout)
            s.connect((host, port))
            s.sendall(json.dumps(request).encode('utf-8'))
            response_data = s.recv(4096)
            return json.loads(response_data.decode('utf-8'))
    except Exception as e:
        return {'status': 'error', 'message': str(e)}

# --- Gossip Protocol ---
def ping_peer(peer_addr):
    response = send_rpc_call(peer_addr[0], peer_addr[1], {'task': 'ping'}, timeout=2)
    return response and response.get('status') == 'success'

def gossip_with_peers():
    while True:
        with peer_list_lock:
            # Remove dead peers
            for peer in list(peer_list):
                if not ping_peer(peer):
                    peer_list.discard(peer)
                    print(f"[Gossip] Removed unresponsive peer: {peer}")

            # Gossip with random peer
            if peer_list:
                target_peer = random.choice(list(peer_list))
                request = {'task': 'gossip', 'known_peers': list(peer_list | {MY_ADDR})}
                response = send_rpc_call(target_peer[0], target_peer[1], request)
                if response and response.get('status') == 'success':
                    new_peers = {tuple(p) for p in response.get('peers', [])}
                    new_peers.discard(MY_ADDR)
                    peer_list.update(new_peers)
                else:
                    peer_list.discard(target_peer)
        time.sleep(10)

# --- Server ---
def handle_rpc_request(conn, addr):
    try:
        data = conn.recv(4096)
        if not data:
            return

        request = json.loads(data.decode('utf-8'))
        task = request.get('task')
        print(f"[LOG] Received task '{task}' from peer {addr}")

        if task == 'ping':
            response = {'status': 'success'}
            conn.sendall(json.dumps(response).encode('utf-8'))
            return

        if task == 'register':
            with peer_list_lock:
                new_peer = tuple(request['peer'])
                peer_list.add(new_peer)
                print(f"[LOG] Registered new peer: {new_peer}")
            response = {'status': 'success', 'peers': list(peer_list | {MY_ADDR})}
            conn.sendall(json.dumps(response).encode('utf-8'))
            return

        if task == 'gossip':
            with peer_list_lock:
                known_peers = {tuple(p) for p in request.get('known_peers', [])}
                known_peers.discard(MY_ADDR)
                peer_list.update(known_peers)
            response = {'status': 'success', 'peers': list(peer_list | {MY_ADDR})}
            conn.sendall(json.dumps(response).encode('utf-8'))
            return

        # File-based tasks
        if 'file_data' in request:
            file_data = request['file_data']
            file_name = request['file_name']
            os.makedirs(FILE_UPLOAD_DIR, exist_ok=True)
            file_path = os.path.join(FILE_UPLOAD_DIR, file_name)
            with open(file_path, 'w') as f:
                f.write(file_data)

        result = None
        if task == 'word_count':
            result = count_words(file_path)
        elif task == 'sha256':
            result = sha256_checksum(file_path)
        elif task == 'line_count':
            result = line_count(file_path)
        elif task == 'file_count':
            result = file_count(request.get('directory_path'))
        else:
            raise ValueError(f"Unknown task: {task}")

        if 'file_data' in request and os.path.exists(file_path):
            os.remove(file_path)

        response = {'status': 'success', 'result': result}
        conn.sendall(json.dumps(response).encode('utf-8'))

    except Exception as e:
        response = {'status': 'error', 'message': str(e)}
        conn.sendall(json.dumps(response).encode('utf-8'))
    finally:
        conn.close()


def peer_server():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(MY_ADDR)
        s.listen()
        print(f"[Server] Listening on {MY_ADDR}")
        while True:
            conn, addr = s.accept()
            threading.Thread(target=handle_rpc_request, args=(conn,), daemon=True).start()

# --- Client UI ---
def user_interface():
    time.sleep(3)
    while True:
        task_choice = input("\nEnter task (word_count, file_count, sha256, line_count, show_peers, exit): ").strip()

        if task_choice == 'exit':
            break

        if task_choice == 'show_peers':
            with peer_list_lock:
                print(f"Known peers: {peer_list | {MY_ADDR}}")
            continue

        if task_choice not in ['word_count', 'file_count', 'sha256', 'line_count']:
            print("Invalid task.")
            continue

        path = input("Enter file/directory path: ")
        if not os.path.exists(path):
            print("Path not found.")
            continue

        with peer_list_lock:
            if not peer_list:
                print("No peers available.")
                continue
            target_peer = random.choice(list(peer_list))

        request = {'task': task_choice}
        if task_choice == 'file_count':
            request['directory_path'] = path
        else:
            with open(path, 'r') as f:
                request['file_data'] = f.read()
                request['file_name'] = os.path.basename(path)

        response = send_rpc_call(target_peer[0], target_peer[1], request)
        if response and response.get('status') == 'success':
            print(f"Result from {target_peer}: {response['result']}")
        else:
            print(f"Error from {target_peer}: {response.get('message')}")

# --- Startup ---
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python peer.py <port> [known_peer_host:known_peer_port]")
        sys.exit(1)

    MY_PORT = int(sys.argv[1])
    MY_HOST = '127.0.0.1'
    MY_ADDR = (MY_HOST, MY_PORT)

    known_peer = None
    if len(sys.argv) == 3:
        host, port = sys.argv[2].split(':')
        known_peer = (host, int(port))

    # Start server and gossip
    threading.Thread(target=peer_server, daemon=True).start()
    threading.Thread(target=gossip_with_peers, daemon=True).start()

    # Register with known peer if provided
    if known_peer:
        print(f"[Startup] Registering with {known_peer}...")
        response = send_rpc_call(known_peer[0], known_peer[1], {'task': 'register', 'peer_addr': MY_ADDR})
        if response and response.get('status') == 'success':
            with peer_list_lock:
                new_peers = {tuple(p) for p in response.get('peers', [])}
                new_peers.discard(MY_ADDR)
                peer_list.update(new_peers)
            print(f"[Startup] Known peers: {peer_list}")
        else:
            print(f"[Startup] Failed to register with {known_peer}")

    user_interface()
