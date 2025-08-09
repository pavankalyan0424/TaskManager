import socket
import json
import hashlib
import os
import sys
import time
import threading


# --- Task functions (same as before) ---
def count_words(file_path):
    with open(file_path, 'r') as f:
        return len(f.read().split())


def sha256_checksum(file_path):
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()


def line_count(file_path):
    with open(file_path, 'r') as f:
        return sum(1 for line in f)


# --- Communication functions ---
def send_rpc_call(host, port, request):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((host, port))
            s.sendall(json.dumps(request).encode('utf-8'))
            response_data = s.recv(4096)
            return json.loads(response_data.decode('utf-8'))
    except Exception as e:
        return {'status': 'error', 'message': str(e)}


def heartbeat(scheduler_host, scheduler_port, worker_addr):
    while True:
        request = {
            'task': 'heartbeat',
            'worker_addr': worker_addr
        }
        response = send_rpc_call(scheduler_host, scheduler_port, request)
        if response['status'] == 'error':
            print(f"Failed to send heartbeat to scheduler: {response['message']}")
        time.sleep(5)  # Send a heartbeat every 5 seconds


def handle_rpc_request(conn, FILE_UPLOAD_DIR):
    try:
        data = conn.recv(4096)
        if not data:
            return

        request = json.loads(data.decode('utf-8'))
        task = request['task']
        file_data = request['file_data']
        file_name = request['file_name']

        if not os.path.exists(FILE_UPLOAD_DIR):
            os.makedirs(FILE_UPLOAD_DIR)

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
        else:
            raise ValueError(f"Unknown task: {task}")

        os.remove(file_path)

        response = {'status': 'success', 'result': result}
        conn.sendall(json.dumps(response).encode('utf-8'))

    except Exception as e:
        response = {'status': 'error', 'message': str(e)}
        conn.sendall(json.dumps(response).encode('utf-8'))


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python worker.py <port> <scheduler_port>")
        sys.exit(1)

    PORT = int(sys.argv[1])
    SCHEDULER_PORT = int(sys.argv[2])
    HOST = '127.0.0.1'
    SCHEDULER_HOST = '127.0.0.1'

    worker_addr = (HOST, PORT)
    scheduler_addr = (SCHEDULER_HOST, SCHEDULER_PORT)

    # 1. Register with the scheduler
    print("Registering with scheduler...")
    registration_request = {
        'task': 'register',
        'worker_addr': worker_addr
    }
    response = send_rpc_call(SCHEDULER_HOST, SCHEDULER_PORT, registration_request)
    if response['status'] == 'error':
        print(f"Failed to register with scheduler: {response['message']}")
        sys.exit(1)
    print("Registration successful.")

    # 2. Start heartbeat thread
    heartbeat_thread = threading.Thread(target=heartbeat, args=(SCHEDULER_HOST, SCHEDULER_PORT, worker_addr))
    heartbeat_thread.daemon = True
    heartbeat_thread.start()

    # 3. Listen for tasks from the scheduler
    FILE_UPLOAD_DIR = f'./worker_uploads_{PORT}/'
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((HOST, PORT))
        s.listen()
        print(f"Worker listening for tasks on {HOST}:{PORT}")

        while True:
            conn, addr = s.accept()
            print(f"Task request received from {addr}")
            task_thread = threading.Thread(target=handle_rpc_request, args=(conn, FILE_UPLOAD_DIR))
            task_thread.start()