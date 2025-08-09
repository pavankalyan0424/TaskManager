import socket
import json
import threading
import time

SCHEDULER_HOST = '127.0.0.1'
SCHEDULER_PORT = 50000

# Dynamic list of active workers
active_workers = {}  # {worker_addr: last_heartbeat_time}
tasks = {}  # {task_id: {'status': 'pending', 'result': None}}
task_id_counter = 0
lock = threading.Lock()


def check_workers_health():
    while True:
        with lock:
            offline_workers = [addr for addr, last_time in active_workers.items() if time.time() - last_time > 10]
            for addr in offline_workers:
                print(f"Worker {addr} timed out, removing.")
                del active_workers[addr]
        time.sleep(5)


def get_next_worker():
    with lock:
        if not active_workers:
            return None
        worker_list = list(active_workers.keys())
        next_worker_addr = worker_list[0]
        # Simple round-robin: move the first worker to the end
        worker_list.pop(0)
        worker_list.append(next_worker_addr)
        active_workers.clear()
        for worker in worker_list:
            active_workers[worker] = time.time()
        return next_worker_addr


def forward_to_worker(worker_addr, task_id, task, file_data, file_name):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect(worker_addr)
            request = {
                'task_id': task_id,
                'task': task,
                'file_name': file_name,
                'file_data': file_data
            }
            s.sendall(json.dumps(request).encode('utf-8'))
            response_data = s.recv(4096)
            print(response_data)
            return json.loads(response_data.decode('utf-8'))
    except Exception as e:
        return {'status': 'error', 'message': f"Worker at {worker_addr} failed: {str(e)}"}


def handle_request(conn):
    data = conn.recv(4096)
    if not data:
        conn.close()
        return

    request = json.loads(data.decode('utf-8'))

    # Check the type of request
    if 'task' in request:
        if request['task'] == 'register':
            worker_addr = tuple(request['worker_addr'])
            with lock:
                active_workers[worker_addr] = time.time()
            print(f"Worker {worker_addr} registered.")
            conn.sendall(json.dumps({'status': 'success'}).encode('utf-8'))
        elif request['task'] == 'heartbeat':
            worker_addr = tuple(request['worker_addr'])
            with lock:
                if worker_addr in active_workers:
                    active_workers[worker_addr] = time.time()
            conn.sendall(json.dumps({'status': 'success'}).encode('utf-8'))
        elif request['task'] == 'result':
            # This is a result from a worker
            task_id = request.get('task_id')
            result = request.get('result')
            if task_id is not None and result is not None:
                with lock:
                    if task_id in tasks:
                        tasks[task_id]['status'] = 'completed'
                        tasks[task_id]['result'] = result
                        print(f"Task {task_id} completed. Result: {result}")
            conn.sendall(json.dumps({'status': 'success'}).encode('utf-8'))
        else:  # This is a new task from a client
            with lock:
                global task_id_counter
                task_id = task_id_counter
                task_id_counter += 1
                tasks[task_id] = {'status': 'pending'}

            worker_addr = get_next_worker()
            if not worker_addr:
                response = {'status': 'error', 'message': 'No workers available.'}
            else:
                print(f"Assigning task {task_id} to worker {worker_addr}")
                response = forward_to_worker(worker_addr, task_id, request['task'], request['file_data'],
                                             request['file_name'])

            conn.sendall(json.dumps(response).encode('utf-8'))
    else:
        conn.sendall(json.dumps({'status': 'error', 'message': 'Invalid request format'}).encode('utf-8'))
    conn.close()


if __name__ == "__main__":
    health_check_thread = threading.Thread(target=check_workers_health)
    health_check_thread.daemon = True
    health_check_thread.start()

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((SCHEDULER_HOST, SCHEDULER_PORT))
        s.listen()
        print(f"Scheduler listening on {SCHEDULER_HOST}:{SCHEDULER_PORT}")

        while True:
            conn, addr = s.accept()
            thread = threading.Thread(target=handle_request, args=(conn,))
            thread.start()