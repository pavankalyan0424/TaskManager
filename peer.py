#!/usr/bin/env python3
"""
P2P peer (simple gossip + RPC with distributed file_metadata + shared secret auth).
Usage:
  python peer.py <port> [known_peer_host:known_peer_port]
"""

import socket, json, hashlib, os, sys, threading, time, random

# --------------------
# Config & Security
# --------------------
FILE_UPLOAD_DIR = './peer_uploads/'
AUTH_TOKEN = "supersecret123"  # all peers must have the same token

# --------------------
# Task utilities
# --------------------
def count_words(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
            return len(f.read().split())
    except FileNotFoundError:
        return 0

def sha256_checksum(file_path):
    try:
        h = hashlib.sha256()
        with open(file_path, 'rb') as f:
            for block in iter(lambda: f.read(4096), b''):
                h.update(block)
        return h.hexdigest()
    except FileNotFoundError:
        return "File not found"

def line_count(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
            return sum(1 for _ in f)
    except FileNotFoundError:
        return 0

def file_count(directory_path):
    if not os.path.isdir(directory_path):
        return "Path is not a directory"
    return len(os.listdir(directory_path))

def get_file_metadata(file_path):
    try:
        size_bytes = os.path.getsize(file_path)
        return {
            'size_bytes': size_bytes,
            'word_count': count_words(file_path),
            'line_count': line_count(file_path),
            'sha256': sha256_checksum(file_path)
        }
    except Exception as e:
        return {'error': str(e)}

TASKS = {
    'word_count': count_words,
    'sha256': sha256_checksum,
    'line_count': line_count,
    'file_count': file_count
}

# --------------------
# Global P2P state
# --------------------
MY_ADDR = None
peer_list = set()
peer_list_lock = threading.Lock()
SEED_PEERS = []

# --------------------
# RPC
# --------------------
def send_rpc_call(host, port, request, timeout=5):
    if 'sender' not in request and MY_ADDR is not None:
        request['sender'] = [MY_ADDR[0], MY_ADDR[1]]
    request['auth_token'] = AUTH_TOKEN

    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(timeout)
            s.connect((host, port))
            s.sendall(json.dumps(request).encode('utf-8'))
            resp_bytes = s.recv(65536)
            if not resp_bytes:
                return {'status': 'error', 'message': 'no response'}
            return json.loads(resp_bytes.decode('utf-8'))
    except Exception as e:
        return {'status': 'error', 'message': str(e)}

def register_with_peer(peer):
    host, port = peer
    print(f"[BOOT] Registering with {peer} ...")
    resp = send_rpc_call(host, port, {'task': 'register'})
    if resp and resp.get('status') == 'success':
        peers = resp.get('peers', [])
        new = {tuple(p) for p in peers}
        new.discard(MY_ADDR)
        with peer_list_lock:
            peer_list.update(new)
            peer_list.add(peer)
        print(f"[BOOT] Received peers: {new}")
        return True
    else:
        print(f"[BOOT] Failed to register with {peer}: {resp.get('message') if resp else resp}")
        return False

# --------------------
# Gossip
# --------------------
def ping_peer(peer_addr):
    resp = send_rpc_call(peer_addr[0], peer_addr[1], {'task': 'ping'}, timeout=2)
    return bool(resp and resp.get('status') == 'success')

def gossip_with_peers():
    while True:
        try:
            with peer_list_lock:
                peers_snapshot = list(peer_list)
            for p in peers_snapshot:
                if not ping_peer(p):
                    with peer_list_lock:
                        peer_list.discard(p)
                    print(f"[GOSSIP] Removed unresponsive {p}")

            with peer_list_lock:
                empty = len(peer_list) == 0
            if empty and SEED_PEERS:
                for s in SEED_PEERS:
                    if s != MY_ADDR and ping_peer(s):
                        with peer_list_lock:
                            peer_list.add(s)
                        print(f"[GOSSIP] Added seed {s}")
                        break

            with peer_list_lock:
                peers_snapshot = list(peer_list)
            if peers_snapshot:
                target = random.choice(peers_snapshot)
                req = {'task': 'gossip', 'known_peers': [list(p) for p in (set(peers_snapshot) | {MY_ADDR})]}
                resp = send_rpc_call(target[0], target[1], req)
                if resp and resp.get('status') == 'success':
                    new_peers = {tuple(p) for p in resp.get('peers', [])}
                    new_peers.discard(MY_ADDR)
                    with peer_list_lock:
                        before = set(peer_list)
                        peer_list.update(new_peers)
                    if new_peers - before:
                        print(f"[GOSSIP] Learned peers {new_peers - before}")
                else:
                    with peer_list_lock:
                        peer_list.discard(target)
        except Exception as e:
            print(f"[GOSSIP] error: {e}")
        time.sleep(10)

# --------------------
# Server
# --------------------
def handle_rpc_request(conn, addr):
    try:
        data = conn.recv(65536)
        if not data:
            return
        request = json.loads(data.decode('utf-8'))

        # auth check
        if request.get('auth_token') != AUTH_TOKEN:
            conn.sendall(json.dumps({'status': 'error', 'message': 'Auth failed'}).encode('utf-8'))
            return

        task = request.get('task')
        sender = None
        if request.get('sender'):
            try:
                sender = (request['sender'][0], int(request['sender'][1]))
            except Exception:
                sender = None
        if not sender:
            sender = (addr[0], addr[1])

        # explicit log that receiver got task from sender
        print(f"[PROC] Received task '{task}' from peer {sender}")

        if sender != MY_ADDR:
            with peer_list_lock:
                peer_list.add(sender)

        if task == 'ping':
            conn.sendall(json.dumps({'status': 'success'}).encode('utf-8'))
            return

        if task == 'register':
            with peer_list_lock:
                peer_list.add(sender)
                peers_out = [list(p) for p in (peer_list | {MY_ADDR})]
            conn.sendall(json.dumps({'status': 'success', 'peers': peers_out}).encode('utf-8'))
            return

        if task == 'gossip':
            incoming = request.get('known_peers', [])
            incoming_set = {tuple(p) for p in incoming}
            incoming_set.discard(MY_ADDR)
            with peer_list_lock:
                peer_list.update(incoming_set)
                peers_out = [list(p) for p in (peer_list | {MY_ADDR})]
            conn.sendall(json.dumps({'status': 'success', 'peers': peers_out}).encode('utf-8'))
            return

        if task == 'file_metadata':
            fp = request.get('file_path')
            if not fp:
                conn.sendall(json.dumps({'status': 'error', 'message': 'No file path provided'}).encode('utf-8'))
                return
            # explicit log for file_metadata processing
            print(f"[PROC] Received file_metadata for '{fp}' from peer {sender}")
            res = get_file_metadata(fp)
            conn.sendall(json.dumps({'status': 'success', 'result': res}).encode('utf-8'))
            return

        if task in TASKS:
            if task == 'file_count':
                arg = request.get('directory_path')
            else:
                arg = request.get('file_path')
            result = TASKS[task](arg)
            conn.sendall(json.dumps({'status': 'success', 'result': result}).encode('utf-8'))
            return

        conn.sendall(json.dumps({'status': 'error', 'message': f'Unknown task {task}'}).encode('utf-8'))

    except Exception as e:
        try:
            conn.sendall(json.dumps({'status': 'error', 'message': str(e)}).encode('utf-8'))
        except:
            pass
        print(f"[ERR] in handler: {e}")
    finally:
        try:
            conn.close()
        except:
            pass

def peer_server():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(MY_ADDR)
        s.listen(5)
        print(f"[SERVER] Listening on {MY_ADDR}")
        while True:
            conn, addr = s.accept()
            threading.Thread(target=handle_rpc_request, args=(conn, addr), daemon=True).start()

# --------------------
# CLI / UI
# --------------------
def user_interface():
    time.sleep(1.5)
    prompt = "\nEnter task (word_count, file_count, sha256, line_count, file_metadata, show_peers, exit): "
    while True:
        cmd = input(prompt).strip()
        if cmd == 'exit':
            break

        if cmd == 'show_peers':
            with peer_list_lock:
                print(f"[PEERS] {peer_list | {MY_ADDR}}")
            continue

        if cmd == 'file_metadata':
            paths = input("Enter file path(s), separated by commas: ").strip()
            if not paths:
                continue
            file_list = [p.strip() for p in paths.split(',') if p.strip()]

            with peer_list_lock:
                targets = list(peer_list | {MY_ADDR})
            if not targets:
                print("No peers available.")
                continue

            results = {}
            threads = []

            # Single-file -> pick 1 peer (could be self)
            if len(file_list) == 1:
                chosen_peer = random.choice(targets)
                print(f"[SEND] Sending file_metadata for '{file_list[0]}' to {chosen_peer}")
                if chosen_peer == MY_ADDR:
                    print(f"[PROC] Local processing of '{file_list[0]}'")
                    results[(chosen_peer, file_list[0])] = {'status': 'success', 'result': get_file_metadata(file_list[0])}
                else:
                    resp = send_rpc_call(chosen_peer[0], chosen_peer[1], {'task': 'file_metadata', 'file_path': file_list[0]})
                    results[(chosen_peer, file_list[0])] = resp
            else:
                # Multiple files -> distribute round-robin across targets (including self)
                print(f"[SEND] Distributing {len(file_list)} files among {len(targets)} peers...")
                for i, fp in enumerate(file_list):
                    peer = targets[i % len(targets)]
                    def query_peer(p=peer, fpath=fp):
                        print(f"[SEND] {fpath} -> {p}")
                        if p == MY_ADDR:
                            results[(p, fpath)] = {'status': 'success', 'result': get_file_metadata(fpath)}
                        else:
                            resp = send_rpc_call(p[0], p[1], {'task': 'file_metadata', 'file_path': fpath})
                            results[(p, fpath)] = resp
                    t = threading.Thread(target=query_peer)
                    threads.append(t)
                    t.start()

            for t in threads:
                t.join()

            # print collected results
            for (peer, fpath), resp in results.items():
                if resp and resp.get('status') == 'success':
                    print(f"\n[REPLY] from {peer} for '{fpath}':")
                    for k, v in resp['result'].items():
                        print(f"  {k}: {v}")
                else:
                    print(f"[ERR] from {peer} for '{fpath}': {resp.get('message') if resp else resp}")
            continue

        if cmd not in TASKS:
            print("Invalid task.")
            continue

        path = input("Enter file/directory path: ").strip()
        if not path:
            continue
        with peer_list_lock:
            targets = list(peer_list)
        if not targets:
            print("No peers available.")
            continue
        target = random.choice(targets)

        req = {'task': cmd}
        if cmd == 'file_count':
            req['directory_path'] = path
        else:
            req['file_path'] = path

        print(f"[SEND] Sending '{cmd}' to {target}")
        resp = send_rpc_call(target[0], target[1], req)
        if resp and resp.get('status') == 'success':
            print(f"[RESULT] from {target}: {resp.get('result')}")
        else:
            print(f"[ERR] from {target}: {resp.get('message') if resp else resp}")

# --------------------
# Main
# --------------------
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python peer.py <port> [known_peer_host:known_peer_port]")
        sys.exit(1)

    port = int(sys.argv[1])
    host = '127.0.0.1'
    MY_ADDR = (host, port)

    bootstrap = None
    if len(sys.argv) == 3:
        h, p = sys.argv[2].split(':')
        bootstrap = (h, int(p))

    threading.Thread(target=peer_server, daemon=True).start()
    threading.Thread(target=gossip_with_peers, daemon=True).start()

    if bootstrap:
        register_with_peer(bootstrap)

    user_interface()
