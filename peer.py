#!/usr/bin/env python3
"""
P2P peer (simple gossip + RPC). Usage:
  python peer.py <port> [known_peer_host:known_peer_port]

Notes:
 - If you start a "first" peer, run with only port.
 - Later peers can bootstrap by passing any single known peer (host:port).
 - Tasks are executed remotely via RPC. Logging added for send/receive/process.
"""

import socket, json, hashlib, os, sys, threading, time, random

# --------------------
# Task utilities
# --------------------
FILE_UPLOAD_DIR = './peer_uploads/'

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
MY_ADDR = None               # set at startup to (host, port)
peer_list = set()            # set of (host, port)
peer_list_lock = threading.Lock()

# Optional seed peers (you can leave empty to force explicit bootstrap)
SEED_PEERS = []  # e.g. [('127.0.0.1', 65431)]

# --------------------
# RPC (connection per request)
# --------------------
def send_rpc_call(host, port, request, timeout=5):
    # attach our logical listening address so the receiver knows who is asking
    if 'sender' not in request and MY_ADDR is not None:
        request['sender'] = [MY_ADDR[0], MY_ADDR[1]]

    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(timeout)
            s.connect((host, port))
            s.sendall(json.dumps(request).encode('utf-8'))
            # simple single-read response (ok for this assignment)
            resp_bytes = s.recv(65536)
            if not resp_bytes:
                return {'status': 'error', 'message': 'no response'}
            return json.loads(resp_bytes.decode('utf-8'))
    except Exception as e:
        return {'status': 'error', 'message': str(e)}

def register_with_peer(peer):
    """Ask a peer to register us and return its known peer list."""
    host, port = peer
    print(f"[BOOT] Registering with {peer} ...")
    resp = send_rpc_call(host, port, {'task': 'register'})
    if resp and resp.get('status') == 'success':
        peers = resp.get('peers', [])
        new = {tuple(p) for p in peers}
        # remove self if present
        new.discard(MY_ADDR)
        with peer_list_lock:
            peer_list.update(new)
            peer_list.add(peer)  # ensure bootstrap peer included
        print(f"[BOOT] Received peers: {new}")
        return True
    else:
        print(f"[BOOT] Failed to register with {peer}: {resp.get('message') if resp else resp}")
        return False

# --------------------
# Gossip protocol
# --------------------
def ping_peer(peer_addr):
    resp = send_rpc_call(peer_addr[0], peer_addr[1], {'task': 'ping'}, timeout=2)
    return bool(resp and resp.get('status') == 'success')

def gossip_with_peers():
    while True:
        try:
            # ping & prune
            with peer_list_lock:
                peers_snapshot = list(peer_list)
            for p in peers_snapshot:
                if not ping_peer(p):
                    with peer_list_lock:
                        peer_list.discard(p)
                    print(f"[GOSSIP] Removed unresponsive {p}")

            # try seeds if empty
            with peer_list_lock:
                empty = len(peer_list) == 0
            if empty and SEED_PEERS:
                for s in SEED_PEERS:
                    if s != MY_ADDR and ping_peer(s):
                        with peer_list_lock:
                            peer_list.add(s)
                        print(f"[GOSSIP] Added seed {s}")
                        break

            # gossip with a random peer (exchange known peers)
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
                        after = peer_list
                    if new_peers - before:
                        print(f"[GOSSIP] Learned peers {new_peers - before}")
                else:
                    # remove target if failed
                    with peer_list_lock:
                        peer_list.discard(target)
        except Exception as e:
            print(f"[GOSSIP] error: {e}")
        time.sleep(10)

# --------------------
# Server (handle requests)
# --------------------
def handle_rpc_request(conn, addr):
    try:
        data = conn.recv(65536)
        if not data:
            return
        request = json.loads(data.decode('utf-8'))
        task = request.get('task')
        # Prefer sender field (explicit), otherwise use TCP addr (may be ephemeral)
        sender = None
        if request.get('sender'):
            s = request.get('sender')
            try:
                sender = (s[0], int(s[1]))
            except Exception:
                sender = None
        if not sender:
            # fallback (note: this is the client's ephemeral port)
            sender = (addr[0], addr[1])

        # log receipt
        print(f"[RECV] Task '{task}' from sender {sender}")

        # If sender is a real peer (listening addr), add to peer list.
        if sender != MY_ADDR:
            with peer_list_lock:
                if sender not in peer_list:
                    peer_list.add(sender)
                    print(f"[PEER] Added {sender} to peer_list")

        # Handle maintenance tasks
        if task == 'ping':
            conn.sendall(json.dumps({'status': 'success'}).encode('utf-8'))
            return

        if task == 'register':
            # add the sender to our peer list and return our known peers
            with peer_list_lock:
                if sender != MY_ADDR:
                    peer_list.add(sender)
                peers_out = [list(p) for p in (peer_list | {MY_ADDR})]
            print(f"[RECV] register from {sender}, replying with {peers_out}")
            conn.sendall(json.dumps({'status': 'success', 'peers': peers_out}).encode('utf-8'))
            return

        if task == 'gossip':
            incoming = request.get('known_peers', [])
            incoming_set = {tuple(p) for p in incoming}
            incoming_set.discard(MY_ADDR)
            with peer_list_lock:
                old = set(peer_list)
                peer_list.update(incoming_set)
                peers_out = [list(p) for p in (peer_list | {MY_ADDR})]
            print(f"[RECV] gossip from {sender}. learned {incoming_set - old}")
            conn.sendall(json.dumps({'status': 'success', 'peers': peers_out}).encode('utf-8'))
            return

        # file_metadata: compute metadata for given file path on the receiver
        if task == 'file_metadata':
            fp = request.get('file_path')
            print(f"[PROC] file_metadata request for '{fp}' from {sender}")
            res = get_file_metadata(fp)
            conn.sendall(json.dumps({'status': 'success', 'result': res}).encode('utf-8'))
            return

        # generic file-based tasks: may include 'file_data' or reference dir/path
        file_path = None
        temp_written = False

        if 'file_data' in request:
            # writer writes data to temp file and executes task on it
            fname = request.get('file_name', f"tmp_{int(time.time()*1000)}")
            os.makedirs(FILE_UPLOAD_DIR, exist_ok=True)
            file_path = os.path.join(FILE_UPLOAD_DIR, fname)
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(request['file_data'])
            temp_written = True

        # execute requested task
        if task in TASKS:
            # file_count expects directory_path argument instead of file_path
            if task == 'file_count':
                arg = request.get('directory_path')
            else:
                # prefer an explicit file_path in request if provided (rare)
                arg = request.get('file_path') or file_path
            print(f"[PROC] Executing '{task}' for {sender} with arg={arg}")
            result = TASKS[task](arg)
            # cleanup
            if temp_written and file_path and os.path.exists(file_path):
                try:
                    os.remove(file_path)
                except Exception:
                    pass
            conn.sendall(json.dumps({'status': 'success', 'result': result}).encode('utf-8'))
            return

        # unknown task
        conn.sendall(json.dumps({'status': 'error', 'message': f'Unknown task {task}'}).encode('utf-8'))

    except Exception as e:
        try:
            conn.sendall(json.dumps({'status': 'error', 'message': str(e)}).encode('utf-8'))
        except Exception:
            pass
        print(f"[ERR] in handler: {e}")
    finally:
        try:
            conn.close()
        except Exception:
            pass

def peer_server():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(MY_ADDR)
        s.listen(5)
        print(f"[SERVER] Listening on {MY_ADDR}")
        while True:
            conn, addr = s.accept()
            t = threading.Thread(target=handle_rpc_request, args=(conn, addr), daemon=True)
            t.start()

# --------------------
# CLI / UI
# --------------------
def user_interface():
    time.sleep(1.5)
    prompt = "\nEnter task (word_count, file_count, sha256, line_count, file_metadata, show_peers, exit): "
    while True:
        cmd = input(prompt).strip()
        if cmd == 'exit':
            print("[UI] Exiting.")
            break

        if cmd == 'show_peers':
            with peer_list_lock:
                print(f"[PEERS] {peer_list | {MY_ADDR}}")
            continue

        if cmd == 'file_metadata':
            path = input("Enter path to the file (path as seen on each peer): ").strip()
            if not path:
                print("Empty path.")
                continue
            with peer_list_lock:
                targets = list(peer_list)
            if not targets:
                print("No peers available to query.")
                continue
            print(f"[SEND] Broadcasting file_metadata to {len(targets)} peers...")
            for p in targets:
                print(f"[SEND] -> {p} (task=file_metadata)")
                resp = send_rpc_call(p[0], p[1], {'task': 'file_metadata', 'file_path': path})
                if resp and resp.get('status') == 'success':
                    print(f"\n[REPLY] from {p}:")
                    for k, v in resp['result'].items():
                        print(f"  {k}: {v}")
                else:
                    print(f"[ERR] from {p}: {resp.get('message') if resp else resp}")
            continue

        # other single-target tasks
        if cmd not in TASKS:
            print("Invalid task.")
            continue

        path = input("Enter file/directory path: ").strip()
        if not path:
            print("Empty path.")
            continue

        # choose a peer
        with peer_list_lock:
            targets = list(peer_list)
        if not targets:
            print("No peers available to run the task.")
            continue
        target = random.choice(targets)

        # prepare request: send file_data for file-based tasks except file_count (directory)
        req = {'task': cmd}
        if cmd == 'file_count':
            req['directory_path'] = path
        else:
            try:
                with open(path, 'r', encoding='utf-8', errors='replace') as f:
                    data = f.read()
                req['file_data'] = data
                req['file_name'] = os.path.basename(path)
            except Exception as e:
                print(f"Error reading file: {e}")
                continue

        # logging
        print(f"[SEND] Sending task '{cmd}' to {target}")
        resp = send_rpc_call(target[0], target[1], req)
        if resp and resp.get('status') == 'success':
            print(f"[RESULT] from {target}: {resp.get('result')}")
        else:
            print(f"[ERR] from {target}: {resp.get('message') if resp else resp}")

# --------------------
# Main / startup
# --------------------
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python peer.py <port> [known_peer_host:known_peer_port]")
        sys.exit(1)

    try:
        port = int(sys.argv[1])
    except:
        print("Port must be integer")
        sys.exit(1)

    host = '127.0.0.1'
    MY_ADDR = (host, port)

    # Optional bootstrap peer
    bootstrap = None
    if len(sys.argv) == 3:
        try:
            h, p = sys.argv[2].split(':')
            bootstrap = (h, int(p))
        except:
            print("Bootstrap peer format must be host:port")
            sys.exit(1)

    # start server + gossip
    threading.Thread(target=peer_server, daemon=True).start()
    threading.Thread(target=gossip_with_peers, daemon=True).start()

    # if bootstrap provided, register with it and get initial peers
    if bootstrap:
        success = register_with_peer(bootstrap)
        if not success:
            print("[BOOT] Warning: could not register with bootstrap peer.")

    # UI loop
    user_interface()
