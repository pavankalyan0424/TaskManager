"""
Module Client.py
"""
import json
import random
import socket
import time

from config import GOSSIP_INTERVAL_SECONDS


def send_rpc_call(host, port, request, auth_token, my_addr, timeout=5):
    if "sender" not in request and my_addr is not None:
        request["sender"] = [my_addr[0], my_addr[1]]
    request["auth_token"] = auth_token

    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(timeout)
            s.connect((host, port))
            s.sendall(json.dumps(request).encode("utf-8"))
            # read until socket closes or one large recv (assignment simplicity)
            resp_bytes = s.recv(65536)
            if not resp_bytes:
                return {"status": "error", "message": "no response"}
            return json.loads(resp_bytes.decode("utf-8"))
    except Exception as e:
        return {"status": "error", "message": str(e)}


def register_with_peer(peer, peer_list, peer_list_lock, my_addr, auth_token):
    host, port = peer
    # keep feedback but minimal
    resp = send_rpc_call(host, port, {"task": "register"}, auth_token, my_addr)
    if resp and resp.get("status") == "success":
        peers = resp.get("peers", [])
        new = {tuple(p) for p in peers}
        new.discard(my_addr)
        with peer_list_lock:
            peer_list.update(new)
            peer_list.add(peer)
        return True
    return False


# --------------------
# Gossip (silent)
# --------------------
def ping_peer(peer_addr, auth_token, my_addr):
    resp = send_rpc_call(
        peer_addr[0], peer_addr[1], {"task": "ping"}, auth_token, my_addr, timeout=2
    )
    return bool(resp and resp.get("status") == "success")


def gossip_with_peers(peer_list, peer_list_lock, my_addr, seed_peers, auth_token):
    while True:
        try:
            with peer_list_lock:
                peers_snapshot = list(peer_list)
            for p in peers_snapshot:
                if not ping_peer(p, auth_token, my_addr):
                    with peer_list_lock:
                        peer_list.discard(p)

            with peer_list_lock:
                empty = len(peer_list) == 0
            if empty and seed_peers:
                for s in seed_peers:
                    if s != my_addr and ping_peer(s, auth_token, my_addr):
                        with peer_list_lock:
                            peer_list.add(s)
                        break

            with peer_list_lock:
                peers_snapshot = list(peer_list)
            if peers_snapshot:
                target = random.choice(peers_snapshot)
                req = {
                    "task": "gossip",
                    "known_peers": [list(p) for p in (set(peers_snapshot) | {my_addr})],
                }
                resp = send_rpc_call(target[0], target[1], req, auth_token, my_addr)
                if resp and resp.get("status") == "success":
                    new_peers = {tuple(p) for p in resp.get("peers", [])}
                    new_peers.discard(my_addr)
                    with peer_list_lock:
                        peer_list.update(new_peers)
                else:
                    with peer_list_lock:
                        peer_list.discard(target)
        except Exception:
            pass
        time.sleep(GOSSIP_INTERVAL_SECONDS)
