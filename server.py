"""
Module Server.py
"""
import json
import logging
import socket
import threading


from tasks import get_file_metadata, TASKS

logger = logging.getLogger(__name__)


def handle_rpc_request(conn, addr, peer_list, peer_list_lock, my_addr, auth_token):
    try:
        data = conn.recv(65536)
        if not data:
            return
        request = json.loads(data.decode("utf-8"))

        # auth check
        if request.get("auth_token") != auth_token:
            conn.sendall(
                json.dumps({"status": "error", "message": "Auth failed"}).encode(
                    "utf-8"
                )
            )
            return

        task = request.get("task")
        sender = None
        if request.get("sender"):
            try:
                sender = (request["sender"][0], int(request["sender"][1]))
            except Exception:
                sender = None
        if not sender:
            sender = (addr[0], addr[1])

        # explicit log that receiver got task from sender
        if task not in ("ping", "gossip"):
            logger.info(f"\n[PROC] Received task '{task}' from peer {sender}")

        # add sender to peer list (silently)
        if sender != my_addr:
            with peer_list_lock:
                peer_list.add(sender)

        # maintenance tasks
        if task == "ping":
            conn.sendall(json.dumps({"status": "success"}).encode("utf-8"))
            return

        if task == "register":
            with peer_list_lock:
                peer_list.add(sender)
                peers_out = [list(p) for p in (peer_list | {my_addr})]
            conn.sendall(
                json.dumps({"status": "success", "peers": peers_out}).encode("utf-8")
            )
            return

        if task == "gossip":
            incoming = request.get("known_peers", [])
            incoming_set = {tuple(p) for p in incoming}
            incoming_set.discard(my_addr)
            with peer_list_lock:
                peer_list.update(incoming_set)
                peers_out = [list(p) for p in (peer_list | {my_addr})]
            conn.sendall(
                json.dumps({"status": "success", "peers": peers_out}).encode("utf-8")
            )
            return

        # file metadata handling
        if task == "file_metadata":
            fp = request.get("file_path")
            if not fp:
                conn.sendall(
                    json.dumps(
                        {"status": "error", "message": "No file path provided"}
                    ).encode("utf-8")
                )
                return
            # explicit log for file_metadata processing
            logger.info(f"[PROC] Received file_metadata for '{fp}' from peer {sender}")
            res = get_file_metadata(fp)
            conn.sendall(
                json.dumps({"status": "success", "result": res}).encode("utf-8")
            )
            return

        # generic tasks
        tasks = TASKS.keys()
        if task in tasks:
            if task == "file_count":
                arg = request.get("directory_path")
            else:
                arg = request.get("file_path")
            result = TASKS[task](arg)
            conn.sendall(
                json.dumps({"status": "success", "result": result}).encode("utf-8")
            )
            return

        conn.sendall(
            json.dumps({"status": "error", "message": f"Unknown task {task}"}).encode(
                "utf-8"
            )
        )

    except Exception as e:
        try:
            conn.sendall(
                json.dumps({"status": "error", "message": str(e)}).encode("utf-8")
            )
        except Exception:
            pass
        logger.error(f"in handler: {e}", exc_info=True)
    finally:
        try:
            conn.close()
        except Exception:
            pass


def peer_server(my_addr, peer_list, peer_list_lock, auth_token):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(my_addr)
        s.listen(5)
        logger.info(f"[SERVER] Listening on {my_addr}")
        while True:
            conn, addr = s.accept()
            threading.Thread(
                target=handle_rpc_request,
                args=(conn, addr, peer_list, peer_list_lock, my_addr, auth_token),
                daemon=True,
            ).start()
