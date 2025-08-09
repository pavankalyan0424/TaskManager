"""
Module for command line interface
"""
import threading
import random
import logging
from client import send_rpc_call
from tasks import TASKS

logger = logging.getLogger(__name__)


def user_interface(peer_list, peer_list_lock, my_addr, auth_token):
    prompt = "\nEnter task (word_count, file_count, sha256, line_count, file_metadata, show_peers, help, exit): "
    while True:
        try:
            cmd = input(prompt).strip().lower()

            if cmd == "":
                # Just re-prompt on empty input
                continue

            if cmd == "exit":
                print("Exiting...")
                break

            if cmd == "help":
                print("Available commands:")
                for task in sorted(list(TASKS.keys()) + ["show_peers", "help", "exit"]):
                    print(f" - {task}")
                continue

            if cmd == "show_peers":
                with peer_list_lock:
                    peers = peer_list | {my_addr}
                print(f"[PEERS] {peers}")
                continue

            if cmd == "file_metadata":
                paths = input("Enter file path(s), separated by commas: ").strip()
                if not paths:
                    continue
                file_list = [p.strip() for p in paths.split(",") if p.strip()]

                with peer_list_lock:
                    targets = list(peer_list | {my_addr})
                if not targets:
                    print("No peers available.")
                    continue

                results = {}
                results_lock = threading.Lock()
                threads = []

                def query_peer(p, fpath):
                    try:
                        logger.info(
                            f"Sending file_metadata task for '{fpath}' to peer {p}"
                        )
                        if p == my_addr:
                            res = {
                                "status": "success",
                                "result": TASKS["file_metadata"](fpath),
                            }
                        else:
                            res = send_rpc_call(
                                p[0],
                                p[1],
                                {"task": "file_metadata", "file_path": fpath},
                                auth_token,my_addr
                            )
                    except Exception as e:
                        res = {"status": "error", "message": str(e)}
                    with results_lock:
                        results[(p, fpath)] = res

                if len(file_list) == 1:
                    chosen_peer = random.choice(targets)
                    query_peer(chosen_peer, file_list[0])
                else:
                    for i, fp in enumerate(file_list):
                        peer = targets[i % len(targets)]
                        t = threading.Thread(
                            target=query_peer, args=(peer, fp), daemon=True
                        )
                        threads.append(t)
                        t.start()

                    for t in threads:
                        t.join()

                for (peer, fpath), resp in results.items():
                    if resp and resp.get("status") == "success":
                        print(f"\n[REPLY] from {peer} for '{fpath}':")
                        for k, v in resp["result"].items():
                            print(f"  {k}: {v}")
                    else:
                        print(
                            f"[ERR] from {peer} for '{fpath}': {resp.get('message') if resp else 'No response'}"
                        )

                continue

            if cmd not in TASKS:
                print("Invalid task. Type 'help' to see available commands.")
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

            req = {"task": cmd}
            if cmd == "file_count":
                req["directory_path"] = path
            else:
                req["file_path"] = path

            logger.info(f"Sending task '{cmd}' to peer {target}")
            resp = send_rpc_call(target[0], target[1], req, auth_token,my_addr)
            if resp and resp.get("status") == "success":
                print(f"[RESULT] from {target}: {resp.get('result')}")
            else:
                print(
                    f"[ERR] from {target}: {resp.get('message') if resp else 'No response'}"
                )

        except KeyboardInterrupt:
            print("\nInterrupted by user. Exiting.")
            break
        except Exception as e:
            logger.error(f"CLI error: {e}", exc_info=True)
            print(f"Error: {e}")
