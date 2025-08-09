"""
Module Main.py
"""
import logging
import sys
import threading
from config import AUTH_TOKEN
from interface import user_interface
from server import peer_server
from client import register_with_peer, gossip_with_peers
from threading import Lock

peer_list = set()
peer_list_lock = Lock()
SEED_PEERS = []

logging.basicConfig(
    level=logging.INFO,  # Change to DEBUG for verbose logs
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
def main():
    if len(sys.argv) < 2:
        print("Usage: python main.py <port> [known_peer_host:known_peer_port]")
        sys.exit(1)

    port = int(sys.argv[1])
    host = "127.0.0.1"
    my_addr = (host, port)

    bootstrap = None
    if len(sys.argv) == 3:
        h, p = sys.argv[2].split(":")
        bootstrap = (h, int(p))

    # Start server thread
    threading.Thread(
        target=peer_server,
        args=(my_addr, peer_list, peer_list_lock, AUTH_TOKEN),
        daemon=True,
    ).start()

    # Start gossip thread
    threading.Thread(
        target=gossip_with_peers,
        args=(peer_list, peer_list_lock, my_addr, SEED_PEERS, AUTH_TOKEN),
        daemon=True,
    ).start()

    # Register with bootstrap peer if provided
    if bootstrap:
        register_with_peer(bootstrap, peer_list, peer_list_lock, my_addr, AUTH_TOKEN)

    # Run CLI
    user_interface(peer_list, peer_list_lock, my_addr, AUTH_TOKEN)


if __name__ == "__main__":
    main()
