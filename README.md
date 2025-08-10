# Task Manager

>> This project implements a Peer to Peer (P2P) network that supports distributed file metadata queries via RPC calls. It uses a simple gossip protocol to maintain an up-to-date list of peers and a shared secret for basic authentication.

## Features

- Register and discover peers dynamically using gossip protocol
- - RPC based communication based peers
- Distributed file metadata retrieval like word count, line count, sha256 sum, file count
- Shared secret authentication for secure communication
- Command line interface for interaction

## Getting Started

### Requirements

- Python 3.8+
- Standard libraries

### Usage

- Start the first peer (no bootstrap peer):

```
python main.py <port>
```
Eg: 
```
python main.py 65432
```
- Start the subsequent peers with a known peer address to join the network - 

```
python main.py <port> <known_peer_host>:<known_peer_port>
```
Eg:
```
python peer.py 65433 127.0.0.1:65432
```
This starts a peer on port 65433 and bootstraps with a known peer at 127.0.0.1:65432

### Usage
After starting peers, use the interactive cli to run commands:
- `word_count` - count words in a file
- `line_count` - count lines in a file
- `sha256` - compute SHA256 hash of a file
- `file_count` - count files in a directory
- `file_metadata` - get combined metadata for files
- `show_peers` - list known peers
- `help` - show commands
- `exit` - quit CLI

### Configuration
Configure Network settings and authentication in `config.py`
- `AUTH_TOKEN`: shared secret for authentication
- `GOSSIP_INTERVAL_SECONDS`:interval between gossip rounds

### Logging
Uses Python's standard `logging` module with INFO level by default. Currently, logs server activity, RPC requests, errors.

## Joining the Network Using a Known Peer
When starting the first peer in the network, it operates standalone, waiting for others to join. For subsequent peers, you must provide the address of a known existing peer address (host:port). This allows the new peer to register itself, obtain the current list of active peers and begin participating in the network gossip protocol to stay updated on all peers. This bootstrap process ensures smooth peer discovery and network integration without manually configuring all peer addresses

## Future Improvements
- Add TLS encryption for secure communication
- Support asynchronous networking (`asyncio`)
- Expand authentication mechanisms like certificates, tokens
- Persistent peer storage and recovery
- Add file transfer capability
- Support for dynamic IP and port discovery
- GUI for easier interaction