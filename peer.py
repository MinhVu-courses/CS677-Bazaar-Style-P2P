import socket
import time


def run_peer(config):
    peer_id = config["peer_id"]
    host = config["host"]
    port = config["port"]
    neighbors = config["neighbors"]

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((host, port))
    server.settimeout(1.0)
    server.listen()

    print(f"Peer {peer_id} listening on {host}:{port}")
    print(f"Peer {peer_id} neighbors: {[n['peer_id'] for n in neighbors]}")

    # Give all peers time to start listening
    time.sleep(2)

    # Only peer 0 sends one test message
    if peer_id == 0:
        first_neighbor = neighbors[1]  # this should be peer 1 in your ring
        msg = f"hello from peer {peer_id}"
        send_message(first_neighbor["host"], first_neighbor["port"], msg)
        print(f"Peer {peer_id} sent: {msg} to peer {first_neighbor['peer_id']}")

    try:
        while True:
            try:
                # At this stage server just open and wait for connections
                conn, addr = server.accept()

                # mimic the data received
                data = conn.recv(4096)
                print(f"Peer {peer_id} received: {data.decode()}")
                conn.close()
            except socket.timeout:
                continue
    except KeyboardInterrupt:
        pass
    finally:
        server.close()


# Add a send helper (this is the tiny client)
def send_message(host, port, text):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((host, port))
    s.sendall(text.encode())
    s.close()
