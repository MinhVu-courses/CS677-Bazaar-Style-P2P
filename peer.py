import socket

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

    try:
        while True:
            try:
                # At this stage
                conn, addr = server.accept()
                # mimic the data received
                print(f"Peer {peer_id} accepted connection from {addr}")
                conn.close()
            except socket.timeout:
                continue
    except KeyboardInterrupt:
        pass
    finally:
        server.close()