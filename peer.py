import socket
import time
import json


def run_peer(config):
    """
    Right now, this place feels like a hub that process request

        Inside here, we also defined message type:
        {
                "type": "lookup",
                "buyer_id": 0,
                "product_name": "fish",
                "hopcount": 3,
                "path": [0],
                "request_id": "0-fish-1"
        }


        Args:
                config (_type_): _description_
    """
    # data structures
    seen_requests = set()

    # data processing
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
        lookup_msg = {
            "type": "lookup",
            "buyer_id": 0,
            "product_name": "fish",
            "hopcount": 3,
            "path": [0],
            "request_id": "0-fish-1",
        }

        for neighbor in neighbors:
            send_message(neighbor["host"], neighbor["port"], lookup_msg)
            print(f"Peer 0 sent lookup to peer {neighbor['peer_id']}")

    try:
        while True:
            try:
                conn, addr = server.accept()

                data = conn.recv(4096)
                conn.close()

                message = json.loads(data.decode())
                msg_type = message["type"]

                if msg_type == "lookup":
                    handle_lookup(message, peer_id, neighbors, seen_requests)
            except socket.timeout:
                continue
    except KeyboardInterrupt:
        pass
    finally:
        server.close()


# Add a send helper (this is the tiny client)
def send_message(host, port, msg_dict):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((host, port))
    s.sendall(json.dumps(msg_dict).encode())
    s.close()


def handle_lookup(message, peer_id, neighbors, seen_requests):
    request_id = message["request_id"]

    # if request has already been seen
    if request_id in seen_requests:
        print(f"Peer {peer_id} ignoring duplicate lookup {request_id}")
        return

    seen_requests.add(request_id)

    print(
        f"Peer {peer_id} received lookup for {message['product_name']} "
        f"with hopcount={message['hopcount']} path={message['path']}"
    )

    if message["hopcount"] == 0:
        print(f"Peer {peer_id} dropping lookup {request_id} because hopcount reached 0")
        return

    new_message = {
        "type": "lookup",
        "buyer_id": message["buyer_id"],
        "product_name": message["product_name"],
        "hopcount": message["hopcount"] - 1,
        "path": message["path"] + [peer_id],
        "request_id": request_id,
    }

    previous_peer = None
    if len(message["path"]) > 0:
        previous_peer = message["path"][-1]

    for neighbor in neighbors:
        # Avoid resend message to previous peer
        if neighbor["peer_id"] == previous_peer:
            continue

        send_message(neighbor["host"], neighbor["port"], new_message)
        print(
            f"Peer {peer_id} forwarded lookup {request_id} to peer {neighbor['peer_id']}"
        )
