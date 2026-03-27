import socket
import time
import json


def run_peer(config):
    """
    Right now, this place feels like a hub that process request

        Args:
                config (_type_): _description_
    """
    # data structures
    seen_requests = set()
    seller_state = {"stock": 2}  # maybe for testing
    replies = []

    # data processing
    peer_id = config["peer_id"]
    host = config["host"]
    port = config["port"]
    neighbors = config["neighbors"]
    role = config["role"]
    product = config["product"]

    print(f"Peer {peer_id} role={role} product={product}")

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
                    handle_lookup(
                        message,
                        peer_id,
                        neighbors,
                        seen_requests,
                        role,
                        product,
                        host,
                        port,
                    )
                elif msg_type == "reply":
                    handle_reply(message, peer_id, neighbors, replies)
                elif msg_type == "buy":
                    handle_buy(message, peer_id, role, product, seller_state)
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


def handle_lookup(
    message, peer_id, neighbors, seen_requests, role, product, host, port
):
    request_id = message["request_id"]

    # if request has already been seen
    if request_id in seen_requests:
        print(f"Peer {peer_id} ignoring duplicate lookup {request_id}")
        return

    seen_requests.add(request_id)

    current_path = message["path"] + [peer_id]

    print(
        f"Peer {peer_id} received lookup for {message['product_name']} "
        f"with hopcount={message['hopcount']} path={message['path']}"
    )

    # If I am a matching seller, send reply back along reverse path
    if role == "seller" and product == message["product_name"]:
        # generate reply
        reply_msg = {
            "type": "reply",
            "seller_id": peer_id,
            "seller_host": host,
            "seller_port": port,
            "buyer_id": message["buyer_id"],
            "product_name": message["product_name"],
            "path": current_path,
            "request_id": message["request_id"],
        }

        if len(current_path) >= 2:
            next_peer_id = current_path[-2]

            for neighbor in neighbors:
                if neighbor["peer_id"] == next_peer_id:
                    send_message(neighbor["host"], neighbor["port"], reply_msg)
                    print(f"Seller {peer_id} sent reply toward peer {next_peer_id}")
                    break

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


def handle_reply(message, peer_id, neighbors, replies):
    path = message["path"]

    if peer_id == message["buyer_id"]:
        # For testing purpose first
        print(
            f"Buyer {peer_id} received reply from seller {message['seller_id']} "
            f"for product {message['product_name']}"
        )

        replies.append(message["seller_id"])

        # simplest version: buy immediately from first reply
        seller_id = message["seller_id"]
        buy_msg = {
            "type": "buy",
            "buyer_id": peer_id,
            "seller_id": seller_id,
            "product_name": message["product_name"],
        }

        send_message(message["seller_host"], message["seller_port"], buy_msg)

        print(f"Buyer {peer_id} sent buy to seller {message['seller_id']}")
        return

    my_index = path.index(peer_id)
    next_peer_id = path[my_index - 1]  # move backward toward buyer

    for neighbor in neighbors:
        if neighbor["peer_id"] == next_peer_id:
            send_message(neighbor["host"], neighbor["port"], message)
            print(f"Peer {peer_id} forwarded reply to peer {next_peer_id}")
            return


def handle_buy(message, peer_id, role, product, seller_state):
    if role != "seller":
        return

    if peer_id != message["seller_id"]:
        return

    if product != message["product_name"]:
        return

    global stock

    if seller_state["stock"] > 0:
        seller_state["stock"] -= 1
        print(
            f"Seller {peer_id} sold {product} to buyer {message['buyer_id']} "
            f"(remaining={seller_state['stock']})"
        )
    else:
        print(f"Seller {peer_id} OUT OF STOCK")
