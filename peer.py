import socket
import time
import json
import threading
from datetime import datetime


def log(msg):
    ts = datetime.now().strftime("%m.%d.%Y %H:%M:%S.%f")[:-3]
    print(f"{ts} {msg}")


def run_peer(config):
    seen_requests = set()
    seen_lock = threading.Lock()

    seller_state = {"stock": 2}
    stock_lock = threading.Lock()

    replies = []

    peer_id = config["peer_id"]
    host = config["host"]
    port = config["port"]
    neighbors = config["neighbors"]
    role = config["role"]
    product = config["product"]

    log(f"Peer {peer_id} role={role} product={product}")

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((host, port))
    server.settimeout(1.0)
    server.listen()

    log(f"Peer {peer_id} listening on {host}:{port}")
    log(f"Peer {peer_id} neighbors: {[n['peer_id'] for n in neighbors]}")

    time.sleep(2)

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
            log(f"Peer 0 sent lookup to peer {neighbor['peer_id']}")

    try:
        while True:
            try:
                conn, addr = server.accept()

                t = threading.Thread(
                    target=process_connection,
                    args=(
                        conn,
                        peer_id,
                        host,
                        port,
                        neighbors,
                        seen_requests,
                        seen_lock,
                        role,
                        product,
                        replies,
                        seller_state,
                        stock_lock,
                    ),
                    daemon=True,
                )
                t.start()

            except socket.timeout:
                continue

    except KeyboardInterrupt:
        pass
    finally:
        server.close()


def send_message(host, port, msg_dict):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((host, port))
    s.sendall(json.dumps(msg_dict).encode())
    s.close()


def handle_lookup(
    message,
    peer_id,
    host,
    port,
    neighbors,
    seen_requests,
    seen_lock,
    role,
    product,
):
    request_id = message["request_id"]

    with seen_lock:
        if request_id in seen_requests:
            log(f"Peer {peer_id} ignoring duplicate lookup {request_id}")
            return
        seen_requests.add(request_id)

    current_path = message["path"] + [peer_id]

    log(
        f"Peer {peer_id} received lookup for {message['product_name']} "
        f"with hopcount={message['hopcount']} path={current_path}"
    )

    if role == "seller" and product == message["product_name"]:
        reply_msg = {
            "type": "reply",
            "seller_id": peer_id,
            "seller_host": host,
            "seller_port": port,
            "buyer_id": message["buyer_id"],
            "product_name": message["product_name"],
            "path": current_path,
            "request_id": request_id,
        }

        if len(current_path) >= 2:
            next_peer_id = current_path[-2]

            for neighbor in neighbors:
                if neighbor["peer_id"] == next_peer_id:
                    send_message(neighbor["host"], neighbor["port"], reply_msg)
                    log(f"Seller {peer_id} sent reply toward peer {next_peer_id}")
                    break

    if message["hopcount"] == 0:
        log(f"Peer {peer_id} dropping lookup {request_id} because hopcount reached 0")
        return

    new_message = {
        "type": "lookup",
        "buyer_id": message["buyer_id"],
        "product_name": message["product_name"],
        "hopcount": message["hopcount"] - 1,
        "path": current_path,
        "request_id": request_id,
    }

    previous_peer = message["path"][-1] if len(message["path"]) > 0 else None

    for neighbor in neighbors:
        if neighbor["peer_id"] == previous_peer:
            continue

        send_message(neighbor["host"], neighbor["port"], new_message)
        log(
            f"Peer {peer_id} forwarded lookup {request_id} to peer {neighbor['peer_id']}"
        )


def handle_reply(message, peer_id, neighbors, replies):
    path = message["path"]

    if peer_id == message["buyer_id"]:
        log(
            f"Buyer {peer_id} received reply from seller {message['seller_id']} "
            f"for product {message['product_name']}"
        )

        replies.append(message["seller_id"])

        buy_msg = {
            "type": "buy",
            "buyer_id": peer_id,
            "seller_id": message["seller_id"],
            "product_name": message["product_name"],
        }

        send_message(message["seller_host"], message["seller_port"], buy_msg)
        log(f"Buyer {peer_id} sent buy to seller {message['seller_id']}")
        return

    my_index = path.index(peer_id)
    next_peer_id = path[my_index - 1]

    for neighbor in neighbors:
        if neighbor["peer_id"] == next_peer_id:
            send_message(neighbor["host"], neighbor["port"], message)
            log(f"Peer {peer_id} forwarded reply to peer {next_peer_id}")
            return


def handle_buy(message, peer_id, role, product, seller_state, stock_lock):
    if role != "seller":
        return

    if peer_id != message["seller_id"]:
        return

    if product != message["product_name"]:
        return

    with stock_lock:
        if seller_state["stock"] > 0:
            seller_state["stock"] -= 1
            log(
                f"Seller {peer_id} sold {product} to buyer {message['buyer_id']} "
                f"(remaining={seller_state['stock']})"
            )
        else:
            log(f"Seller {peer_id} OUT OF STOCK")


def process_connection(
    conn,
    peer_id,
    host,
    port,
    neighbors,
    seen_requests,
    seen_lock,
    role,
    product,
    replies,
    seller_state,
    stock_lock,
):
    try:
        data = conn.recv(4096)
        if not data:
            return

        message = json.loads(data.decode())
        msg_type = message["type"]

        if msg_type == "lookup":
            handle_lookup(
                message,
                peer_id,
                host,
                port,
                neighbors,
                seen_requests,
                seen_lock,
                role,
                product,
            )
        elif msg_type == "reply":
            handle_reply(message, peer_id, neighbors, replies)
        elif msg_type == "buy":
            handle_buy(message, peer_id, role, product, seller_state, stock_lock)
    finally:
        conn.close()
