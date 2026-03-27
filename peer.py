import socket
import time
import random
import json
import threading
from datetime import datetime


def log(msg):
    ts = datetime.now().strftime("%m.%d.%Y %H:%M:%S.%f")[:-3]
    print(f"{ts} {msg}", flush=True)


def run_peer(config):
    replies = []

    peer_id = config["peer_id"]
    host = config["host"]
    port = config["port"]
    neighbors = config["neighbors"]
    role = config["role"]

    pending_requests = {}
    pending_lock = threading.Lock()
    latency_file = f"latency_peer_{peer_id}.csv"

    seen_requests = set()
    seen_lock = threading.Lock()

    seller_state = {
        "stock": 2 if role == "seller" else 0,
        "product": config["product"],
    }
    stock_lock = threading.Lock()

    completed_buys = set()
    completed_lock = threading.Lock()

    log(f"Peer {peer_id} role={role} product={seller_state['product']}")

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((host, port))
    server.settimeout(1.0)
    server.listen()

    log(f"Peer {peer_id} listening on {host}:{port}")
    log(f"Peer {peer_id} neighbors: {[n['peer_id'] for n in neighbors]}")

    time.sleep(0.02)

    if role == "buyer":
        with open(latency_file, "w") as f:
            f.write("request_id,product_name,latency_ms\n")

        t = threading.Thread(
            target=buyer_loop,
            args=(peer_id, neighbors, pending_requests, pending_lock, 100),
            daemon=True,
        )
        t.start()

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
                        replies,
                        seller_state,
                        stock_lock,
                        completed_buys,
                        completed_lock,
                        pending_requests,
                        pending_lock,
                        latency_file,
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
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((host, port))
        s.sendall(json.dumps(msg_dict).encode())
        s.close()
    except Exception as e:
        log(f"send_message failed to {host}:{port}: {e}")


def handle_lookup(
    message,
    peer_id,
    host,
    port,
    neighbors,
    seen_requests,
    seen_lock,
    role,
    seller_state,
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

    if (
        role == "seller"
        and seller_state["product"] == message["product_name"]
        and seller_state["stock"] > 0
    ):
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


def handle_reply(
    message,
    peer_id,
    neighbors,
    replies,
    completed_buys,
    completed_lock,
    pending_requests,
    pending_lock,
    latency_file,
):
    path = message["path"]

    if peer_id == message["buyer_id"]:
        log(
            f"Buyer {peer_id} received reply from seller {message['seller_id']} "
            f"for product {message['product_name']}"
        )

        replies.append(message["seller_id"])

        with completed_lock:
            if message["request_id"] in completed_buys:
                log(
                    f"Buyer {peer_id} already completed buy for {message['request_id']}"
                )
                return
            completed_buys.add(message["request_id"])

        with pending_lock:
            req = pending_requests.get(message["request_id"])

        if req is not None:
            latency_ms = (time.time() - req["start_time"]) * 1000.0
            with open(latency_file, "a") as f:
                f.write(
                    f"{message['request_id']},{message['product_name']},{latency_ms:.3f}\n"
                )

        buy_msg = {
            "type": "buy",
            "buyer_id": peer_id,
            "seller_id": message["seller_id"],
            "product_name": message["product_name"],
            "request_id": message["request_id"],
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


def handle_buy(message, peer_id, role, seller_state, stock_lock):
    if role != "seller":
        return

    if peer_id != message["seller_id"]:
        return

    if seller_state["product"] != message["product_name"]:
        return

    with stock_lock:
        if seller_state["stock"] > 0:
            seller_state["stock"] -= 1
            log(
                f"Seller {peer_id} sold {seller_state['product']} to buyer {message['buyer_id']} "
                f"(remaining={seller_state['stock']})"
            )

            if seller_state["stock"] == 0:
                new_product = random.choice(["fish", "salt", "boar"])
                seller_state["product"] = new_product
                seller_state["stock"] = 2
                log(
                    f"Seller {peer_id} switched to {new_product} "
                    f"with restocked stock={seller_state['stock']}"
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
    replies,
    seller_state,
    stock_lock,
    completed_buys,
    completed_lock,
    pending_requests,
    pending_lock,
    latency_file,
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
                seller_state,
            )
        elif msg_type == "reply":
            handle_reply(
                message,
                peer_id,
                neighbors,
                replies,
                completed_buys,
                completed_lock,
                pending_requests,
                pending_lock,
                latency_file,
            )
        elif msg_type == "buy":
            handle_buy(message, peer_id, role, seller_state, stock_lock)
    finally:
        conn.close()


def buyer_loop(peer_id, neighbors, pending_requests, pending_lock, num_requests=10):
    products = ["fish", "salt", "boar"]

    for request_num in range(1, num_requests + 1):
        product_name = random.choice(products)

        lookup_msg = {
            "type": "lookup",
            "buyer_id": peer_id,
            "product_name": product_name,
            "hopcount": 3,
            "path": [peer_id],
            "request_id": f"{peer_id}-{product_name}-{request_num}",
        }

        send_time = time.time()
        with pending_lock:
            pending_requests[lookup_msg["request_id"]] = {
                "start_time": send_time,
                "product_name": product_name,
            }

        for neighbor in neighbors:
            send_message(neighbor["host"], neighbor["port"], lookup_msg)
            log(
                f"Buyer {peer_id} sent lookup {lookup_msg['request_id']} "
                f"for {product_name} to peer {neighbor['peer_id']}"
            )

        time.sleep(random.uniform(2, 4))
