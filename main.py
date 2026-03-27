from multiprocessing import Process
from peer import run_peer
import random
import time

BASE_PORT = 5000
RUN_DURATION = 30  # seconds


def build_ring_topology(n):
    peers = []

    for i in range(n):
        peers.append(
            {"peer_id": i, "host": "127.0.0.1", "port": BASE_PORT + i, "neighbors": []}
        )

    for i in range(n):
        left = (i - 1) % n
        right = (i + 1) % n

        peers[i]["neighbors"] = [
            {
                "peer_id": peers[left]["peer_id"],
                "host": peers[left]["host"],
                "port": peers[left]["port"],
            },
            {
                "peer_id": peers[right]["peer_id"],
                "host": peers[right]["host"],
                "port": peers[right]["port"],
            },
        ]

    return peers


if __name__ == "__main__":
    N = 6
    peer_configs = build_ring_topology(N)

    products = ["fish", "salt", "boar"]

    # random role assignment, guaranteeing at least one buyer and one seller
    roles = ["buyer", "seller"]
    while len(roles) < N:
        roles.append(random.choice(["buyer", "seller"]))
    random.shuffle(roles)

    for i, peer in enumerate(peer_configs):
        peer["role"] = roles[i]
        if roles[i] == "seller":
            peer["product"] = random.choice(products)
        else:
            peer["product"] = None
            # if i == 0:
            #     peer["role"] = "buyer"
            #     peer["product"] = None
            # else:
            #     peer["role"] = "seller"
            #     peer["product"] = random.choice(products)

    processes = []
    try:
        for config in peer_configs:
            p = Process(target=run_peer, args=(config,))
            p.start()
            processes.append(p)

        print(f"Experiment running for {RUN_DURATION} seconds...")
        time.sleep(RUN_DURATION)

        print("\nExperiment finished. Stopping all peer processes...")
        for p in processes:
            if p.is_alive():
                p.terminate()

        for p in processes:
            p.join()

        print("All peers stopped.")

    except KeyboardInterrupt:
        print("\nStopping all peer processes...")

        for p in processes:
            if p.is_alive():
                p.terminate()

        for p in processes:
            p.join()

        print("All peers stopped.")
