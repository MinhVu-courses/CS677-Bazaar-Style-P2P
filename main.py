from multiprocessing import Process
from peer import run_peer

BASE_PORT = 5000


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

    processes = []
    try:
        for config in peer_configs:
            p = Process(target=run_peer, args=(config,))
            p.start()
            processes.append(p)

        for p in processes:
            p.join()

    except KeyboardInterrupt:
        print("\nStopping all peer processes...")

        for p in processes:
            if p.is_alive():
                p.terminate()

        for p in processes:
            p.join()

        print("All peers stopped.")