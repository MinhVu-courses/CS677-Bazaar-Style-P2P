# CS677 HW3 — Peer-to-Peer Bazaar (Gnutella-Style)

## Overview

This project simulates a **decentralized peer-to-peer marketplace** inspired by the Gnutella protocol. Peers in the network can act as **buyers** or **sellers**, trading goods (fish, salt, boar) by flooding lookup messages through the network and completing transactions directly.

---

## Architecture

### Network Topology

Peers are arranged in a **ring topology**. Each peer knows only its two immediate neighbors (left and right). There is no central directory or coordinator — all discovery is done through message flooding.

```
  0 — 1 — 2
  |       |
  5 — 4 — 3
```

### Roles

| Role   | Behavior |
|--------|----------|
| Buyer  | Periodically floods the network looking for a product to buy |
| Seller | Listens for lookups; replies if it stocks the requested product |

At least one buyer and one seller are guaranteed per run. Roles are otherwise assigned randomly.

---

## How It Works

### Message Flow

```
Buyer                   Intermediate Peers              Seller
  |                            |                           |
  |--- lookup (flood) -------->|--- lookup (flood) ------->|
  |                            |                           |
  |<-- reply (path-reverse) ---|<-- reply (path-reverse) --|
  |                            |                           |
  |--- buy (direct) ---------------------------------------->|
```

1. **Lookup** — A buyer floods a `lookup` message with a `hopcount` (TTL=3). Each intermediate peer forwards it to all neighbors except the one it came from. The path of visited peers is tracked in the message.
2. **Reply** — A seller that has the requested product in stock sends a `reply` back along the reverse path recorded in the message.
3. **Buy** — Upon receiving the first reply, the buyer sends a direct `buy` message to the seller's host/port. Subsequent replies for the same request are ignored (first-reply wins).

### Seller Restocking

When a seller's stock hits 0, it **immediately restocks** with a randomly chosen new product (fish, salt, or boar) and resets stock to 2.

### Duplicate Suppression

Each peer tracks `seen_requests` so a lookup message that loops back is silently dropped.

---

## Files

| File | Description |
|------|-------------|
| `peer.py` | Core peer logic: server loop, message handlers, buyer loop |
| `main.py` | Spawns N=6 peers as separate processes, runs experiment for 30s |
| `plot_results.py` | Reads latency CSVs and plots a histogram |

---

## Running

```bash
# Run the experiment (30 seconds, 6 peers)
python main.py

# Plot latency results after the run
python plot_results.py
```

Output logs print to stdout with millisecond timestamps. Each buyer peer writes a `latency_peer_<id>.csv` tracking lookup-to-first-reply latency for every request.

---

## Latency Measurement

Latency is measured as the time from when a buyer **sends the lookup** to when it **receives the first reply**. This is recorded per request in CSV files and aggregated by `plot_results.py` into a histogram with min/avg/max statistics.

---

## Key Design Decisions

- **First-reply wins**: buyers only buy from the first seller to respond, preventing duplicate purchases for the same request.
- **Flooding with TTL**: hopcount=3 limits the search radius, avoiding unbounded network traffic.
- **Path-reversed replies**: replies route back hop-by-hop through the exact path the lookup took, requiring no global routing table.
- **Thread-per-connection**: each incoming connection is handled in a new daemon thread, enabling concurrent buyers and sellers.
