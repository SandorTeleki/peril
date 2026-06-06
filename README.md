# Peril

A multiplayer strategy game built with TypeScript and RabbitMQ to explore pub/sub messaging patterns. Players control armies, move units across territories, and wage wars — all coordinated through AMQP message passing.

Built as part of Boot.dev's [Learn Pub/Sub](https://www.boot.dev/courses/learn-pub-sub-rabbitmq-typescript) course.

## Prerequisites

- Node.js (v18+)
- Docker (for running RabbitMQ)

## Installation

```
npm install
```

## Running RabbitMQ

Start a RabbitMQ container with the management UI:

```
npm run rabbit:start
```

The management UI is available at http://localhost:15672 (guest/guest).

Other RabbitMQ commands:

```
npm run rabbit:stop    # Stop the container
npm run rabbit:logs    # View container logs
```

## Running the Game

Start the server (manages game state, pause/resume, and consumes game logs):

```
npm run server
```

Start a client (one per player, in separate terminals):

```
npm run client
```

## Architecture

- **Server** — publishes pause/resume commands to a direct exchange, subscribes to game logs via a durable queue with MessagePack serialization.
- **Clients** — subscribe to pause messages, army moves (topic exchange), and war events (shared durable queue). Publish moves and war declarations.
- **Exchanges**
  - `peril_direct` — direct exchange for pause/resume broadcasts
  - `peril_topic` — topic exchange for army moves, war declarations, and game logs
  - `peril_dlx` — fanout dead-letter exchange for failed messages
- **Queues** use dead-letter routing to `peril_dlx` → `peril_dlq` for inspection of rejected messages.

## Project Structure

```
src/
├── client/          # Client REPL and message handlers
├── server/          # Server REPL and log consumer
├── internal/
│   ├── gamelogic/   # Game state, moves, wars, spawning
│   ├── pubsub/      # RabbitMQ helpers (publish, subscribe, queue management)
│   └── routing/     # Exchange and routing key constants
└── scripts/         # Shell scripts for RabbitMQ and multi-server testing
```
