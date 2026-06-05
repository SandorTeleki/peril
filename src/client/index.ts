import amqp from "amqplib";
import { clientWelcome, getInput, printClientHelp, printQuit, commandStatus } from "../internal/gamelogic/gamelogic.js";
import { GameState } from "../internal/gamelogic/gamestate.js";
import type { PlayingState } from "../internal/gamelogic/gamestate.js";
import type { ArmyMove } from "../internal/gamelogic/gamedata.js";
import { commandSpawn } from "../internal/gamelogic/spawn.js";
import { commandMove } from "../internal/gamelogic/move.js";
import { SimpleQueueType } from "../internal/pubsub/queue.js";
import { subscribeJSON } from "../internal/pubsub/subscribe.js";
import { publishJSON } from "../internal/pubsub/publish.js";
import { ExchangePerilDirect, ExchangePerilTopic, PauseKey, ArmyMovesPrefix } from "../internal/routing/routing.js";
import { handlerPause, handlerMove } from "./handlers.js";

async function main() {
  console.log("Starting Peril client...");

  const rabbitConnString = "amqp://guest:guest@localhost:5672/";
  const conn = await amqp.connect(rabbitConnString);
  console.log("Connection to RabbitMQ successful!");

  const username = await clientWelcome();

  const gs = new GameState(username);

  // Subscribe to pause messages
  const pauseQueue = `pause.${username}`;
  await subscribeJSON<PlayingState>(
    conn,
    ExchangePerilDirect,
    pauseQueue,
    PauseKey,
    SimpleQueueType.Transient,
    handlerPause(gs),
  );
  console.log(`Subscribed to ${pauseQueue}.`);

  // Subscribe to army move messages
  const movesQueue = `${ArmyMovesPrefix}.${username}`;
  await subscribeJSON<ArmyMove>(
    conn,
    ExchangePerilTopic,
    movesQueue,
    `${ArmyMovesPrefix}.*`,
    SimpleQueueType.Transient,
    handlerMove(gs),
  );
  console.log(`Subscribed to ${movesQueue}.`);

  // Create a confirm channel for publishing moves
  const publishCh = await conn.createConfirmChannel();

  while (true) {
    const words = await getInput();
    if (words.length === 0) {
      continue;
    }

    const command = words[0];

    try {
      if (command === "spawn") {
        commandSpawn(gs, words);
      } else if (command === "move") {
        const move = commandMove(gs, words);
        const routingKey = `${ArmyMovesPrefix}.${username}`;
        await publishJSON(publishCh, ExchangePerilTopic, routingKey, move);
        console.log("Move published successfully.");
      } else if (command === "status") {
        await commandStatus(gs);
      } else if (command === "help") {
        printClientHelp();
      } else if (command === "spam") {
        console.log("Spamming not allowed yet!");
      } else if (command === "quit") {
        printQuit();
        break;
      } else {
        console.log(`I don't understand the command: ${command}`);
      }
    } catch (err: any) {
      console.log(err.message);
    }
  }

  await conn.close();
  process.exit(0);
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
