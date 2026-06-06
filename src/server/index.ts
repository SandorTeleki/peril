import amqp from "amqplib";
import { publishJSON } from "../internal/pubsub/publish.js";
import { SimpleQueueType } from "../internal/pubsub/queue.js";
import { subscribeMsgPack, AckType } from "../internal/pubsub/subscribe.js";
import { ExchangePerilDirect, ExchangePerilTopic, PauseKey, GameLogSlug } from "../internal/routing/routing.js";
import type { PlayingState } from "../internal/gamelogic/gamestate.js";
import type { GameLog } from "../internal/gamelogic/logs.js";
import { writeLog } from "../internal/gamelogic/logs.js";
import { printServerHelp, getInput } from "../internal/gamelogic/gamelogic.js";

async function main() {
  console.log("Starting Peril server...");

  const rabbitConnString = "amqp://guest:guest@localhost:5672/";
  const conn = await amqp.connect(rabbitConnString);
  console.log("Connection to RabbitMQ successful!");

  const ch = await conn.createConfirmChannel();
  console.log("Confirm channel created.");

  await subscribeMsgPack<GameLog>(
    conn,
    ExchangePerilTopic,
    GameLogSlug,
    `${GameLogSlug}.*`,
    SimpleQueueType.Durable,
    async (gameLog: GameLog) => {
      try {
        await writeLog(gameLog);
        process.stdout.write("> ");
        return AckType.Ack;
      } catch {
        process.stdout.write("> ");
        return AckType.NackRequeue;
      }
    },
  );
  console.log("Subscribed to game logs.");

  printServerHelp();

  while (true) {
    const words = await getInput();
    if (words.length === 0) {
      continue;
    }

    const command = words[0];

    if (command === "pause") {
      console.log("Sending pause message...");
      const state: PlayingState = { isPaused: true };
      await publishJSON(ch, ExchangePerilDirect, PauseKey, state);
      console.log("Pause message sent.");
    } else if (command === "resume") {
      console.log("Sending resume message...");
      const state: PlayingState = { isPaused: false };
      await publishJSON(ch, ExchangePerilDirect, PauseKey, state);
      console.log("Resume message sent.");
    } else if (command === "quit") {
      console.log("Exiting...");
      break;
    } else {
      console.log(`I don't understand the command: ${command}`);
    }
  }

  await conn.close();
  process.exit(0);
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
