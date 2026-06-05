import amqp from "amqplib";
import { publishJSON } from "../internal/pubsub/publish.js";
import { ExchangePerilDirect, PauseKey } from "../internal/routing/routing.js";
import type { PlayingState } from "../internal/gamelogic/gamestate.js";

async function main() {
  console.log("Starting Peril server...");

  const rabbitConnString = "amqp://guest:guest@localhost:5672/";
  const conn = await amqp.connect(rabbitConnString);
  console.log("Connection to RabbitMQ successful!");

  const ch = await conn.createConfirmChannel();
  console.log("Confirm channel created.");

  const state: PlayingState = { isPaused: true };
  await publishJSON(ch, ExchangePerilDirect, PauseKey, state);
  console.log("Pause message published.");

  process.on("SIGINT", async () => {
    console.log("Shutting down...");
    await conn.close();
    process.exit(0);
  });
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
