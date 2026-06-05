import amqp from "amqplib";
import { clientWelcome, getInput, printClientHelp, printQuit, commandStatus } from "../internal/gamelogic/gamelogic.js";
import { GameState } from "../internal/gamelogic/gamestate.js";
import { commandSpawn } from "../internal/gamelogic/spawn.js";
import { commandMove } from "../internal/gamelogic/move.js";
import { declareAndBind, SimpleQueueType } from "../internal/pubsub/queue.js";
import { ExchangePerilDirect, PauseKey } from "../internal/routing/routing.js";

async function main() {
  console.log("Starting Peril client...");

  const rabbitConnString = "amqp://guest:guest@localhost:5672/";
  const conn = await amqp.connect(rabbitConnString);
  console.log("Connection to RabbitMQ successful!");

  const username = await clientWelcome();

  const queueName = `pause.${username}`;
  const [ch, queue] = await declareAndBind(
    conn,
    ExchangePerilDirect,
    queueName,
    PauseKey,
    SimpleQueueType.Transient,
  );
  console.log(`Queue ${queueName} declared and bound.`);

  const gs = new GameState(username);

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
        commandMove(gs, words);
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
