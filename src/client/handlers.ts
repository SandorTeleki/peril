import type { GameState, PlayingState } from "../internal/gamelogic/gamestate.js";
import { handlePause } from "../internal/gamelogic/pause.js";
import { handleMove, MoveOutcome } from "../internal/gamelogic/move.js";
import { handleWar, WarOutcome } from "../internal/gamelogic/war.js";
import type { ArmyMove, RecognitionOfWar } from "../internal/gamelogic/gamedata.js";
import { AckType } from "../internal/pubsub/subscribe.js";
import { publishJSON } from "../internal/pubsub/publish.js";
import { ExchangePerilTopic, WarRecognitionsPrefix } from "../internal/routing/routing.js";
import type { ConfirmChannel } from "amqplib";

export function handlerPause(gs: GameState): (ps: PlayingState) => AckType {
  return (ps: PlayingState) => {
    handlePause(gs, ps);
    process.stdout.write("> ");
    return AckType.Ack;
  };
}

export function handlerMove(gs: GameState, publishCh: ConfirmChannel): (move: ArmyMove) => Promise<AckType> {
  return async (move: ArmyMove) => {
    const outcome = handleMove(gs, move);
    process.stdout.write("> ");

    switch (outcome) {
      case MoveOutcome.Safe:
        return AckType.Ack;
      case MoveOutcome.MakeWar: {
        const rw: RecognitionOfWar = {
          attacker: move.player,
          defender: gs.getPlayerSnap(),
        };
        const routingKey = `${WarRecognitionsPrefix}.${gs.getUsername()}`;
        try {
          await publishJSON(publishCh, ExchangePerilTopic, routingKey, rw);
        } catch {
          return AckType.NackRequeue;
        }
        return AckType.Ack;
      }
      case MoveOutcome.SamePlayer:
      default:
        return AckType.NackDiscard;
    }
  };
}

export function handlerWar(gs: GameState): (rw: RecognitionOfWar) => AckType {
  return (rw: RecognitionOfWar) => {
    const resolution = handleWar(gs, rw);
    process.stdout.write("> ");

    switch (resolution.result) {
      case WarOutcome.NotInvolved:
        return AckType.NackRequeue;
      case WarOutcome.NoUnits:
        return AckType.NackDiscard;
      case WarOutcome.YouWon:
        return AckType.Ack;
      case WarOutcome.OpponentWon:
        return AckType.Ack;
      case WarOutcome.Draw:
        return AckType.Ack;
      default:
        console.error("Unknown war outcome");
        return AckType.NackDiscard;
    }
  };
}
