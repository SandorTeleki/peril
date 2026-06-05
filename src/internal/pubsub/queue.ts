import amqp from "amqplib";
import type { Channel } from "amqplib";

export enum SimpleQueueType {
  Durable,
  Transient,
}

export async function declareAndBind(
  conn: amqp.ChannelModel,
  exchange: string,
  queueName: string,
  key: string,
  queueType: SimpleQueueType,
): Promise<[Channel, amqp.Replies.AssertQueue]> {
  const ch = await conn.createChannel();

  const durable = queueType === SimpleQueueType.Durable;
  const autoDelete = queueType === SimpleQueueType.Transient;
  const exclusive = queueType === SimpleQueueType.Transient;

  const queue = await ch.assertQueue(queueName, {
    durable,
    autoDelete,
    exclusive,
    arguments: {
      "x-dead-letter-exchange": "peril_dlx",
    },
  });

  await ch.bindQueue(queueName, exchange, key);

  return [ch, queue];
}
