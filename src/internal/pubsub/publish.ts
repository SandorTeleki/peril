import type { ConfirmChannel } from "amqplib";

export function publishJSON<T>(
  ch: ConfirmChannel,
  exchange: string,
  routingKey: string,
  value: T,
): Promise<void> {
  return new Promise((resolve, reject) => {
    const data = JSON.stringify(value);
    const buffer = Buffer.from(data);
    ch.publish(
      exchange,
      routingKey,
      buffer,
      { contentType: "application/json" },
      (err) => {
        if (err) {
          reject(err);
        } else {
          resolve();
        }
      },
    );
  });
}
