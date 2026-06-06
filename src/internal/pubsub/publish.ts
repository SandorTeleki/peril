import type { ConfirmChannel } from "amqplib";
import { encode } from "@msgpack/msgpack";

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

export function publishMsgPack<T>(
  ch: ConfirmChannel,
  exchange: string,
  routingKey: string,
  value: T,
): Promise<void> {
  return new Promise((resolve, reject) => {
    const encoded = encode(value);
    const buffer = Buffer.from(encoded.buffer, encoded.byteOffset, encoded.byteLength);
    ch.publish(
      exchange,
      routingKey,
      buffer,
      { contentType: "application/x-msgpack" },
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
