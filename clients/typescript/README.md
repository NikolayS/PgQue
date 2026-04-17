# pgque-ts-example

Example TypeScript client for PgQue.

This is an unpublished example library for now. It is meant to demonstrate
integration patterns, not to promise a stable SDK yet.

## Usage

```ts
import { PgqueClient } from './src/index.js';

const client = new PgqueClient('postgresql://localhost/mydb');
await client.connect();

await client.send('orders', { order_id: 42 }, 'order.created');
await client.subscribe('orders', 'processor');

const messages = await client.receive('orders', 'processor', 100);
for (const msg of messages) {
  console.log(msg.type, msg.payload);
}

if (messages.length > 0) {
  await client.ack(messages[0].batch_id);
}

await client.close();
```

Important: `ack(batch_id)` acknowledges the whole batch, not just one returned
row.
