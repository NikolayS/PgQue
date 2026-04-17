import { PgqueClient } from './index.js';

async function main(): Promise<void> {
  const client = new PgqueClient('postgresql://postgres:pgque_test@localhost:5432/pgque_test');
  await client.connect();

  await client.subscribe('smoke_ts', 'ts-smoke');
  await client.send('smoke_ts', { hello: 'world' }, 'smoke.test');
  await client.forceTick('smoke_ts');
  await client.ticker();

  const messages = await client.receive('smoke_ts', 'ts-smoke', 10);
  if (messages.length === 0) {
    throw new Error('expected at least one message in TypeScript smoke test');
  }

  await client.ack(messages[0].batch_id);
  await client.close();
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
