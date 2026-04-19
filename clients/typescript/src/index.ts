import { Client, type ClientConfig } from 'pg';

export interface LogresMessage {
  msg_id: number;
  batch_id: number;
  type: string;
  payload: unknown;
  retry_count: number | null;
  created_at: string;
  extra1: string | null;
  extra2: string | null;
  extra3: string | null;
  extra4: string | null;
}

export class LogresClient {
  private readonly client: Client;

  constructor(config: string | ClientConfig) {
    this.client = typeof config === 'string'
      ? new Client({ connectionString: config })
      : new Client(config);
  }

  async connect(): Promise<void> {
    await this.client.connect();
  }

  async close(): Promise<void> {
    await this.client.end();
  }

  async send(queue: string, payload: unknown, type = 'message'): Promise<void> {
    await this.client.query(
      'select logres.send($1, $2, $3::jsonb)',
      [queue, type, JSON.stringify(payload)]
    );
  }

  async subscribe(queue: string, consumer: string): Promise<void> {
    await this.client.query('select logres.subscribe($1, $2)', [queue, consumer]);
  }

  async receive(queue: string, consumer: string, limit = 100): Promise<LogresMessage[]> {
    const result = await this.client.query<LogresMessage>(
      'select * from logres.receive($1, $2, $3)',
      [queue, consumer, limit]
    );
    return result.rows;
  }

  async ack(batchId: number): Promise<void> {
    await this.client.query('select logres.ack($1)', [batchId]);
  }

  async forceTick(queue: string): Promise<void> {
    await this.client.query('select logres.force_tick($1)', [queue]);
  }

  async ticker(): Promise<void> {
    await this.client.query('select logres.ticker()');
  }
}
