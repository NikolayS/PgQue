// pgque -- TypeScript client for PgQue
// Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

import { describe, expect, it, vi } from 'vitest';
import { Client } from '../src/client.js';

const messageRow = {
  status: 'page' as const, page_batch_id: 9007199254740993n,
  page_token: '00000000-0000-0000-0000-000000000001', page_number: 1n,
  is_last: true, lease_until: new Date('2026-01-01T00:00:00Z'), fence_epoch: null,
  msg_id: 9007199254740995n, batch_id: 9007199254740993n, type: 'x', payload: '{}',
  retry_count: null, created_at: new Date('2026-01-01T00:00:00Z'),
  extra1: null, extra2: null, extra3: null, extra4: null, ordinality: 1n,
};

describe('paged client', () => {
  it('does not ack an unexecuted generator or async generator handler', async () => {
    for (const handler of [function* () { yield 1; }, async function* () { yield 1; }]) {
      const pool = {query: vi.fn().mockResolvedValueOnce({rows:[messageRow]})
        .mockResolvedValueOnce({rows:[{status:'acked',batch_finished:true}]})};
      const client = new Client(pool as never);
      await expect(client.processPage('q', 'c', 'w', handler as never)).rejects.toThrow('iterator');
      expect(pool.query).toHaveBeenCalledTimes(1);
    }
  });

  it('does not ack a deferred callable returned by a handler', async () => {
    const pool = {query: vi.fn().mockResolvedValueOnce({rows:[messageRow]})};
    const client = new Client(pool as never);
    await expect(client.processPage('q', 'c', 'w', (() => () => undefined) as never))
      .rejects.toThrow('callable');
    expect(pool.query).toHaveBeenCalledTimes(1);
  });

  it('expands typed rows and preserves bigint precision', async () => {
    const pool = {query: vi.fn().mockResolvedValue({rows: [messageRow]})};
    const page = await new Client(pool as never).receivePage('q', 'c', 'w');
    expect(pool.query.mock.calls[0]![0]).toContain('left join lateral unnest(p.messages)');
    expect(page.batchId).toBe(9007199254740993n);
    expect(page.messages[0]!.msgId).toBe(9007199254740995n);
  });

  it('does not ack when the handler throws', async () => {
    const pool = {query: vi.fn().mockResolvedValue({rows: [messageRow]})};
    const client = new Client(pool as never);
    await expect(client.processPage('q', 'c', 'w', () => { throw new Error('boom'); }))
      .rejects.toThrow('boom');
    expect(pool.query).toHaveBeenCalledTimes(1);
  });

  it('propagates ambiguous ack errors without retrying', async () => {
    const pool = {query: vi.fn().mockResolvedValueOnce({rows: [messageRow]}).mockRejectedValueOnce(new Error('lost response'))};
    const client = new Client(pool as never);
    await expect(client.processPage('q', 'c', 'w', () => undefined)).rejects.toThrow();
    expect(pool.query).toHaveBeenCalledTimes(2);
  });

  it('decodes a metadata-only response without manufacturing a message', async () => {
    const row = {...messageRow, status: 'idle' as const, page_batch_id: null,
      page_token: null, page_number: null, is_last: null, lease_until: null,
      msg_id: null, batch_id: null, type: null, payload: null, created_at: null};
    const page = await new Client({query: vi.fn().mockResolvedValue({rows:[row]})} as never)
      .receivePage('q','c','w');
    expect(page).toMatchObject({status:'idle', pageToken:null, messages:[]});
  });

  it('serializes bigint failure IDs as decimal strings', async () => {
    const pool = {query: vi.fn().mockResolvedValue({rows:[{status:'acked',batch_finished:false}]})};
    await new Client(pool as never).ackPage('token','worker', [{msgId:'9223372036854775807',retryAfterSeconds:0}]);
    expect(JSON.parse(pool.query.mock.calls[0]![1][2])).toEqual([{msg_id:'9223372036854775807',retry_after_seconds:0}]);
  });
});
