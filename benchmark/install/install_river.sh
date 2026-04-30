#!/usr/bin/env bash
set -Eeuo pipefail
echo "=== install river v0.34.0 + Go worker ==="

# i4i.2xlarge is x86_64
curl -fsSL https://go.dev/dl/go1.22.4.linux-amd64.tar.gz | sudo tar -C /usr/local -xz
export PATH=$PATH:/usr/local/go/bin
echo 'export PATH=$PATH:/usr/local/go/bin:/root/go/bin' | sudo tee -a /root/.bashrc >/dev/null

sudo bash -c "export PATH=\$PATH:/usr/local/go/bin; go install github.com/riverqueue/river/cmd/river@v0.34.0"

sudo -u postgres psql -d bench -c "CREATE EXTENSION IF NOT EXISTS pgcrypto;"
sudo /root/go/bin/river migrate-up --database-url "postgres://postgres@127.0.0.1/bench"

# Go worker program (river CLI has no 'work' subcommand in 0.34)
sudo mkdir -p /root/riverworker
sudo tee /root/riverworker/go.mod >/dev/null <<'EOF'
module riverworker

go 1.22
EOF

sudo tee /root/riverworker/main.go >/dev/null <<'EOF'
package main

import (
    "context"
    "log"
    "os/signal"
    "syscall"

    "github.com/jackc/pgx/v5/pgxpool"
    "github.com/riverqueue/river"
    "github.com/riverqueue/river/riverdriver/riverpgxv5"
)

type BenchArgs struct{}

func (BenchArgs) Kind() string { return "bench" }

type BenchWorker struct {
    river.WorkerDefaults[BenchArgs]
}

func (w *BenchWorker) Work(ctx context.Context, job *river.Job[BenchArgs]) error {
    return nil
}

func main() {
    ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
    defer cancel()

    pool, err := pgxpool.New(ctx, "postgres://postgres@127.0.0.1/bench?sslmode=disable")
    if err != nil {
        log.Fatal(err)
    }
    defer pool.Close()

    workers := river.NewWorkers()
    river.AddWorker(workers, &BenchWorker{})

    client, err := river.NewClient(riverpgxv5.New(pool), &river.Config{
        Queues:  map[string]river.QueueConfig{"default": {MaxWorkers: 8}},
        Workers: workers,
    })
    if err != nil {
        log.Fatal(err)
    }

    if err := client.Start(ctx); err != nil {
        log.Fatal(err)
    }
    <-ctx.Done()
}
EOF

cd /root/riverworker
sudo bash -c "cd /root/riverworker && export PATH=\$PATH:/usr/local/go/bin && go mod tidy && go build -o /root/riverworker/bench_worker main.go"

echo "=== river installed, worker binary at /root/riverworker/bench_worker ==="
