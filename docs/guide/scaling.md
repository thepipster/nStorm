# Scaling out

With the `bullmq` or `simple` provider, a topology scales by running more copies of it.

## Adding workers

Start the same program in as many processes, containers or servers as you like, all pointing at the same Redis:

```ts
// worker.ts - run this N times
const cloud = new nStorm({
    cloudName: "orders",
    redis: { host: "redis.internal" },
});

cloud.addBlock("incoming", new IncomingOrders());
cloud.addBlock("validate", new Validate(), 5).input("incoming");
cloud.addBlock("save", new Save(), 5).input("validate");

await cloud.start();
```

Workers find each other through the queues. Two workers share work when they have the same `cloudName` and the same block names. Each message is handled by exactly one of them.

There is nothing to register and no coordinator. Start a worker to add capacity; [stop](./shutdown) one to remove it.

## Sources run in every worker

Every worker runs every block's `start()`. A source that generates data, such as a timer or a poll of an external API, will therefore do so once per worker, and you will get each item several times.

A source that _takes_ items from somewhere shared, such as a message broker or a database table with row locking, is fine: each item is taken once.

For a generating source, run it in one process only. A simple way is to make the source conditional:

```ts
if (process.env.RUN_SOURCES === "true") {
    cloud.addBlock("incoming", new IncomingOrders());
} else {
    cloud.addBlock("incoming", new BaseBolt()); // same name, emits nothing
}
```

The placeholder matters. Other blocks name `incoming` as an input, so a block with that name must exist in every worker.

## Concurrency

The limit passed to `addBlock` applies per worker. Total concurrency for a block is its limit multiplied by the number of workers.

Raise the limit when a block spends its time waiting, for example on network calls. Add workers when a block is using a whole CPU core, since one Node.js process runs JavaScript on one core.

## State

Anything a block keeps in memory belongs to its own process. Counters, caches and buffers are not shared between workers, and are lost when a worker stops. Keep shared state in Redis or a database.

## Not everything has to be the same

Workers only need to agree on the blocks they share. Because each block has its own queue, you can run a worker containing only some of the processing blocks, to give a slow stage more capacity than the others. Every worker must still define each block that its own blocks take input from, since those names are checked at start-up.
