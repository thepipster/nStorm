---
layout: home

hero:
    name: nStorm
    text: Stream processing for Node.js
    tagline: Write small processing blocks, chain them into a topology, and scale by adding workers. Inspired by Apache Storm.
    actions:
        - theme: brand
          text: Get started
          link: /guide/getting-started
        - theme: alt
          text: API reference
          link: /reference/nstorm
        - theme: alt
          text: GitHub
          link: https://github.com/thepipster/nStorm

features:
    - title: Small blocks, wired together
      details: Each block is a class with a start method, a process method, or both. Connect them by name, with optional filters on each connection.
    - title: Replay on failure
      details: A message whose block fails is delivered again, up to a limit you set. With the default provider it also survives a worker crash.
    - title: Scale by adding workers
      details: Run the same topology in more processes or on more servers against one Redis, and they share the work.
    - title: Pluggable queues
      details: BullMQ for durability, a lightweight Redis list for throughput, or an in-memory queue for tests and development.
    - title: Graceful shutdown
      details: Stopping a topology lets messages in progress finish, within a timeout you choose. The library never takes over your process.
    - title: TypeScript throughout
      details: Written in TypeScript with types included. Runs on Node.js 20 and later.
---
