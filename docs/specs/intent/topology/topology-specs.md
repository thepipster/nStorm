# Topology — requirements

Design: [topology-design.md](topology-design.md). Source: `src/nStorm.ts`.

## Configuration

### TOPO-CONFIG-001

If the `queue` setting does not name a known queue provider, then the topology shall throw when it is constructed.

### TOPO-CONFIG-002

If an option is explicitly set to `undefined`, then the topology shall use that option's default value.

### TOPO-CONFIG-003

The topology shall not change the logger's level.

### TOPO-CONFIG-004

Where the host assigns its own queue provider before the topology is set up, the topology shall use that provider.

## Setup

### TOPO-SETUP-001

When the topology is set up, the topology shall subscribe each block to every block it lists as an input, carrying that input's options.

### TOPO-SETUP-002

If a block lists an input that names no block in the topology, then the topology shall reject setup and leave no subscribers wired.

### TOPO-SETUP-003

If setup is requested more than once, then the topology shall reject the later request and leave the existing subscribers unchanged.

### TOPO-SETUP-004

If a block is added after the topology has been set up, then the topology shall throw.

### TOPO-SETUP-005

If a block is added with a name already used by another block, then the topology shall throw.

## Start

### TOPO-START-001

When the topology is started without having been set up, the topology shall set itself up first.

### TOPO-START-002

When the topology is started, the topology shall call every block's `start()` without waiting for another block's `start()` to finish.

### TOPO-START-003

If start is requested more than once, then the topology shall reject the later request.

### TOPO-START-004

If a block's `start()` rejects, then the topology shall stop itself and reject the start with that error.

## Stop

### TOPO-STOP-001

When the topology is stopped, the topology shall call every block's `stop()` and then close the queue provider.

### TOPO-STOP-002

If a block's `stop()` rejects, then the topology shall still close the queue provider.

### TOPO-STOP-003

If stop is requested more than once, or before the topology was started, then the topology shall stop at most once and shall refuse any later start.

### TOPO-STOP-004

When the topology is stopped, the topology shall give messages in progress up to the requested timeout to finish before the queue provider releases its connections.
