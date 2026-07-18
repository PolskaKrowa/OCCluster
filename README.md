# OCCluster
Distributed computing in OpenComputers made easy


It is recommended that you install this using my [OpenComputers git implementation](https://github.com/PolskaKrowa/OCgit):
```bash
OCgit clone https://github.com/PolskaKrowa/OCCluster
```

---

A lightweight distributed-computing layer for OpenComputers (Lua 5.3). One
script, `cluster_node.lua`, runs on every machine you want in the cluster.
Nodes find each other automatically, elect a master, and let you submit
plain Lua "job" scripts that run across as many nodes as you like, with a
small MPI-style API for the nodes to talk to each other while a job runs.

No config files to hand-edit, no separate scheduler process, no MPI
toolchain - just run the daemon on each machine.

## Requirements

- Each machine needs a network card (wired) or wireless network card,
  and needs to actually be able to reach the others (same wired network /
  within wireless range, relays as needed).
- OpenOS, Lua 5.3 (standard OpenComputers setup).

## Running

On every machine:

```
cluster_node
```

Optional flags:

```
cluster_node --priority 100 --name build-master --port 4210
```

- `--priority` - higher wins the master election (default 50). Set a
  clearly higher priority on the machine you want to be master by
  default (e.g. a always-on server), or set it to a negative number
  (e.g. `-1`) on a node that should never become master.
- `--name` - a friendly name shown in `nodes` output (default: first 8
  characters of the machine's address).
- `--port` - network port used for cluster traffic (default `4210`, must
  match across all nodes).

You can also drop these defaults into `/etc/cluster.cfg` as a serialized
table so you don't need the flags every time:

```lua
{port = 4210, priority = 50, name = "worker-1"}
```

Within a few seconds of starting, the nodes will discover each other and
elect a master automatically - watch for `This node is now MASTER` on the
console of whichever one wins. You don't need to do anything else; if
that machine goes offline, the rest re-elect a new master on their own.

## Submitting a job

From **any** node's console (not just the master - it'll forward the
request):

```
submit examples/hello_cluster.lua all hello
submit examples/parallel_sum.lua 4 sum-test
```

```
submit <file.lua> [n|all] [jobName] [self]
```

- `n|all` - how many worker nodes to use (`all` = every known node).
- `jobName` - a label, purely for display.
- `self` - also include the master node itself in the job (off by
  default, since the master is busy running the cluster and running a
  job blocks it from doing anything else until the job finishes).

Live `cluster.log()` output from every rank streams back to your console
as the job runs, and the final per-rank results are printed when it's
done.

Other console commands: `nodes`, `status`, `priority <n>`, `help`, `quit`.

## Writing a job script

A job is just a normal Lua file. It receives the cluster API as its only
vararg:

```lua
local cluster = ...

cluster.log("hello from rank " .. cluster.rank .. " of " .. cluster.size)
cluster.result("some result value")
```

API reference:

| Call                        | Behaviour                                                                 |
|-----------------------------|-----------------------------------------------------------------------------|
| `cluster.rank`               | this node's rank, `0 .. size-1`                                             |
| `cluster.size`                | total number of nodes running this job                                     |
| `cluster.send(rank, data)`     | send a value to another rank                                               |
| `cluster.recv(rank, timeout)`   | receive from a given rank (or `nil` = any rank), waits up to `timeout`s (default 30), returns `nil, "timeout"` on timeout |
| `cluster.broadcast(data)`       | rank 0 passes `data`; every other rank calls it with no args and gets the value back |
| `cluster.scatter(arrayByRank)`  | rank 0 passes a table indexed `0..size-1`; each rank (including 0) gets its own slice back |
| `cluster.gather(data)`         | every rank passes its own value; rank 0 gets back a table indexed by rank  |
| `cluster.barrier()`            | blocks until every rank has called it                                       |
| `cluster.log(message)`         | streamed live to whoever submitted the job                                  |
| `cluster.result(data)`         | reports the job's final result for this rank (optional - the script's return value is used automatically if you don't call this) |

See `examples/hello_cluster.lua` for the basics and
`examples/parallel_sum.lua` for a real (if simple) parallel workload using
scatter/gather.

## How it works (short version)

- **Discovery**: every node broadcasts a small `HELLO` every few seconds
  so everyone knows who's currently on the network.
- **Election**: nodes run a simple priority-based election (broadcast
  your priority, wait briefly, whoever has the highest priority - address
  as tiebreaker - announces itself as master). If two nodes ever announce
  themselves as master at the same time, whichever has lower priority
  gets corrected the next time it hears from/about the winner and steps
  down, so the cluster always converges back to one master.
- **Heartbeat**: the master broadcasts a heartbeat; if workers stop
  hearing it, they start a fresh election.
- **Jobs**: whichever node currently holds "master" assigns each
  participating node a rank and the job source, then the nodes talk
  directly to each other (not routed through the master) using the API
  above while the job runs, and report results back to the master, which
  forwards the aggregate result to whoever submitted the job.

## Known limitations

- A node runs one job at a time, synchronously - while a job is running
  on a node it won't process other cluster traffic (except messages
  belonging to that same job), so it can't also be doing something else
  useful mid-job. This is why the master doesn't include itself in jobs
  by default.
- No persistence: if a node reboots mid-job, that job's results from that
  rank are lost (the rest of the cluster will simply never get a
  `TASK_DONE` for that rank).
- Job source is plain Lua run with `load()` on each worker - only run
  jobs you trust, the same as you would any other script.
