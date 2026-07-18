-- hello_cluster.lua
--
-- Minimal example job. Run with, e.g.:
--   submit examples/hello_cluster.lua all hello
--
-- Every job script receives the cluster API as its first (and only)
-- vararg - grab it with `local cluster = ...` at the top of the file.

local cluster = ...

cluster.log("hello from rank " .. cluster.rank .. " of " .. cluster.size)

-- Wait until every node has reached this point before continuing.
cluster.barrier()

if cluster.rank == 0 then
  cluster.log("everyone has said hello!")
end

-- Report a result back to whoever submitted the job. If you don't call
-- cluster.result() yourself, the node will automatically report the
-- return value of this script (or an error, if one occurred).
cluster.result({rank = cluster.rank, greeting = "hi from " .. cluster.rank})
