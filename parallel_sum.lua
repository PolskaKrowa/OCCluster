-- parallel_sum.lua
--
-- Sums the numbers 1..N by splitting the range across every node in the
-- job, computing partial sums in parallel, and gathering + combining the
-- results back on rank 0. Demonstrates scatter, gather and result.
--
-- Run with, e.g.:
--   submit examples/parallel_sum.lua all parallel_sum

local cluster = ...
local N = 5000000

local chunks
if cluster.rank == 0 then
  chunks = {}
  local per = math.ceil(N / cluster.size)
  for r = 0, cluster.size - 1 do
    local lo = r * per + 1
    local hi = math.min((r + 1) * per, N)
    chunks[r] = {lo, hi}
  end
end

-- rank 0 hands each node its own {lo, hi} range
local range = cluster.scatter(chunks)
local lo, hi = range[1], range[2]

local sum = 0
for i = lo, hi do sum = sum + i end
cluster.log(("summed %d..%d = %d"):format(lo, hi, sum))

-- everyone sends their partial sum back to rank 0
local partials = cluster.gather(sum)

if cluster.rank == 0 then
  local total = 0
  for _, v in pairs(partials) do total = total + v end
  cluster.result(total)
else
  cluster.result(sum)
end
