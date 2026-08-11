-- examples/parallel_merge_sort.lua
--
-- Distributed parallel merge sort across the OCCluster.
--
-- This is an "advanced" example: it exercises essentially every
-- primitive in the cluster API (broadcast, scatter, gather, barrier,
-- send, recv, log, result) inside one real, verifiable algorithm
-- instead of just calling them one at a time.
--
-- Algorithm overview
-- ------------------
--   1. Rank 0 picks a wall-clock baseline and broadcasts it to every
--      rank so all per-rank timings share the same origin (this is
--      what cluster.broadcast() is for: rank 0 publishes, everyone
--      else subscribes).
--   2. Rank 0 generates N integers using a deterministic LCG so the
--      output is reproducible across runs and across cluster sizes.
--   3. Rank 0 scatters N/size-sized slices to every rank (any
--      remainder is distributed one element at a time across the
--      first few ranks so the load stays balanced).
--   4. Each rank locally sorts its slice (iterative bottom-up merge
--      sort - avoids Lua recursion limits on large inputs) and
--      records how long it took.
--   5. Tree reduction ("recursive doubling"): in round k, every rank
--      whose bit k is set sends its sorted run to (rank XOR 2^k)
--      and becomes idle for the rest of the merge; the partner
--      receives and merges the two runs. After ceil(log2(size))
--      rounds, rank 0 holds the entire sorted array. Idle ranks
--      still participate in the per-round barrier so the active
--      ranks don't deadlock - this is the kind of subtlety that
--      bites people writing their first parallel reduction.
--   6. Rank 0 verifies the array is in ascending order and uses
--      cluster.gather() to pull a small stats table from every
--      rank (elements in/out, sort time, merge time, handed-off
--      flag). It then logs a per-rank summary and reports a
--      human-readable final result string. Non-rank-0 ranks report
--      their own one-line summary.
--
-- Tuning
-- ------
--   Edit N below to change the input size. As a rule of thumb, N
--   should be at least ~10x the cluster size so the parallel merge
--   pays for the communication overhead; below that, a single-rank
--   sort will usually be faster.
--
-- Submitting
-- ----------
--   submit examples/parallel_merge_sort.lua 4    sort-10k
--   submit examples/parallel_merge_sort.lua all  sort-10k
--   submit examples/parallel_merge_sort.lua 8    sort-10k self
--
-- The `self` flag includes the master node in the job (by default
-- the master sits the job out so it can keep managing the cluster).

local cluster = ...

------------------------------------------------------------------
-- Tunable input size
------------------------------------------------------------------

local N = 10000

------------------------------------------------------------------
-- Local utilities
------------------------------------------------------------------

-- Standard merge of two sorted arrays into a new sorted array.
local function merge(a, b)
  local na, nb = #a, #b
  local out = {}
  -- Pre-size the result table so Lua doesn't reallocate it on every
  -- insertion (matters a lot when merging big runs).
  for _ = 1, na + nb do out[#out + 1] = false end
  local i, j, k = 1, 1, 0
  while i <= na and j <= nb do
    k = k + 1
    if a[i] <= b[j] then out[k] = a[i]; i = i + 1
    else                 out[k] = b[j]; j = j + 1 end
  end
  while i <= na do k = k + 1; out[k] = a[i]; i = i + 1 end
  while j <= nb do k = k + 1; out[k] = b[j]; j = j + 1 end
  return out
end

-- Iterative bottom-up merge sort on a single rank. We avoid the
-- naive recursive top-down version because Lua's default stack is
-- shallow and a slice of several thousand elements would overflow
-- it on a deep recursion. Bottom-up is O(n log n) and uses O(n)
-- auxiliary space, same as top-down.
local function localSort(arr)
  local n = #arr
  if n <= 1 then return arr end

  local buf = {}
  for i = 1, n do buf[i] = arr[i] end

  local width = 1
  while width < n do
    local next = {}
    for lo = 1, n, 2 * width do
      local mid = math.min(lo + width - 1, n)
      local hi  = math.min(lo + 2 * width - 1, n)
      local p, q = lo, mid + 1
      for k = lo, hi do
        if p <= mid and (q > hi or buf[p] <= buf[q]) then
          next[k] = buf[p]; p = p + 1
        else
          next[k] = buf[q]; q = q + 1
        end
      end
    end
    buf = next
    width = width * 2
  end
  return buf
end

-- Returns true if arr is non-decreasing. On failure also returns
-- the index of the first out-of-order element so the caller can
-- log where things went wrong.
local function isSorted(arr)
  for i = 2, #arr do
    if arr[i - 1] > arr[i] then return false, i end
  end
  return true
end

------------------------------------------------------------------
-- Setup
------------------------------------------------------------------

local computer = require("computer")
local rank, size = cluster.rank, cluster.size

cluster.log(("rank %d/%d starting parallel_merge_sort (N=%d)")
    :format(rank, size, N))

------------------------------------------------------------------
-- Phase 1: rank 0 broadcasts a wall-clock baseline
--
-- This is what cluster.broadcast() is for: rank 0 publishes a
-- value, every other rank receives it. We use it to give every
-- rank the same t0 so per-rank timings in the final summary are
-- directly comparable (handy for spotting stragglers).
------------------------------------------------------------------

local t0
if rank == 0 then
  t0 = computer.uptime()
  cluster.broadcast(t0)
else
  t0 = cluster.broadcast()
end

------------------------------------------------------------------
-- Phase 2: rank 0 generates the input
------------------------------------------------------------------

local input
if rank == 0 then
  cluster.log(("phase 2: rank 0 generating %d integers (deterministic LCG)")
      :format(N))
  input = {}
  local seed = 12345
  for i = 1, N do
    -- Numerical Recipes LCG constants, masked to 31 bits so the
    -- multiplication can't overflow Lua's float-or-int representation.
    seed = (seed * 1103515245 + 12345) % 2147483648
    input[i] = seed % 100000
  end
  cluster.log(("phase 2: input ready (first=%d, last=%d)")
      :format(input[1], input[N]))
end

cluster.barrier()

------------------------------------------------------------------
-- Phase 3: scatter the input into per-rank slices
------------------------------------------------------------------

local slices = {}
if rank == 0 then
  local chunk     = N // size         -- floor division (Lua 5.3)
  local remainder = N % size
  local idx = 1
  for r = 0, size - 1 do
    -- First `remainder` ranks get one extra element so the total
    -- adds up to exactly N with no leftover.
    local len = chunk + (r < remainder and 1 or 0)
    local slice = {}
    for i = 1, len do slice[i] = input[idx]; idx = idx + 1 end
    slices[r] = slice
  end
  cluster.log(("phase 3: scattering %d slices (chunk=%d, remainder=%d)")
      :format(size, chunk, remainder))
end

local mySlice = cluster.scatter(slices)
cluster.log(("phase 3: rank %d received %d elements")
    :format(rank, #mySlice))

cluster.barrier()

------------------------------------------------------------------
-- Phase 4: local sort
------------------------------------------------------------------

local sortStart = computer.uptime()
local sorted    = localSort(mySlice)
local sortTime  = computer.uptime() - sortStart

local okLocal = isSorted(sorted)
cluster.log(("phase 4: rank %d sorted %d elements in %.3fs (sorted=%s)")
    :format(rank, #sorted, sortTime, tostring(okLocal)))

cluster.barrier()

------------------------------------------------------------------
-- Phase 5: tree-based parallel merge (recursive doubling)
--
-- In round k (k = 0, 1, ...):
--   * bit     = 2^k
--   * partner = rank XOR bit
--   * if partner >= size: this rank is idle this round (can happen
--     when size is not a power of two - just wait at the barrier).
--   * if (rank AND bit) == 0: "lower" partner - receive from the
--     upper partner and merge the two sorted runs into one.
--   * else: "upper" partner - send our sorted run to the lower
--     partner and become idle for all subsequent rounds. We do NOT
--     return early: idle ranks still call cluster.barrier() every
--     round so the active ranks don't deadlock waiting for them.
--
-- After ceil(log2(size)) rounds, rank 0 holds the entire array.
------------------------------------------------------------------

local current     = sorted
local rounds      = math.ceil(math.log(size) / math.log(2))
local mergeTime   = 0
local handedOff   = false

if rounds > 0 then
  cluster.log(("phase 5: rank %d entering tree merge (%d rounds)")
      :format(rank, rounds))
end

for k = 0, rounds - 1 do
  local bit     = 2^k
  local partner = rank ~ bit           -- Lua 5.3 bitwise XOR

  if handedOff then
    -- Already gave our data to a partner in an earlier round; just
    -- keep participating in barriers so nobody deadlocks.
  elseif partner >= size then
    cluster.log(("  round %d: rank %d idle (partner %d out of bounds)")
        :format(k, rank, partner))
  elseif (rank & bit) == 0 then        -- Lua 5.3 bitwise AND
    -- Lower partner: receive and merge.
    local mStart = computer.uptime()
    local other, err = cluster.recv(partner, 30)
    if not other then
      cluster.log(("  round %d: rank %d recv from %d FAILED: %s")
          :format(k, rank, partner, tostring(err)))
      cluster.result(("rank %d: ERROR recv timeout in round %d")
          :format(rank, k))
      return
    end
    current   = merge(current, other)
    mergeTime = mergeTime + (computer.uptime() - mStart)
    cluster.log(("  round %d: rank %d merged to %d elements")
        :format(k, rank, #current))
  else
    -- Upper partner: send and become idle.
    cluster.send(partner, current)
    cluster.log(("  round %d: rank %d sent %d elements to rank %d (handing off)")
        :format(k, rank, #current, partner))
    handedOff = true
  end

  cluster.barrier()
end

cluster.barrier()

------------------------------------------------------------------
-- Phase 6: gather per-rank statistics
--
-- Every rank reports a small stats table back to rank 0 via
-- cluster.gather(). Rank 0 then has both the final sorted array
-- and a per-rank breakdown it can include in its summary.
------------------------------------------------------------------

local myStats = {
  rank        = rank,
  elementsIn  = #mySlice,
  elementsOut = handedOff and 0 or #current,
  sortTime    = sortTime,
  mergeTime   = mergeTime,
  handedOff   = handedOff,
  totalLocal  = computer.uptime() - t0,
}

local allStats = cluster.gather(myStats)
if rank == 0 then
  cluster.log(("phase 6: gathered stats from %d ranks"):format(size))
end

------------------------------------------------------------------
-- Phase 7: verification & final result
------------------------------------------------------------------

if rank == 0 then
  local ok, badIdx = isSorted(current)
  local totalElapsed = computer.uptime() - t0

  cluster.log(("phase 7: rank 0 final array length=%d (expected %d), sorted=%s%s")
      :format(#current, N, tostring(ok),
              ok and "" or (" (FIRST FAILURE at idx " .. badIdx .. ")")))

  -- Per-rank summary, one line per rank.
  local lines = {}
  for r = 0, size - 1 do
    local s = allStats[r]
    if s then
      lines[#lines + 1] = ("  rank %d: in=%d out=%d sort=%.3fs merge=%.3fs total=%.3fs %s")
          :format(s.rank, s.elementsIn, s.elementsOut,
                  s.sortTime, s.mergeTime, s.totalLocal,
                  s.handedOff and "(handed off)" or "(final holder)")
    end
  end
  cluster.log("phase 7: per-rank summary:\n" .. table.concat(lines, "\n"))

  local summary = ("ok=%s N=%d size=%d len=%d total=%.3fs first=%d last=%d median=%d")
      :format(tostring(ok), N, size, #current, totalElapsed,
              current[1], current[#current], current[math.floor(#current / 2)])
  cluster.log("FINAL: " .. summary)
  cluster.result(summary)
else
  -- Non-rank-0 ranks report a one-line summary so the submitter's
  -- final per-rank table shows something readable for every rank,
  -- not just "table: 0x...".
  cluster.result(("rank %d: in=%d out=%d sort=%.3fs merge=%.3fs total=%.3fs %s")
      :format(rank, #mySlice, handedOff and 0 or #current,
              sortTime, mergeTime, computer.uptime() - t0,
              handedOff and "(handed off)" or "(final holder)"))
end
