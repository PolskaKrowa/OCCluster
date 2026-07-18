-- cluster_node.lua
--
-- Distributed computing node/daemon for OpenComputers.
-- Run this program on every machine you want to be part of the cluster.
-- It takes care of:
--   * discovering other nodes on the network
--   * electing a single "master" node (with automatic conflict resolution
--     if two nodes ever declare themselves master at the same time)
--   * accepting job submissions and farming them out across worker nodes
--   * running user-supplied Lua job scripts and giving them a small
--     MPI-style API (rank/size/send/recv/broadcast/scatter/gather/barrier)
--     for talking to each other while a job is running
--
-- See README.md for usage instructions and examples/ for sample jobs.

local component     = require("component")
local event         = require("event")
local serialization = require("serialization")
local computer       = require("computer")
local term          = require("term")
local keyboard       = require("keyboard")
local unicode        = require("unicode")
local fs             = require("filesystem")

if not component.isAvailable("modem") then
  io.stderr:write("cluster_node: no network/modem card found - install one and retry.\n")
  return
end
local modem = component.modem

-------------------------------------------------------------------------
-- Config
-------------------------------------------------------------------------

local CFG_PATH = "/etc/cluster.cfg"
local cfg = {port = 4210, priority = 50, name = nil}

if fs.exists(CFG_PATH) then
  local f = io.open(CFG_PATH, "r")
  if f then
    local raw = f:read("*a")
    f:close()
    local ok, data = pcall(serialization.unserialize, raw)
    if ok and type(data) == "table" then
      for k, v in pairs(data) do cfg[k] = v end
    end
  end
end

local args = {...}
for i = 1, #args do
  if args[i] == "--priority" and args[i + 1] then
    cfg.priority = tonumber(args[i + 1])
  elseif args[i] == "--name" and args[i + 1] then
    cfg.name = args[i + 1]
  elseif args[i] == "--port" and args[i + 1] then
    cfg.port = tonumber(args[i + 1])
  end
end

local PORT         = cfg.port
local ID           = computer.address()
local NAME         = cfg.name or ID:sub(1, 8)
local MY_PRIORITY  = cfg.priority

modem.open(PORT)

-------------------------------------------------------------------------
-- Timing constants (seconds)
-------------------------------------------------------------------------

local HELLO_INTERVAL     = 3
local HELLO_EXPIRE       = 9
local HEARTBEAT_INTERVAL = 2
local HEARTBEAT_TIMEOUT  = 6
local ELECTION_WINDOW    = 1.5
local COORDINATOR_WAIT   = 3

-------------------------------------------------------------------------
-- State
-------------------------------------------------------------------------

local role            = "worker"   -- "worker" | "master"
local masterAddress   = nil
local masterPriority  = nil

local peers = {} -- [address] = {priority=, name=, role=, lastSeen=}

local electionInProgress = false
local electionDeadline   = 0
local bestSeen           = {priority = -math.huge, address = ""}
local pendingCoordinatorDeadline = nil

local lastHelloSent     = 0
local lastHeartbeatSent = 0
local lastHeartbeatRecv = 0

local taskCounter = 0
local jobs = {} -- master-side bookkeeping: [taskId] = {expected, got, results, requester, jobName, startTime}

-- ASSIGNs that arrived while this node was already busy executing a job.
-- They cannot be run inline (the worker is synchronous and single-tasking),
-- so they are queued here and re-dispatched from the main loop once the
-- current job returns. The master-side scheduler is informed so it can
-- re-roll the job without the dropped rank.
local pendingAssigns = {}

-------------------------------------------------------------------------
-- Small helpers
-------------------------------------------------------------------------

local function log(fmt, ...)
  print(("[%s] " .. fmt):format(os.date("%H:%M:%S"), ...))
end

local function send(address, msg)
  msg.from = ID
  modem.send(address, PORT, serialization.serialize(msg))
end

local function broadcastMsg(msg)
  msg.from = ID
  modem.broadcast(PORT, serialization.serialize(msg))
end

local function newTaskId()
  taskCounter = taskCounter + 1
  return ID .. "-" .. taskCounter .. "-" .. math.floor(computer.uptime() * 100)
end

local function tablen(t)
  local n = 0
  for _ in pairs(t) do n = n + 1 end
  return n
end

-------------------------------------------------------------------------
-- Master election
--
-- Any node with priority >= 0 can become master. Whoever has the
-- highest priority wins; ties are broken by comparing addresses so the
-- result is deterministic across the whole cluster. If two nodes ever
-- announce themselves as master at once, the loser is corrected the
-- next time it hears from (or about) the winner and steps down.
-------------------------------------------------------------------------

local function considerMasterClaim(fromAddr, claimedMaster, claimedPriority)
  if claimedMaster == nil then return end

  local better = (masterAddress == nil)
    or (claimedPriority > masterPriority)
    or (claimedPriority == masterPriority and claimedMaster > masterAddress)

  if better then
    local wasMaster = (role == "master" and masterAddress == ID)
    masterAddress  = claimedMaster
    masterPriority = claimedPriority
    role = (claimedMaster == ID) and "master" or "worker"
    lastHeartbeatRecv = computer.uptime()
    electionInProgress = false
    pendingCoordinatorDeadline = nil

    if wasMaster and role == "worker" then
      log("Stepping down: higher-priority master %s (p=%d) found", claimedMaster:sub(1, 8), claimedPriority)
    elseif not wasMaster and role == "master" then
      log("This node is now MASTER (priority %d)", MY_PRIORITY)
    elseif role == "worker" then
      log("Master is %s (priority %d)", claimedMaster:sub(1, 8), claimedPriority)
    end
  elseif claimedMaster ~= masterAddress then
    -- Someone is claiming mastership but we already know a better master.
    -- Correct them so the cluster converges back to a single master.
    send(fromAddr, {type = "COORDINATOR", master = masterAddress, priority = masterPriority})
  end
end

local function startElection()
  if electionInProgress then return end
  electionInProgress = true
  electionDeadline = computer.uptime() + ELECTION_WINDOW
  if MY_PRIORITY >= 0 then
    bestSeen = {priority = MY_PRIORITY, address = ID}
  else
    bestSeen = {priority = -math.huge, address = ""}
  end
  broadcastMsg({type = "ELECTION", priority = MY_PRIORITY})
  log("Starting master election...")
end

-------------------------------------------------------------------------
-- Message dispatch table (filled in further down)
-------------------------------------------------------------------------

local handlers = {}

-------------------------------------------------------------------------
-- Job execution
--
-- Worker nodes run the received job source with load(), passing a small
-- API table (rank/size/send/recv/broadcast/scatter/gather/barrier/log/
-- result) as the chunk's varargs. Execution is synchronous: while a
-- node is running a job it will not process other cluster traffic
-- except messages belonging to that job (which are intercepted inside
-- send/recv below), so avoid assigning long jobs to a node you need to
-- stay responsive as master.
-------------------------------------------------------------------------

local function buildTaskAPI(taskId, rank, size, ranks, reportTo)
  local api = {rank = rank, size = size, taskId = taskId}
  local resultSent = false

  -- Per-task inbox of TASK_MSG messages that arrived for this task but
  -- didn't match the specific (fromRank, tag) the caller was waiting for.
  -- Without this, an out-of-order message (e.g. rank 2's BARRIER reaching
  -- rank 0 while rank 0 is still waiting on rank 1) would be dispatched
  -- to handlers.TASK_MSG (a no-op) and silently dropped, hanging the job.
  local inbox = {}

  local function rawSend(toRank, tag, payload)
    local addr = ranks[toRank]
    if not addr then error("cluster.send: no such rank " .. tostring(toRank)) end
    send(addr, {type = "TASK_MSG", taskId = taskId, fromRank = rank, toRank = toRank, tag = tag, payload = payload})
  end

  local function matches(msg, fromRank, tag)
    return (fromRank == nil or msg.fromRank == fromRank)
       and (tag == nil or msg.tag == tag)
  end

  local function rawRecv(fromRank, tag, timeout)
    -- First, scan the inbox for anything that already satisfies this wait.
    for i, m in ipairs(inbox) do
      if matches(m, fromRank, tag) then
        table.remove(inbox, i)
        return m.payload, m.fromRank
      end
    end

    timeout = timeout or 30
    local deadline = computer.uptime() + timeout
    while true do
      local remaining = deadline - computer.uptime()
      if remaining <= 0 then return nil, "timeout" end
      local e, _, _, port, _, data = event.pull(remaining, "modem_message")
      if e == "modem_message" and port == PORT then
        local ok, msg = pcall(serialization.unserialize, data)
        if ok and type(msg) == "table" then
          if msg.type == "TASK_MSG" and msg.taskId == taskId then
            if matches(msg, fromRank, tag) then
              return msg.payload, msg.fromRank
            else
              -- Belongs to this task but not what we're waiting for right
              -- now. Queue it so a later rawRecv() with different filters
              -- can pick it up. Previously this was dropped, which broke
              -- barrier/gather any time messages arrived out of order.
              table.insert(inbox, msg)
            end
          else
            -- Not a TASK_MSG for this task. Dispatch to its handler so
            -- the cluster keeps ticking (heartbeats, etc.) while this
            -- job blocks on I/O.
            -- EXCEPTION: never dispatch ASSIGN here. While a worker is
            -- already running a job it cannot start another one
            -- synchronously without re-entering executeTask and
            -- corrupting the in-flight job's state. Stash the late
            -- ASSIGN so the main loop can deal with it after we return.
            if msg.type == "ASSIGN" then
              table.insert(pendingAssigns, msg)
            else
              local h = handlers[msg.type]
              if h then h(msg) end
            end
          end
        end
      end
    end
  end

  function api.send(toRank, data) rawSend(toRank, "DATA", data) end
  function api.recv(fromRank, timeout) return rawRecv(fromRank, "DATA", timeout) end

  function api.broadcast(data)
    if rank == 0 then
      for r = 1, size - 1 do rawSend(r, "BCAST", data) end
      return data
    else
      return rawRecv(0, "BCAST")
    end
  end

  function api.barrier()
    if rank == 0 then
      for r = 1, size - 1 do rawRecv(r, "BARRIER") end
      for r = 1, size - 1 do rawSend(r, "BARRIER_DONE", true) end
    else
      rawSend(0, "BARRIER", true)
      rawRecv(0, "BARRIER_DONE")
    end
  end

  function api.gather(data)
    if rank == 0 then
      local out = {[0] = data}
      for r = 1, size - 1 do out[r] = rawRecv(r, "GATHER") end
      return out
    else
      rawSend(0, "GATHER", data)
      return nil
    end
  end

  function api.scatter(dataArray)
    if rank == 0 then
      for r = 1, size - 1 do rawSend(r, "SCATTER", dataArray[r]) end
      return dataArray[0]
    else
      return rawRecv(0, "SCATTER")
    end
  end

  function api.log(message)
    send(reportTo, {type = "LOG", taskId = taskId, rank = rank, message = tostring(message)})
  end

  function api.result(data)
    resultSent = true
    send(reportTo, {type = "TASK_DONE", taskId = taskId, rank = rank, ok = true, result = data})
  end

  return api, function() return resultSent end
end

local function executeTask(msg)
  local taskId, rank, size, ranks, source = msg.taskId, msg.rank, msg.size, msg.ranks, msg.source
  local reportTo = msg.from

  log("Running task %s (job=%s) as rank %d/%d", taskId:sub(1, 12), tostring(msg.jobName), rank, size)

  local api, resultSent = buildTaskAPI(taskId, rank, size, ranks, reportTo)

  local chunk, cerr = load(source, "=job:" .. tostring(msg.jobName), "t")
  local ok, result
  if not chunk then
    ok, result = false, "compile error: " .. tostring(cerr)
  else
    ok, result = pcall(chunk, api)
  end

  if not resultSent() then
    send(reportTo, {
      type = "TASK_DONE", taskId = taskId, rank = rank,
      ok = ok, result = ok and result or nil,
      error = (not ok) and tostring(result) or nil,
    })
  end

  log("Task %s finished (rank %d) ok=%s", taskId:sub(1, 12), rank, tostring(ok))
end

-------------------------------------------------------------------------
-- Handlers
-------------------------------------------------------------------------

handlers.HELLO = function(msg)
  peers[msg.from] = {priority = msg.priority, name = msg.name, role = msg.role, lastSeen = computer.uptime()}
end

handlers.ELECTION = function(msg)
  peers[msg.from] = peers[msg.from] or {}
  peers[msg.from].priority = msg.priority
  peers[msg.from].lastSeen = computer.uptime()

  if not electionInProgress then startElection() end

  if msg.priority > bestSeen.priority
     or (msg.priority == bestSeen.priority and msg.from > bestSeen.address) then
    bestSeen = {priority = msg.priority, address = msg.from}
  end
end

handlers.COORDINATOR = function(msg)
  considerMasterClaim(msg.from, msg.master or msg.from, msg.priority)
end

handlers.HEARTBEAT = function(msg)
  considerMasterClaim(msg.from, msg.from, msg.priority)
  if masterAddress == msg.from then lastHeartbeatRecv = computer.uptime() end
end

handlers.ASSIGN = executeTask

handlers.TASK_MSG = function() end -- stray/late task traffic; safe to ignore

handlers.SUBMIT = function(msg)
  if role ~= "master" then
    if masterAddress then send(masterAddress, msg) end
    return
  end

  local candidates = {}
  local now = computer.uptime()
  for addr, info in pairs(peers) do
    if now - info.lastSeen <= HELLO_EXPIRE then table.insert(candidates, addr) end
  end
  table.sort(candidates)

  local ranks, n = {}, 0
  if msg.includeSelf then ranks[0] = ID; n = 1 end
  for _, addr in ipairs(candidates) do
    if msg.nodeCount ~= "all" and n >= msg.nodeCount then break end
    ranks[n] = addr
    n = n + 1
  end

  if n == 0 then
    send(msg.from, {type = "SUBMIT_ACK", ok = false, error = "no worker nodes available"})
    return
  end

  local taskId = newTaskId()
  jobs[taskId] = {expected = n, got = 0, results = {}, requester = msg.from, jobName = msg.jobName, startTime = now}

  -- Send ASSIGN to every *remote* node first and only run our own (self)
  -- task last. Running a self-task inline blocks this whole event loop
  -- (including heartbeats) until it finishes, so if it ran first the
  -- other nodes would never even receive their ASSIGN until we were
  -- done - and might time out on us and start a new election in the
  -- meantime. Dispatching remotely first avoids that entirely.
  local selfAssign = nil
  for rank, addr in pairs(ranks) do
    local assignMsg = {
      type = "ASSIGN", taskId = taskId, rank = rank, size = n,
      ranks = ranks, source = msg.source, jobName = msg.jobName,
    }
    if addr == ID then
      assignMsg.from = ID
      selfAssign = assignMsg
    else
      send(addr, assignMsg)
    end
  end

  send(msg.from, {type = "SUBMIT_ACK", ok = true, taskId = taskId, size = n})
  log("Dispatched job '%s' as task %s across %d node(s)", tostring(msg.jobName), taskId:sub(1, 12), n)

  if selfAssign then
    executeTask(selfAssign)
  end
end

handlers.SUBMIT_ACK = function(msg)
  if msg.ok then
    log("Job accepted: taskId=%s size=%d", msg.taskId:sub(1, 12), msg.size)
  else
    log("Job rejected: %s", tostring(msg.error))
  end
end

handlers.SUBMIT_RESULT = function(msg)
  print(("Job '%s' complete in %.2fs (task %s):"):format(tostring(msg.jobName), msg.elapsed, msg.taskId:sub(1, 8)))
  for rank, r in pairs(msg.results) do
    if r.ok then
      print(("  rank %d -> %s"):format(rank, tostring(r.result)))
    else
      print(("  rank %d FAILED: %s"):format(rank, tostring(r.error)))
    end
  end
end

handlers.LOG = function(msg)
  print(("  [%s][rank %d] %s"):format(msg.taskId:sub(1, 8), msg.rank, msg.message))
end

handlers.TASK_DONE = function(msg)
  local job = jobs[msg.taskId]
  if not job then return end

  job.got = job.got + 1
  job.results[msg.rank] = {ok = msg.ok, result = msg.result, error = msg.error}

  if job.requester ~= ID then
    send(job.requester, {type = "LOG", taskId = msg.taskId, rank = msg.rank,
      message = ("done ok=%s"):format(tostring(msg.ok))})
  end

  if job.got >= job.expected then
    local resultMsg = {
      type = "SUBMIT_RESULT", taskId = msg.taskId, jobName = job.jobName,
      results = job.results, elapsed = computer.uptime() - job.startTime,
    }
    if job.requester == ID then
      resultMsg.from = ID
      handlers.SUBMIT_RESULT(resultMsg)
    else
      send(job.requester, resultMsg)
    end
    jobs[msg.taskId] = nil
  end
end

-------------------------------------------------------------------------
-- Console
-------------------------------------------------------------------------

local function readFile(path)
  if not fs.exists(path) then return nil, "file not found: " .. path end
  local f, err = io.open(path, "r")
  if not f then return nil, err end
  local data = f:read("*a")
  f:close()
  return data
end

local function handleCommand(line)
  line = line:gsub("^%s+", ""):gsub("%s+$", "")
  if line == "" then return end

  local parts = {}
  for w in line:gmatch("%S+") do table.insert(parts, w) end
  local cmd = parts[1]

  if cmd == "submit" then
    local path = parts[2]
    if not path then print("usage: submit <file.lua> [n|all] [jobName] [self]"); return end
    local nodeCount = parts[3] or "all"
    if nodeCount ~= "all" then nodeCount = tonumber(nodeCount) or "all" end
    local jobName = parts[4] or fs.name(path)
    local includeSelf = false
    for i = 3, #parts do if parts[i] == "self" then includeSelf = true end end

    local source, err = readFile(path)
    if not source then print("Error: " .. err); return end
    if not masterAddress then print("No master elected yet - try again shortly."); return end

    local m = {type = "SUBMIT", jobName = jobName, source = source, nodeCount = nodeCount, includeSelf = includeSelf}
    if masterAddress == ID then
      m.from = ID
      handlers.SUBMIT(m)
    else
      send(masterAddress, m)
    end
    print(("Submitted '%s' to master %s (requesting %s node(s))"):format(jobName, masterAddress:sub(1, 8), tostring(nodeCount)))

  elseif cmd == "nodes" then
    print(("%-10s %-10s %-6s %-8s"):format("NAME", "ADDR", "PRI", "ROLE"))
    local tag = (masterAddress == ID) and "  (you, MASTER)" or "  (you)"
    print(("%-10s %-10s %-6d %-8s"):format(NAME, ID:sub(1, 8), MY_PRIORITY, role) .. tag)
    for addr, info in pairs(peers) do
      local isMaster = (addr == masterAddress) and "  (MASTER)" or ""
      print(("%-10s %-10s %-6s %-8s"):format(tostring(info.name), addr:sub(1, 8), tostring(info.priority), tostring(info.role)) .. isMaster)
    end

  elseif cmd == "status" then
    print("Role:        " .. role)
    print("Master:      " .. (masterAddress and masterAddress:sub(1, 8) or "unknown (electing...)"))
    print("Known peers: " .. tablen(peers))
    if role == "master" then print("Active jobs: " .. tablen(jobs)) end

  elseif cmd == "priority" then
    local p = tonumber(parts[2])
    if not p then print("usage: priority <number>"); return end
    MY_PRIORITY = p
    print("Priority set to " .. p .. " (used from the next election onwards)")

  elseif cmd == "help" then
    print("commands:")
    print("  submit <file.lua> [n|all] [jobName] [self]  - run a job across n nodes")
    print("  nodes                                       - list known cluster nodes")
    print("  status                                      - show this node's status")
    print("  priority <n>                                - change master-election priority")
    print("  quit                                         - exit")

  elseif cmd == "quit" or cmd == "exit" then
    print("bye")
    os.exit()

  else
    print("unknown command '" .. cmd .. "' - type 'help'")
  end
end

-------------------------------------------------------------------------
-- Main event loop
--
-- We deliberately avoid term.read() here: it discards any signal that
-- isn't a keyboard event while it blocks, which would silently drop
-- incoming network traffic while someone is typing a command. Instead
-- we pull *any* event with a short timeout and handle keyboard input
-- by hand, so nothing gets lost.
-------------------------------------------------------------------------

local inputBuffer = ""
local PROMPT = "cluster> "

local function redrawPrompt()
  term.write("\r" .. PROMPT .. inputBuffer .. string.rep(" ", 4) .. "\r" .. PROMPT .. inputBuffer)
end

print(("cluster_node starting - id=%s name=%s priority=%d port=%d"):format(ID:sub(1, 8), NAME, MY_PRIORITY, PORT))
print("type 'help' for commands")
term.write(PROMPT)

while true do
  local e, p1, p2, p3, p4, p5 = event.pull(0.5)

  if e == "modem_message" then
    local port, data = p3, p5
    if port == PORT then
      local ok, msg = pcall(serialization.unserialize, data)
      if ok and type(msg) == "table" and msg.type and handlers[msg.type] then
        handlers[msg.type](msg)
      end
    end
    redrawPrompt()

  elseif e == "key_down" then
    local char, code = p2, p3
    if code == keyboard.keys.enter then
      term.write("\n")
      local line = inputBuffer
      inputBuffer = ""
      handleCommand(line)
      term.write(PROMPT)
    elseif code == keyboard.keys.back then
      if #inputBuffer > 0 then
        inputBuffer = inputBuffer:sub(1, -2)
        redrawPrompt()
      end
    elseif char and char > 31 then
      inputBuffer = inputBuffer .. unicode.char(char)
      term.write(unicode.char(char))
    end
  end

  -- Drain any ASSIGNs that were queued while we were busy running a job.
  -- We do this after event handling and outside any handler so that the
  -- queued job runs cleanly in an idle state, the same way a freshly
  -- arrived ASSIGN would.
  while #pendingAssigns > 0 do
    local queued = table.remove(pendingAssigns, 1)
    if handlers.ASSIGN then handlers.ASSIGN(queued) end
  end

  local now = computer.uptime()

  if now - lastHelloSent >= HELLO_INTERVAL then
    lastHelloSent = now
    broadcastMsg({type = "HELLO", priority = MY_PRIORITY, name = NAME, role = role})
    for addr, info in pairs(peers) do
      if now - info.lastSeen > HELLO_EXPIRE then peers[addr] = nil end
    end
  end

  if role == "master" and now - lastHeartbeatSent >= HEARTBEAT_INTERVAL then
    lastHeartbeatSent = now
    broadcastMsg({type = "HEARTBEAT", master = ID, priority = MY_PRIORITY})
  end

  if role == "worker" and masterAddress and now - lastHeartbeatRecv > HEARTBEAT_TIMEOUT then
    log("Lost contact with master %s - starting new election", masterAddress:sub(1, 8))
    masterAddress, masterPriority = nil, nil
    startElection()
  end

  if not masterAddress and not electionInProgress and now - lastHeartbeatRecv > COORDINATOR_WAIT then
    startElection()
  end

  if electionInProgress and now >= electionDeadline then
    electionInProgress = false
    if bestSeen.address == ID then
      role, masterAddress, masterPriority = "master", ID, MY_PRIORITY
      lastHeartbeatSent = 0
      broadcastMsg({type = "COORDINATOR", master = ID, priority = MY_PRIORITY})
      log("Elected as MASTER (priority %d)", MY_PRIORITY)
    else
      pendingCoordinatorDeadline = now + COORDINATOR_WAIT
    end
  end

  if pendingCoordinatorDeadline and now > pendingCoordinatorDeadline and not masterAddress then
    pendingCoordinatorDeadline = nil
    startElection()
  end
end