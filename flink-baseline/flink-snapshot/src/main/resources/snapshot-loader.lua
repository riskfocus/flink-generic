local function isempty(s)
    return s == nil or s == '' or s == "NIL" or s == cjson.null or s == false
end

local function getByContext(ctxId)
    return redis.call("GET", ctxId)
end

local function findContext(ctxId, contextsList)
    local candidate = 0
    local ctxIdInt = tonumber(ctxId)
    for k, indexContext in pairs(contextsList) do
        --redis.call("ECHO", "ctxId: " .. ctxId .. " indexContext: " .. indexContext)
        if (tonumber(indexContext) <= ctxIdInt) then
            candidate = indexContext
        end
    end
    --redis.call("ECHO", "Found Candidate: " .. candidate)
    return candidate
end

local getKey = KEYS[1]
local indexKey = KEYS[2]
local snapshotPrefixKey = KEYS[3]

local secondaryId = ARGV[1] == "NIL" and nil or ARGV[1]
local ctxId = ARGV[2] == "NIL" and nil or ARGV[2]
local date = ARGV[3] == "NIL" and nil or ARGV[3]

local getResults = getByContext(getKey)

if not isempty(getResults) then
    return ctxId .. ":" .. getResults
else
    local contextsList = redis.call("ZRANGEBYSCORE", indexKey, 0, ctxId)
    if isempty(contextsList) then
        return nil
    else
        local foundCtxId = findContext(ctxId, contextsList)
        local res = getByContext(snapshotPrefixKey .. ":" .. date .. ":" .. foundCtxId .. ":" .. secondaryId)
        if not isempty(res) then
            return foundCtxId .. ":" .. res
        else
            return nil
        end
    end
end
