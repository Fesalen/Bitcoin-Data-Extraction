--[[
Save Address
KEYS[1]:key
ARGV[1]:direction
ARGV[2]:txid
ARGV[3]:height
--]]

local key, direction, txid, height = KEYS[1], ARGV[1], ARGV[2], tonumber(ARGV[3]);

if direction == "IN" then
    local db_height = redis.call("hget", key, "firstInputHeight")
    if not db_height or tonumber(db_height) > height then
        redis.call("hset", key, "firstInputHeight", height)
        redis.call("hset", key, "firstInputTxId", txid)
    end
else
    local db_height = redis.call("hget", key, "firstOutputHeight")
    if not db_height or tonumber(db_height) > height then
        redis.call("hset", key, "firstOutputHeight", height)
        redis.call("hset", key, "firstOutputTxId", txid)
    end
end
local db_height2 = redis.call("hget", key, "latestHeight")
if not db_height2 or tonumber(db_height2) < height then
    redis.call("hset", key, "latestHeight", height)
    redis.call("hset", key, "latestTxId", txid)
end
