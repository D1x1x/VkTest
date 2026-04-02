box.cfg{
    listen = 3301
}

if not box.schema.user.exists('app') then
    box.schema.user.create('app', { password = 'app', if_not_exists = true })
end

box.schema.user.grant('app', 'read,write,execute', 'universe', nil, { if_not_exists = true })

local kv = box.space.KV
if kv == nil then
    kv = box.schema.space.create('KV', { if_not_exists = true })
end

kv:format({
    { name = 'key', type = 'string' },
    { name = 'value', type = 'varbinary', is_nullable = true }
})

kv:create_index('primary', {
    type = 'TREE',
    unique = true,
    parts = {
        { field = 'key', type = 'string' }
    },
    if_not_exists = true
})

function kv_count()
    return box.space.KV:count()
end

print('Tarantool is ready')