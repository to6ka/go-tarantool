local nodes_load = require("config_load_nodes")

-- Do not set listen for now so connector won't be
-- able to send requests until everything is configured.
box.cfg{
    work_dir = os.getenv("TEST_TNT_WORK_DIR"),
}

-- Function to call for getting address list, part of tarantool/multi API.
local get_cluster_nodes = nodes_load.get_cluster_nodes
rawset(_G, 'get_cluster_nodes', get_cluster_nodes)

box.once("init", function()
    box.schema.user.create('test', { password = 'test' })
    box.schema.user.grant('test', 'read,write,execute', 'universe')

    local s = box.schema.space.create('testPool', {
        id = 520,
        if_not_exists = true,
        format = {
            {name = "key", type = "string"},
            {name = "value", type = "string"},
        },
    })
    s:create_index('pk', {
        type = 'tree',
        parts = {{ field = 1, type = 'string' }},
        if_not_exists = true
    })
end)

-- Set listen only when every other thing is configured.
box.cfg{
    listen = os.getenv("TEST_TNT_LISTEN"),
}
