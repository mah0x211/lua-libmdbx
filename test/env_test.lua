local testcase = require('testcase')
local libmdbx = require('libmdbx')

local PATHNAME = './test.db'
local LOCKFILE = PATHNAME .. libmdbx.LOCK_SUFFIX

function testcase.before_each()
    os.remove(PATHNAME)
    os.remove(LOCKFILE)
end

function testcase.after_each()
    os.remove(PATHNAME)
    os.remove(LOCKFILE)
end

local function openenv()
    local env = assert(libmdbx.new())
    assert(env:open(PATHNAME, nil, libmdbx.NOSUBDIR, libmdbx.COALESCE,
                    libmdbx.LIFORECLAIM))
    return env
end

function testcase.new()
    -- test that create an MDBX environment instance
    local env = assert(libmdbx.new())
    assert.match(env, '^libmdbx.env: ', false)
end

function testcase.set_get_option()
    local env = assert(libmdbx.new())

    -- test that sets the value of a runtime options for an environment
    assert(env:set_option(libmdbx.opt_max_readers, 123))

    -- test that gets the value of runtime options from an environment
    assert.equal(env:get_option(libmdbx.opt_max_readers), 123)

    -- test that return error if option is invalid
    local v, err, eno = env:get_option(-123)
    assert.is_nil(v)
    assert.equal(err, libmdbx.errno.EINVAL.message)
    assert.equal(eno, libmdbx.errno.EINVAL.errno)
end

function testcase.open()
    local env = assert(libmdbx.new())

    -- test that open an environment instance
    assert(env:open(PATHNAME, tonumber('0644', 8), libmdbx.NOSUBDIR,
                    libmdbx.COALESCE, libmdbx.LIFORECLAIM))
end

function testcase.close()
    local env = openenv()

    -- test that close the environment and release the memory map
    assert(env:close())

    -- test that close the unopened environment
    env = assert(libmdbx.new())
    assert(env:open(PATHNAME, tonumber('0644', 8), libmdbx.NOSUBDIR,
                    libmdbx.COALESCE, libmdbx.LIFORECLAIM))

end

function testcase.begin()
    local env = assert(libmdbx.new())
    assert(env:open(PATHNAME, nil, libmdbx.NOSUBDIR, libmdbx.COALESCE,
                    libmdbx.LIFORECLAIM))

    -- test that create new libmdbx.txn object
    local txn = assert(env:begin())
    assert.match(txn, '^libmdbx.txn: ', false)
end
