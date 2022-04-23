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

function testcase.new()
    -- test that create new libmdbx.env object
    local env = assert(libmdbx.new())
    assert.match(env, '^libmdbx.env: ', false)
end

function testcase.env_open()
    local env = assert(libmdbx.new())

    -- test that open
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
