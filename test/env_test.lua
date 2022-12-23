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

function testcase.set_get_option()
    local env = assert(libmdbx.new())

    -- test that set option
    assert(env:set_option(libmdbx.opt_max_readers, 123))

    -- test that get option
    assert.equal(env:get_option(libmdbx.opt_max_readers), 123)

    -- test that return error if option is invalid
    local v, err, errno = env:get_option(-123)
    assert.is_nil(v)
    assert.match(err, 'Invalid')
    assert.equal(errno, libmdbx.errno.EINVAL.errno)
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
