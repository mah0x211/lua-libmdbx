local testcase = require('testcase')
local fileno = require('io.fileno')
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

local function openenv(...)
    local env = assert(libmdbx.new())
    assert(env:open(PATHNAME, nil, libmdbx.NOSUBDIR, libmdbx.COALESCE,
                    libmdbx.LIFORECLAIM, ...))
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

function testcase.delete()
    local env = openenv()

    -- test that delete the environment's files in a proper and multiprocess-safe way
    assert.is_true(env:delete())

    -- test that already deleted
    local ok, err, eno = env:delete()
    assert.is_false(ok)
    assert.equal(err, libmdbx.errno.RESULT_TRUE.message)
    assert.equal(eno, libmdbx.errno.RESULT_TRUE.errno)

    -- test that cannot be deleted if env is not yet open
    env = assert(libmdbx.new())
    ok, err, eno = env:delete()
    assert.is_false(ok)
    assert.equal(err, libmdbx.errno.EPERM.message)
    assert.equal(eno, libmdbx.errno.EPERM.errno)

end

function testcase.copy()
    local env = openenv()

    -- test that copy env to specified path
    local pathname = './copied.db'
    assert.is_true(env:copy(pathname))
    local f = assert(io.open(pathname))
    f:close()
    os.remove(pathname)

    -- test that cannot be copied to the same place as the source db
    local ok = env:copy(PATHNAME)
    assert.is_false(ok)
end

function testcase.copy2fd()
    local env = openenv()

    -- test that copy env to specified path
    local pathname = './copied.db'
    local f = assert(io.open(pathname, 'w+'))
    assert.equal(f:seek('end'), 0)
    local fd = assert.is_int(fileno(f))
    assert.is_true(env:copy2fd(fd))
    assert.greater(f:seek('end'), 0)
    f:close()
    os.remove(pathname)

    -- test that cannot be copied to the same place as the source db
    f = assert(io.open(PATHNAME))
    fd = assert.is_int(fileno(f))
    local ok = env:copy2fd(fd)
    assert.is_false(ok)
end

function testcase.stat()
    local env = openenv()

    -- test that get the db stat
    assert.is_table(env:stat())

    -- test that cannot be get the db stat
    env = assert(libmdbx.new())
    local stat, err, eno = env:stat()
    assert.is_nil(stat)
    assert.equal(err, libmdbx.errno.EPERM.message)
    assert.equal(eno, libmdbx.errno.EPERM.errno)
end

function testcase.info()
    local env = openenv()

    -- test that get the db info
    assert.is_table(env:info())

    -- test that can be get the db info even if its not yet open
    env = assert(libmdbx.new())
    assert.is_table(env:info())
end

function testcase.sync()
    local env = openenv()

    -- test that flush the env data buffers to disk
    assert.is_true(env:sync())

    -- test that cannot be flush the env data if its not yet open
    env = assert(libmdbx.new())
    local ok, err, eno = env:sync()
    assert.is_false(ok)
    assert.equal(err, libmdbx.errno.EPERM.message)
    assert.equal(eno, libmdbx.errno.EPERM.errno)
end

function testcase.sync_poll()
    local env = openenv()

    -- test that flush the env data buffers to disk
    assert.is_true(env:sync_poll())

    -- test that cannot be flush the env data if its not yet open
    env = assert(libmdbx.new())
    local ok, err, eno = env:sync_poll()
    assert.is_false(ok)
    assert.equal(err, libmdbx.errno.EPERM.message)
    assert.equal(eno, libmdbx.errno.EPERM.errno)
end

function testcase.set_get_syncbytes()
    local env = openenv()

    -- test that sets threshold to force flush the data buffers to disk
    assert.is_true(env:set_syncbytes(4096 * 2))
    assert.equal(env:get_syncbytes(), 4096 * 2)

    -- test that cannot be flush the env data if its not yet open
    env = assert(libmdbx.new())
    local ok, err, eno = env:set_syncbytes(1024)
    assert.is_false(ok)
    assert.equal(err, libmdbx.errno.EPERM.message)
    assert.equal(eno, libmdbx.errno.EPERM.errno)
end

function testcase.set_get_syncperiod()
    local env = openenv()

    -- test that sets relative period since the last unsteady commit to force
    -- flush the data buffers to disk, even of MDBX_SAFE_NOSYNC flag in the
    -- environment
    assert.is_true(env:set_syncperiod(16 * 16))
    assert.equal(env:get_syncperiod(), 16 * 16)

    -- test that cannot be sets the sync period
    env = assert(libmdbx.new())
    local ok, err, eno = env:set_syncperiod(1000)
    assert.is_false(ok)
    assert.equal(err, libmdbx.errno.EPERM.message)
    assert.equal(eno, libmdbx.errno.EPERM.errno)

    -- test that cannot be get the sync period
    local v
    v, err, eno = env:get_syncperiod()
    assert.is_nil(v)
    assert.equal(err, libmdbx.errno.EPERM.message)
    assert.equal(eno, libmdbx.errno.EPERM.errno)
end

function testcase.set_get_flags()
    local env = assert(libmdbx.new())

    -- test that set environment flags
    assert.is_true(env:set_flags(true, libmdbx.NOSUBDIR, libmdbx.EXCLUSIVE,
                                 libmdbx.ACCEDE, libmdbx.WRITEMAP,
                                 libmdbx.NOTLS, libmdbx.NORDAHEAD,
                                 libmdbx.NOMEMINIT, libmdbx.COALESCE,
                                 libmdbx.LIFORECLAIM, libmdbx.PAGEPERTURB,
                                 libmdbx.NOMETASYNC, libmdbx.SAFE_NOSYNC))

    -- test that get environment flags
    local flgs = assert.is_table(env:get_flags())
    assert.equal(flgs, {
        NOSUBDIR = libmdbx.NOSUBDIR,
        EXCLUSIVE = libmdbx.EXCLUSIVE,
        ACCEDE = libmdbx.ACCEDE,
        WRITEMAP = libmdbx.WRITEMAP,
        NOTLS = libmdbx.NOTLS,
        NORDAHEAD = libmdbx.NORDAHEAD,
        NOMEMINIT = libmdbx.NOMEMINIT,
        COALESCE = libmdbx.COALESCE,
        LIFORECLAIM = libmdbx.LIFORECLAIM,
        PAGEPERTURB = libmdbx.PAGEPERTURB,
        NOMETASYNC = libmdbx.NOMETASYNC,
        SAFE_NOSYNC = libmdbx.SAFE_NOSYNC,
        UTTERLY_NOSYNC = libmdbx.UTTERLY_NOSYNC,
    })

    -- test that unset environment flags
    assert.is_true(env:set_flags(false, libmdbx.NOSUBDIR, libmdbx.NOTLS,
                                 libmdbx.NORDAHEAD))
    flgs = assert.is_table(env:get_flags())
    assert.equal(flgs, {
        EXCLUSIVE = libmdbx.EXCLUSIVE,
        ACCEDE = libmdbx.ACCEDE,
        WRITEMAP = libmdbx.WRITEMAP,
        NOMEMINIT = libmdbx.NOMEMINIT,
        COALESCE = libmdbx.COALESCE,
        LIFORECLAIM = libmdbx.LIFORECLAIM,
        PAGEPERTURB = libmdbx.PAGEPERTURB,
        NOMETASYNC = libmdbx.NOMETASYNC,
        SAFE_NOSYNC = libmdbx.SAFE_NOSYNC,
        UTTERLY_NOSYNC = libmdbx.UTTERLY_NOSYNC,
    })
end

function testcase.get_path()
    local env = openenv()

    -- test that return the path that was used in mdbx_env_open()
    assert.equal(env:get_path(), PATHNAME)
    env:close()

    env = assert(libmdbx.new())
    assert.is_nil(env:get_path())
end

function testcase.get_fd()
    local env = openenv()

    -- test that return the path that was used in mdbx_env_open()
    assert.is_uint(env:get_fd())
    env:close()

    env = assert(libmdbx.new())
    assert.is_nil(env:get_fd())
end

function testcase.set_geometry()
    -- TODO
end

function testcase.set_get_maxreaders()
    local env = assert(libmdbx.new())

    -- test that set the maximum number of threads/reader slots
    assert.is_true(env:set_maxreaders(10))
    -- test that get the maximum number of threads/reader slots
    assert.equal(env:get_maxreaders(), 10)
    assert(env:close())

    -- test that fail if env is open
    env = openenv()
    local ok, err, eno = env:set_maxreaders(10)
    assert.is_false(ok)
    assert.equal(err, libmdbx.errno.EPERM.message)
    assert.equal(eno, libmdbx.errno.EPERM.errno)
end

function testcase.set_get_maxdbs()
    local env = assert(libmdbx.new())

    -- test that set the maximum number of named databases for the environment
    assert.is_true(env:set_maxdbs(10))
    -- test that get the maximum number of named databases for the environment
    assert.equal(env:get_maxdbs(), 10)
    assert(env:close())

    -- test that fail if env is open
    env = openenv()
    local ok, err, eno = env:set_maxdbs(10)
    assert.is_false(ok)
    assert.equal(err, libmdbx.errno.EPERM.message)
    assert.equal(eno, libmdbx.errno.EPERM.errno)
end

function testcase.get_maxkeysize()
    local env = assert(libmdbx.new())

    -- test that returns the maximum size of keys can put
    assert.is_int(env:get_maxkeysize())
end

function testcase.get_maxvalsize()
    local env = assert(libmdbx.new())

    -- test that returns the maximum size of data we can put
    assert.is_int(env:get_maxvalsize())
end

function testcase.begin()
    local env = openenv()

    -- test that create new libmdbx.txn object
    local txn = assert(env:begin())
    assert.match(txn, '^libmdbx.txn: ', false)
end

function testcase.reader_list()
    local env = openenv(libmdbx.NOTLS)
    -- luacheck: ignore txn1 txn2
    local txn1 = assert(env:begin(libmdbx.TXN_RDONLY))
    local txn2 = assert(env:begin(libmdbx.TXN_RDONLY))

    -- test that returns the maximum size of data we can put
    local count = 0
    -- luacheck: ignore ...
    assert.is_true(env:reader_list(function(...)
        count = count + 1
    end))
    assert.equal(count, 2)
end

function testcase.reader_check()
    local env = openenv()

    -- test that check for stale entries in the reader lock table
    assert.is_int(env:reader_check())
end
