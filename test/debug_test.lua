local unpack = unpack or table.unpack
local testcase = require('testcase')
local libmdbx = require('libmdbx')

-- print(dump(libmdbx.errno))

local PATHNAME = './test.db'
local LOCKFILE = PATHNAME .. libmdbx.LOCK_SUFFIX
local DEBUG_DEFAULTS

function testcase.before_each()
    os.remove(PATHNAME)
    os.remove(LOCKFILE)

    DEBUG_DEFAULTS = {
        libmdbx.debug.setup(),
    }
    for i, v in ipairs(DEBUG_DEFAULTS) do
        DEBUG_DEFAULTS[i] = libmdbx[v]
    end
end

function testcase.after_each()
    os.remove(PATHNAME)
    os.remove(LOCKFILE)
    libmdbx.debug.setup(nil, unpack(DEBUG_DEFAULTS))
end

local function openenv(...)
    local env = assert(libmdbx.new())
    assert(env:open(PATHNAME, nil, libmdbx.NOSUBDIR, libmdbx.COALESCE,
                    libmdbx.LIFORECLAIM, ...))
    return env
end

function testcase.setup_log_level()
    -- test that set log-level and return previous values
    local prev
    for _, v in ipairs({
        'LOG_FATAL',
        'LOG_ERROR',
        'LOG_WARN',
        'LOG_NOTICE',
        'LOG_VERBOSE',
        'LOG_DEBUG',
        'LOG_TRACE',
        'LOG_EXTRA',
    }) do
        local lv = libmdbx.debug.setup(nil, libmdbx[v])
        if prev then
            assert.equal(lv, prev)
        end
        prev = v
    end
    assert.equal(libmdbx.debug.setup(), prev)
end

function testcase.setup_debug_flags()
    -- test that get current log-level and debug flags
    libmdbx.debug.setup(nil, nil,
    -- libmdbx.DBG_ASSERT, libmdbx.DBG_AUDIT, libmdbx.DBG_JITTER
                        libmdbx.DBG_DUMP, libmdbx.DBG_LEGACY_MULTIOPEN,
                        libmdbx.DBG_LEGACY_OVERLAP, libmdbx.DBG_DONT_UPGRADE)

    -- test that get current log-level and debug flags
    local lvflgs = {
        libmdbx.debug.setup(),
    }
    local current_flgs = {
        DBG_DUMP = true,
        DBG_LEGACY_MULTIOPEN = true,
        DBG_LEGACY_OVERLAP = true,
        DBG_DONT_UPGRADE = true,
    }
    for i = 2, #lvflgs do
        local flg = lvflgs[i]
        assert.is_true(current_flgs[flg])
        current_flgs[flg] = nil
    end
end

function testcase.setup_logger()
    -- test that call the debug function
    local ncall = 0
    libmdbx.debug.setup(function(loglv, fname, line, msg)
        assert.match(loglv, '^LOG_', false)
        assert.is_string(fname)
        assert.is_int(line)
        assert.is_string(msg)
        ncall = ncall + 1
    end, libmdbx.LOG_EXTRA)
    assert.equal(ncall, 0)
    local env = openenv()
    assert.greater(ncall, 0)
    assert(env:close())

    -- test that ignore debug function error
    ncall = 0
    libmdbx.debug.setup(function()
        -- luacheck: ignore a
        a = a + 1
        ncall = ncall + 1
    end)
    env = openenv()
    env:close()
    assert.equal(ncall, 0)

    -- test that remove the debug logger function
    ncall = 0
    assert(libmdbx.debug.setup(nil, libmdbx.LOG_FATAL))
    env = openenv()
    env:close()
    assert.equal(ncall, 0)
end

