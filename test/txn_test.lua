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

local function openenv(...)
    local env = assert(libmdbx.new())
    assert(env:open(PATHNAME, nil, libmdbx.NOSUBDIR, libmdbx.COALESCE,
                    libmdbx.LIFORECLAIM, ...))
    return env
end

local function opentxn(...)
    local env = assert(openenv(...))
    return env:begin()
end

function testcase.env_stat()
    local txn = assert(opentxn())

    -- test that return statistics about the MDBX environment
    assert.is_table(txn:env_stat())
end

function testcase.env_info()
    local txn = assert(opentxn())

    -- test that return information about the MDBX environment
    assert.is_table(txn:env_info())
end

function testcase.begin()
    local txn = assert(opentxn())

    -- test that create a child transaction
    local ctxn = assert(txn:begin())
    assert.not_equal(ctxn, txn)
    assert.match(ctxn, 'libmdbx.txn: ')
end

function testcase.info()
    local txn = assert(opentxn())

    -- test that return information about the MDBX transaction
    assert.is_table(txn:info(true))

    -- test that throws an error if argument is invalid
    local err = assert.throws(txn.info, txn, 1)
    assert.match(err, '(boolean expected,')
end

function testcase.env()
    local env = assert(openenv())
    local txn = assert(env:begin())

    -- test that return the associated MDBX environment
    assert.equal(txn:env(), env)
end

function testcase.flags()
    local env = assert(openenv())

    -- test that get the transaction's flags
    local txn = assert(env:begin(libmdbx.TXN_RDONLY_PREPARE))
    local flgs = assert.is_table(txn:flags())
    assert.equal(flgs, {
        TXN_RDONLY = libmdbx.TXN_RDONLY,
        TXN_RDONLY_PREPARE = libmdbx.TXN_RDONLY_PREPARE,
    })

    -- test that get the transaction's flags
    txn = assert(env:begin(libmdbx.TXN_TRY, libmdbx.TXN_NOMETASYNC,
                           libmdbx.TXN_NOSYNC))
    flgs = assert.is_table(txn:flags())
    assert.equal(flgs, {
        TXN_TRY = libmdbx.TXN_TRY,
        TXN_NOMETASYNC = libmdbx.TXN_NOMETASYNC,
        TXN_NOSYNC = libmdbx.TXN_NOSYNC,
    })
end

function testcase.id()
    local txn = assert(opentxn())

    -- test that return the transaction's ID
    assert.is_int(txn:id())
end

function testcase.commit()
    local txn = assert(opentxn())

    -- test that commit all the operations of a transaction into the database
    assert.is_true(txn:commit())

    -- confirm that txn cannot be used after committed
    assert.is_nil(txn:env())
    assert.is_false(txn:commit())
end

function testcase.abort()
    local txn = assert(opentxn())

    -- test that marks transaction as broken
    assert.is_true(txn:abort(true))

    -- test that abandon all the operations of the transaction instead of saving them
    assert.is_true(txn:abort())

    -- confirm that txn cannot be used after aborted
    assert.is_nil(txn:env())
    assert.is_false(txn:commit())
end

function testcase.reset()
    local env = assert(openenv())
    local txn = assert(env:begin(libmdbx.TXN_RDONLY))

    -- test that reset a read-only transaction
    assert.is_true(txn:reset())
    assert(txn:abort())

    -- test that cannot reset non read-only transaction
    txn = assert(env:begin())
    local ok, err = txn:reset()
    assert.is_false(ok)
    assert.equal(err, libmdbx.errno.EINVAL)
end

function testcase.renew()
    local env = assert(openenv())
    local txn = assert(env:begin(libmdbx.TXN_RDONLY))

    -- test that renew a read-only transaction
    assert.is_true(txn:renew())
    assert(txn:abort())

    -- test that cannot renew non read-only transaction
    txn = assert(env:begin())
    local ok, err = txn:renew()
    assert.is_false(ok)
    assert.equal(err, libmdbx.errno.EINVAL)
end

function testcase.dbi_open()
    local txn = assert(opentxn())

    -- test that open or create a database in the environment
    assert.match(txn:dbi_open(), '^libmdbx.dbi: ', false)
end

function testcase.is_dirty()
    -- TODO: Determines whether the given address is on a dirty database page
    -- of the transaction or not
end

