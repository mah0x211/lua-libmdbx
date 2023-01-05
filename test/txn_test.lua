local unpack = unpack or table.unpack
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

local function opendbi(...)
    local txn = assert(opentxn())
    assert(txn:dbi_open(...))
    return txn
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

function testcase.dbi()
    local txn = assert(opentxn())

    -- test that get the id of dbi
    assert.equal(txn:dbi(), 0)
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
    local ok, err, eno = txn:reset()
    assert.is_false(ok)
    assert.equal(err, libmdbx.errno.EINVAL.message)
    assert.equal(eno, libmdbx.errno.EINVAL.errno)
end

function testcase.renew()
    local env = assert(openenv())
    local txn = assert(env:begin(libmdbx.TXN_RDONLY))

    -- test that renew a read-only transaction
    assert.is_true(txn:renew())
    assert(txn:abort())

    -- test that cannot renew non read-only transaction
    txn = assert(env:begin())
    local ok, err, eno = txn:renew()
    assert.is_false(ok)
    assert.equal(err, libmdbx.errno.EINVAL.message)
    assert.equal(eno, libmdbx.errno.EINVAL.errno)
end

function testcase.dbi_open()
    local txn = assert(opentxn())

    -- test that open or create a database in the environment
    assert(txn:dbi_open())
end

function testcase.dbi_stat()
    local txn = assert(opendbi())
    -- test that retrieve statistics for a database
    assert.is_table(txn:dbi_stat())
end

function testcase.dbi_dupsort_depthmask()
    local env = assert(openenv())
    local txn = assert(env:begin(libmdbx.TXN_RDONLY))

    -- test that retrieve depth (bitmask) information of nested
    -- dupsort (multi-value) B+trees for given database
    assert.equal(txn:dbi_dupsort_depthmask(), 0)
end

function testcase.dbi_flags()
    local txn = assert(opendbi(nil, libmdbx.DUPSORT, libmdbx.CREATE,
                               libmdbx.REVERSEKEY))

    -- test that retrieve the DB flags and status for a database handle
    assert(txn:put('hello', 'world'))
    local flags = assert.is_table(txn:dbi_flags())
    assert.equal(flags, {
        flags = {
            DUPSORT = libmdbx.DUPSORT,
            REVERSEKEY = libmdbx.REVERSEKEY,
        },
        state = {
            DBI_DIRTY = libmdbx.DBI_DIRTY,
        },
    })
end

function testcase.dbi_close()
    -- TODO
end

function testcase.drop()
    local txn = assert(opendbi())

    -- test that empty or delete and close a database
    assert.is_true(txn:drop())
end

function testcase.put_get()
    local txn = assert(opendbi(nil, libmdbx.DUPSORT, libmdbx.CREATE))

    -- test that store items into a database
    assert(txn:put('foo', 'bar'))
    assert(txn:put('foo', 'baz'))

    -- test that get item from a database
    local v, err, eno, count = txn:get('foo')
    assert.equal(v, 'bar')
    assert.is_nil(err)
    assert.is_nil(eno)
    assert.is_nil(count)

    -- test that get item and count from a database
    v, err, eno, count = txn:get('foo', true)
    assert.equal(v, 'bar')
    assert.is_nil(err)
    assert.is_nil(eno)
    assert.equal(count, 2)
end

function testcase.op_insert()
    local txn = assert(opendbi())

    -- test that insert value for key
    assert(txn:op_insert('hello', 'world'))
    assert.equal(txn:get('hello'), 'world')

    -- test that return KEYEXIST error
    local ok, err, errno = txn:op_insert('hello', 'world2')
    assert.is_false(ok)
    assert.equal(errno, libmdbx.errno.KEYEXIST.errno)
    assert.equal(err, libmdbx.errno.KEYEXIST.message)
end

function testcase.op_upsert()
    local txn = assert(opendbi())

    -- test that upsert value for key
    assert(txn:op_upsert('hello', 'world'))
    assert.equal(txn:get('hello'), 'world')

    -- test that overwrite value for key
    assert(txn:op_upsert('hello', 'world2'))
    assert.equal(txn:get('hello'), 'world2')
end

function testcase.op_update()
    local txn = assert(opendbi(nil, libmdbx.DUPSORT, libmdbx.CREATE))
    assert(txn:put('hello', 'world'))

    -- test that update value for key
    assert(txn:op_update('hello', 'world2'))
    assert.equal(txn:get('hello'), 'world2')

    -- test that update value if matches to specified value
    local ok, err, errno = txn:op_update('hello', 'foo', 'world2')
    assert(ok, err)
    assert.is_true(ok)
    assert.is_nil(errno)
    assert.equal(txn:get('hello'), 'foo')

    -- test that return NOTFOUND error
    ok, err, errno = txn:op_update('hello', 'bar', 'world')
    assert.is_false(ok)
    assert.equal(errno, libmdbx.errno.NOTFOUND.errno)
    assert.equal(err, libmdbx.errno.NOTFOUND.message)
end

function testcase.replace()
    local txn = assert(opendbi())
    assert(txn:put('hello', 'world'))

    -- test that replace items in a database
    local old, err, errno = txn:replace('hello', 'foo')
    assert(old, err)
    assert.is_nil(errno)
    assert.equal(old, 'world')
    assert.equal(txn:get('hello'), 'foo')

    -- test that add new value for key
    old, err, errno = txn:replace('bar', 'baz')
    assert(old, err)
    assert.is_nil(errno)
    assert.equal(old, '')
    assert.equal(txn:get('bar'), 'baz')
end

function testcase.del()
    local txn = assert(opendbi(nil, libmdbx.DUPSORT, libmdbx.CREATE))
    assert(txn:put('hello', 'world'))
    assert(txn:put('foo', 'bar'))
    assert(txn:put('qux', 'quux'))

    -- test that delete items from a database
    assert.is_true(txn:del('hello'))
    assert.is_nil(txn:get('hello'))

    -- test that delete key if data matches to specified value
    assert.is_true(txn:del('foo', 'bar'))
    assert.is_nil(txn:get('foo'))

    -- test that return NOTFOUND error
    local ok, err, errno = txn:del('qux', 'baz')
    assert.is_false(ok)
    assert.equal(errno, libmdbx.errno.NOTFOUND.errno)
    assert.equal(err, libmdbx.errno.NOTFOUND.message)
    assert.equal(txn:get('qux'), 'quux')
end

function testcase.cursor()
    local txn = assert(opendbi())

    -- test that create a cursor handle for the specified transaction and DBI handle
    local cur = assert(txn:cursor())
    assert.match(cur, '^libmdbx.cursor: ', false)
end

function testcase.estimate_range()
    local txn = assert(opendbi())
    assert(txn:put('hello', 'world'))
    assert(txn:put('foo', 'bar'))
    assert(txn:put('baa', 'baz'))
    assert(txn:put('qux', 'quux'))

    -- test that estimates the size of a range as a number of elements
    assert.is_int(txn:estimate_range('hello', 'foo'))

    -- test that throws an error if argument is not string
    for i = 1, 4 do
        local argv = {
            '',
            '',
            '',
            '',
        }
        argv[i] = {}
        local err = assert.throws(function()
            txn:estimate_range(unpack(argv))
        end)
        assert.match(err, string.format(
                         'bad argument #%d .+ [(]string expected,', i), false)
    end
end

function testcase.is_dirty()
    -- TODO
end

function testcase.dbi_sequence()
    local txn = assert(opendbi())

    -- test that sequence generation for a database
    local seq = 0
    for i = 1, 5 do
        seq = i - 1 + seq
        assert.equal(txn:dbi_sequence(i), seq)
    end
    seq = seq + 5

    -- test that get a sequence
    assert.equal(txn:dbi_sequence(), seq)

    -- test that throws an error if argument is invalid
    local err = assert.throws(txn.dbi_sequence, txn, 'foo')
    assert.match(err, ', got string')
end
