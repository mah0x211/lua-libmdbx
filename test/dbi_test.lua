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

    assert(env:set_maxdbs(10))
    assert(env:open(PATHNAME, nil, libmdbx.NOSUBDIR, libmdbx.COALESCE,
                    libmdbx.LIFORECLAIM, ...))
    return env
end

local function opentxn(...)
    local env = assert(openenv(...))
    return assert(env:begin())
end

local function opendbi(...)
    local txn = opentxn()
    local dbi = assert(txn:dbi_open(...))
    return dbi, txn
end

function testcase.txn()
    local txn = assert(opentxn())
    local dbi = assert(txn:dbi_open())

    -- test that return the associated transaction
    assert.equal(dbi:txn(), txn)
end

function testcase.close()
    -- TODO
end

function testcase.drop()
    local dbi = opendbi()

    -- test that empty or delete and close a database
    assert.is_true(dbi:drop())
end

function testcase.stat()
    local dbi = opendbi()

    -- test that retrieve statistics for a database
    assert.is_table(dbi:stat())
end

function testcase.dupsort_depthmask()
    local env = openenv()
    local txn = assert(env:begin(libmdbx.TXN_RDONLY))
    local dbi = assert(txn:dbi_open())

    -- test that retrieve depth (bitmask) information of nested
    -- dupsort (multi-value) B+trees for given database
    assert.equal(dbi:dupsort_depthmask(), 0)
end

function testcase.flags()
    local dbi =
        opendbi(nil, libmdbx.DUPSORT, libmdbx.CREATE, libmdbx.REVERSEKEY)

    -- test that retrieve the DB flags and status for a database handle
    assert(dbi:put('hello', 'world'))
    local flags = assert.is_table(dbi:flags())
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

function testcase.put_get()
    local dbi = opendbi(nil, libmdbx.DUPSORT, libmdbx.CREATE)

    -- test that store items into a database
    assert(dbi:put('foo', 'bar'))
    assert(dbi:put('foo', 'baz'))

    -- test that get item from a database
    local v, err, count = dbi:get('foo')
    assert.equal(v, 'bar')
    assert.is_nil(err)
    assert.is_nil(count)

    -- test that get item and count from a database
    v, err, count = dbi:get('foo', true)
    assert.equal(v, 'bar')
    assert.is_nil(err)
    assert.equal(count, 2)
end

function testcase.op_insert()
    local dbi = opendbi()

    -- test that insert value for key
    assert(dbi:op_insert('hello', 'world'))
    assert.equal(dbi:get('hello'), 'world')

    -- test that return KEYEXIST error
    local ok, err = dbi:op_insert('hello', 'world2')
    assert.is_false(ok)
    assert.equal(err, libmdbx.errno.KEYEXIST)
end

function testcase.op_upsert()
    local dbi = opendbi()

    -- test that upsert value for key
    assert(dbi:op_upsert('hello', 'world'))
    assert.equal(dbi:get('hello'), 'world')

    -- test that overwrite value for key
    assert(dbi:op_upsert('hello', 'world2'))
    assert.equal(dbi:get('hello'), 'world2')
end

function testcase.op_update()
    local dbi = assert(opendbi(nil, libmdbx.DUPSORT, libmdbx.CREATE))
    assert(dbi:put('hello', 'world'))

    -- test that update value for key
    assert(dbi:op_update('hello', 'world2'))
    assert.equal(dbi:get('hello'), 'world2')

    -- test that update value if matches to specified value
    local ok, err = dbi:op_update('hello', 'foo', 'world2')
    assert(ok, err)
    assert.is_true(ok)
    assert.equal(dbi:get('hello'), 'foo')

    -- test that return NOTFOUND error
    ok, err = dbi:op_update('hello', 'bar', 'world')
    assert.is_false(ok)
    assert.is_nil(err)
end

function testcase.replace()
    local dbi = opendbi()
    assert(dbi:put('hello', 'world'))

    -- test that replace items in a database
    local old, err = dbi:replace('hello', 'foo')
    assert(old, err)
    assert.equal(old, 'world')
    assert.equal(dbi:get('hello'), 'foo')

    -- test that add new value for key
    old, err = dbi:replace('bar', 'baz')
    assert(old, err)
    assert.equal(old, '')
    assert.equal(dbi:get('bar'), 'baz')
end

function testcase.del()
    local dbi = opendbi(nil, libmdbx.DUPSORT, libmdbx.CREATE)
    assert(dbi:put('hello', 'world'))
    assert(dbi:put('foo', 'bar'))
    assert(dbi:put('qux', 'quux'))

    -- test that delete items from a database
    assert.is_true(dbi:del('hello'))
    assert.is_nil(dbi:get('hello'))

    -- test that delete key if data matches to specified value
    assert.is_true(dbi:del('foo', 'bar'))
    assert.is_nil(dbi:get('foo'))

    -- test that return false without error
    local ok, err = dbi:del('qux', 'baz')
    assert.is_false(ok)
    assert.is_nil(err)
    assert.equal(dbi:get('qux'), 'quux')
end

function testcase.cursor_open()
    local dbi, txn = opendbi()

    -- test that create a cursor handle for the specified transaction and DBI handle
    local cur = assert(dbi:cursor_open(txn))
    assert.match(cur, '^libmdbx.cursor: ', false)
end

function testcase.estimate_range()
    local dbi = opendbi()
    assert(dbi:put('hello', 'world'))
    assert(dbi:put('foo', 'bar'))
    assert(dbi:put('baa', 'baz'))
    assert(dbi:put('qux', 'quux'))

    -- test that estimates the size of a range as a number of elements
    assert.is_int(dbi:estimate_range('hello', 'foo'))

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
            dbi:estimate_range(unpack(argv))
        end)
        assert.match(err, string.format(
                         'bad argument #%d .+ [(]string expected,', i), false)
    end
end

function testcase.sequence()
    local dbi = opendbi()

    -- test that sequence generation for a database
    local seq = 0
    for i = 1, 5 do
        seq = i - 1 + seq
        assert.equal(dbi:sequence(i), seq)
    end
    seq = seq + 5

    -- test that get a sequence
    assert.equal(dbi:sequence(), seq)

    -- test that throws an error if argument is invalid
    local err = assert.throws(dbi.sequence, dbi, 'foo')
    assert.match(err, ', got string')
end
