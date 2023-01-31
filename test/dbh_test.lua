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

local function opendbh(...)
    local dbi, txn = opendbi(...)
    return assert(dbi:dbh_open(txn))
end

function testcase.dbi()
    local dbi, txn = opendbi()
    local dbh = assert(dbi:dbh_open(txn))

    -- test that return the associated dbi
    assert.equal(dbh:dbi(), dbi)
end

function testcase.txn()
    local dbi, txn = opendbi()
    local dbh = assert(dbi:dbh_open(txn))

    -- test that return the associated transaction
    assert.equal(dbh:txn(), txn)
end

function testcase.close()
    local dbh = opendbh()

    -- test that release associated refenreces
    dbh:close()
    assert.is_nil(dbh:dbi())
    assert.is_nil(dbh:txn())
end

function testcase.drop()
    local dbh = opendbh()

    -- test that empty or delete and close a database
    assert.is_true(dbh:drop())
end

function testcase.stat()
    local dbh = opendbh()

    -- test that retrieve statistics for a database
    assert.is_table(dbh:stat())
end

function testcase.dupsort_depthmask()
    local env = openenv()
    local txn = assert(env:begin(libmdbx.TXN_RDONLY))
    local dbi = assert(txn:dbi_open())
    local dbh = assert(dbi:dbh_open(txn))

    -- test that retrieve depth (bitmask) information of nested
    -- dupsort (multi-value) B+trees for given database
    assert.equal(dbh:dupsort_depthmask(), 0)
end

function testcase.flags()
    local dbh =
        opendbh(nil, libmdbx.DUPSORT, libmdbx.CREATE, libmdbx.REVERSEKEY)
    assert(dbh:put('hello', 'world'))

    -- test that retrieve the DB flags and status for a database handle
    local flags = assert.is_table(dbh:flags())
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
    local dbh = opendbh(nil, libmdbx.DUPSORT, libmdbx.CREATE)

    -- test that store items into a database
    assert(dbh:put('foo', 'bar'))
    assert(dbh:put('foo', 'baz'))

    -- test that get item from a database
    local v, err, count = dbh:get('foo')
    assert.equal(v, 'bar')
    assert.is_nil(err)
    assert.is_nil(count)

    -- test that get item and count from a database
    v, err, count = dbh:get('foo', true)
    assert.equal(v, 'bar')
    assert.is_nil(err)
    assert.equal(count, 2)
end

function testcase.op_insert()
    local dbh = opendbh()

    -- test that insert value for key
    assert(dbh:op_insert('hello', 'world'))
    assert.equal(dbh:get('hello'), 'world')

    -- test that return KEYEXIST error
    local ok, err = dbh:op_insert('hello', 'world2')
    assert.is_false(ok)
    assert.equal(err, libmdbx.errno.KEYEXIST)
end

function testcase.op_upsert()
    local dbh = opendbh()

    -- test that upsert value for key
    assert(dbh:op_upsert('hello', 'world'))
    assert.equal(dbh:get('hello'), 'world')

    -- test that overwrite value for key
    assert(dbh:op_upsert('hello', 'world2'))
    assert.equal(dbh:get('hello'), 'world2')
end

function testcase.op_upsert_for_dupsort()
    local dbh = opendbh(nil, libmdbx.DUPSORT, libmdbx.CREATE)

    -- test that upsert multi-value for key
    assert(dbh:op_upsert('hello', 'hello-1', true))
    assert(dbh:op_upsert('hello', 'hello-2', true))
    assert(dbh:op_upsert('hello', 'hello-3', true))
    assert(dbh:op_upsert('world', 'world-1', true))
    -- confirm
    local tbl = {}
    local cur = dbh:cursor_open()
    local k, v = cur:get_first()
    while k do
        local list = tbl[k]
        if not list then
            list = {}
            tbl[k] = list
        end
        list[#list + 1] = v
        table.sort(list)
        k, v = cur:get_next()
    end
    cur:close()
    assert.equal(tbl, {
        hello = {
            'hello-1',
            'hello-2',
            'hello-3',
        },
        world = {
            'world-1',
        },
    })

    -- test that upsert value for key
    assert(dbh:op_upsert('hello', 'world'))
    assert.equal(dbh:get('hello'), 'world')
    -- confirm
    tbl = {}
    cur = dbh:cursor_open()
    k, v = cur:get_first()
    while k do
        local list = tbl[k]
        if not list then
            list = {}
            tbl[k] = list
        end
        list[#list + 1] = v
        table.sort(list)
        k, v = cur:get_next()
    end
    cur:close()
    assert.equal(tbl, {
        hello = {
            'world',
        },
        world = {
            'world-1',
        },
    })
end

function testcase.op_update()
    local dbh = opendbh(nil, libmdbx.DUPSORT, libmdbx.CREATE)
    assert(dbh:put('hello', 'world'))

    -- test that update value for key
    assert(dbh:op_update('hello', 'world2'))
    assert.equal(dbh:get('hello'), 'world2')

    -- test that update value if matches to specified value
    local ok, err = dbh:op_update('hello', 'foo', 'world2')
    assert(ok, err)
    assert.is_true(ok)
    assert.equal(dbh:get('hello'), 'foo')

    -- test that return NOTFOUND error
    ok, err = dbh:op_update('hello', 'bar', 'world')
    assert.is_false(ok)
    assert.is_nil(err)
end

function testcase.replace()
    local dbh = opendbh()
    assert(dbh:put('hello', 'world'))

    -- test that replace items in a database
    local old, err = dbh:replace('hello', 'foo')
    assert(old, err)
    assert.equal(old, 'world')
    assert.equal(dbh:get('hello'), 'foo')

    -- test that add new value for key
    old, err = dbh:replace('bar', 'baz')
    assert(old, err)
    assert.equal(old, '')
    assert.equal(dbh:get('bar'), 'baz')
end

function testcase.del()
    local dbh = opendbh(nil, libmdbx.DUPSORT, libmdbx.CREATE)
    assert(dbh:put('hello', 'world'))
    assert(dbh:put('foo', 'bar'))
    assert(dbh:put('qux', 'quux'))

    -- test that delete items from a database
    assert.is_true(dbh:del('hello'))
    assert.is_nil(dbh:get('hello'))

    -- test that delete key if data matches to specified value
    assert.is_true(dbh:del('foo', 'bar'))
    assert.is_nil(dbh:get('foo'))

    -- test that return false without error
    local ok, err = dbh:del('qux', 'baz')
    assert.is_false(ok)
    assert.is_nil(err)
    assert.equal(dbh:get('qux'), 'quux')
end

function testcase.cursor_open()
    local dbh = opendbh()

    -- test that create a cursor handle for the specified transaction and DBI handle
    local cur = assert(dbh:cursor_open())
    assert.match(cur, '^libmdbx.cursor: ', false)
end

function testcase.estimate_range()
    local dbh = opendbh()
    assert(dbh:put('hello', 'world'))
    assert(dbh:put('foo', 'bar'))
    assert(dbh:put('baa', 'baz'))
    assert(dbh:put('qux', 'quux'))

    -- test that estimates the size of a range as a number of elements
    assert.is_int(dbh:estimate_range('hello', 'foo'))

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
            dbh:estimate_range(unpack(argv))
        end)
        assert.match(err, string.format(
                         'bad argument #%d .+ [(]string expected,', i), false)
    end
end

function testcase.sequence()
    local dbh = opendbh()

    -- test that sequence generation for a database
    local seq = 0
    for i = 1, 5 do
        seq = i - 1 + seq
        assert.equal(dbh:sequence(i), seq)
    end
    seq = seq + 5

    -- test that get a sequence
    assert.equal(dbh:sequence(), seq)

    -- test that throws an error if argument is invalid
    local err = assert.throws(dbh.sequence, dbh, 'foo')
    assert.match(err, ', got string')
end
