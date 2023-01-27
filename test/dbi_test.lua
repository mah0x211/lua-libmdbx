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

local function opentxn(...)
    local env = assert(libmdbx.new())

    assert(env:set_maxdbs(10))
    assert(env:open(PATHNAME, nil, libmdbx.NOSUBDIR, libmdbx.COALESCE,
                    libmdbx.LIFORECLAIM, ...))

    local txn = assert(env:begin())
    return txn, env
end

function testcase.dbi()
    local txn = opentxn()
    local dbi = assert(txn:dbi_open('foo', libmdbx.CREATE))
    local dbi2 = assert(txn:dbi_open('bar', libmdbx.CREATE))

    -- test that return dbi number
    assert.is_int(dbi:dbi())
    assert.not_equal(dbi:dbi(), dbi2:dbi())
end

function testcase.env()
    local txn, env = opentxn()
    local dbi = assert(txn:dbi_open())

    -- test that return the associated transaction
    assert.equal(dbi:env(), env)
end

function testcase.close()
    local txn, env = opentxn()
    local dbi = assert(txn:dbi_open('foo', libmdbx.CREATE))

    -- test that dbi cannot be closed if dbi has not created
    assert(txn:abort())
    assert.is_false(dbi:close())

    -- test that dbi cannot be closed if transaction not finished yet
    txn = assert(env:begin())
    dbi = assert(txn:dbi_open('foo', libmdbx.CREATE))
    assert.is_false(dbi:close())

    -- test that dbi can be closed if dbi has created
    assert(txn:commit())
    assert.is_true(dbi:close())

    -- test that dbi can be closed if dbi already exists
    txn = assert(env:begin())
    dbi = assert(txn:dbi_open('foo', libmdbx.CREATE))
    assert.is_true(dbi:close())
end

function testcase.dbi_is_reusable()
    local txn, env = opentxn()
    local dbi = assert(txn:dbi_open('hello', libmdbx.CREATE))
    local dbh = assert(dbi:dbh_open(txn))
    assert(dbh:put('foo', 'bar'))
    assert.equal(dbh:get('foo'), 'bar')
    assert(txn:commit())

    -- test that dbi is reusable
    txn = assert(env:begin(libmdbx.TXN_RDONLY))
    dbh = assert(dbi:dbh_open(txn))
    assert.equal(dbh:get('foo'), 'bar')
    assert(txn:commit())
end

function testcase.dbh_open()
    local txn = opentxn()
    local dbi = assert(txn:dbi_open())

    -- test that create a dbh handle for the specified transaction and DBI handle
    local dbh = assert(dbi:dbh_open(txn))
    assert.match(dbh, '^libmdbx.dbh: ', false)
end

