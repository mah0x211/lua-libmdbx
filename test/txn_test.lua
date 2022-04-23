local testcase = require('testcase')
local libmdbx = require('libmdbx')

local PATHNAME = './test.db'
local LOCKFILE = PATHNAME .. libmdbx.LOCK_SUFFIX
local MDBX_ENV

function testcase.before_each()
    os.remove(PATHNAME)
    os.remove(LOCKFILE)

    local env = assert(libmdbx.new())
    assert(env:open(PATHNAME, nil, libmdbx.NOSUBDIR, libmdbx.COALESCE,
                    libmdbx.LIFORECLAIM))
    assert(env:delete())
    MDBX_ENV = env
end

function testcase.after_each()
    os.remove(LOCKFILE)
end

function testcase.dbi_open()
    local txn = assert(MDBX_ENV:begin())

    -- test that open and close dbi
    assert(txn:dbi_open())
end

function testcase.put()
    local txn = assert(MDBX_ENV:begin())
    assert(txn:dbi_open())

    -- test that put value for key
    assert(txn:put("hello", "world"))
    assert.equal(txn:get("hello"), "world")
end

function testcase.op_insert()
    local txn = assert(MDBX_ENV:begin())
    assert(txn:dbi_open())

    -- test that insert value for key
    assert(txn:op_insert("hello", "world"))
    assert.equal(txn:get("hello"), "world")

    -- test that return KEYEXIST error
    local ok, err, errno = txn:op_insert("hello", "world2")
    assert.is_false(ok)
    assert.equal(errno, libmdbx.errno.KEYEXIST.errno)
    assert.equal(err, libmdbx.errno.KEYEXIST.message)
end

function testcase.op_upsert()
    local txn = assert(MDBX_ENV:begin())
    assert(txn:dbi_open())

    -- test that upsert value for key
    assert(txn:op_upsert("hello", "world"))
    assert.equal(txn:get("hello"), "world")

    -- test that overwrite value for key
    assert(txn:op_upsert("hello", "world2"))
    assert.equal(txn:get("hello"), "world2")
end

function testcase.op_update()
    local txn = assert(MDBX_ENV:begin())
    assert(txn:dbi_open(nil, libmdbx.DUPSORT, libmdbx.CREATE))
    assert(txn:put("hello", "world"))

    -- test that update value for key
    assert(txn:op_update("hello", "world2"))
    assert.equal(txn:get("hello"), "world2")

    -- test that update value if matches to specified value
    local ok, err, errno = txn:op_update("hello", "foo", "world2")
    assert(ok, err)
    assert.is_true(ok)
    assert.is_nil(errno)
    assert.equal(txn:get("hello"), "foo")

    -- test that return NOTFOUND error
    ok, err, errno = txn:op_update("hello", "bar", "world")
    assert.is_false(ok)
    assert.equal(errno, libmdbx.errno.NOTFOUND.errno)
    assert.equal(err, libmdbx.errno.NOTFOUND.message)
end

function testcase.replace()
    local txn = assert(MDBX_ENV:begin())
    assert(txn:dbi_open())
    assert(txn:put("hello", "world"))

    -- test that replace existing value
    local old, err, errno = txn:replace("hello", "foo")
    assert(old, err)
    assert.is_nil(errno)
    assert.equal(old, "world")
    assert.equal(txn:get("hello"), "foo")

    -- test that add new value for key
    old, err, errno = txn:replace("bar", "baz")
    assert(old, err)
    assert.is_nil(errno)
    assert.equal(old, "")
    assert.equal(txn:get("bar"), "baz")

    -- assert.equal(txn:get("hello"), "world")
    -- assert.equal(txn:replace("hello", "foobar", nil, libmdbx.CURRENT,
    --                          libmdbx.NODUPDATA), "world")
    -- assert(txn:del("hello"))
    -- assert.is_nil(txn:get("hello"))
end

function testcase.del()
    local txn = assert(MDBX_ENV:begin())
    assert(txn:dbi_open())
    assert(txn:put("hello", "world"))
    assert(txn:put("foo", "bar"))
    assert(txn:put("qux", "quux"))

    -- test that delete key
    assert(txn:del("hello"))
    assert.is_nil(txn:get("hello"))

    -- test that delete key if data matches to specified value
    assert(txn:del("foo", "bar"))
    assert.is_nil(txn:get("foo"))

    -- test that return NOTFOUND error
    local ok, err, errno = txn:del("qux", "baz")
    assert.is_false(ok)
    assert.equal(errno, libmdbx.errno.NOTFOUND.errno)
    assert.equal(err, libmdbx.errno.NOTFOUND.message)
    assert.equal(txn:get("qux"), "quux")
end

