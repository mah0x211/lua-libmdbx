local testcase = require('testcase')
local libmdbx = require('libmdbx')

-- print(dump(libmdbx.errno))

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

function testcase.close()
    local txn = assert(opendbi())
    local cur = assert(txn:cursor())

    -- test that close a cursor handle
    cur:close()
end

function testcase.renew()
    local env = assert(openenv(libmdbx.NOTLS))
    local txn1 = assert(env:begin(libmdbx.TXN_RDONLY))
    local txn2 = assert(env:begin(libmdbx.TXN_RDONLY))
    local cur = assert(txn1:cursor())
    assert.equal(cur:txn(), txn1)

    -- test that renew a cursor handle
    assert.is_true(cur:renew(txn2))
    assert.equal(cur:txn(), txn2)
end

function testcase.dbi()
    local txn = assert(opendbi())
    local cur = assert(txn:cursor())

    -- test that return the cursor's database handle
    assert.equal(cur:dbi(), txn:dbi())
end

function testcase.copy()
    local txn = assert(opendbi())
    local cur = assert(txn:cursor())

    -- test that copy cursor position and state
    local cur2 = assert(cur:copy())
    assert.not_equal(cur2, cur)
    assert.match(cur2, '^libmdbx.cursor: ', false)
    assert.equal(cur2:txn(), cur:txn())
    assert.equal(cur2:dbi(), cur:dbi())
end

function testcase.get()
    local txn = assert(opendbi(nil, libmdbx.DUPSORT, libmdbx.CREATE))
    assert(txn:put('foo', 'foo-value-1'))
    assert(txn:put('foo', 'foo-value-2'))
    assert(txn:put('bar', 'bar-value-1'))
    assert(txn:put('qux', 'qux-value-1'))
    assert(txn:put('qux', 'qux-value-2'))
    local cur = assert(txn:cursor())

    -- test that retrieve key-value pairs by cursor
    local res = {}
    local k, v = assert(cur:get())
    while k do
        local list = res[k]
        if not list then
            list = {}
            res[k] = list
        end
        list[#list + 1] = v
        k, v = cur:get(libmdbx.NEXT)
    end
    assert.equal(res, {
        foo = {
            'foo-value-1',
            'foo-value-2',
        },
        bar = {
            'bar-value-1',
        },
        qux = {
            'qux-value-1',
            'qux-value-2',
        },
    })

    -- test that retrieve key-value pairs by cursor but skip duplicate values
    res = {}
    k, v = assert(cur:get())
    while k do
        local list = res[k]
        if not list then
            list = {}
            res[k] = list
        end
        list[#list + 1] = v
        k, v = cur:get(libmdbx.NEXT_NODUP)
    end
    assert.equal(res, {
        foo = {
            'foo-value-1',
        },
        bar = {
            'bar-value-1',
        },
        qux = {
            'qux-value-1',
        },
    })
end

function testcase.get_batch()
    local txn = assert(opendbi())
    assert(txn:put('foo', 'foo-value'))
    assert(txn:put('bar', 'bar-value'))
    assert(txn:put('qux', 'qux-value'))
    local cur = assert(txn:cursor())

    -- test that retrieve multiple non-dupsort key/value pairs by cursor
    local res = assert(cur:get_batch())
    assert.equal(res, {
        foo = 'foo-value',
        bar = 'bar-value',
        qux = 'qux-value',
    })
end

function testcase.put()
    local txn = assert(opendbi())
    assert(txn:put('foo', 'foo-value'))
    assert(txn:put('bar', 'bar-value'))
    assert(txn:put('qux', 'qux-value'))
    local cur = assert(txn:cursor())

    -- test that store by cursor
    assert.is_true(cur:put('foo', 'updated'))
    assert.equal(txn:get('foo'), 'updated')

    -- test that store current value by cursor
    local k, v = assert(cur:get())
    assert.is_true(cur:put(k, v .. '-updated', libmdbx.CURRENT))
    assert.equal(txn:get(k), v .. '-updated')

    -- test that cannot store current value if key does not matched
    k = assert(cur:get())
    local ok, err, eno = cur:put(k .. '-mismatch', 'updated', libmdbx.CURRENT)
    assert.is_false(ok)
    assert.equal(err, libmdbx.errno.EKEYMISMATCH.message)
    assert.equal(eno, libmdbx.errno.EKEYMISMATCH.errno)
end

function testcase.del()
    local txn = assert(opendbi(nil, libmdbx.DUPSORT, libmdbx.CREATE))
    assert(txn:put('foo', 'foo-value-1'))
    assert(txn:put('foo', 'foo-value-2'))
    assert(txn:put('bar', 'bar-value-1'))
    assert(txn:put('qux', 'qux-value-1'))
    assert(txn:put('qux', 'qux-value-2'))

    local cur = assert(txn:cursor())
    local is_qux = false
    repeat
        local k = cur:get(libmdbx.NEXT)

        if k == 'foo' then
            -- test that delete all of the data items for the current key
            assert.is_true(cur:del(libmdbx.ALLDUPS))
            assert.is_nil(txn:get(k))
        elseif k == 'bar' then
            -- test that delete current key/data pair
            assert.is_true(cur:del())
            assert.is_nil(txn:get(k))
        elseif k == 'qux' then
            -- test that delete current key/data pair
            assert.is_true(cur:del())
            if not is_qux then
                is_qux = true
                assert.equal(txn:get(k), 'qux-value-2')
            else
                assert.is_nil(txn:get(k))
            end
        end
    until k == nil
end

function testcase.eof()
    local txn = assert(opendbi())
    assert(txn:put('foo', 'foo-value'))
    assert(txn:put('bar', 'bar-value'))
    assert(txn:put('qux', 'qux-value'))

    -- test that true if the cursor is points to the end of data
    local cur = assert(txn:cursor())
    assert.is_true(cur:eof())

    repeat
        local k = cur:get(libmdbx.NEXT)
        if k then
            -- test that false if the cursor is not points to the end of data
            assert.is_false(cur:eof())
        else
            -- test that true if the cursor is not points to the end of data
            assert.is_true(cur:eof())
        end
    until k == nil

end

function testcase.on_first()
    local txn = assert(opendbi())
    assert(txn:put('foo', 'foo-value'))
    assert(txn:put('bar', 'bar-value'))
    assert(txn:put('qux', 'qux-value'))

    -- test that false if the cursor is not pointed to the first key-value pair
    local cur = assert(txn:cursor())
    assert.is_false(cur:on_first())

    -- test that true if the cursor is not pointed to the first key-value pair
    assert(cur:get(libmdbx.NEXT))
    assert.is_true(cur:on_first())

    repeat
        -- test that false if the cursor is not pointed to the first key-value pair
        local k = cur:get(libmdbx.NEXT)
        assert.is_false(cur:on_first())
    until k == nil
end

function testcase.on_last()
    local txn = assert(opendbi())
    assert(txn:put('foo', 'foo-value'))
    assert(txn:put('bar', 'bar-value'))
    assert(txn:put('qux', 'qux-value'))

    -- test that false if the cursor is not pointed to the last key-value pair
    local n = 0
    local cur = assert(txn:cursor())
    assert.is_false(cur:on_last())
    repeat
        -- test that false if the cursor is not pointed to the last key-value pair
        local k = cur:get(libmdbx.NEXT)
        n = n + 1
        if n == 3 or k == nil then
            assert.is_true(cur:on_last())
        else
            assert.is_false(cur:on_last())
        end
    until k == nil
end

function testcase.estimate_distance()
    local txn = assert(opendbi())
    assert(txn:put('foo', 'foo-value'))
    assert(txn:put('bar', 'bar-value'))
    assert(txn:put('qux', 'qux-value'))
    assert(txn:put('quux', 'quux-value'))

    -- test that estimates the distance between cursors as a number of elements
    local cur1 = assert(txn:cursor())
    assert(cur1:get(libmdbx.NEXT))
    local cur2 = assert(txn:cursor())
    local prev = 0
    repeat
        local k = cur2:get(libmdbx.NEXT)
        local dist = assert(cur1:estimate_distance(cur2))
        assert.greater_or_equal(dist, prev)
        prev = dist
    until k == nil

    repeat
        local k = cur1:get(libmdbx.NEXT)
        local dist = assert(cur1:estimate_distance(cur2))
        assert.less_or_equal(dist, prev)
        prev = dist
    until k == nil
end

function testcase.estimate_move()
    local txn = assert(opendbi())
    assert(txn:put('foo', 'foo-value'))
    assert(txn:put('bar', 'bar-value'))
    assert(txn:put('qux', 'qux-value'))
    assert(txn:put('quux', 'quux-value'))

    -- test that estimates the move distance
    local cur = assert(txn:cursor())
    assert(cur:get(libmdbx.NEXT))
    for op, dist in pairs({
        FIRST = 0,
        NEXT = 1,
        LAST = 3,
    }) do
        assert.equal(cur:estimate_move(libmdbx[op]), dist)
    end
end

