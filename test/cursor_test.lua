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
    return assert(txn:dbi_open(...))
end

function testcase.dbi()
    local dbi = assert(opendbi())
    local cur = assert(dbi:cursor())

    -- test that return the associated dbi
    assert.equal(cur:dbi(), dbi)
end

function testcase.close()
    local dbi = assert(opendbi())
    local cur = assert(dbi:cursor())

    -- test that close a cursor handle
    cur:close()
end

function testcase.renew()
    local env = assert(openenv(libmdbx.NOTLS))
    local txn1 = assert(env:begin(libmdbx.TXN_RDONLY))
    local dbi1 = assert(txn1:dbi_open())
    local cur = assert(dbi1:cursor())
    assert.equal(cur:dbi(), dbi1)

    -- test that renew a cursor handle
    local txn2 = assert(env:begin(libmdbx.TXN_RDONLY))
    local dbi2 = assert(txn2:dbi_open())
    assert.is_true(cur:renew(dbi2))
    assert.equal(cur:dbi(), dbi2)
end

function testcase.copy()
    local dbi = assert(opendbi())
    local cur = assert(dbi:cursor())

    -- test that copy cursor position and state
    local cur2 = assert(cur:copy())
    assert.not_equal(cur2, cur)
    assert.match(cur2, '^libmdbx.cursor: ', false)
    assert.equal(cur2:dbi(), cur:dbi())
end

function testcase.set()
    local dbi = assert(opendbi(nil, libmdbx.DUPSORT, libmdbx.CREATE))
    assert(dbi:put('foo', 'foo-value-1'))
    assert(dbi:put('foo', 'foo-value-2'))
    assert(dbi:put('bar', 'bar-value-1'))
    assert(dbi:put('bar', 'bar-value-2'))
    assert(dbi:put('qux', 'qux-value-1'))
    local cur = assert(dbi:cursor())

    -- test that retrieve value at given key
    local v, err = assert(cur:set('foo'))
    assert.equal(v, 'foo-value-1')
    assert.is_nil(err)

    -- test that return nil
    v, err = cur:set('baz')
    assert.is_nil(v)
    assert.is_nil(err)
end

function testcase.set_range()
    local dbi = assert(opendbi(nil, libmdbx.DUPSORT, libmdbx.CREATE))
    assert(dbi:put('foo', 'foo-value-1'))
    assert(dbi:put('foo', 'foo-value-2'))
    assert(dbi:put('bar', 'bar-value-1'))
    assert(dbi:put('bar', 'bar-value-2'))
    assert(dbi:put('qux', 'qux-value-1'))
    local cur = assert(dbi:cursor())

    -- test that retrieve first key-value pair greater to given key
    local k, v, err = assert(cur:set_range('ether'))
    assert.equal(k, 'foo')
    assert.equal(v, 'foo-value-1')
    assert.is_nil(err)

    -- test that retrieve first key-value pair equal to given key
    k, v, err = assert(cur:set_range('qux'))
    assert.equal(k, 'qux')
    assert.equal(v, 'qux-value-1')
    assert.is_nil(err)

    -- test that return nil
    k, v, err = cur:set_range('sar')
    assert.is_nil(k)
    assert.is_nil(v)
    assert.is_nil(err)
end

function testcase.set_lowerbound()
    local dbi = assert(opendbi(nil, libmdbx.DUPSORT, libmdbx.CREATE))
    assert(dbi:put('foo', 'a-value'))
    assert(dbi:put('foo', 'b-value'))
    assert(dbi:put('bar', 'a-value'))
    assert(dbi:put('bar', 'b-value'))
    assert(dbi:put('qux', 'a-value'))
    local cur = assert(dbi:cursor())

    -- test that retrieve first key-value pair greater to given key
    local k, v, err = cur:set_lowerbound('ether')
    assert.equal(k, 'foo')
    assert.equal(v, 'a-value')
    assert.is_nil(err)

    -- test that retrieve first key-value pair equal to given key
    k, v, err = assert(cur:set_lowerbound('bar'))
    assert.equal(k, 'bar')
    assert.equal(v, 'a-value')
    assert.is_nil(err)

    -- test that retrieve first key-value pair greater to given key-value
    k, v, err = assert(cur:set_lowerbound('bar', 'c-value'))
    assert.equal(k, 'foo')
    assert.equal(v, 'a-value')
    assert.is_nil(err)

    -- test that return nil
    k, v, err = cur:set_lowerbound('sar')
    assert.is_nil(k)
    assert.is_nil(v)
    assert.is_nil(err)
end

function testcase.set_upperbound()
    local dbi = assert(opendbi(nil, libmdbx.DUPSORT, libmdbx.CREATE))
    assert(dbi:put('foo', 'a-value'))
    assert(dbi:put('foo', 'b-value'))
    assert(dbi:put('bar', 'a-value'))
    assert(dbi:put('bar', 'b-value'))
    assert(dbi:put('qux', 'a-value'))
    local cur = assert(dbi:cursor())

    -- test that retrieve first key-value pair greater to given key
    local k, v, err = cur:set_upperbound('ether')
    assert.equal(k, 'foo')
    assert.equal(v, 'a-value')
    assert.is_nil(err)

    -- test that retrieve first key-value pair greater to given key-value
    k, v, err = cur:set_upperbound('foo', 'b-value')
    assert.equal(k, 'qux')
    assert.equal(v, 'a-value')
    assert.is_nil(err)

    -- test that return nil
    k, v, err = cur:set_upperbound('sar')
    assert.is_nil(k)
    assert.is_nil(v)
    assert.is_nil(err)
end

function testcase.get()
    local dbi = assert(opendbi(nil, libmdbx.DUPSORT, libmdbx.CREATE))
    assert(dbi:put('foo', 'foo-value-1'))
    assert(dbi:put('foo', 'foo-value-2'))
    assert(dbi:put('bar', 'bar-value-1'))
    assert(dbi:put('qux', 'qux-value-1'))
    assert(dbi:put('qux', 'qux-value-2'))
    local cur = assert(dbi:cursor())

    -- test that retrieve first key-value pairs
    local k, v = assert(cur:get(libmdbx.FIRST))
    assert.equal(k, 'bar')
    assert.equal(v, 'bar-value-1')

    -- test that retrieve next key-value pairs
    k, v = assert(cur:get(libmdbx.NEXT))
    assert.equal(k, 'foo')
    assert.equal(v, 'foo-value-1')

    -- test that retrieve current key-value pairs
    k, v = assert(cur:get())
    assert.equal(k, 'foo')
    assert.equal(v, 'foo-value-1')

    -- test that retrieve key-value pairs by cursor
    local res = {}
    k, v = assert(cur:get(libmdbx.FIRST))
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
    k, v = assert(cur:get(libmdbx.FIRST))
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
    local dbi = assert(opendbi())
    assert(dbi:put('foo', 'foo-value'))
    assert(dbi:put('bar', 'bar-value'))
    assert(dbi:put('qux', 'qux-value'))
    local cur = assert(dbi:cursor())

    -- test that retrieve multiple non-dupsort key/value pairs by cursor
    local res = assert(cur:get_batch())
    assert.equal(res, {
        foo = 'foo-value',
        bar = 'bar-value',
        qux = 'qux-value',
    })
end

function testcase.put()
    local dbi = assert(opendbi())
    assert(dbi:put('foo', 'foo-value'))
    assert(dbi:put('bar', 'bar-value'))
    assert(dbi:put('qux', 'qux-value'))
    local cur = assert(dbi:cursor())

    -- test that store by cursor
    assert.is_true(cur:put('foo', 'updated'))
    assert.equal(dbi:get('foo'), 'updated')

    -- test that store current value by cursor
    local k, v = assert(cur:get())
    assert.is_true(cur:put(k, v .. '-updated', libmdbx.CURRENT))
    assert.equal(dbi:get(k), v .. '-updated')

    -- test that cannot store current value if key does not matched
    k = assert(cur:get())
    local ok, err, eno = cur:put(k .. '-mismatch', 'updated', libmdbx.CURRENT)
    assert.is_false(ok)
    assert.equal(err, libmdbx.errno.EKEYMISMATCH.message)
    assert.equal(eno, libmdbx.errno.EKEYMISMATCH.errno)
end

function testcase.del()
    local dbi = assert(opendbi(nil, libmdbx.DUPSORT, libmdbx.CREATE))
    assert(dbi:put('foo', 'foo-value-1'))
    assert(dbi:put('foo', 'foo-value-2'))
    assert(dbi:put('bar', 'bar-value-1'))
    assert(dbi:put('qux', 'qux-value-1'))
    assert(dbi:put('qux', 'qux-value-2'))

    local cur = assert(dbi:cursor())
    local is_qux = false
    repeat
        local k = cur:get(libmdbx.NEXT)

        if k == 'foo' then
            -- test that delete all of the data items for the current key
            assert.is_true(cur:del(libmdbx.ALLDUPS))
            assert.is_nil(dbi:get(k))
        elseif k == 'bar' then
            -- test that delete current key/data pair
            assert.is_true(cur:del())
            assert.is_nil(dbi:get(k))
        elseif k == 'qux' then
            -- test that delete current key/data pair
            assert.is_true(cur:del())
            if not is_qux then
                is_qux = true
                assert.equal(dbi:get(k), 'qux-value-2')
            else
                assert.is_nil(dbi:get(k))
            end
        end
    until k == nil
end

function testcase.eof()
    local dbi = assert(opendbi())
    assert(dbi:put('foo', 'foo-value'))
    assert(dbi:put('bar', 'bar-value'))
    assert(dbi:put('qux', 'qux-value'))

    -- test that true if the cursor is points to the end of data
    local cur = assert(dbi:cursor())
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
    local dbi = assert(opendbi())
    assert(dbi:put('foo', 'foo-value'))
    assert(dbi:put('bar', 'bar-value'))
    assert(dbi:put('qux', 'qux-value'))

    -- test that false if the cursor is not pointed to the first key-value pair
    local cur = assert(dbi:cursor())
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
    local dbi = assert(opendbi())
    assert(dbi:put('foo', 'foo-value'))
    assert(dbi:put('bar', 'bar-value'))
    assert(dbi:put('qux', 'qux-value'))

    -- test that false if the cursor is not pointed to the last key-value pair
    local n = 0
    local cur = assert(dbi:cursor())
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
    local dbi = assert(opendbi())
    assert(dbi:put('foo', 'foo-value'))
    assert(dbi:put('bar', 'bar-value'))
    assert(dbi:put('qux', 'qux-value'))
    assert(dbi:put('quux', 'quux-value'))

    -- test that estimates the distance between cursors as a number of elements
    local cur1 = assert(dbi:cursor())
    assert(cur1:get(libmdbx.NEXT))
    local cur2 = assert(dbi:cursor())
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
    local dbi = assert(opendbi())
    assert(dbi:put('foo', 'foo-value'))
    assert(dbi:put('bar', 'bar-value'))
    assert(dbi:put('qux', 'qux-value'))
    assert(dbi:put('quux', 'quux-value'))

    -- test that estimates the move distance
    local cur = assert(dbi:cursor())
    assert(cur:get(libmdbx.NEXT))
    for op, dist in pairs({
        FIRST = 0,
        NEXT = 1,
        LAST = 3,
    }) do
        assert.equal(cur:estimate_move(libmdbx[op]), dist)
    end
end

