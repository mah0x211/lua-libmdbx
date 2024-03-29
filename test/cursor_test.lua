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
    local env = openenv(...)
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

function testcase.txn()
    local dbh = opendbh()
    local cur = assert(dbh:cursor_open())

    -- test that return the associated txn
    assert.equal(cur:txn(), dbh:txn())
end

function testcase.close()
    local dbh = opendbh()
    local cur = assert(dbh:cursor_open())

    -- test that close a cursor handle
    cur:close()
end

function testcase.renew()
    local env = openenv(libmdbx.NOTLS)
    local txn = assert(env:begin(libmdbx.TXN_RDONLY))
    local dbi = assert(txn:dbi_open())
    local dbh = assert(dbi:dbh_open(txn))
    local cur = assert(dbh:cursor_open())
    assert.equal(cur:txn(), txn)

    -- test that renew a cursor handle
    local txn2 = assert(env:begin(libmdbx.TXN_RDONLY))
    assert.not_equal(txn2, txn)
    assert.is_true(cur:renew(txn2))
    assert.equal(cur:txn(), txn2)
end

function testcase.copy()
    local dbh = opendbh()
    local cur = assert(dbh:cursor_open())

    -- test that copy cursor position and state
    local cur2 = assert(cur:copy())
    assert.not_equal(cur2, cur)
    assert.match(cur2, '^libmdbx.cursor: ', false)
    assert.equal(cur2:txn(), cur:txn())
end

function testcase.set()
    local dbh = opendbh(nil, libmdbx.DUPSORT, libmdbx.CREATE)
    assert(dbh:put('foo', 'foo-value-1'))
    assert(dbh:put('foo', 'foo-value-2'))
    assert(dbh:put('bar', 'bar-value-1'))
    assert(dbh:put('bar', 'bar-value-2'))
    assert(dbh:put('qux', 'qux-value-1'))
    local cur = assert(dbh:cursor_open())

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
    local dbh = opendbh(nil, libmdbx.DUPSORT, libmdbx.CREATE)
    assert(dbh:put('foo', 'foo-value-1'))
    assert(dbh:put('foo', 'foo-value-2'))
    assert(dbh:put('bar', 'bar-value-1'))
    assert(dbh:put('bar', 'bar-value-2'))
    assert(dbh:put('qux', 'qux-value-1'))
    local cur = assert(dbh:cursor_open())

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
    local dbh = opendbh(nil, libmdbx.DUPSORT, libmdbx.CREATE)
    assert(dbh:put('foo', 'a-value'))
    assert(dbh:put('foo', 'b-value'))
    assert(dbh:put('bar', 'a-value'))
    assert(dbh:put('bar', 'b-value'))
    assert(dbh:put('qux', 'a-value'))
    local cur = assert(dbh:cursor_open())

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
    local dbh = opendbh(nil, libmdbx.DUPSORT, libmdbx.CREATE)
    assert(dbh:put('foo', 'a-value'))
    assert(dbh:put('foo', 'b-value'))
    assert(dbh:put('bar', 'a-value'))
    assert(dbh:put('bar', 'b-value'))
    assert(dbh:put('qux', 'a-value'))
    local cur = assert(dbh:cursor_open())

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

function testcase.get_both()
    local dbh = opendbh(nil, libmdbx.DUPSORT, libmdbx.CREATE)
    assert(dbh:put('foo', 'foo-value-1'))
    assert(dbh:put('bar', 'bar-value-1'))
    assert(dbh:put('bar', 'bar-value-2'))
    assert(dbh:put('qux', 'qux-value-1'))
    assert(dbh:put('qux', 'qux-value-2'))
    local cur = assert(dbh:cursor_open())

    -- test that retrieve key-value pair equal to given key-value
    local k, v, err = cur:get_both('qux', 'qux-value-2')
    assert.equal(k, 'qux')
    assert.equal(v, 'qux-value-2')
    assert.is_nil(err)

    -- test that return nil
    k, v, err = cur:get_both('foo', 'foo-value-2')
    assert.is_nil(k)
    assert.is_nil(v)
    assert.is_nil(err)
end

function testcase.get_both_range()
    local dbh = opendbh(nil, libmdbx.DUPSORT, libmdbx.CREATE)
    assert(dbh:put('foo', 'foo-value-1'))
    assert(dbh:put('bar', 'bar-value-1'))
    assert(dbh:put('bar', 'bar-value-2'))
    assert(dbh:put('qux', 'qux-value-1'))
    assert(dbh:put('qux', 'qux-value-3'))
    assert(dbh:put('qux', 'qux-value-5'))
    local cur = assert(dbh:cursor_open())

    -- test that retrieve first key-value pair equal to given key
    local k, v, err = cur:get_both_range('qux')
    assert.equal(k, 'qux')
    assert.equal(v, 'qux-value-1')
    assert.is_nil(err)

    -- test that retrieve key-value pair equal to given key-value
    k, v, err = cur:get_both_range('qux', 'qux-value-5')
    assert.equal(k, 'qux')
    assert.equal(v, 'qux-value-5')
    assert.is_nil(err)

    -- test that retrieve key-value pair equal to given key and value is greater than given value
    k, v, err = cur:get_both_range('qux', 'qux-value-2')
    assert.equal(k, 'qux')
    assert.equal(v, 'qux-value-3')
    assert.is_nil(err)

    -- test that return nil
    k, v, err = cur:get_both_range('qux', 'raa')
    assert.is_nil(k)
    assert.is_nil(v)
    assert.is_nil(err)
end

function testcase.get_first()
    local dbh = opendbh(nil, libmdbx.DUPSORT, libmdbx.CREATE)
    assert(dbh:put('foo', 'foo-value-1'))
    assert(dbh:put('bar', 'bar-value-1'))
    assert(dbh:put('bar', 'bar-value-2'))
    assert(dbh:put('qux', 'qux-value-1'))
    local cur = assert(dbh:cursor_open())

    -- test that retrieve first key-value pair
    local k, v, err = cur:get_first()
    assert.equal(k, 'bar')
    assert.equal(v, 'bar-value-1')
    assert.is_nil(err)
end

function testcase.get_first_dup()
    local dbh = opendbh(nil, libmdbx.DUPSORT, libmdbx.CREATE)
    assert(dbh:put('foo', 'foo-value-1'))
    assert(dbh:put('bar', 'bar-value-1'))
    assert(dbh:put('bar', 'bar-value-2'))
    assert(dbh:put('qux', 'qux-value-1'))
    local cur = assert(dbh:cursor_open())

    local k, v, err = cur:get_first()
    assert.equal(k, 'bar')
    assert.equal(v, 'bar-value-1')
    assert.is_nil(err)
    k, v = cur:get(libmdbx.NEXT)
    assert.equal(k, 'bar')
    assert.equal(v, 'bar-value-2')
    assert.is_nil(err)

    -- test that retrieve first value of current key
    k, v, err = cur:get_first_dup()
    assert.equal(k, '')
    assert.equal(v, 'bar-value-1')
    assert.is_nil(err)
end

function testcase.get_last()
    local dbh = opendbh(nil, libmdbx.DUPSORT, libmdbx.CREATE)
    assert(dbh:put('foo', 'foo-value-1'))
    assert(dbh:put('bar', 'bar-value-1'))
    assert(dbh:put('qux', 'qux-value-1'))
    assert(dbh:put('qux', 'qux-value-2'))
    local cur = assert(dbh:cursor_open())

    -- test that retrieve last key-value pair
    local k, v, err = cur:get_last()
    assert.equal(k, 'qux')
    assert.equal(v, 'qux-value-2')
    assert.is_nil(err)
end

function testcase.get_last_dup()
    local dbh = opendbh(nil, libmdbx.DUPSORT, libmdbx.CREATE)
    assert(dbh:put('foo', 'foo-value-1'))
    assert(dbh:put('bar', 'bar-value-1'))
    assert(dbh:put('bar', 'bar-value-2'))
    assert(dbh:put('bar', 'bar-value-3'))
    assert(dbh:put('qux', 'qux-value-1'))
    assert(dbh:put('qux', 'qux-value-2'))
    local cur = assert(dbh:cursor_open())
    local k, v, err = cur:get_first()
    assert.equal(k, 'bar')
    assert.equal(v, 'bar-value-1')
    assert.is_nil(err)

    -- test that retrieve last value of current key
    k, v, err = cur:get_last_dup()
    assert.equal(k, '')
    assert.equal(v, 'bar-value-3')
    assert.is_nil(err)
end

function testcase.get_next()
    local dbh = opendbh(nil, libmdbx.DUPSORT, libmdbx.CREATE)
    assert(dbh:put('foo', 'foo-value-1'))
    assert(dbh:put('bar', 'bar-value-1'))
    assert(dbh:put('bar', 'bar-value-2'))
    assert(dbh:put('qux', 'qux-value-1'))
    local cur = assert(dbh:cursor_open())
    local k, v, err = cur:get_first()
    assert.equal(k, 'bar')
    assert.equal(v, 'bar-value-1')
    assert.is_nil(err)

    -- test that retrieve next item
    k, v, err = cur:get_next()
    assert.equal(k, 'bar')
    assert.equal(v, 'bar-value-2')
    assert.is_nil(err)
end

function testcase.get_next_dup()
    local dbh = opendbh(nil, libmdbx.DUPSORT, libmdbx.CREATE)
    assert(dbh:put('foo', 'foo-value-1'))
    assert(dbh:put('bar', 'bar-value-1'))
    assert(dbh:put('bar', 'bar-value-2'))
    assert(dbh:put('qux', 'qux-value-1'))
    local cur = assert(dbh:cursor_open())
    local k, v, err = cur:get_first()
    assert.equal(k, 'bar')
    assert.equal(v, 'bar-value-1')
    assert.is_nil(err)

    -- test that retrieve next value of current key
    k, v, err = cur:get_next_dup()
    assert.equal(k, 'bar')
    assert.equal(v, 'bar-value-2')
    assert.is_nil(err)
end

function testcase.get_next_nodup()
    local dbh = opendbh(nil, libmdbx.DUPSORT, libmdbx.CREATE)
    assert(dbh:put('foo', 'foo-value-1'))
    assert(dbh:put('bar', 'bar-value-1'))
    assert(dbh:put('bar', 'bar-value-2'))
    assert(dbh:put('qux', 'qux-value-1'))
    local cur = assert(dbh:cursor_open())
    local k, v, err = cur:get_first()
    assert.equal(k, 'bar')
    assert.equal(v, 'bar-value-1')
    assert.is_nil(err)

    -- test that retrieve key-value pair of next key
    k, v, err = cur:get_next_nodup()
    assert.equal(k, 'foo')
    assert.equal(v, 'foo-value-1')
    assert.is_nil(err)
end

function testcase.get_prev()
    local dbh = opendbh(nil, libmdbx.DUPSORT, libmdbx.CREATE)
    assert(dbh:put('foo', 'foo-value-1'))
    assert(dbh:put('bar', 'bar-value-1'))
    assert(dbh:put('qux', 'qux-value-1'))
    assert(dbh:put('qux', 'qux-value-2'))
    local cur = assert(dbh:cursor_open())
    local k, v, err = cur:get_both_range('foo')
    assert.equal(k, 'foo')
    assert.equal(v, 'foo-value-1')
    assert.is_nil(err)

    -- test that retrieve prev item
    k, v, err = cur:get_prev()
    assert.equal(k, 'bar')
    assert.equal(v, 'bar-value-1')
    assert.is_nil(err)
end

function testcase.get_prev_dup()
    local dbh = opendbh(nil, libmdbx.DUPSORT, libmdbx.CREATE)
    assert(dbh:put('foo', 'foo-value-1'))
    assert(dbh:put('bar', 'bar-value-1'))
    assert(dbh:put('qux', 'qux-value-1'))
    assert(dbh:put('qux', 'qux-value-2'))
    local cur = assert(dbh:cursor_open())
    local k, v, err = cur:get_last()
    assert.equal(k, 'qux')
    assert.equal(v, 'qux-value-2')
    assert.is_nil(err)

    -- test that retrieve prev value of current key
    k, v, err = cur:get_prev_dup()
    assert.equal(k, 'qux')
    assert.equal(v, 'qux-value-1')
    assert.is_nil(err)

    -- test that return nil
    k, v, err = cur:get_prev_dup()
    assert.is_nil(k)
    assert.is_nil(v)
    assert.is_nil(err)
end

function testcase.get_prev_nodup()
    local dbh = opendbh(nil, libmdbx.DUPSORT, libmdbx.CREATE)
    assert(dbh:put('foo', 'foo-value-1'))
    assert(dbh:put('bar', 'bar-value-1'))
    assert(dbh:put('qux', 'qux-value-1'))
    assert(dbh:put('qux', 'qux-value-2'))
    local cur = assert(dbh:cursor_open())
    local k, v, err = cur:get_last()
    assert.equal(k, 'qux')
    assert.equal(v, 'qux-value-2')
    assert.is_nil(err)

    -- test that retrieve key-value pair of prev key
    k, v, err = cur:get_prev_nodup()
    assert.equal(k, 'foo')
    assert.equal(v, 'foo-value-1')
    assert.is_nil(err)
end

function testcase.get()
    local dbh = opendbh(nil, libmdbx.DUPSORT, libmdbx.CREATE)
    assert(dbh:put('foo', 'foo-value-1'))
    assert(dbh:put('foo', 'foo-value-2'))
    assert(dbh:put('bar', 'bar-value-1'))
    assert(dbh:put('qux', 'qux-value-1'))
    assert(dbh:put('qux', 'qux-value-2'))
    local cur = assert(dbh:cursor_open())

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
    local dbh = opendbh()
    assert(dbh:put('foo', 'foo-value'))
    assert(dbh:put('bar', 'bar-value'))
    assert(dbh:put('qux', 'qux-value'))
    local cur = assert(dbh:cursor_open())

    -- test that retrieve multiple non-dupsort key/value pairs by cursor
    local res = assert(cur:get_batch())
    assert.equal(res, {
        foo = 'foo-value',
        bar = 'bar-value',
        qux = 'qux-value',
    })
end

function testcase.put()
    local dbh = opendbh()
    assert(dbh:put('foo', 'foo-value'))
    assert(dbh:put('bar', 'bar-value'))
    assert(dbh:put('qux', 'qux-value'))
    local cur = assert(dbh:cursor_open())

    -- test that store by cursor
    assert.is_true(cur:put('foo', 'updated'))
    assert.equal(dbh:get('foo'), 'updated')

    -- test that store current value by cursor
    local k, v = assert(cur:get())
    assert.is_true(cur:put(k, v .. '-updated', libmdbx.CURRENT))
    assert.equal(dbh:get(k), v .. '-updated')

    -- test that cannot store current value if key does not matched
    k = assert(cur:get())
    local ok, err = cur:put(k .. '-mismatch', 'updated', libmdbx.CURRENT)
    assert.is_false(ok)
    assert.equal(err, libmdbx.errno.EKEYMISMATCH)
end

function testcase.del()
    local dbh = opendbh(nil, libmdbx.DUPSORT, libmdbx.CREATE)
    assert(dbh:put('foo', 'foo-value-1'))
    assert(dbh:put('foo', 'foo-value-2'))
    assert(dbh:put('bar', 'bar-value-1'))
    assert(dbh:put('qux', 'qux-value-1'))
    assert(dbh:put('qux', 'qux-value-2'))

    local cur = assert(dbh:cursor_open())
    local is_qux = false
    repeat
        local k = cur:get(libmdbx.NEXT)

        if k == 'foo' then
            -- test that delete all of the data items for the current key
            assert.is_true(cur:del(libmdbx.ALLDUPS))
            assert.is_nil(dbh:get(k))
        elseif k == 'bar' then
            -- test that delete current key/data pair
            assert.is_true(cur:del())
            assert.is_nil(dbh:get(k))
        elseif k == 'qux' then
            -- test that delete current key/data pair
            assert.is_true(cur:del())
            if not is_qux then
                is_qux = true
                assert.equal(dbh:get(k), 'qux-value-2')
            else
                assert.is_nil(dbh:get(k))
            end
        end
    until k == nil
end

function testcase.eof()
    local dbh = opendbh()
    assert(dbh:put('foo', 'foo-value'))
    assert(dbh:put('bar', 'bar-value'))
    assert(dbh:put('qux', 'qux-value'))

    -- test that true if the cursor is points to the end of data
    local cur = assert(dbh:cursor_open())
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
    local dbh = opendbh()
    assert(dbh:put('foo', 'foo-value'))
    assert(dbh:put('bar', 'bar-value'))
    assert(dbh:put('qux', 'qux-value'))

    -- test that false if the cursor is not pointed to the first key-value pair
    local cur = assert(dbh:cursor_open())
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
    local dbh = opendbh()
    assert(dbh:put('foo', 'foo-value'))
    assert(dbh:put('bar', 'bar-value'))
    assert(dbh:put('qux', 'qux-value'))

    -- test that false if the cursor is not pointed to the last key-value pair
    local n = 0
    local cur = assert(dbh:cursor_open())
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
    local dbh = opendbh()
    assert(dbh:put('foo', 'foo-value'))
    assert(dbh:put('bar', 'bar-value'))
    assert(dbh:put('qux', 'qux-value'))
    assert(dbh:put('quux', 'quux-value'))

    -- test that estimates the distance between cursors as a number of elements
    local cur1 = assert(dbh:cursor_open())
    assert(cur1:get(libmdbx.NEXT))
    local cur2 = assert(dbh:cursor_open())
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
    local dbh = opendbh()
    assert(dbh:put('foo', 'foo-value'))
    assert(dbh:put('bar', 'bar-value'))
    assert(dbh:put('qux', 'qux-value'))
    assert(dbh:put('quux', 'quux-value'))

    -- test that estimates the move distance
    local cur = assert(dbh:cursor_open())
    assert(cur:get(libmdbx.NEXT))
    for op, dist in pairs({
        FIRST = 0,
        NEXT = 1,
        LAST = 3,
    }) do
        assert.equal(cur:estimate_move(libmdbx[op]), dist)
    end
end

