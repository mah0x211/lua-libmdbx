local testcase = require('testcase')
local libmdbx = require('libmdbx')

function testcase.limits_dbsize_min()
    -- test that return size
    assert.is_int(libmdbx.limits_dbsize_min(4096))
end

function testcase.limits_dbsize_max()
    -- test that return size
    assert.is_int(libmdbx.limits_dbsize_max(4096))
end

function testcase.limits_keysize_max()
    -- test that return size
    assert.is_int(libmdbx.limits_keysize_max(4096))
end

function testcase.limits_valsize_max()
    -- test that return size
    assert.is_int(libmdbx.limits_valsize_max(4096))
end

function testcase.limits_txnsize_max()
    -- test that return size
    assert.is_int(libmdbx.limits_txnsize_max(4096))
end

function testcase.default_pagesize()
    -- test that return size
    assert.is_int(libmdbx.default_pagesize())
end

function testcase.get_sysraminfo()
    -- test that return size
    assert.is_table(libmdbx.get_sysraminfo())
end

