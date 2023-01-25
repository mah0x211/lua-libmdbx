/**
 * Copyright (C) 2023 Masatoshi Fukunaga
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 **/

#include "lmdbx.h"

// TODO: mdbx_get_attr
// TODO: mdbx_set_attr
// TODO: mdbx_put_attr

static int sequence_lua(lua_State *L)
{
    lmdbx_dbi_t *dbi = lauxh_checkudata(L, 1, LMDBX_DBI_MT);
    uintptr_t incr   = lauxh_optuint64(L, 2, 0);
    uint64_t result  = 0;
    int rc = mdbx_dbi_sequence(dbi->txn->txn, dbi->dbi, &result, incr);

    if (rc) {
        lua_pushnil(L);
        lmdbx_pusherror(L, rc);
        return 3;
    }
    lua_pushinteger(L, result);
    return 1;
}

static int estimate_range_lua(lua_State *L)
{
    lmdbx_dbi_t *dbi      = lauxh_checkudata(L, 1, LMDBX_DBI_MT);
    size_t begin_klen     = 0;
    const char *begin_key = lauxh_optlstring(L, 2, NULL, &begin_klen);
    size_t end_klen       = 0;
    const char *end_key   = lauxh_optlstring(L, 3, NULL, &end_klen);
    size_t begin_vlen     = 0;
    const char *begin_val = lauxh_optlstring(L, 4, NULL, &begin_vlen);
    size_t end_vlen       = 0;
    const char *end_val   = lauxh_optlstring(L, 5, NULL, &end_vlen);
    MDBX_val begin_k = {.iov_base = (void *)begin_key, .iov_len = begin_klen};
    MDBX_val begin_v = {.iov_base = (void *)begin_val, .iov_len = begin_vlen};
    MDBX_val end_k   = {.iov_base = (void *)end_key, .iov_len = end_klen};
    MDBX_val end_v   = {.iov_base = (void *)end_val, .iov_len = end_vlen};
    ptrdiff_t distance_items = 0;
    int rc                   = mdbx_estimate_range(
        dbi->txn->txn, dbi->dbi, (begin_klen) ? &begin_k : NULL,
        (begin_vlen) ? &begin_v : NULL, (end_klen) ? &end_k : NULL,
        (end_vlen) ? &end_v : NULL, &distance_items);

    if (rc) {
        lua_pushnil(L);
        lmdbx_pusherror(L, rc);
        return 3;
    }
    lua_pushinteger(L, distance_items);
    return 1;
}

static int cursor_lua(lua_State *L)
{
    return lmdbx_cursor_open_lua(L);
}

static int del_lua(lua_State *L)
{
    lmdbx_dbi_t *dbi = lauxh_checkudata(L, 1, LMDBX_DBI_MT);
    size_t klen      = 0;
    const char *key  = lauxh_checklstring(L, 2, &klen);
    size_t vlen      = 0;
    const char *val  = lauxh_optlstring(L, 3, NULL, &vlen);
    MDBX_val k       = {.iov_base = (void *)key, .iov_len = klen};
    MDBX_val v       = {.iov_base = (void *)val, .iov_len = vlen};
    int rc           = mdbx_del(dbi->txn->txn, dbi->dbi, &k, (val) ? &v : NULL);

    if (rc) {
        lua_pushboolean(L, 0);
        if (rc == MDBX_NOTFOUND) {
            return 1;
        }
        lmdbx_pusherror(L, rc);
        return 3;
    }
    lua_pushboolean(L, 1);
    return 1;
}

static int replace_lua(lua_State *L)
{
    lmdbx_dbi_t *dbi  = lauxh_checkudata(L, 1, LMDBX_DBI_MT);
    size_t klen       = 0;
    const char *key   = lauxh_checklstring(L, 2, &klen);
    size_t vlen       = 0;
    const char *val   = lauxh_optlstring(L, 3, NULL, &vlen);
    size_t olen       = 0;
    const char *oval  = lauxh_optlstring(L, 4, NULL, &olen);
    lua_Integer flags = lmdbx_checkflags(L, 5);
    MDBX_val k        = {.iov_base = (void *)key, .iov_len = klen};
    MDBX_val v        = {.iov_base = (void *)val, .iov_len = vlen};
    MDBX_val old      = {.iov_base = (void *)oval, .iov_len = olen};
    MDBX_val *new     = (val) ? &v : NULL;
    int rc = mdbx_replace(dbi->txn->txn, dbi->dbi, &k, new, &old, flags);

    if (rc == MDBX_RESULT_TRUE) {
        // the buffer passed in is too small
        old.iov_base = alloca(old.iov_len);
        rc = mdbx_replace(dbi->txn->txn, dbi->dbi, &k, new, &old, flags);
    }

    switch (rc) {
    case MDBX_SUCCESS:
        lua_pushlstring(L, old.iov_base, old.iov_len);
        return 1;

    default:
        if (rc == MDBX_NOTFOUND) {
            return 0;
        }
        lua_pushnil(L);
        lmdbx_pusherror(L, rc);
        return 3;
    }
}

static int put_lua(lua_State *L)
{
    lmdbx_dbi_t *dbi  = lauxh_checkudata(L, 1, LMDBX_DBI_MT);
    size_t klen       = 0;
    const char *key   = lauxh_checklstring(L, 2, &klen);
    size_t vlen       = 0;
    const char *val   = lauxh_checklstring(L, 3, &vlen);
    lua_Integer flags = lmdbx_checkflags(L, 4);
    MDBX_val k        = {.iov_base = (void *)key, .iov_len = klen};
    MDBX_val v        = {.iov_base = (void *)val, .iov_len = vlen};
    int rc            = mdbx_put(dbi->txn->txn, dbi->dbi, &k, &v, flags);

    if (rc) {
        lua_pushboolean(L, 0);
        if (rc == MDBX_NOTFOUND) {
            return 1;
        }
        lmdbx_pusherror(L, rc);
        return 3;
    }
    lua_pushboolean(L, 1);
    return 1;
}

// helper methods
static int op_update_lua(lua_State *L)
{
    // txn:op_update(key, val, old)
    // update one multi-value entry
    if (lua_gettop(L) > 3) {
        int rc = 0;

        lua_settop(L, 4);
        lua_pushinteger(L, MDBX_CURRENT);
        lua_pushinteger(L, MDBX_NOOVERWRITE);
        rc = replace_lua(L);
        if (rc <= 1) {
            lua_pushboolean(L, rc);
            return 1;
        }
        lua_pushboolean(L, 0);
        lua_replace(L, -(rc + 1));
        return rc;
    }

    // txn:op_update(key, val)
    // overwrite by single new value
    lua_settop(L, 3);
    lua_pushinteger(L, MDBX_CURRENT);
    lua_pushinteger(L, MDBX_ALLDUPS);
    return put_lua(L);
}

static int op_upsert_lua(lua_State *L)
{
    int multi = lauxh_optboolean(L, 4, 0);

    // txn:op_upsert(key, val [, multi])
    lua_settop(L, 3);
    lua_pushinteger(L, MDBX_UPSERT);
    if (multi) {
        // Has effect only for MDBX_DUPSORT databases. For upsertion:
        // don't write if the key-value pair already exist.
        lua_pushinteger(L, MDBX_NODUPDATA);
    } else {
        // overwrite by single new value
        lua_pushinteger(L, MDBX_ALLDUPS);
    }

    return put_lua(L);
}

static int op_insert_lua(lua_State *L)
{
    lua_settop(L, 3);
    lua_pushinteger(L, MDBX_NOOVERWRITE);
    return put_lua(L);
}

static int get_equal_or_great_lua(lua_State *L)
{
    lmdbx_dbi_t *dbi = lauxh_checkudata(L, 1, LMDBX_DBI_MT);
    size_t klen      = 0;
    const char *key  = lauxh_checklstring(L, 2, &klen);
    MDBX_val k       = {.iov_base = (void *)key, .iov_len = klen};
    MDBX_val v       = {0};
    int rc           = mdbx_get_equal_or_great(dbi->txn->txn, dbi->dbi, &k, &v);

    switch (rc) {
    case MDBX_SUCCESS:
    case MDBX_RESULT_TRUE:
        lua_createtable(L, 0, 2);
        lauxh_pushlstr2tbl(L, "key", k.iov_base, k.iov_len);
        lauxh_pushlstr2tbl(L, "data", v.iov_base, v.iov_len);
        return 1;

    case MDBX_NOTFOUND:
        return 0;

    default:
        lua_pushnil(L);
        lmdbx_pusherror(L, rc);
        return 3;
    }
}

static int get_lua(lua_State *L)
{
    lmdbx_dbi_t *dbi = lauxh_checkudata(L, 1, LMDBX_DBI_MT);
    size_t klen      = 0;
    const char *key  = lauxh_checklstring(L, 2, &klen);
    int do_count     = lauxh_optboolean(L, 3, 0);
    MDBX_val k       = {.iov_base = (void *)key, .iov_len = klen};
    MDBX_val v       = {0};
    size_t count     = 0;
    int rc = (do_count) ? mdbx_get_ex(dbi->txn->txn, dbi->dbi, &k, &v, &count) :
                          mdbx_get(dbi->txn->txn, dbi->dbi, &k, &v);

    if (rc) {
        if (rc == MDBX_NOTFOUND) {
            return 0;
        }
        lua_pushnil(L);
        lmdbx_pusherror(L, rc);
        return 3;
    }
    lua_pushlstring(L, v.iov_base, v.iov_len);
    if (do_count) {
        lua_pushnil(L);
        lua_pushnil(L);
        lua_pushinteger(L, count);
        return 4;
    }
    return 1;
}

static int flags_lua(lua_State *L)
{
    lmdbx_dbi_t *dbi = lauxh_checkudata(L, 1, LMDBX_DBI_MT);
    unsigned flags   = 0;
    unsigned state   = 0;
    int rc = mdbx_dbi_flags_ex(dbi->txn->txn, dbi->dbi, &flags, &state);

    if (rc) {
        lua_pushnil(L);
        lmdbx_pusherror(L, rc);
        return 3;
    }
    lua_createtable(L, 0, 2);
    lua_createtable(L, 0, 0);
    if (flags & MDBX_REVERSEKEY) {
        lauxh_pushint2tbl(L, "REVERSEKEY", MDBX_REVERSEKEY);
    }
    if (flags & MDBX_DUPSORT) {
        lauxh_pushint2tbl(L, "DUPSORT", MDBX_DUPSORT);
    }
    if (flags & MDBX_INTEGERKEY) {
        lauxh_pushint2tbl(L, "INTEGERKEY", MDBX_INTEGERKEY);
    }
    if (flags & MDBX_DUPFIXED) {
        lauxh_pushint2tbl(L, "DUPFIXED", MDBX_DUPFIXED);
    }
    if (flags & MDBX_INTEGERDUP) {
        lauxh_pushint2tbl(L, "INTEGERDUP", MDBX_INTEGERDUP);
    }
    if (flags & MDBX_REVERSEDUP) {
        lauxh_pushint2tbl(L, "REVERSEDUP", MDBX_REVERSEDUP);
    }
    if (flags & MDBX_CREATE) {
        lauxh_pushint2tbl(L, "CREATE", MDBX_CREATE);
    }
    if (flags & MDBX_DB_ACCEDE) {
        lauxh_pushint2tbl(L, "DB_ACCEDE", MDBX_DB_ACCEDE);
    }
    lua_setfield(L, -2, "flags");
    lua_createtable(L, 0, 0);
    if (state & MDBX_DBI_DIRTY) {
        lauxh_pushint2tbl(L, "DBI_DIRTY", MDBX_DBI_DIRTY);
    }
    if (state & MDBX_DBI_STALE) {
        lauxh_pushint2tbl(L, "DBI_STALE", MDBX_DBI_STALE);
    }
    if (state & MDBX_DBI_FRESH) {
        lauxh_pushint2tbl(L, "DBI_FRESH", MDBX_DBI_FRESH);
    }
    if (state & MDBX_DBI_CREAT) {
        lauxh_pushint2tbl(L, "DBI_CREATE", MDBX_DBI_CREAT);
    }
    lua_setfield(L, -2, "state");

    return 1;
}

static int dupsort_depthmask_lua(lua_State *L)
{
    lmdbx_dbi_t *dbi = lauxh_checkudata(L, 1, LMDBX_DBI_MT);
    uint32_t mask    = 0;
    int rc = mdbx_dbi_dupsort_depthmask(dbi->txn->txn, dbi->dbi, &mask);

    switch (rc) {
    case 0:
        lua_pushinteger(L, mask);
        return 1;

    case MDBX_RESULT_TRUE:
        lua_pushinteger(L, 0);
        return 1;

    default:
        lua_pushnil(L);
        lmdbx_pusherror(L, rc);
        return 3;
    }
}

static int stat_lua(lua_State *L)
{
    lmdbx_dbi_t *dbi = lauxh_checkudata(L, 1, LMDBX_DBI_MT);
    MDBX_stat stat   = {0};
    int rc = mdbx_dbi_stat(dbi->txn->txn, dbi->dbi, &stat, sizeof(MDBX_stat));

    if (rc) {
        lua_pushnil(L);
        lmdbx_pusherror(L, rc);
        return 3;
    }
    lmdbx_pushstat(L, &stat);
    return 1;
}

static lmdbx_txn_t TXN_NULL = {
    .env_ref = LUA_NOREF,
    .txn     = NULL,
};

static int drop_lua(lua_State *L)
{
    lmdbx_dbi_t *dbi = lauxh_checkudata(L, 1, LMDBX_DBI_MT);
    int del          = lauxh_optboolean(L, 2, 0);
    int rc           = mdbx_drop(dbi->txn->txn, dbi->dbi, del);

    if (rc) {
        lua_pushboolean(L, 0);
        lmdbx_pusherror(L, rc);
        return 3;
    } else if (del) {
        dbi->txn     = &TXN_NULL;
        dbi->txn_ref = lauxh_unref(L, dbi->txn_ref);
    }
    lua_pushboolean(L, 1);
    return 1;
}

static int close_lua(lua_State *L)
{
    lmdbx_dbi_t *dbi = lauxh_checkudata(L, 1, LMDBX_DBI_MT);

    if (dbi->txn != &TXN_NULL) {
        int rc = mdbx_dbi_close(mdbx_txn_env(dbi->txn->txn), dbi->dbi);

        if (rc) {
            lua_pushboolean(L, 0);
            lmdbx_pusherror(L, rc);
            return 3;
        }
        dbi->txn     = &TXN_NULL;
        dbi->txn_ref = lauxh_unref(L, dbi->txn_ref);
    }
    lua_pushboolean(L, 1);

    return 1;
}

static int txn_lua(lua_State *L)
{
    lmdbx_dbi_t *dbi = lauxh_checkudata(L, 1, LMDBX_DBI_MT);
    lauxh_pushref(L, dbi->txn_ref);
    return 1;
}

static int gc_lua(lua_State *L)
{
    lmdbx_dbi_t *dbi = lauxh_checkudata(L, 1, LMDBX_DBI_MT);
    lauxh_unref(L, dbi->txn_ref);
    return 0;
}

static int tostring_lua(lua_State *L)
{
    lmdbx_dbi_t *dbi = lauxh_checkudata(L, 1, LMDBX_DBI_MT);
    lua_pushfstring(L, LMDBX_DBI_MT ": %p", dbi);
    return 1;
}

int lmdbx_dbi_open_lua(lua_State *L)
{
    lmdbx_txn_t *txn  = lauxh_checkudata(L, 1, LMDBX_TXN_MT);
    const char *name  = lauxh_optstring(L, 2, NULL);
    lua_Integer flags = lmdbx_checkflags(L, 3);
    lmdbx_dbi_t *dbi  = lua_newuserdata(L, sizeof(lmdbx_dbi_t));
    int rc            = mdbx_dbi_open(txn->txn, name, flags, &dbi->dbi);

    if (rc) {
        if (rc == MDBX_NOTFOUND) {
            return 0;
        }
        lua_pushnil(L);
        lmdbx_pusherror(L, rc);
        return 3;
    }
    lauxh_setmetatable(L, LMDBX_DBI_MT);
    dbi->txn     = txn;
    dbi->txn_ref = lauxh_refat(L, 1);

    return 1;
}

void lmdbx_dbi_init(lua_State *L)
{
    struct luaL_Reg mmethod[] = {
        {"__tostring", tostring_lua},
        {"__gc",       gc_lua      },
        {NULL,         NULL        }
    };
    struct luaL_Reg method[] = {
        {"txn",                txn_lua               },
        {"close",              close_lua             },
        {"drop",               drop_lua              },
        {"stat",               stat_lua              },
        {"dupsort_depthmask",  dupsort_depthmask_lua },
        {"flags",              flags_lua             },
        {"get",                get_lua               },
        {"get_equal_or_great", get_equal_or_great_lua},
        {"op_insert",          op_insert_lua         }, // helper func
        {"op_upsert",          op_upsert_lua         }, // helper func
        {"op_update",          op_update_lua         }, // helper func
        {"put",                put_lua               },
        {"replace",            replace_lua           },
        {"del",                del_lua               },
        {"cursor",             cursor_lua            },
        {"estimate_range",     estimate_range_lua    },
        {"sequence",           sequence_lua          },
        {NULL,                 NULL                  }
    };

    // create metatable
    luaL_newmetatable(L, LMDBX_DBI_MT);
    // metamethods
    lmdbx_register(L, mmethod);
    // methods
    lua_pushstring(L, "__index");
    lua_newtable(L);
    lmdbx_register(L, method);
    lua_rawset(L, -3);
    lua_pop(L, 1);
}
