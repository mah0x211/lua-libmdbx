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

static inline MDBX_txn *GET_TXN(lmdbx_dbh_t *dbh)
{
    return (dbh->txn) ? dbh->txn->txn : NULL;
}

static inline MDBX_dbi GET_DBI(lmdbx_dbh_t *dbh)
{
    return (dbh->dbi) ? dbh->dbi->dbi : 0;
}

static int sequence_lua(lua_State *L)
{
    lmdbx_dbh_t *dbh = lauxh_checkudata(L, 1, LMDBX_DBH_MT);
    uintptr_t incr   = lauxh_optuint64(L, 2, 0);
    uint64_t result  = 0;
    int rc = mdbx_dbi_sequence(GET_TXN(dbh), GET_DBI(dbh), &result, incr);

    if (rc) {
        lua_pushnil(L);
        lmdbx_pusherror(L, rc);
        return 2;
    }
    lua_pushinteger(L, result);
    return 1;
}

static int estimate_range_lua(lua_State *L)
{
    lmdbx_dbh_t *dbh      = lauxh_checkudata(L, 1, LMDBX_DBH_MT);
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
        GET_TXN(dbh), GET_DBI(dbh), (begin_klen) ? &begin_k : NULL,
        (begin_vlen) ? &begin_v : NULL, (end_klen) ? &end_k : NULL,
        (end_vlen) ? &end_v : NULL, &distance_items);

    if (rc) {
        lua_pushnil(L);
        lmdbx_pusherror(L, rc);
        return 2;
    }
    lua_pushinteger(L, distance_items);
    return 1;
}

static int cursor_open_lua(lua_State *L)
{
    return lmdbx_cursor_open_lua(L);
}

static int del_lua(lua_State *L)
{
    lmdbx_dbh_t *dbh = lauxh_checkudata(L, 1, LMDBX_DBH_MT);
    size_t klen      = 0;
    const char *key  = lauxh_checklstring(L, 2, &klen);
    size_t vlen      = 0;
    const char *val  = lauxh_optlstring(L, 3, NULL, &vlen);
    MDBX_val k       = {.iov_base = (void *)key, .iov_len = klen};
    MDBX_val v       = {.iov_base = (void *)val, .iov_len = vlen};
    int rc = mdbx_del(GET_TXN(dbh), GET_DBI(dbh), &k, (val) ? &v : NULL);

    if (rc) {
        lua_pushboolean(L, 0);
        if (rc == MDBX_NOTFOUND) {
            return 1;
        }
        lmdbx_pusherror(L, rc);
        return 2;
    }
    lua_pushboolean(L, 1);
    return 1;
}

static int replace_lua(lua_State *L)
{
    lmdbx_dbh_t *dbh  = lauxh_checkudata(L, 1, LMDBX_DBH_MT);
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
    int rc = mdbx_replace(GET_TXN(dbh), GET_DBI(dbh), &k, new, &old, flags);

    if (rc == MDBX_RESULT_TRUE) {
        // the buffer passed in is too small
        old.iov_base = alloca(old.iov_len);
        rc = mdbx_replace(GET_TXN(dbh), GET_DBI(dbh), &k, new, &old, flags);
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
        return 2;
    }
}

static int put_lua(lua_State *L)
{
    lmdbx_dbh_t *dbh  = lauxh_checkudata(L, 1, LMDBX_DBH_MT);
    size_t klen       = 0;
    const char *key   = lauxh_checklstring(L, 2, &klen);
    size_t vlen       = 0;
    const char *val   = lauxh_checklstring(L, 3, &vlen);
    lua_Integer flags = lmdbx_checkflags(L, 4);
    MDBX_val k        = {.iov_base = (void *)key, .iov_len = klen};
    MDBX_val v        = {.iov_base = (void *)val, .iov_len = vlen};
    int rc            = mdbx_put(GET_TXN(dbh), GET_DBI(dbh), &k, &v, flags);

    if (rc) {
        lua_pushboolean(L, 0);
        if (rc == MDBX_NOTFOUND) {
            return 1;
        }
        lmdbx_pusherror(L, rc);
        return 2;
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
        lua_pushinteger(L, MDBX_CURRENT | MDBX_NOOVERWRITE);
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
    lua_pushinteger(L, MDBX_CURRENT | MDBX_ALLDUPS);
    return put_lua(L);
}

static int op_upsert_lua(lua_State *L)
{
    lmdbx_dbh_t *dbh = lauxh_checkudata(L, 1, LMDBX_DBH_MT);
    size_t klen      = 0;
    const char *key  = lauxh_checklstring(L, 2, &klen);
    size_t vlen      = 0;
    const char *val  = lauxh_checklstring(L, 3, &vlen);
    int multi        = lauxh_optboolean(L, 4, 0);
    MDBX_val k       = {.iov_base = (void *)key, .iov_len = klen};
    MDBX_val v       = {.iov_base = (void *)val, .iov_len = vlen};
    MDBX_val nouse   = {.iov_base = (void *)NULL, .iov_len = 0};
    int rc           = 0;

    // txn:op_upsert(key, val [, multi])
    if (multi) {
        rc = mdbx_put(GET_TXN(dbh), GET_DBI(dbh), &k, &v, MDBX_UPSERT);
    } else if (mdbx_get(GET_TXN(dbh), GET_DBI(dbh), &k, &nouse) == 0) {
        // replace current value
        rc = mdbx_put(GET_TXN(dbh), GET_DBI(dbh), &k, &v,
                      MDBX_UPSERT | MDBX_CURRENT | MDBX_ALLDUPS);
    } else {
        rc = mdbx_put(GET_TXN(dbh), GET_DBI(dbh), &k, &v, MDBX_UPSERT);
    }

    if (rc) {
        lua_pushboolean(L, 0);
        lmdbx_pusherror(L, rc);
        return 2;
    }
    lua_pushboolean(L, 1);
    return 1;
}

static int op_insert_lua(lua_State *L)
{
    lua_settop(L, 3);
    lua_pushinteger(L, MDBX_NOOVERWRITE);
    return put_lua(L);
}

static int get_equal_or_great_lua(lua_State *L)
{
    lmdbx_dbh_t *dbh = lauxh_checkudata(L, 1, LMDBX_DBH_MT);
    size_t klen      = 0;
    const char *key  = lauxh_checklstring(L, 2, &klen);
    MDBX_val k       = {.iov_base = (void *)key, .iov_len = klen};
    MDBX_val v       = {0};
    int rc = mdbx_get_equal_or_great(GET_TXN(dbh), GET_DBI(dbh), &k, &v);

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
        return 2;
    }
}

static int get_lua(lua_State *L)
{
    lmdbx_dbh_t *dbh = lauxh_checkudata(L, 1, LMDBX_DBH_MT);
    size_t klen      = 0;
    const char *key  = lauxh_checklstring(L, 2, &klen);
    int do_count     = lauxh_optboolean(L, 3, 0);
    MDBX_val k       = {.iov_base = (void *)key, .iov_len = klen};
    MDBX_val v       = {0};
    size_t count     = 0;
    int rc           = (do_count) ?
                           mdbx_get_ex(GET_TXN(dbh), GET_DBI(dbh), &k, &v, &count) :
                           mdbx_get(GET_TXN(dbh), GET_DBI(dbh), &k, &v);

    if (rc) {
        if (rc == MDBX_NOTFOUND) {
            return 0;
        }
        lua_pushnil(L);
        lmdbx_pusherror(L, rc);
        return 2;
    }
    lua_pushlstring(L, v.iov_base, v.iov_len);
    if (do_count) {
        lua_pushnil(L);
        lua_pushinteger(L, count);
        return 3;
    }
    return 1;
}

static int flags_lua(lua_State *L)
{
    lmdbx_dbh_t *dbh = lauxh_checkudata(L, 1, LMDBX_DBH_MT);
    unsigned flags   = 0;
    unsigned state   = 0;
    int rc = mdbx_dbi_flags_ex(GET_TXN(dbh), GET_DBI(dbh), &flags, &state);

    if (rc) {
        lua_pushnil(L);
        lmdbx_pusherror(L, rc);
        return 2;
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
    lmdbx_dbh_t *dbh = lauxh_checkudata(L, 1, LMDBX_DBH_MT);
    uint32_t mask    = 0;
    int rc = mdbx_dbi_dupsort_depthmask(GET_TXN(dbh), GET_DBI(dbh), &mask);

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
        return 2;
    }
}

static int stat_lua(lua_State *L)
{
    lmdbx_dbh_t *dbh = lauxh_checkudata(L, 1, LMDBX_DBH_MT);
    MDBX_stat stat   = {0};
    int rc =
        mdbx_dbi_stat(GET_TXN(dbh), GET_DBI(dbh), &stat, sizeof(MDBX_stat));

    if (rc) {
        lua_pushnil(L);
        lmdbx_pusherror(L, rc);
        return 2;
    }
    lmdbx_pushstat(L, &stat);
    return 1;
}

static lmdbx_txn_t TXN_NULL = {
    .env_ref = LUA_NOREF,
    .txn     = NULL,
};

static lmdbx_dbi_t DBI_NULL = {
    .env_ref = LUA_NOREF,
    .dbi     = 0,
};

static int close_lua(lua_State *L)
{
    lmdbx_dbh_t *dbh = lauxh_checkudata(L, 1, LMDBX_DBH_MT);

    dbh->dbi_ref = lauxh_unref(L, dbh->dbi_ref);
    dbh->txn_ref = lauxh_unref(L, dbh->txn_ref);
    dbh->dbi     = &DBI_NULL;
    dbh->txn     = &TXN_NULL;

    return 0;
}

static int drop_lua(lua_State *L)
{
    lmdbx_dbh_t *dbh = lauxh_checkudata(L, 1, LMDBX_DBH_MT);
    int del          = lauxh_optboolean(L, 2, 0);
    int rc           = mdbx_drop(GET_TXN(dbh), GET_DBI(dbh), del);

    if (rc) {
        lua_pushboolean(L, 0);
        lmdbx_pusherror(L, rc);
        return 2;
    } else if (del) {
        close_lua(L);
    }
    lua_pushboolean(L, 1);
    return 1;
}

static int txn_lua(lua_State *L)
{
    lmdbx_dbh_t *dbh = lauxh_checkudata(L, 1, LMDBX_DBH_MT);
    lauxh_pushref(L, dbh->txn_ref);
    return 1;
}

static int dbi_lua(lua_State *L)
{
    lmdbx_dbh_t *dbh = lauxh_checkudata(L, 1, LMDBX_DBH_MT);
    lauxh_pushref(L, dbh->dbi_ref);
    return 1;
}

static int gc_lua(lua_State *L)
{
    close_lua(L);
    return 0;
}

static int tostring_lua(lua_State *L)
{
    lmdbx_dbh_t *dbh = lauxh_checkudata(L, 1, LMDBX_DBH_MT);
    lua_pushfstring(L, LMDBX_DBH_MT ": %p", dbh);
    return 1;
}

int lmdbx_dbh_open_lua(lua_State *L)
{
    lmdbx_dbi_t *dbi = lauxh_checkudata(L, 1, LMDBX_DBI_MT);
    lmdbx_txn_t *txn = lauxh_checkudata(L, 2, LMDBX_TXN_MT);
    lmdbx_dbh_t *dbh = lua_newuserdata(L, sizeof(lmdbx_dbh_t));

    lauxh_setmetatable(L, LMDBX_DBH_MT);
    dbh->dbi_ref = lauxh_refat(L, 1);
    dbh->txn_ref = lauxh_refat(L, 2);
    dbh->dbi     = dbi;
    dbh->txn     = txn;

    return 1;
}

void lmdbx_dbh_init(lua_State *L, int errno_ref)
{
    struct luaL_Reg mmethod[] = {
        {"__tostring", tostring_lua},
        {"__gc",       gc_lua      },
        {NULL,         NULL        }
    };
    struct luaL_Reg method[] = {
        {"dbi",                dbi_lua               },
        {"txn",                txn_lua               },
        {"drop",               drop_lua              },
        {"close",              close_lua             },
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
        {"cursor_open",        cursor_open_lua       },
        {"estimate_range",     estimate_range_lua    },
        {"sequence",           sequence_lua          },
        {NULL,                 NULL                  }
    };

    // create metatable
    luaL_newmetatable(L, LMDBX_DBH_MT);
    // metamethods
    lmdbx_register(L, mmethod, errno_ref);
    // methods
    lua_pushstring(L, "__index");
    lua_newtable(L);
    lmdbx_register(L, method, errno_ref);
    lua_rawset(L, -3);
    lua_pop(L, 1);
}
