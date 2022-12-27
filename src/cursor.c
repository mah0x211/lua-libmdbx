/**
 * Copyright (C) 2022 Masatoshi Fukunaga
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

// TODO: mdbx_cursor_get_attr
// TODO: mdbx_cursor_put_attr

static int estimate_move_lua(lua_State *L)
{
    lmdbx_cursor_t *cur      = lauxh_checkudata(L, 1, LMDBX_CURSOR_MT);
    size_t klen              = 0;
    const char *key          = lauxh_checklstring(L, 2, &klen);
    size_t vlen              = 0;
    const char *val          = lauxh_checklstring(L, 3, &vlen);
    MDBX_cursor_op move_op   = lauxh_checkinteger(L, 4);
    MDBX_val k               = {.iov_base = (void *)key, .iov_len = klen};
    MDBX_val v               = {.iov_base = (void *)val, .iov_len = vlen};
    ptrdiff_t distance_items = 0;
    int rc = mdbx_estimate_move(cur->cur, &k, &v, move_op, &distance_items);

    if (rc) {
        lua_pushnil(L);
        lmdbx_pusherror(L, rc);
        return 3;
    }
    lua_createtable(L, 0, 3);
    lauxh_pushstr2tbl(L, "key", k.iov_base);
    lauxh_pushstr2tbl(L, "data", v.iov_base);
    lauxh_pushint2tbl(L, "distance", distance_items);
    return 1;
}

static int estimate_distance_lua(lua_State *L)
{
    lmdbx_cursor_t *cur      = lauxh_checkudata(L, 1, LMDBX_CURSOR_MT);
    lmdbx_cursor_t *last     = lauxh_checkudata(L, 2, LMDBX_CURSOR_MT);
    ptrdiff_t distance_items = 0;
    int rc = mdbx_estimate_distance(cur->cur, last->cur, &distance_items);

    if (rc) {
        lua_pushnil(L);
        lmdbx_pusherror(L, rc);
        return 3;
    }
    lua_pushinteger(L, distance_items);
    return 1;
}

static int on_last_lua(lua_State *L)
{
    lmdbx_cursor_t *cur = lauxh_checkudata(L, 1, LMDBX_CURSOR_MT);
    int rc              = mdbx_cursor_on_last(cur->cur);

    switch (rc) {
    case MDBX_RESULT_TRUE:
        lua_pushboolean(L, 1);
        return 1;

    case MDBX_RESULT_FALSE:
        lua_pushboolean(L, 0);
        return 1;

    default:
        lua_pushnil(L);
        lmdbx_pusherror(L, rc);
        return 3;
    }
}

static int on_first_lua(lua_State *L)
{
    lmdbx_cursor_t *cur = lauxh_checkudata(L, 1, LMDBX_CURSOR_MT);
    int rc              = mdbx_cursor_on_first(cur->cur);

    switch (rc) {
    case MDBX_RESULT_TRUE:
        lua_pushboolean(L, 1);
        return 1;

    case MDBX_RESULT_FALSE:
        lua_pushboolean(L, 0);
        return 1;

    default:
        lua_pushnil(L);
        lmdbx_pusherror(L, rc);
        return 3;
    }
}

static int eof_lua(lua_State *L)
{
    lmdbx_cursor_t *cur = lauxh_checkudata(L, 1, LMDBX_CURSOR_MT);
    int rc              = mdbx_cursor_eof(cur->cur);

    switch (rc) {
    case MDBX_RESULT_TRUE:
        lua_pushboolean(L, 1);
        return 1;

    case MDBX_RESULT_FALSE:
        lua_pushboolean(L, 0);
        return 1;

    default:
        lua_pushnil(L);
        lmdbx_pusherror(L, rc);
        return 3;
    }
}

static int count_lua(lua_State *L)
{
    lmdbx_cursor_t *cur = lauxh_checkudata(L, 1, LMDBX_CURSOR_MT);
    size_t count        = 0;
    int rc              = mdbx_cursor_count(cur->cur, &count);

    if (rc) {
        lua_pushnil(L);
        lmdbx_pusherror(L, rc);
        return 3;
    }
    lua_pushinteger(L, count);
    return 1;
}

static int del_lua(lua_State *L)
{
    lmdbx_cursor_t *cur = lauxh_checkudata(L, 1, LMDBX_CURSOR_MT);
    lua_Integer flags   = lmdbx_checkflags(L, 2);
    int rc              = mdbx_cursor_del(cur->cur, flags);

    if (rc) {
        lua_pushboolean(L, 0);
        lmdbx_pusherror(L, rc);
        return 3;
    }
    lua_pushboolean(L, 1);
    return 1;
}

static int put_lua(lua_State *L)
{
    lmdbx_cursor_t *cur = lauxh_checkudata(L, 1, LMDBX_CURSOR_MT);
    size_t klen         = 0;
    const char *key     = lauxh_checklstring(L, 2, &klen);
    size_t vlen         = 0;
    const char *val     = lauxh_checklstring(L, 3, &vlen);
    lua_Integer flags   = lmdbx_checkflags(L, 4);
    MDBX_val k          = {.iov_base = (void *)key, .iov_len = klen};
    MDBX_val v          = {.iov_base = (void *)val, .iov_len = vlen};
    int rc              = mdbx_cursor_put(cur->cur, &k, &v, flags);

    if (rc) {
        lua_pushboolean(L, 0);
        lmdbx_pusherror(L, rc);
        return 3;
    }
    lua_pushboolean(L, 1);
    return 1;
}

// helper methods
static int op_delete_lua(lua_State *L)
{
    int multi = lauxh_optboolean(L, 2, 0);

    if (multi) {
        // deletion all duplicates of key at the current position
        lua_pushinteger(L, MDBX_ALLDUPS);
    } else {
        // deletion only the current entry
        lua_pushinteger(L, MDBX_CURRENT);
    }
    lua_insert(L, 2);
    return del_lua(L);
}

static int op_update_lua(lua_State *L)
{
    if (lua_gettop(L) < 3) {
        lua_settop(L, 3);
    }
    lua_pushinteger(L, MDBX_CURRENT);
    lua_insert(L, 4);
    return put_lua(L);
}

static int get_batch_lua(lua_State *L)
{
    lmdbx_cursor_t *cur = lauxh_checkudata(L, 1, LMDBX_CURSOR_MT);
    lua_Integer limit   = lauxh_optuint16(L, 2, 0x7F);
    lua_Integer op      = lauxh_optinteger(L, 3, MDBX_FIRST);
    size_t count        = 0;
    MDBX_val *pairs     = lua_newuserdata(L, sizeof(MDBX_val) * limit);
    int rc = mdbx_cursor_get_batch(cur->cur, &count, pairs, limit, op);

    if (rc) {
        lua_pushnil(L);
        if (rc == MDBX_NOTFOUND) {
            return 1;
        }
        lmdbx_pusherror(L, rc);
        return 3;
    }
    lua_createtable(L, count, 0);
    for (size_t i = 0; i < count; i++) {
        lua_pushlstring(L, pairs[i].iov_base, pairs[i].iov_len);
        lua_rawseti(L, -2, i + 1);
    }
    return 1;
}

static int get_lua(lua_State *L)
{
    lmdbx_cursor_t *cur = lauxh_checkudata(L, 1, LMDBX_CURSOR_MT);
    lua_Integer op      = lauxh_optinteger(L, 2, MDBX_FIRST);
    MDBX_val k          = {0};
    MDBX_val v          = {0};
    int rc              = mdbx_cursor_get(cur->cur, &k, &v, op);

    if (rc) {
        if (rc == MDBX_NOTFOUND) {
            return 0;
        }
        lua_pushnil(L);
        lua_pushnil(L);
        lmdbx_pusherror(L, rc);
        return 4;
    }
    lua_pushlstring(L, k.iov_base, k.iov_len);
    lua_pushlstring(L, v.iov_base, v.iov_len);
    return 2;
}

static int copy_lua(lua_State *L)
{
    lmdbx_cursor_t *cur = lauxh_checkudata(L, 1, LMDBX_CURSOR_MT);
    lmdbx_cursor_t *dst = lua_newuserdata(L, sizeof(lmdbx_cursor_t));
    int rc              = 0;

    dst->txn_ref = LUA_NOREF;
    dst->cur     = mdbx_cursor_create(NULL);
    if (!dst->cur) {
        lua_pushnil(L);
        lmdbx_pusherror(L, MDBX_ENOMEM);
        return 3;
    } else if ((rc = mdbx_cursor_copy(cur->cur, dst->cur))) {
        lua_pushnil(L);
        lmdbx_pusherror(L, rc);
        return 3;
    }
    lauxh_setmetatable(L, LMDBX_CURSOR_MT);
    lauxh_pushref(L, cur->txn_ref);
    dst->txn_ref = lauxh_ref(L);

    return 1;
}

static int dbi_lua(lua_State *L)
{
    lmdbx_cursor_t *cur = lauxh_checkudata(L, 1, LMDBX_CURSOR_MT);
    MDBX_dbi dbi        = mdbx_cursor_dbi(cur->cur);

    lua_pushinteger(L, dbi);
    return 1;
}

static int txn_lua(lua_State *L)
{
    lmdbx_cursor_t *cur = lauxh_checkudata(L, 1, LMDBX_CURSOR_MT);

    lauxh_pushref(L, cur->txn_ref);
    return 1;
}

static int renew_lua(lua_State *L)
{
    lmdbx_cursor_t *cur = lauxh_checkudata(L, 1, LMDBX_CURSOR_MT);
    lmdbx_txn_t *txn    = lauxh_checkudata(L, 2, LMDBX_TXN_MT);
    int rc              = mdbx_cursor_renew(txn->txn, cur->cur);

    if (rc) {
        lua_pushboolean(L, 0);
        lmdbx_pusherror(L, rc);
        return 3;
    }
    lauxh_unref(L, cur->txn_ref);
    cur->txn_ref = lauxh_refat(L, 2);
    lua_pushboolean(L, 1);

    return 1;
}

static int close_lua(lua_State *L)
{
    lmdbx_cursor_t *cur = lauxh_checkudata(L, 1, LMDBX_CURSOR_MT);

    if (cur->cur) {
        mdbx_cursor_close(cur->cur);
        cur->cur     = NULL;
        cur->txn_ref = lauxh_unref(L, cur->txn_ref);
    }
    return 0;
}

static int gc_lua(lua_State *L)
{
    lmdbx_cursor_t *cur = lauxh_checkudata(L, 1, LMDBX_CURSOR_MT);

    if (cur->cur) {
        mdbx_cursor_close(cur->cur);
        cur->cur     = NULL;
        cur->txn_ref = lauxh_unref(L, cur->txn_ref);
    }
    return 0;
}

static int tostring_lua(lua_State *L)
{
    lmdbx_cursor_t *cur = lauxh_checkudata(L, 1, LMDBX_CURSOR_MT);
    lua_pushfstring(L, LMDBX_CURSOR_MT ": %p", cur);
    return 1;
}

void lmdbx_cursor_init(lua_State *L)
{
    struct luaL_Reg mmethod[] = {
        {"__tostring", tostring_lua},
        {"__gc",       gc_lua      },
        {NULL,         NULL        }
    };
    struct luaL_Reg method[] = {
        {"close",             close_lua            },
        {"renew",             renew_lua            },
        {"txn",               txn_lua              },
        {"dbi",               dbi_lua              },
        {"copy",              copy_lua             },
        {"get",               get_lua              },
        {"get_batch",         get_batch_lua        },
        {"op_delete",         op_delete_lua        },
        {"op_update",         op_update_lua        },
        {"put",               put_lua              },
        {"del",               del_lua              },
        {"count",             count_lua            },
        {"eof",               eof_lua              },
        {"on_first",          on_first_lua         },
        {"on_last",           on_last_lua          },
        {"estimate_distance", estimate_distance_lua},
        {"estimate_move",     estimate_move_lua    },
        {NULL,                NULL                 }
    };

    // create metatable
    luaL_newmetatable(L, LMDBX_CURSOR_MT);
    // metamethods
    lmdbx_register(L, mmethod);
    // methods
    lua_pushstring(L, "__index");
    lua_newtable(L);
    lmdbx_register(L, method);
    lua_rawset(L, -3);
    lua_pop(L, 1);
}
