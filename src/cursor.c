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
    MDBX_cursor_op move_op   = lauxh_checkinteger(L, 2);
    size_t klen              = 0;
    const char *key          = lauxh_optlstring(L, 3, NULL, &klen);
    size_t vlen              = 0;
    const char *val          = lauxh_optlstring(L, 4, NULL, &vlen);
    MDBX_val k               = {.iov_base = (void *)key, .iov_len = klen};
    MDBX_val v               = {.iov_base = (void *)val, .iov_len = vlen};
    ptrdiff_t distance_items = 0;
    int rc = mdbx_estimate_move(cur->cur, &k, &v, move_op, &distance_items);

    if (rc) {
        lua_pushnil(L);
        lmdbx_pusherror(L, rc);
        return 3;
    }
    lua_pushinteger(L, distance_items);
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
        if (rc == MDBX_NOTFOUND) {
            return 1;
        }
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
        if (rc == MDBX_NOTFOUND) {
            return 1;
        }
        lmdbx_pusherror(L, rc);
        return 3;
    }
    lua_pushboolean(L, 1);
    return 1;
}

static int get_batch_lua(lua_State *L)
{
    lmdbx_cursor_t *cur = lauxh_checkudata(L, 1, LMDBX_CURSOR_MT);
    lua_Integer npair   = lauxh_optuint16(L, 2, 0xFF);
    lua_Integer op      = lauxh_optinteger(L, 3, MDBX_FIRST);
    size_t limit        = npair * 2;
    size_t count        = 0;
    MDBX_val *pairs     = lua_newuserdata(L, sizeof(MDBX_val) * limit);
    int rc = mdbx_cursor_get_batch(cur->cur, &count, pairs, limit, op);

    if (rc) {
        if (rc == MDBX_NOTFOUND) {
            return 0;
        }
        lua_pushnil(L);
        lmdbx_pusherror(L, rc);
        return 3;
    }
    lua_createtable(L, 0, count / 2);
    for (size_t i = 0; i < count; i += 2) {
        lua_pushlstring(L, pairs[i].iov_base, pairs[i].iov_len);
        lua_pushlstring(L, pairs[i + 1].iov_base, pairs[i + 1].iov_len);
        lua_rawset(L, -3);
    }
    return 1;
}

static int get_lua(lua_State *L)
{
    lmdbx_cursor_t *cur = lauxh_checkudata(L, 1, LMDBX_CURSOR_MT);
    lua_Integer op      = lauxh_optinteger(L, 2, MDBX_GET_CURRENT);
    MDBX_val k          = {0};
    MDBX_val v          = {0};
    int rc              = 0;

    k.iov_base = (void *)lauxh_optlstring(L, 3, NULL, &k.iov_len);
    v.iov_base = (void *)lauxh_optlstring(L, 4, NULL, &v.iov_len);
    rc         = mdbx_cursor_get(cur->cur, &k, &v, op);
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

static inline int cursor_get_with_noarg_lua(lua_State *L, MDBX_cursor_op op)
{
    lmdbx_cursor_t *cur = lauxh_checkudata(L, 1, LMDBX_CURSOR_MT);
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

static int get_prev_nodup_lua(lua_State *L)
{
    return cursor_get_with_noarg_lua(L, MDBX_PREV_NODUP);
}

static int get_prev_dup_lua(lua_State *L)
{
    return cursor_get_with_noarg_lua(L, MDBX_PREV_DUP);
}

static int get_prev_lua(lua_State *L)
{
    return cursor_get_with_noarg_lua(L, MDBX_PREV);
}

static int get_next_nodup_lua(lua_State *L)
{
    return cursor_get_with_noarg_lua(L, MDBX_NEXT_NODUP);
}

static int get_next_dup_lua(lua_State *L)
{
    return cursor_get_with_noarg_lua(L, MDBX_NEXT_DUP);
}

static int get_next_lua(lua_State *L)
{
    return cursor_get_with_noarg_lua(L, MDBX_NEXT);
}

static int get_last_dup_lua(lua_State *L)
{
    return cursor_get_with_noarg_lua(L, MDBX_LAST_DUP);
}

static int get_last_lua(lua_State *L)
{
    return cursor_get_with_noarg_lua(L, MDBX_LAST);
}

static int get_first_dup_lua(lua_State *L)
{
    return cursor_get_with_noarg_lua(L, MDBX_FIRST_DUP);
}

static int get_first_lua(lua_State *L)
{
    return cursor_get_with_noarg_lua(L, MDBX_FIRST);
}

static inline int cursor_get_with_key_and_optvalue(lua_State *L, MDBX_val *k,
                                                   MDBX_val *v,
                                                   MDBX_cursor_op op)
{
    lmdbx_cursor_t *cur = lauxh_checkudata(L, 1, LMDBX_CURSOR_MT);
    k->iov_base         = (void *)lauxh_checklstring(L, 2, &k->iov_len);
    v->iov_base         = (void *)lauxh_optlstring(L, 3, NULL, &v->iov_len);
    return mdbx_cursor_get(cur->cur, k, v, op);
}

static int get_both_range_lua(lua_State *L)
{
    MDBX_val k = {0};
    MDBX_val v = {0};
    int rc = cursor_get_with_key_and_optvalue(L, &k, &v, MDBX_GET_BOTH_RANGE);

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

static int get_both_lua(lua_State *L)
{
    lmdbx_cursor_t *cur = lauxh_checkudata(L, 1, LMDBX_CURSOR_MT);
    MDBX_val k          = {0};
    MDBX_val v          = {0};
    int rc              = 0;

    k.iov_base = (void *)lauxh_checklstring(L, 2, &k.iov_len);
    v.iov_base = (void *)lauxh_checklstring(L, 3, &v.iov_len);
    rc         = mdbx_cursor_get(cur->cur, &k, &v, MDBX_GET_BOTH);
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

static int set_upperbound_lua(lua_State *L)
{
    MDBX_val k = {0};
    MDBX_val v = {0};
    int rc = cursor_get_with_key_and_optvalue(L, &k, &v, MDBX_SET_UPPERBOUND);

    if (rc) {
        switch (rc) {
        case MDBX_SUCCESS:
            break;

        case MDBX_NOTFOUND:
            return 0;

        default:
            lua_pushnil(L);
            lua_pushnil(L);
            lmdbx_pusherror(L, rc);
            return 4;
        }
    }

    lua_pushlstring(L, k.iov_base, k.iov_len);
    lua_pushlstring(L, v.iov_base, v.iov_len);
    return 2;
}

static int set_lowerbound_lua(lua_State *L)
{
    MDBX_val k = {0};
    MDBX_val v = {0};
    int rc = cursor_get_with_key_and_optvalue(L, &k, &v, MDBX_SET_LOWERBOUND);

    if (rc) {
        switch (rc) {
        case MDBX_SUCCESS:
        case MDBX_RESULT_TRUE:
            break;

        case MDBX_NOTFOUND:
            return 0;

        default:
            lua_pushnil(L);
            lua_pushnil(L);
            lmdbx_pusherror(L, rc);
            return 4;
        }
    }

    lua_pushlstring(L, k.iov_base, k.iov_len);
    lua_pushlstring(L, v.iov_base, v.iov_len);
    return 2;
}

static inline int cursor_get_with_key(lua_State *L, MDBX_val *k, MDBX_val *v,
                                      MDBX_cursor_op op)
{
    lmdbx_cursor_t *cur = lauxh_checkudata(L, 1, LMDBX_CURSOR_MT);
    k->iov_base         = (void *)lauxh_checklstring(L, 2, &k->iov_len);
    return mdbx_cursor_get(cur->cur, k, v, op);
}

static int set_range_lua(lua_State *L)
{
    MDBX_val k = {0};
    MDBX_val v = {0};
    int rc     = cursor_get_with_key(L, &k, &v, MDBX_SET_RANGE);

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

static int set_lua(lua_State *L)
{
    MDBX_val k = {0};
    MDBX_val v = {0};
    int rc     = cursor_get_with_key(L, &k, &v, MDBX_SET);

    if (rc) {
        if (rc == MDBX_NOTFOUND) {
            return 0;
        }
        lua_pushnil(L);
        lmdbx_pusherror(L, rc);
        return 3;
    }
    lua_pushlstring(L, v.iov_base, v.iov_len);
    return 1;
}

static int copy_lua(lua_State *L)
{
    lmdbx_cursor_t *cur = lauxh_checkudata(L, 1, LMDBX_CURSOR_MT);
    lmdbx_cursor_t *dst = lua_newuserdata(L, sizeof(lmdbx_cursor_t));
    int rc              = 0;

    dst->dbi_ref = LUA_NOREF;
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
    lauxh_pushref(L, cur->dbi_ref);
    dst->dbi_ref = lauxh_ref(L);

    return 1;
}

static int renew_lua(lua_State *L)
{
    lmdbx_cursor_t *cur = lauxh_checkudata(L, 1, LMDBX_CURSOR_MT);
    lmdbx_dbi_t *dbi    = lauxh_checkudata(L, 2, LMDBX_DBI_MT);
    int rc              = mdbx_cursor_renew(dbi->txn->txn, cur->cur);

    if (rc) {
        lua_pushboolean(L, 0);
        lmdbx_pusherror(L, rc);
        return 3;
    }
    lauxh_unref(L, cur->dbi_ref);
    cur->dbi_ref = lauxh_refat(L, 2);
    lua_pushboolean(L, 1);

    return 1;
}

static int close_lua(lua_State *L)
{
    lmdbx_cursor_t *cur = lauxh_checkudata(L, 1, LMDBX_CURSOR_MT);

    if (cur->cur) {
        mdbx_cursor_close(cur->cur);
        cur->cur     = NULL;
        cur->dbi_ref = lauxh_unref(L, cur->dbi_ref);
    }
    return 0;
}

static int dbi_lua(lua_State *L)
{
    lmdbx_cursor_t *cur = lauxh_checkudata(L, 1, LMDBX_CURSOR_MT);
    lauxh_pushref(L, cur->dbi_ref);
    return 1;
}

static int gc_lua(lua_State *L)
{
    lmdbx_cursor_t *cur = lauxh_checkudata(L, 1, LMDBX_CURSOR_MT);

    if (cur->cur) {
        mdbx_cursor_close(cur->cur);
        cur->cur     = NULL;
        cur->dbi_ref = lauxh_unref(L, cur->dbi_ref);
    }
    return 0;
}

static int tostring_lua(lua_State *L)
{
    lmdbx_cursor_t *cur = lauxh_checkudata(L, 1, LMDBX_CURSOR_MT);
    lua_pushfstring(L, LMDBX_CURSOR_MT ": %p", cur);
    return 1;
}

int lmdbx_cursor_open_lua(lua_State *L)
{
    lmdbx_dbi_t *dbi    = lauxh_checkudata(L, 1, LMDBX_DBI_MT);
    lmdbx_cursor_t *cur = lua_newuserdata(L, sizeof(lmdbx_cursor_t));
    int rc              = mdbx_cursor_open(dbi->txn->txn, dbi->dbi, &cur->cur);

    if (rc) {
        lua_pushnil(L);
        lmdbx_pusherror(L, rc);
        return 3;
    }
    lauxh_setmetatable(L, LMDBX_CURSOR_MT);
    cur->dbi_ref = lauxh_refat(L, 1);

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
        {"dbi",               dbi_lua              },
        {"close",             close_lua            },
        {"renew",             renew_lua            },
        {"copy",              copy_lua             },
        {"set",               set_lua              }, // helper func
        {"set_range",         set_range_lua        }, // helper func
        {"set_lowerbound",    set_lowerbound_lua   }, // helper func
        {"set_upperbound",    set_upperbound_lua   }, // helper func
        {"get_both",          get_both_lua         }, // helper func
        {"get_both_range",    get_both_range_lua   }, // helper func
        {"get_first",         get_first_lua        }, // helper func
        {"get_first_dup",     get_first_dup_lua    }, // helper func
        {"get_last",          get_last_lua         }, // helper func
        {"get_last_dup",      get_last_dup_lua     }, // helper func
        {"get_next",          get_next_lua         }, // helper func
        {"get_next_dup",      get_next_dup_lua     }, // helper func
        {"get_next_nodup",    get_next_nodup_lua   }, // helper func
        {"get_prev",          get_prev_lua         }, // helper func
        {"get_prev_dup",      get_prev_dup_lua     }, // helper func
        {"get_prev_nodup",    get_prev_nodup_lua   }, // helper func
        {"get",               get_lua              },
        {"get_batch",         get_batch_lua        },
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
