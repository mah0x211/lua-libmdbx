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

static int dbh_open_lua(lua_State *L)
{
    return lmdbx_dbh_open_lua(L);
}

static int close_lua(lua_State *L)
{
    lmdbx_dbi_t *dbi = lauxh_checkudata(L, 1, LMDBX_DBI_MT);

    lauxh_pushref(L, dbi->env_ref);
    if (!lua_isnoneornil(L, -1)) {
        lmdbx_env_t *env = lauxh_checkudata(L, -1, LMDBX_ENV_MT);
        int rc           = mdbx_dbi_close(env->env, dbi->dbi);

        if (rc) {
            lua_pushboolean(L, 0);
            lmdbx_pusherror(L, rc);
            return 2;
        }

        // push index table
        lauxh_pushref(L, env->dbis_ref);
        // remove dbi from index table
        lua_pushnil(L);
        lua_rawseti(L, -2, dbi->dbi);
        dbi->env_ref = lauxh_unref(L, dbi->env_ref);
    }
    lua_pushboolean(L, 1);
    return 1;
}

static int env_lua(lua_State *L)
{
    lmdbx_dbi_t *dbi = lauxh_checkudata(L, 1, LMDBX_DBI_MT);
    lauxh_pushref(L, dbi->env_ref);
    return 1;
}

static int dbi_lua(lua_State *L)
{
    lmdbx_dbi_t *dbi = lauxh_checkudata(L, 1, LMDBX_DBI_MT);
    lua_pushinteger(L, dbi->dbi);
    return 1;
}

static int gc_lua(lua_State *L)
{
    lmdbx_dbi_t *dbi = lauxh_checkudata(L, 1, LMDBX_DBI_MT);
    lauxh_unref(L, dbi->env_ref);
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
    lmdbx_env_t *env  = NULL;
    lmdbx_dbi_t *dbi  = NULL;
    int rc            = 0;

    // push index table
    lua_settop(L, 2);
    lauxh_pushref(L, txn->env_ref);
    env = lauxh_checkudata(L, -1, LMDBX_ENV_MT);
    lauxh_pushref(L, env->dbis_ref);

    dbi = lua_newuserdata(L, sizeof(lmdbx_dbi_t));
    rc  = mdbx_dbi_open(txn->txn, name, flags, &dbi->dbi);
    if (rc) {
        if (rc == MDBX_NOTFOUND) {
            return 0;
        }
        lua_pushnil(L);
        lmdbx_pusherror(L, rc);
        return 2;
    }

    // get the dbi with same id
    lua_rawgeti(L, 4, dbi->dbi);
    if (lua_isnoneornil(L, -1)) {
        lua_pop(L, 1);
        // create new dbi object
        lauxh_setmetatable(L, LMDBX_DBI_MT);
        dbi->env_ref = lauxh_refat(L, 3);
        // add dbi to index table
        lua_pushvalue(L, -1);
        lua_rawseti(L, 4, dbi->dbi);
    }

    return 1;
}

void lmdbx_dbi_init(lua_State *L, int errno_ref)
{
    struct luaL_Reg mmethod[] = {
        {"__tostring", tostring_lua},
        {"__gc",       gc_lua      },
        {NULL,         NULL        }
    };
    struct luaL_Reg method[] = {
        {"dbi",      dbi_lua     },
        {"env",      env_lua     },
        {"close",    close_lua   },
        {"dbh_open", dbh_open_lua},
        {NULL,       NULL        }
    };

    // create metatable
    luaL_newmetatable(L, LMDBX_DBI_MT);
    // metamethods
    lmdbx_register(L, mmethod, errno_ref);
    // methods
    lua_pushstring(L, "__index");
    lua_newtable(L);
    lmdbx_register(L, method, errno_ref);
    lua_rawset(L, -3);
    lua_pop(L, 1);
}
