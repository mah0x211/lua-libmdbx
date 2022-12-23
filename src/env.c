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

// NOTE: not export the mdbx_env_open_for_recovery()
// TODO: mdbx_env_pgwalk
// TODO: mdbx_env_get_hsr
// TODO: mdbx_env_set_hsr

static int thread_unregister_lua(lua_State *L)
{
    lmdbx_env_t *env = lauxh_checkudata(L, 1, LMDBX_ENV_MT);
    int rc           = mdbx_thread_unregister(env->env);

    switch (rc) {
    case 0:
    case MDBX_RESULT_TRUE:
        lua_pushboolean(L, 1);

    default:
        lua_pushboolean(L, 0);
        lmdbx_pusherror(L, rc);
        return 3;
    }
}

static int thread_register_lua(lua_State *L)
{
    lmdbx_env_t *env = lauxh_checkudata(L, 1, LMDBX_ENV_MT);
    int rc           = mdbx_thread_register(env->env);

    switch (rc) {
    case 0:
    case MDBX_RESULT_TRUE:
        lua_pushboolean(L, 1);

    default:
        lua_pushboolean(L, 0);
        lmdbx_pusherror(L, rc);
        return 3;
    }
}

static int reader_check_lua(lua_State *L)
{
    lmdbx_env_t *env = lauxh_checkudata(L, 1, LMDBX_ENV_MT);
    int dead         = 0;
    int rc           = mdbx_reader_check(env->env, &dead);

    switch (rc) {
    case 0:
    case MDBX_RESULT_TRUE:
        lua_pushinteger(L, dead);

    default:
        lua_pushnil(L);
        lmdbx_pusherror(L, rc);
        return 3;
    }
}

static int reader_list_func(void *ctx, int num, int slot, mdbx_pid_t pid,
                            mdbx_tid_t thread, uint64_t txnid, uint64_t lag,
                            size_t bytes_used, size_t bytes_retained)
{
    (void)thread;

    lua_State *L = (lua_State *)ctx;
    int top      = lua_gettop(L);

    lua_pushvalue(L, 2);
    lua_pushinteger(L, num);
    lua_pushinteger(L, slot);
    lua_pushinteger(L, pid);
    lua_pushinteger(L, txnid);
    lua_pushinteger(L, lag);
    lua_pushinteger(L, bytes_used);
    lua_pushinteger(L, bytes_retained);
    if (lua_pcall(L, 0, lua_gettop(L) - top - 1, (top == 3) ? 3 : 0)) {
        return -1;
    }
    lua_settop(L, top);
    return 0;
}

static int reader_list_lua(lua_State *L)
{
    lmdbx_env_t *env = lauxh_checkudata(L, 1, LMDBX_ENV_MT);
    int rc           = 0;

    luaL_checktype(L, 2, LUA_TFUNCTION);
    lua_settop(L, 2);
    // get debug.traceback function
    lauxh_getglobal(L, "debug");
    if (lua_type(L, -1) == LUA_TTABLE) {
        lua_pushliteral(L, "traceback");
        lua_rawget(L, -2);
    }
    if (lua_type(L, -1) != LUA_TFUNCTION) {
        lua_pop(L, 1);
    }

    rc = mdbx_reader_list(env->env, reader_list_func, (void *)L);
    // callback function throws an error
    if (lua_gettop(L) > 3) {
        return lua_error(L);
    }

    switch (rc) {
    case 0:
    case MDBX_RESULT_TRUE:
        lua_pushboolean(L, 1);

    default:
        lua_pushboolean(L, 0);
        lmdbx_pusherror(L, rc);
        return 3;
    }
}

static int begin_lua(lua_State *L)
{
    lmdbx_env_t *env  = lauxh_checkudata(L, 1, LMDBX_ENV_MT);
    lua_Integer flags = lmdbx_checkflags(L, 2);
    lmdbx_txn_t *txn  = lua_newuserdata(L, sizeof(lmdbx_txn_t));
    int rc            = 0;

    txn->env_ref = LUA_NOREF;
    txn->dbi     = 0;
    txn->txn     = NULL;
    rc           = mdbx_txn_begin(env->env, NULL, flags, &txn->txn);
    if (rc) {
        lua_pushnil(L);
        lmdbx_pusherror(L, rc);
        return 3;
    }
    lauxh_setmetatable(L, LMDBX_TXN_MT);
    txn->env_ref = lauxh_refat(L, 1);

    return 1;
}

static int get_maxvalsize_lua(lua_State *L)
{
    lmdbx_env_t *env  = lauxh_checkudata(L, 1, LMDBX_ENV_MT);
    lua_Integer flags = lmdbx_checkflags(L, 2);
    int size          = mdbx_env_get_maxvalsize_ex(env->env, flags);

    lua_pushinteger(L, size);
    return 1;
}

static int get_maxkeysize_lua(lua_State *L)
{
    lmdbx_env_t *env  = lauxh_checkudata(L, 1, LMDBX_ENV_MT);
    lua_Integer flags = lmdbx_checkflags(L, 2);
    int size          = mdbx_env_get_maxkeysize_ex(env->env, flags);

    lua_pushinteger(L, size);
    return 1;
}

static int get_maxdbs_lua(lua_State *L)
{
    lmdbx_env_t *env = lauxh_checkudata(L, 1, LMDBX_ENV_MT);
    MDBX_dbi dbs     = 0;
    int rc           = mdbx_env_get_maxdbs(env->env, &dbs);

    if (rc) {
        lua_pushnil(L);
        lmdbx_pusherror(L, rc);
        return 3;
    }
    lua_pushinteger(L, dbs);
    return 1;
}

static int set_maxdbs_lua(lua_State *L)
{
    lmdbx_env_t *env = lauxh_checkudata(L, 1, LMDBX_ENV_MT);
    MDBX_dbi dbs     = lauxh_checkuint64(L, 2);
    int rc           = mdbx_env_set_maxdbs(env->env, dbs);

    if (rc) {
        lua_pushboolean(L, 0);
        lmdbx_pusherror(L, rc);
        return 3;
    }
    lua_pushboolean(L, 1);
    return 1;
}

static int get_maxreaders_lua(lua_State *L)
{
    lmdbx_env_t *env = lauxh_checkudata(L, 1, LMDBX_ENV_MT);
    unsigned readers = 0;
    int rc           = mdbx_env_get_maxreaders(env->env, &readers);

    if (rc) {
        lua_pushnil(L);
        lmdbx_pusherror(L, rc);
        return 3;
    }
    lua_pushinteger(L, readers);
    return 1;
}

static int set_maxreaders_lua(lua_State *L)
{
    lmdbx_env_t *env = lauxh_checkudata(L, 1, LMDBX_ENV_MT);
    unsigned readers = lauxh_checkuint64(L, 2);
    int rc           = mdbx_env_set_maxreaders(env->env, readers);

    if (rc) {
        lua_pushboolean(L, 0);
        lmdbx_pusherror(L, rc);
        return 3;
    }
    lua_pushboolean(L, 1);
    return 1;
}

static int set_geometry_lua(lua_State *L)
{
    lmdbx_env_t *env             = lauxh_checkudata(L, 1, LMDBX_ENV_MT);
    lua_Integer size_lower       = lauxh_optinteger(L, 2, -1);
    lua_Integer size_now         = lauxh_optinteger(L, 3, -1);
    lua_Integer size_upper       = lauxh_optinteger(L, 4, -1);
    lua_Integer growth_step      = lauxh_optinteger(L, 5, -1);
    lua_Integer shrink_threshold = lauxh_optinteger(L, 6, -1);
    lua_Integer pagesize         = lauxh_optinteger(L, 7, -1);
    int rc = mdbx_env_set_geometry(env->env, size_lower, size_now, size_upper,
                                   growth_step, shrink_threshold, pagesize);

    if (rc) {
        lua_pushboolean(L, 0);
        lmdbx_pusherror(L, rc);
        return 3;
    }
    lua_pushboolean(L, 1);
    return 1;
}

static int get_fd_lua(lua_State *L)
{
    lmdbx_env_t *env     = lauxh_checkudata(L, 1, LMDBX_ENV_MT);
    mdbx_filehandle_t fd = 0;
    int rc               = mdbx_env_get_fd(env->env, &fd);

    if (rc) {
        lua_pushnil(L);
        lmdbx_pusherror(L, rc);
        return 3;
    }
    lua_pushinteger(L, fd);
    return 1;
}

static int get_path_lua(lua_State *L)
{
    lmdbx_env_t *env = lauxh_checkudata(L, 1, LMDBX_ENV_MT);
    const char *dest = NULL;
    int rc           = mdbx_env_get_path(env->env, &dest);

    if (rc) {
        lua_pushnil(L);
        lmdbx_pusherror(L, rc);
        return 3;
    }
    lua_pushstring(L, dest);
    return 1;
}

static int get_flags_lua(lua_State *L)
{
    lmdbx_env_t *env = lauxh_checkudata(L, 1, LMDBX_ENV_MT);
    unsigned flags   = 0;
    int rc           = mdbx_env_get_flags(env->env, &flags);

    if (rc) {
        lua_pushnil(L);
        lmdbx_pusherror(L, rc);
        return 3;
    }

    lua_newtable(L);
    if (flags & MDBX_NOSUBDIR) {
        lauxh_pushint2tbl(L, "NOSUBDIR", MDBX_NOSUBDIR);
    }
    if (flags & MDBX_RDONLY) {
        lauxh_pushint2tbl(L, "RDONLY", MDBX_RDONLY);
    }
    if (flags & MDBX_EXCLUSIVE) {
        lauxh_pushint2tbl(L, "EXCLUSIVE", MDBX_EXCLUSIVE);
    }
    if (flags & MDBX_ACCEDE) {
        lauxh_pushint2tbl(L, "ACCEDE", MDBX_ACCEDE);
    }
    if (flags & MDBX_WRITEMAP) {
        lauxh_pushint2tbl(L, "WRITEMAP", MDBX_WRITEMAP);
    }
    if (flags & MDBX_NOTLS) {
        lauxh_pushint2tbl(L, "NOTLS", MDBX_NOTLS);
    }
    if (flags & MDBX_NORDAHEAD) {
        lauxh_pushint2tbl(L, "NORDAHEAD", MDBX_NORDAHEAD);
    }
    if (flags & MDBX_NOMEMINIT) {
        lauxh_pushint2tbl(L, "NOMEMINIT", MDBX_NOMEMINIT);
    }
    if (flags & MDBX_COALESCE) {
        lauxh_pushint2tbl(L, "COALESCE", MDBX_COALESCE);
    }
    if (flags & MDBX_LIFORECLAIM) {
        lauxh_pushint2tbl(L, "LIFORECLAIM", MDBX_LIFORECLAIM);
    }
    if (flags & MDBX_PAGEPERTURB) {
        lauxh_pushint2tbl(L, "PAGEPERTURB", MDBX_PAGEPERTURB);
    }
    if (flags & MDBX_SYNC_DURABLE) {
        lauxh_pushint2tbl(L, "SYNC_DURABLE", MDBX_SYNC_DURABLE);
    }
    if (flags & MDBX_NOMETASYNC) {
        lauxh_pushint2tbl(L, "NOMETASYNC", MDBX_NOMETASYNC);
    }
    if (flags & MDBX_SAFE_NOSYNC) {
        lauxh_pushint2tbl(L, "SAFE_NOSYNC", MDBX_SAFE_NOSYNC);
    }
    if (flags & MDBX_UTTERLY_NOSYNC) {
        lauxh_pushint2tbl(L, "UTTERLY_NOSYNC", MDBX_UTTERLY_NOSYNC);
    }

    return 1;
}

static int set_flags_lua(lua_State *L)
{
    lmdbx_env_t *env  = lauxh_checkudata(L, 1, LMDBX_ENV_MT);
    int on            = lauxh_checkboolean(L, 2);
    lua_Integer flags = lmdbx_checkflags(L, 3);
    int rc            = mdbx_env_set_flags(env->env, flags, on);

    if (rc) {
        lua_pushboolean(L, 0);
        lmdbx_pusherror(L, rc);
        return 3;
    }
    lua_pushboolean(L, 1);
    return 1;
}

static int close_lua(lua_State *L)
{
    lmdbx_env_t *env = lauxh_checkudata(L, 1, LMDBX_ENV_MT);
    int dont_sync    = lauxh_optboolean(L, 2, 0);

    if (getpid() != env->pid) {
        lua_pushboolean(L, 0);
        lua_pushstring(
            L, "cannot be closed outside the process in which it was created");
        return 2;
    }

    if (env->env) {
        int rc = mdbx_env_close_ex(env->env, dont_sync);
        if (rc == MDBX_BUSY) {
            lua_pushboolean(L, 0);
            return 1;
        }

        env->env = NULL;
        if (rc) {
            lua_pushboolean(L, 1);
            lmdbx_pusherror(L, rc);
            return 3;
        }
    }
    lua_pushboolean(L, 1);
    return 1;
}

static int get_syncperiod_lua(lua_State *L)
{
    lmdbx_env_t *env                = lauxh_checkudata(L, 1, LMDBX_ENV_MT);
    unsigned period_seconds_16dot16 = 0;
    int rc = mdbx_env_get_syncperiod(env->env, &period_seconds_16dot16);

    if (rc) {
        lua_pushnil(L);
        lmdbx_pusherror(L, rc);
        return 3;
    }
    lua_pushinteger(L, period_seconds_16dot16);
    return 1;
}

static int set_syncperiod_lua(lua_State *L)
{
    lmdbx_env_t *env         = lauxh_checkudata(L, 1, LMDBX_ENV_MT);
    unsigned seconds_16dot16 = lauxh_checkuint64(L, 2);
    int rc = mdbx_env_set_syncperiod(env->env, seconds_16dot16);

    if (rc) {
        lua_pushboolean(L, 0);
        lmdbx_pusherror(L, rc);
        return 3;
    }
    lua_pushboolean(L, 0);
    return 1;
}

static int get_syncbytes_lua(lua_State *L)
{
    lmdbx_env_t *env = lauxh_checkudata(L, 1, LMDBX_ENV_MT);
    size_t threshold = 0;
    int rc           = mdbx_env_get_syncbytes(env->env, &threshold);

    if (rc) {
        lua_pushnil(L);
        lmdbx_pusherror(L, rc);
        return 3;
    }
    lua_pushinteger(L, threshold);
    return 1;
}

static int set_syncbytes_lua(lua_State *L)
{
    lmdbx_env_t *env = lauxh_checkudata(L, 1, LMDBX_ENV_MT);
    size_t threshold = lauxh_checkuint64(L, 2);
    int rc           = mdbx_env_set_syncbytes(env->env, threshold);

    if (rc) {
        lua_pushboolean(L, 0);
        lmdbx_pusherror(L, rc);
        return 3;
    }
    lua_pushboolean(L, 0);
    return 1;
}

static int sync_poll_lua(lua_State *L)
{
    lmdbx_env_t *env = lauxh_checkudata(L, 1, LMDBX_ENV_MT);
    int rc           = mdbx_env_sync_poll(env->env);

    switch (rc) {
    case 0:
    case MDBX_RESULT_TRUE:
        lua_pushboolean(L, 0);
        return 1;

    default:
        lua_pushboolean(L, 0);
        lmdbx_pusherror(L, rc);
        return 3;
    }
}

static int sync_lua(lua_State *L)
{
    lmdbx_env_t *env = lauxh_checkudata(L, 1, LMDBX_ENV_MT);
    int rc           = mdbx_env_sync(env->env);

    switch (rc) {
    case 0:
    case MDBX_RESULT_TRUE:
        lua_pushboolean(L, 1);
        return 1;

    default:
        lua_pushboolean(L, 0);
        lmdbx_pusherror(L, rc);
        return 3;
    }
}

static int info_lua(lua_State *L)
{
    lmdbx_env_t *env  = lauxh_checkudata(L, 1, LMDBX_ENV_MT);
    MDBX_envinfo info = {0};
    int rc = mdbx_env_info_ex(env->env, NULL, &info, sizeof(MDBX_envinfo));

    if (rc) {
        lua_pushnil(L);
        lmdbx_pusherror(L, rc);
        return 3;
    }
    lmdbx_pushenvinfo(L, &info);
    return 1;
}

static int stat_lua(lua_State *L)
{
    lmdbx_env_t *env = lauxh_checkudata(L, 1, LMDBX_ENV_MT);
    MDBX_stat stat   = {0};
    int rc = mdbx_env_stat_ex(env->env, NULL, &stat, sizeof(MDBX_stat));

    if (rc) {
        lua_pushnil(L);
        lmdbx_pusherror(L, rc);
        return 3;
    }
    lmdbx_pushstat(L, &stat);
    return 1;
}

static int copy2fd_lua(lua_State *L)
{
    lmdbx_env_t *env  = lauxh_checkudata(L, 1, LMDBX_ENV_MT);
    lua_Integer fd    = lauxh_checkinteger(L, 2);
    lua_Integer flags = lmdbx_checkflags(L, 3);
    int rc            = mdbx_env_copy2fd(env->env, fd, flags);

    if (rc) {
        lua_pushboolean(L, 0);
        lmdbx_pusherror(L, rc);
        return 3;
    }
    lua_pushboolean(L, 1);
    return 1;
}

static int copy_lua(lua_State *L)
{
    lmdbx_env_t *env  = lauxh_checkudata(L, 1, LMDBX_ENV_MT);
    const char *dest  = lauxh_checkstring(L, 2);
    lua_Integer flags = lmdbx_checkflags(L, 3);
    int rc            = mdbx_env_copy(env->env, dest, flags);

    if (rc) {
        lua_pushboolean(L, 0);
        lmdbx_pusherror(L, rc);
        return 3;
    }
    lua_pushboolean(L, 1);
    return 1;
}

static int delete_lua(lua_State *L)
{
    lmdbx_env_t *env     = lauxh_checkudata(L, 1, LMDBX_ENV_MT);
    lua_Integer mode     = lauxh_optinteger(L, 2, MDBX_ENV_JUST_DELETE);
    const char *pathname = NULL;
    int rc               = mdbx_env_get_path(env->env, &pathname);

    if (rc) {
        lua_pushboolean(L, 0);
        lmdbx_pusherror(L, rc);
        return 3;
    } else if (!pathname) {
        lua_pushboolean(L, 0);
        lmdbx_pusherror(L, MDBX_ENOFILE);
        return 3;
    }

    rc = mdbx_env_delete(pathname, mode);
    if (rc) {
        lua_pushboolean(L, 0);
        lmdbx_pusherror(L, rc);
        return 3;
    }
    lua_pushboolean(L, 1);
    return 1;
}

static int open_lua(lua_State *L)
{
    lmdbx_env_t *env     = lauxh_checkudata(L, 1, LMDBX_ENV_MT);
    const char *pathname = lauxh_checkstring(L, 2);
    uint16_t mode        = lauxh_optuint16(L, 3, 0644);
    lua_Integer flags    = lmdbx_checkflags(L, 4);
    int rc               = mdbx_env_open(env->env, pathname, flags, mode);

    if (rc) {
        lua_pushboolean(L, 0);
        lmdbx_pusherror(L, rc);
        return 3;
    }
    lua_pushboolean(L, 1);
    return 1;
}

static int get_option_lua(lua_State *L)
{
    lmdbx_env_t *env   = lauxh_checkudata(L, 1, LMDBX_ENV_MT);
    lua_Integer option = lauxh_checkinteger(L, 2);
    uint64_t v         = 0;
    int rc             = mdbx_env_get_option(env->env, option, &v);

    if (rc) {
        lua_pushnil(L);
        lmdbx_pusherror(L, rc);
        return 3;
    }
    lua_pushinteger(L, v);
    return 1;
}

static int set_option_lua(lua_State *L)
{
    lmdbx_env_t *env   = lauxh_checkudata(L, 1, LMDBX_ENV_MT);
    lua_Integer option = lauxh_checkinteger(L, 2);
    uint64_t v         = lauxh_checkuint64(L, 3);
    int rc             = mdbx_env_set_option(env->env, option, v);

    if (rc) {
        lua_pushboolean(L, 0);
        lmdbx_pusherror(L, rc);
        return 3;
    }
    lua_pushboolean(L, 1);
    return 1;
}

static int gc_lua(lua_State *L)
{
    lmdbx_env_t *env = lauxh_checkudata(L, 1, LMDBX_ENV_MT);

    if (env->env && getpid() == env->pid) {
        int rc = mdbx_env_close(env->env);

        for (int i = 0; rc == MDBX_BUSY && i < 10; i++) {
            rc = mdbx_env_close(env->env);
        }
        if (rc) {
            fprintf(stderr, "failed to mdbx_env_close(): %s\n",
                    mdbx_strerror(rc));
        }
    }
    return 0;
}

static int tostring_lua(lua_State *L)
{
    lmdbx_env_t *env = lauxh_checkudata(L, 1, LMDBX_ENV_MT);
    lua_pushfstring(L, LMDBX_ENV_MT ": %p", env);
    return 1;
}

int lmdbx_env_create_lua(lua_State *L)
{
    lmdbx_env_t *env = lua_newuserdata(L, sizeof(lmdbx_env_t));
    int rc           = 0;

    env->pid = getpid();
    env->env = NULL;
    rc       = mdbx_env_create(&env->env);
    if (rc) {
        lua_pushnil(L);
        lmdbx_pusherror(L, rc);
        return 3;
    }
    lauxh_setmetatable(L, LMDBX_ENV_MT);
    return 1;
}

void lmdbx_env_init(lua_State *L)
{
    struct luaL_Reg mmethod[] = {
        {"__tostring", tostring_lua},
        {"__gc",       gc_lua      },
        {NULL,         NULL        }
    };
    struct luaL_Reg method[] = {
        {"set_option",        set_option_lua       },
        {"get_option",        get_option_lua       },
        {"open",              open_lua             },
        {"delete",            delete_lua           },
        {"copy",              copy_lua             },
        {"copy2fd",           copy2fd_lua          },
        {"stat",              stat_lua             },
        {"info",              info_lua             },
        {"sync",              sync_lua             },
        {"sync_poll",         sync_poll_lua        },
        {"set_syncbytes",     set_syncbytes_lua    },
        {"get_syncbytes",     get_syncbytes_lua    },
        {"set_syncperiod",    set_syncperiod_lua   },
        {"get_syncperiod",    get_syncperiod_lua   },
        {"close",             close_lua            },
        {"set_flags",         set_flags_lua        },
        {"get_flags",         get_flags_lua        },
        {"get_path",          get_path_lua         },
        {"get_fd",            get_fd_lua           },
        {"set_geometry",      set_geometry_lua     },
        {"set_maxreaders",    set_maxreaders_lua   },
        {"get_maxreaders",    get_maxreaders_lua   },
        {"set_maxdbs",        set_maxdbs_lua       },
        {"get_maxdbs",        get_maxdbs_lua       },
        {"get_maxkeysize",    get_maxkeysize_lua   },
        {"get_maxvalsize",    get_maxvalsize_lua   },
        {"begin",             begin_lua            },
        {"reader_list",       reader_list_lua      },
        {"reader_check",      reader_check_lua     },
        {"thread_register",   thread_register_lua  },
        {"thread_unregister", thread_unregister_lua},
        {NULL,                NULL                 }
    };
    // TODO: not implemented yet
    // mdbx_env_set_assert

    // create metatable
    luaL_newmetatable(L, LMDBX_ENV_MT);
    // metamethods
    lmdbx_register(L, mmethod);
    // methods
    lua_pushstring(L, "__index");
    lua_newtable(L);
    lmdbx_register(L, method);
    lua_rawset(L, -3);
    lua_pop(L, 1);
}
