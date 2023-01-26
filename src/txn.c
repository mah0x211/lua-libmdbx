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

static int is_dirty_lua(lua_State *L)
{
    lmdbx_txn_t *txn = lauxh_checkudata(L, 1, LMDBX_TXN_MT);
    uintptr_t ptr    = lauxh_checkuint64(L, 2);
    int rc           = mdbx_is_dirty(txn->txn, (void *)ptr);

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
        return 2;
    }
}

static int dbi_open_lua(lua_State *L)
{
    return lmdbx_dbi_open_lua(L);
}

static int renew_lua(lua_State *L)
{
    lmdbx_txn_t *txn = lauxh_checkudata(L, 1, LMDBX_TXN_MT);
    int rc           = mdbx_txn_renew(txn->txn);

    if (rc) {
        lua_pushboolean(L, 0);
        lmdbx_pusherror(L, rc);
        return 2;
    }
    lua_pushboolean(L, 1);
    return 1;
}

static int reset_lua(lua_State *L)
{
    lmdbx_txn_t *txn = lauxh_checkudata(L, 1, LMDBX_TXN_MT);
    int rc           = mdbx_txn_reset(txn->txn);

    if (rc) {
        lua_pushboolean(L, 0);
        lmdbx_pusherror(L, rc);
        return 2;
    }
    lua_pushboolean(L, 1);
    return 1;
}

#define EXEC_AS_COMMIT 0
#define EXEC_AS_ABORT  1
#define EXEC_AS_BREAK  2

static inline int exec_txn(lua_State *L, int doas)
{
    lmdbx_txn_t *txn = lauxh_checkudata(L, 1, LMDBX_TXN_MT);
    int rc           = 0;

    switch (doas) {
    case EXEC_AS_COMMIT:
        rc = mdbx_txn_commit(txn->txn);
        break;
    case EXEC_AS_ABORT:
        rc = mdbx_txn_abort(txn->txn);
        break;
    case EXEC_AS_BREAK:
        rc = mdbx_txn_break(txn->txn);
        break;
    }

    if (rc == MDBX_THREAD_MISMATCH) {
        lua_pushboolean(L, 0);
        lmdbx_pusherror(L, rc);
        return 2;
    } else if (rc) {
        lua_pushboolean(L, 0);
        lmdbx_pusherror(L, rc);
        return 2;
    } else if (doas != EXEC_AS_BREAK) {
        txn->env_ref = lauxh_unref(L, txn->env_ref);
        txn->txn     = NULL;
    }

    if (rc) {
        lua_pushboolean(L, 0);
        lmdbx_pusherror(L, rc);
        return 2;
    }
    lua_pushboolean(L, 1);
    return 1;
}

static int abort_lua(lua_State *L)
{
    if (lauxh_optboolean(L, 2, 0)) {
        return exec_txn(L, EXEC_AS_BREAK);
    }
    return exec_txn(L, EXEC_AS_ABORT);
}

static int commit_lua(lua_State *L)
{
    return exec_txn(L, EXEC_AS_COMMIT);
}

static int id_lua(lua_State *L)
{
    lmdbx_txn_t *txn = lauxh_checkudata(L, 1, LMDBX_TXN_MT);
    lua_pushinteger(L, mdbx_txn_id(txn->txn));
    return 1;
}

static int flags_lua(lua_State *L)
{
    lmdbx_txn_t *txn = lauxh_checkudata(L, 1, LMDBX_TXN_MT);
    int flags        = mdbx_txn_flags(txn->txn);

    if (flags == -1) {
        lua_pushnil(L);
        return 1;
    }

    lua_newtable(L);
    if (flags & MDBX_TXN_READWRITE) {
        lauxh_pushint2tbl(L, "TXN_READWRITE", MDBX_TXN_READWRITE);
    }
    if (flags & MDBX_TXN_RDONLY) {
        lauxh_pushint2tbl(L, "TXN_RDONLY", MDBX_TXN_RDONLY);
    }
    if (flags & MDBX_TXN_RDONLY_PREPARE) {
        lauxh_pushint2tbl(L, "TXN_RDONLY_PREPARE", MDBX_TXN_RDONLY_PREPARE);
    }
    if (flags & MDBX_TXN_TRY) {
        lauxh_pushint2tbl(L, "TXN_TRY", MDBX_TXN_TRY);
    }
    if (flags & MDBX_TXN_NOMETASYNC) {
        lauxh_pushint2tbl(L, "TXN_NOMETASYNC", MDBX_TXN_NOMETASYNC);
    }
    if (flags & MDBX_TXN_NOSYNC) {
        lauxh_pushint2tbl(L, "TXN_NOSYNC", MDBX_TXN_NOSYNC);
    }

    return 1;
}

static int env_lua(lua_State *L)
{
    lmdbx_txn_t *txn = lauxh_checkudata(L, 1, LMDBX_TXN_MT);
    lauxh_pushref(L, txn->env_ref);
    return 1;
}

static int info_lua(lua_State *L)
{
    lmdbx_txn_t *txn   = lauxh_checkudata(L, 1, LMDBX_TXN_MT);
    int scan_rlt       = lauxh_optboolean(L, 2, 0);
    MDBX_txn_info info = {0};
    int rc             = mdbx_txn_info(txn->txn, &info, scan_rlt);

    if (rc) {
        lua_pushnil(L);
        lmdbx_pusherror(L, rc);
        return 2;
    }
    lua_createtable(L, 0, 8);
    // struct MDBX_txn_info {
    /** The ID of the transaction. For a READ-ONLY transaction, this corresponds
        to the snapshot being read. */
    lauxh_pushint2tbl(L, "txn_id", info.txn_id);
    /** For READ-ONLY transaction: the lag from a recent MVCC-snapshot, i.e. the
       number of committed transaction since read transaction started.
       For WRITE transaction (provided if `scan_rlt=true`): the lag of the
       oldest reader from current transaction (i.e. at least 1 if any reader
       running). */
    lauxh_pushint2tbl(L, "txn_reader_lag", info.txn_reader_lag);
    /** Used space by this transaction, i.e. corresponding to the last used
     * database page. */
    lauxh_pushint2tbl(L, "txn_space_used", info.txn_space_used);
    /** Current size of database file. */
    lauxh_pushint2tbl(L, "txn_space_limit_soft", info.txn_space_limit_soft);
    /** Upper bound for size the database file, i.e. the value `size_upper`
       argument of the appropriate call of \ref mdbx_env_set_geometry(). */
    lauxh_pushint2tbl(L, "txn_space_limit_hard", info.txn_space_limit_hard);
    /** For READ-ONLY transaction: The total size of the database pages that
       were retired by committed write transactions after the reader's
       MVCC-snapshot, i.e. the space which would be freed after the Reader
       releases the MVCC-snapshot for reuse by completion read transaction. For
       WRITE transaction: The summarized size of the database pages that were
       retired for now due Copy-On-Write during this transaction. */
    lauxh_pushint2tbl(L, "txn_space_retired", info.txn_space_retired);
    /** For READ-ONLY transaction: the space available for writer(s) and that
       must be exhausted for reason to call the Handle-Slow-Readers callback for
       this read transaction.
       For WRITE transaction: the space inside transaction
       that left to `MDBX_TXN_FULL` error. */
    lauxh_pushint2tbl(L, "txn_space_leftover", info.txn_space_leftover);
    /** For READ-ONLY transaction (provided if `scan_rlt=true`): The space that
       actually become available for reuse when only this transaction will be
       finished.
       For WRITE transaction: The summarized size of the dirty database
       pages that generated during this transaction. */
    lauxh_pushint2tbl(L, "txn_space_dirty", info.txn_space_dirty);
    // };

    return 1;
}

static int begin_lua(lua_State *L)
{
    lmdbx_txn_t *txn   = lauxh_checkudata(L, 1, LMDBX_TXN_MT);
    lua_Integer flags  = lmdbx_checkflags(L, 2);
    lmdbx_txn_t *child = lua_newuserdata(L, sizeof(lmdbx_txn_t));
    int rc =
        mdbx_txn_begin(mdbx_txn_env(txn->txn), txn->txn, flags, &child->txn);

    if (rc) {
        lua_pushnil(L);
        lmdbx_pusherror(L, rc);
        return 2;
    }
    lauxh_setmetatable(L, LMDBX_TXN_MT);
    lauxh_pushref(L, txn->env_ref);
    child->env_ref = lauxh_ref(L);

    return 1;
}

static int env_info_lua(lua_State *L)
{
    lmdbx_txn_t *txn  = lauxh_checkudata(L, 1, LMDBX_TXN_MT);
    MDBX_envinfo info = {0};
    int rc = mdbx_env_info_ex(NULL, txn->txn, &info, sizeof(MDBX_envinfo));

    if (rc) {
        lua_pushnil(L);
        lmdbx_pusherror(L, rc);
        return 2;
    }
    lmdbx_pushenvinfo(L, &info);
    return 1;
}

static int env_stat_lua(lua_State *L)
{
    lmdbx_txn_t *txn = lauxh_checkudata(L, 1, LMDBX_TXN_MT);
    MDBX_stat stat   = {0};
    int rc = mdbx_env_stat_ex(NULL, txn->txn, &stat, sizeof(MDBX_stat));

    if (rc) {
        lua_pushnil(L);
        lmdbx_pusherror(L, rc);
        return 2;
    }
    lmdbx_pushstat(L, &stat);
    return 1;
}

static int gc_lua(lua_State *L)
{
    lmdbx_txn_t *txn = lauxh_checkudata(L, 1, LMDBX_TXN_MT);

    if (txn->txn) {
        int rc = mdbx_txn_abort(txn->txn);
        lauxh_unref(L, txn->env_ref);
        if (rc) {
            fprintf(stderr, "failed to mdbx_txn_abort(): %s\n",
                    mdbx_strerror(rc));
        }
    }

    return 0;
}

static int tostring_lua(lua_State *L)
{
    lmdbx_txn_t *txn = lauxh_checkudata(L, 1, LMDBX_TXN_MT);
    lua_pushfstring(L, LMDBX_TXN_MT ": %p", txn);
    return 1;
}

int lmdbx_txn_begin_lua(lua_State *L)
{
    lmdbx_env_t *env  = lauxh_checkudata(L, 1, LMDBX_ENV_MT);
    lua_Integer flags = lmdbx_checkflags(L, 2);
    lmdbx_txn_t *txn  = lua_newuserdata(L, sizeof(lmdbx_txn_t));
    int rc            = mdbx_txn_begin(env->env, NULL, flags, &txn->txn);

    if (rc) {
        lua_pushnil(L);
        lmdbx_pusherror(L, rc);
        return 2;
    }
    lauxh_setmetatable(L, LMDBX_TXN_MT);
    txn->env_ref = lauxh_refat(L, 1);

    return 1;
}

void lmdbx_txn_init(lua_State *L, int errno_ref)
{
    struct luaL_Reg mmethod[] = {
        {"__tostring", tostring_lua},
        {"__gc",       gc_lua      },
        {NULL,         NULL        }
    };
    struct luaL_Reg method[] = {
        {"env_stat", env_stat_lua},
        {"env_info", env_info_lua},
        {"begin",    begin_lua   },
        {"info",     info_lua    },
        {"env",      env_lua     },
        {"flags",    flags_lua   },
        {"id",       id_lua      },
        {"commit",   commit_lua  },
        {"abort",    abort_lua   },
        {"reset",    reset_lua   },
        {"renew",    renew_lua   },
        {"dbi_open", dbi_open_lua},
        {"is_dirty", is_dirty_lua},
        {NULL,       NULL        }
    };

    // create metatable
    luaL_newmetatable(L, LMDBX_TXN_MT);
    // metamethods
    lmdbx_register(L, mmethod, errno_ref);
    // methods
    lua_pushstring(L, "__index");
    lua_newtable(L);
    lmdbx_register(L, method, errno_ref);
    lua_rawset(L, -3);
    lua_pop(L, 1);
}
