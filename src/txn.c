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

// TODO: mdbx_get_attr
// TODO: mdbx_set_attr
// TODO: mdbx_put_attr

static int dbi_sequence_lua(lua_State *L)
{
    lmdbx_txn_t *txn = lauxh_checkudata(L, 1, LMDBX_TXN_MT);
    uintptr_t incr   = lauxh_checkuint64(L, 2);
    uint64_t result  = 0;
    int rc           = mdbx_dbi_sequence(txn->txn, txn->dbi, &result, incr);

    if (rc) {
        lua_pushnil(L);
        lmdbx_pusherror(L, rc);
        return 3;
    }
    lua_pushinteger(L, result);
    return 1;
}

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
        return 3;
    }
}

static int estimate_range_lua(lua_State *L)
{
    lmdbx_txn_t *txn      = lauxh_checkudata(L, 1, LMDBX_TXN_MT);
    size_t begin_klen     = 0;
    const char *begin_key = lauxh_checklstring(L, 2, &begin_klen);
    size_t begin_vlen     = 0;
    const char *begin_val = lauxh_checklstring(L, 3, &begin_vlen);
    size_t end_klen       = 0;
    const char *end_key   = lauxh_checklstring(L, 4, &end_klen);
    size_t end_vlen       = 0;
    const char *end_val   = lauxh_checklstring(L, 5, &end_vlen);
    MDBX_val begin_k = {.iov_base = (void *)begin_key, .iov_len = begin_klen};
    MDBX_val begin_v = {.iov_base = (void *)begin_val, .iov_len = begin_vlen};
    MDBX_val end_k   = {.iov_base = (void *)end_key, .iov_len = end_klen};
    MDBX_val end_v   = {.iov_base = (void *)end_val, .iov_len = end_vlen};
    ptrdiff_t distance_items = 0;
    int rc = mdbx_estimate_range(txn->txn, txn->dbi, &begin_k, &begin_v, &end_k,
                                 &end_v, &distance_items);

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
    lmdbx_txn_t *txn    = lauxh_checkudata(L, 1, LMDBX_TXN_MT);
    lmdbx_cursor_t *cur = lua_newuserdata(L, sizeof(lmdbx_cursor_t));
    int rc              = mdbx_cursor_open(txn->txn, txn->dbi, &cur->cur);

    if (rc) {
        lua_pushnil(L);
        lmdbx_pusherror(L, rc);
        return 3;
    }
    lauxh_setmetatable(L, LMDBX_CURSOR_MT);
    cur->txn_ref = lauxh_refat(L, 1);

    return 1;
}

static int del_lua(lua_State *L)
{
    lmdbx_txn_t *txn = lauxh_checkudata(L, 1, LMDBX_TXN_MT);
    size_t klen      = 0;
    const char *key  = lauxh_checklstring(L, 2, &klen);
    size_t vlen      = 0;
    const char *val  = lauxh_optlstring(L, 3, NULL, &vlen);
    MDBX_val k       = {.iov_base = (void *)key, .iov_len = klen};
    MDBX_val v       = {.iov_base = (void *)val, .iov_len = vlen};
    int rc           = mdbx_del(txn->txn, txn->dbi, &k, (val) ? &v : NULL);

    if (rc) {
        lua_pushboolean(L, 0);
        lmdbx_pusherror(L, rc);
        return 3;
    }
    lua_pushboolean(L, 1);
    return 1;
}

static int replace_lua(lua_State *L)
{
    lmdbx_txn_t *txn  = lauxh_checkudata(L, 1, LMDBX_TXN_MT);
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
    int rc            = mdbx_replace(txn->txn, txn->dbi, &k, new, &old, flags);

    if (rc == MDBX_RESULT_TRUE) {
        old.iov_base = alloca(old.iov_len);
        rc           = mdbx_replace(txn->txn, txn->dbi, &k, new, &old, flags);
    }

    switch (rc) {
    case MDBX_SUCCESS:
        lua_pushlstring(L, old.iov_base, old.iov_len);
        return 1;

    default:
        lua_pushnil(L);
        lmdbx_pusherror(L, rc);
        return 3;
    }
}

static int put_lua(lua_State *L)
{
    lmdbx_txn_t *txn  = lauxh_checkudata(L, 1, LMDBX_TXN_MT);
    size_t klen       = 0;
    const char *key   = lauxh_checklstring(L, 2, &klen);
    size_t vlen       = 0;
    const char *val   = lauxh_checklstring(L, 3, &vlen);
    lua_Integer flags = lmdbx_checkflags(L, 4);
    MDBX_val k        = {.iov_base = (void *)key, .iov_len = klen};
    MDBX_val v        = {.iov_base = (void *)val, .iov_len = vlen};
    int rc            = mdbx_put(txn->txn, txn->dbi, &k, &v, flags);

    if (rc) {
        lua_pushboolean(L, 0);
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
        if (rc == 1) {
            lua_pushboolean(L, 1);
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
    lmdbx_txn_t *txn = lauxh_checkudata(L, 1, LMDBX_TXN_MT);
    size_t klen      = 0;
    const char *key  = lauxh_checklstring(L, 2, &klen);
    MDBX_val k       = {.iov_base = (void *)key, .iov_len = klen};
    MDBX_val v       = {0};
    int rc           = mdbx_get_equal_or_great(txn->txn, txn->dbi, &k, &v);

    switch (rc) {
    case MDBX_SUCCESS:
    case MDBX_RESULT_TRUE:
        lua_createtable(L, 2, 0);
        lua_pushlstring(L, k.iov_base, k.iov_len);
        lua_rawseti(L, -2, 1);
        lua_pushlstring(L, v.iov_base, v.iov_len);
        lua_rawseti(L, -2, 2);
        return 1;

    case MDBX_NOTFOUND:
        lua_pushnil(L);
        return 1;

    default:
        lua_pushnil(L);
        lmdbx_pusherror(L, rc);
        return 3;
    }
}

static int get_lua(lua_State *L)
{
    lmdbx_txn_t *txn = lauxh_checkudata(L, 1, LMDBX_TXN_MT);
    size_t klen      = 0;
    const char *key  = lauxh_checklstring(L, 2, &klen);
    int do_count     = lauxh_optboolean(L, 3, 0);
    MDBX_val k       = {.iov_base = (void *)key, .iov_len = klen};
    MDBX_val v       = {0};
    size_t count     = 0;
    int rc = (do_count) ? mdbx_get_ex(txn->txn, txn->dbi, &k, &v, &count) :
                          mdbx_get(txn->txn, txn->dbi, &k, &v);

    if (rc) {
        lua_pushnil(L);
        if (rc == MDBX_NOTFOUND) {
            return 1;
        }
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

static int drop_lua(lua_State *L)
{
    lmdbx_txn_t *txn = lauxh_checkudata(L, 1, LMDBX_TXN_MT);
    int del          = lauxh_optboolean(L, 2, 0);
    int rc           = mdbx_drop(txn->txn, txn->dbi, del);

    if (rc) {
        lua_pushboolean(L, 0);
        lmdbx_pusherror(L, rc);
        return 3;
    } else if (del) {
        txn->dbi = 0;
    }
    lua_pushboolean(L, 1);
    return 1;
}

static int dbi_flags_lua(lua_State *L)
{
    lmdbx_txn_t *txn = lauxh_checkudata(L, 1, LMDBX_TXN_MT);
    unsigned flags   = 0;
    unsigned state   = 0;
    int rc           = mdbx_dbi_flags_ex(txn->txn, txn->dbi, &flags, &state);

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

static int dbi_dupsort_depthmask_lua(lua_State *L)
{
    lmdbx_txn_t *txn = lauxh_checkudata(L, 1, LMDBX_TXN_MT);
    uint32_t mask    = 0;
    int rc           = mdbx_dbi_dupsort_depthmask(txn->txn, txn->dbi, &mask);

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

static int dbi_stat_lua(lua_State *L)
{
    lmdbx_txn_t *txn = lauxh_checkudata(L, 1, LMDBX_TXN_MT);
    MDBX_stat stat   = {0};
    int rc = mdbx_dbi_stat(txn->txn, txn->dbi, &stat, sizeof(MDBX_stat));

    if (rc) {
        lua_pushnil(L);
        lmdbx_pusherror(L, rc);
        return 3;
    }
    lmdbx_pushstat(L, &stat);
    return 1;
}

static int dbi_close_lua(lua_State *L)
{
    lmdbx_txn_t *txn = lauxh_checkudata(L, 1, LMDBX_TXN_MT);
    int rc           = mdbx_dbi_close(mdbx_txn_env(txn->txn), txn->dbi);

    if (rc) {
        lua_pushboolean(L, 0);
        lmdbx_pusherror(L, rc);
        return 3;
    }
    lua_pushboolean(L, 1);
    return 1;
}

static int dbi_open_lua(lua_State *L)
{
    lmdbx_txn_t *txn  = lauxh_checkudata(L, 1, LMDBX_TXN_MT);
    const char *name  = lauxh_optstring(L, 2, NULL);
    lua_Integer flags = lmdbx_checkflags(L, 3);
    int rc            = mdbx_dbi_open(txn->txn, name, flags, &txn->dbi);

    if (rc) {
        lua_pushboolean(L, 0);
        lmdbx_pusherror(L, rc);
        return 3;
    }
    lua_pushboolean(L, 1);
    return 1;
}

static int renew_lua(lua_State *L)
{
    lmdbx_txn_t *txn = lauxh_checkudata(L, 1, LMDBX_TXN_MT);
    int rc           = mdbx_txn_renew(txn->txn);

    if (rc) {
        lua_pushboolean(L, 0);
        lmdbx_pusherror(L, rc);
        return 3;
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
        return 3;
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
        return 3;
    } else if (rc) {
        lua_pushboolean(L, 0);
        lmdbx_pusherror(L, rc);
        return 3;
    } else if (doas != EXEC_AS_BREAK) {
        txn->env_ref = lauxh_unref(L, txn->env_ref);
        txn->dbi     = 0;
        txn->txn     = NULL;
    }

    if (rc) {
        lua_pushboolean(L, 0);
        lmdbx_pusherror(L, rc);
        return 3;
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
        return 3;
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
    int rc             = 0;

    child->env_ref = LUA_NOREF;
    child->dbi     = 0;
    child->txn     = NULL;
    rc = mdbx_txn_begin(mdbx_txn_env(txn->txn), txn->txn, flags, &child->txn);
    if (rc) {
        lua_pushnil(L);
        lmdbx_pusherror(L, rc);
        return 3;
    }
    lauxh_setmetatable(L, LMDBX_TXN_MT);
    lauxh_pushref(L, txn->env_ref);
    child->env_ref = lauxh_ref(L);

    return 1;
}

static int dbi_lua(lua_State *L)
{
    lmdbx_txn_t *txn = lauxh_checkudata(L, 1, LMDBX_TXN_MT);

    lua_pushinteger(L, txn->dbi);

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
        return 3;
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
        return 3;
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

void lmdbx_txn_init(lua_State *L)
{
    struct luaL_Reg mmethod[] = {
        {"__tostring", tostring_lua},
        {"__gc",       gc_lua      },
        {NULL,         NULL        }
    };
    struct luaL_Reg method[] = {
        {"dbi",                   dbi_lua                  },
        {"env_stat",              env_stat_lua             },
        {"env_info",              env_info_lua             },
        {"begin",                 begin_lua                },
        {"info",                  info_lua                 },
        {"env",                   env_lua                  },
        {"flags",                 flags_lua                },
        {"id",                    id_lua                   },
        {"commit",                commit_lua               },
        {"abort",                 abort_lua                },
        {"reset",                 reset_lua                },
        {"renew",                 renew_lua                },
        {"dbi_open",              dbi_open_lua             },
        {"dbi_stat",              dbi_stat_lua             },
        {"dbi_dupsort_depthmask", dbi_dupsort_depthmask_lua},
        {"dbi_flags",             dbi_flags_lua            },
        {"dbi_close",             dbi_close_lua            },
        {"drop",                  drop_lua                 },
        {"get",                   get_lua                  },
        {"get_equal_or_great",    get_equal_or_great_lua   },
        {"op_insert",             op_insert_lua            }, // helper func
        {"op_upsert",             op_upsert_lua            }, // helper func
        {"op_update",             op_update_lua            }, // helper func
        {"put",                   put_lua                  },
        {"replace",               replace_lua              },
        {"del",                   del_lua                  },
        {"cursor",                cursor_lua               },
        {"estimate_range",        estimate_range_lua       },
        {"is_dirty",              is_dirty_lua             },
        {"dbi_sequence",          dbi_sequence_lua         },
        {NULL,                    NULL                     }
    };

    // create metatable
    luaL_newmetatable(L, LMDBX_TXN_MT);
    // metamethods
    lmdbx_register(L, mmethod);
    // methods
    lua_pushstring(L, "__index");
    lua_newtable(L);
    lmdbx_register(L, method);
    lua_rawset(L, -3);
    lua_pop(L, 1);
}
