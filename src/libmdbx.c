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

static int get_sysraminfo_lua(lua_State *L)
{
    intptr_t pagesize    = 0;
    intptr_t total_pages = 0;
    intptr_t avail_pages = 0;
    int rc = mdbx_get_sysraminfo(&pagesize, &total_pages, &avail_pages);

    if (rc) {
        lua_pushnil(L);
        lmdbx_pusherror(L, rc);
        return 3;
    }
    lua_createtable(L, 0, 3);
    lauxh_pushint2tbl(L, "pagesize", pagesize);
    lauxh_pushint2tbl(L, "total_pages", total_pages);
    lauxh_pushint2tbl(L, "avail_pages", avail_pages);
    return 1;
}

static int default_pagesize_lua(lua_State *L)
{
    size_t pagesize = mdbx_default_pagesize();

    lua_pushinteger(L, pagesize);
    return 1;
}

static int limits_txnsize_max_lua(lua_State *L)
{
    intptr_t pagesize = lauxh_checkint64(L, 1);
    intptr_t size     = mdbx_limits_txnsize_max(pagesize);

    if (size == -1) {
        lua_pushnil(L);
        lmdbx_pusherror(L, MDBX_EINVAL);
        return 3;
    }
    lua_pushinteger(L, size);
    return 1;
}

static int limits_valsize_max_lua(lua_State *L)
{
    intptr_t pagesize = lauxh_checkint64(L, 1);
    lua_Integer flags = lmdbx_checkflags(L, 2);
    intptr_t size     = mdbx_limits_valsize_max(pagesize, flags);

    if (size == -1) {
        lua_pushnil(L);
        lmdbx_pusherror(L, MDBX_EINVAL);
        return 3;
    }
    lua_pushinteger(L, size);
    return 1;
}

static int limits_keysize_max_lua(lua_State *L)
{
    intptr_t pagesize = lauxh_checkint64(L, 1);
    lua_Integer flags = lmdbx_checkflags(L, 2);
    intptr_t size     = mdbx_limits_keysize_max(pagesize, flags);

    if (size == -1) {
        lua_pushnil(L);
        lmdbx_pusherror(L, MDBX_EINVAL);
        return 3;
    }
    lua_pushinteger(L, size);
    return 1;
}

static int limits_dbsize_max_lua(lua_State *L)
{
    intptr_t pagesize = lauxh_checkint64(L, 1);
    intptr_t size     = mdbx_limits_dbsize_max(pagesize);

    if (size == -1) {
        lua_pushnil(L);
        lmdbx_pusherror(L, MDBX_EINVAL);
        return 3;
    }
    lua_pushinteger(L, size);
    return 1;
}

static int limits_dbsize_min_lua(lua_State *L)
{
    intptr_t pagesize = lauxh_checkint64(L, 1);
    intptr_t size     = mdbx_limits_dbsize_min(pagesize);

    if (size == -1) {
        lua_pushnil(L);
        lmdbx_pusherror(L, MDBX_EINVAL);
        return 3;
    }
    lua_pushinteger(L, size);
    return 1;
}

static int is_readahead_reasonable_lua(lua_State *L)
{
    size_t volume       = lauxh_checkuint64(L, 1);
    intptr_t redundancy = lauxh_checkint64(L, 2);
    int rc              = mdbx_is_readahead_reasonable(volume, redundancy);

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

LUALIB_API int luaopen_libmdbx(lua_State *L)
{
    lmdbx_env_init(L);
    lmdbx_txn_init(L);
    lmdbx_dbi_init(L);
    lmdbx_cursor_init(L);

    lua_newtable(L);
    lauxh_pushfn2tbl(L, "is_readahead_reasonable", is_readahead_reasonable_lua);
    lauxh_pushfn2tbl(L, "limits_dbsize_min", limits_dbsize_min_lua);
    lauxh_pushfn2tbl(L, "limits_dbsize_max", limits_dbsize_max_lua);
    lauxh_pushfn2tbl(L, "limits_keysize_max", limits_keysize_max_lua);
    lauxh_pushfn2tbl(L, "limits_valsize_max", limits_valsize_max_lua);
    lauxh_pushfn2tbl(L, "limits_txnsize_max", limits_txnsize_max_lua);
    lauxh_pushfn2tbl(L, "default_pagesize", default_pagesize_lua);
    lauxh_pushfn2tbl(L, "get_sysraminfo", get_sysraminfo_lua);

    lmdbx_errno_init(L);
    lua_setfield(L, -2, "errno");

    lmdbx_debug_init(L);
    lua_setfield(L, -2, "debug");

    lauxh_pushfn2tbl(L, "new", lmdbx_env_create_lua);

    // libmdbx version information
    lua_createtable(L, 0, 6);
    lauxh_pushint2tbl(L, "major", mdbx_version.major);
    lauxh_pushint2tbl(L, "minor", mdbx_version.minor);
    lauxh_pushint2tbl(L, "release", mdbx_version.release);
    lauxh_pushint2tbl(L, "revision", mdbx_version.revision);
    // source information from git
    lua_createtable(L, 0, 4);
    lauxh_pushstr2tbl(L, "datetime", mdbx_version.git.datetime);
    lauxh_pushstr2tbl(L, "tree", mdbx_version.git.tree);
    lauxh_pushstr2tbl(L, "commit", mdbx_version.git.commit);
    lauxh_pushstr2tbl(L, "describe", mdbx_version.git.describe);
    lua_setfield(L, -2, "git");
    lauxh_pushstr2tbl(L, "sourcery", mdbx_version.sourcery);
    lua_setfield(L, -2, "version");

    // libmdbx build information
    lua_createtable(L, 0, 5);
    lauxh_pushstr2tbl(L, "datetime", mdbx_build.datetime);
    lauxh_pushstr2tbl(L, "target", mdbx_build.target);
    lauxh_pushstr2tbl(L, "options", mdbx_build.options);
    lauxh_pushstr2tbl(L, "compiler", mdbx_build.compiler);
    lauxh_pushstr2tbl(L, "flags", mdbx_build.flags);
    lua_setfield(L, -2, "build");

    // MDBX_constants
    // lua_createtable(L, 0, 4);
    // The hard limit for DBI handles
    lauxh_pushint2tbl(L, "MAX_DBI", MDBX_MAX_DBI);
    // The maximum size of a data item
    lauxh_pushint2tbl(L, "MAXDATASIZE", MDBX_MAXDATASIZE);
    // The minimal database page size in bytes
    lauxh_pushint2tbl(L, "MIN_PAGESIZE", MDBX_MIN_PAGESIZE);
    // The maximal database page size in bytes
    lauxh_pushint2tbl(L, "MAX_PAGESIZE", MDBX_MAX_PAGESIZE);
    // lua_setfield(L, -2, "constants");

    // At the file system level, the environment corresponds to a pair of files
    lauxh_pushstr2tbl(L, "LOCKNAME", MDBX_LOCKNAME);
    lauxh_pushstr2tbl(L, "DATANAME", MDBX_DATANAME);
    lauxh_pushstr2tbl(L, "LOCK_SUFFIX", MDBX_LOCK_SUFFIX);

    // Log level
    // NOTE: Levels detailed than (great than) MDBX_LOG_NOTICE
    // requires build libmdbx with MDBX_DEBUG option.
    // lua_createtable(L, 0, 9);
    lauxh_pushint2tbl(L, "LOG_FATAL", MDBX_LOG_FATAL);           // 0
    lauxh_pushint2tbl(L, "LOG_ERROR", MDBX_LOG_ERROR);           // 1
    lauxh_pushint2tbl(L, "LOG_WARN", MDBX_LOG_WARN);             // 2
    lauxh_pushint2tbl(L, "LOG_NOTICE", MDBX_LOG_NOTICE);         // 3
    lauxh_pushint2tbl(L, "LOG_VERBOSE", MDBX_LOG_VERBOSE);       // 4
    lauxh_pushint2tbl(L, "LOG_DEBUG", MDBX_LOG_DEBUG);           // 5
    lauxh_pushint2tbl(L, "LOG_TRACE", MDBX_LOG_TRACE);           // 6
    lauxh_pushint2tbl(L, "LOG_EXTRA", MDBX_LOG_EXTRA);           // 7
    lauxh_pushint2tbl(L, "LOG_DONTCHANGE", MDBX_LOG_DONTCHANGE); // -1
    // lua_setfield(L, -2, "log_level");

    // Runtime debug flags
    // NOTE: `MDBX_DBG_DUMP` and `MDBX_DBG_LEGACY_MULTIOPEN` always have an
    // effect, but `MDBX_DBG_ASSERT`, `MDBX_DBG_AUDIT` and `MDBX_DBG_JITTER`
    // only if libmdbx builded with MDBX_DEBUG.
    // lua_createtable(L, 0, 9);
    lauxh_pushint2tbl(L, "DBG_NONE", MDBX_DBG_NONE);
    lauxh_pushint2tbl(L, "DBG_ASSERT", MDBX_DBG_ASSERT);
    lauxh_pushint2tbl(L, "DBG_AUDIT", MDBX_DBG_AUDIT);
    lauxh_pushint2tbl(L, "DBG_JITTER", MDBX_DBG_JITTER);
    lauxh_pushint2tbl(L, "DBG_DUMP", MDBX_DBG_DUMP);
    lauxh_pushint2tbl(L, "DBG_LEGACY_MULTIOPEN", MDBX_DBG_LEGACY_MULTIOPEN);
    lauxh_pushint2tbl(L, "DBG_LEGACY_OVERLAP", MDBX_DBG_LEGACY_OVERLAP);
    lauxh_pushint2tbl(L, "DBG_DONT_UPGRADE", MDBX_DBG_DONT_UPGRADE);
    lauxh_pushint2tbl(L, "DBG_DONTCHANGE", MDBX_DBG_DONTCHANGE);
    // lua_setfield(L, -2, "debug_flags");

    // Environment flags
    // mdbx_env_open() and mdbx_env_set_flags()
    // lua_createtable(L, 0, 17);
    lauxh_pushint2tbl(L, "ENV_DEFAULTS", MDBX_ENV_DEFAULTS);
    lauxh_pushint2tbl(L, "NOSUBDIR", MDBX_NOSUBDIR);
    lauxh_pushint2tbl(L, "RDONLY", MDBX_RDONLY);
    lauxh_pushint2tbl(L, "EXCLUSIVE", MDBX_EXCLUSIVE);
    lauxh_pushint2tbl(L, "ACCEDE", MDBX_ACCEDE);
    lauxh_pushint2tbl(L, "WRITEMAP", MDBX_WRITEMAP);
    lauxh_pushint2tbl(L, "NOTLS", MDBX_NOTLS);
    lauxh_pushint2tbl(L, "NORDAHEAD", MDBX_NORDAHEAD);
    lauxh_pushint2tbl(L, "NOMEMINIT", MDBX_NOMEMINIT);
    lauxh_pushint2tbl(L, "COALESCE", MDBX_COALESCE);
    lauxh_pushint2tbl(L, "LIFORECLAIM", MDBX_LIFORECLAIM);
    lauxh_pushint2tbl(L, "PAGEPERTURB", MDBX_PAGEPERTURB);
    // SYNC MODES
    lauxh_pushint2tbl(L, "NOMETASYNC", MDBX_NOMETASYNC);
    lauxh_pushint2tbl(L, "SAFE_NOSYNC", MDBX_SAFE_NOSYNC);
    lauxh_pushint2tbl(L, "SYNC_DURABLE", MDBX_SYNC_DURABLE);
    lauxh_pushint2tbl(L, "UTTERLY_NOSYNC", MDBX_UTTERLY_NOSYNC);
    // lua_setfield(L, -2, "env_flags");

    // Transaction flags
    // mdbx_txn_begin() and mdbx_txn_flags() flags
    // lua_createtable(L, 0, 6);
    lauxh_pushint2tbl(L, "TXN_READWRITE", MDBX_TXN_READWRITE);
    lauxh_pushint2tbl(L, "TXN_RDONLY", MDBX_TXN_RDONLY);
    lauxh_pushint2tbl(L, "TXN_RDONLY_PREPARE", MDBX_TXN_RDONLY_PREPARE);
    lauxh_pushint2tbl(L, "TXN_TRY", MDBX_TXN_TRY);
    lauxh_pushint2tbl(L, "TXN_NOMETASYNC", MDBX_TXN_NOMETASYNC);
    lauxh_pushint2tbl(L, "TXN_NOSYNC", MDBX_TXN_NOSYNC);
    // lua_setfield(L, -2, "txn_flags");

    // Database flags
    // mdbx_dbi_open() flags
    // lua_createtable(L, 0, 9);
    lauxh_pushint2tbl(L, "DB_DEFAULTS", MDBX_DB_DEFAULTS);
    lauxh_pushint2tbl(L, "REVERSEKEY", MDBX_REVERSEKEY);
    lauxh_pushint2tbl(L, "DUPSORT", MDBX_DUPSORT);
    lauxh_pushint2tbl(L, "INTEGERKEY", MDBX_INTEGERKEY);
    lauxh_pushint2tbl(L, "DUPFIXED", MDBX_DUPFIXED);
    lauxh_pushint2tbl(L, "INTEGERDUP", MDBX_INTEGERDUP);
    lauxh_pushint2tbl(L, "REVERSEDUP", MDBX_REVERSEDUP);
    lauxh_pushint2tbl(L, "CREATE", MDBX_CREATE);
    lauxh_pushint2tbl(L, "DB_ACCEDE", MDBX_DB_ACCEDE);
    // lua_setfield(L, -2, "db_flags");

    // Data changing flags
    // mdbx_put(), mdbx_cursor_put() and mdbx_replace() flags
    // lua_createtable(L, 0, 9);
    lauxh_pushint2tbl(L, "ALLDUPS", MDBX_ALLDUPS);
    lauxh_pushint2tbl(L, "APPEND", MDBX_APPEND);
    lauxh_pushint2tbl(L, "APPENDDUP", MDBX_APPENDDUP);
    lauxh_pushint2tbl(L, "CURRENT", MDBX_CURRENT);
    lauxh_pushint2tbl(L, "MULTIPLE", MDBX_MULTIPLE);
    lauxh_pushint2tbl(L, "NODUPDATA", MDBX_NODUPDATA);
    lauxh_pushint2tbl(L, "NOOVERWRITE", MDBX_NOOVERWRITE);
    lauxh_pushint2tbl(L, "RESERVE", MDBX_RESERVE);
    lauxh_pushint2tbl(L, "UPSERT", MDBX_UPSERT);
    // lua_setfield(L, -2, "put_flags");

    // Environment copy flags
    // mdbx_env_copy() and mdbx_env_copy2fd()
    // lua_createtable(L, 0, 3);
    lauxh_pushint2tbl(L, "CP_DEFAULTS", MDBX_CP_DEFAULTS);
    lauxh_pushint2tbl(L, "CP_COMPACT", MDBX_CP_COMPACT);
    lauxh_pushint2tbl(L, "CP_FORCE_DYNAMIC_SIZE", MDBX_CP_FORCE_DYNAMIC_SIZE);
    // lua_setfield(L, -2, "copy_flags");

    // Cursor operations
    // mdbx_cursor_get()
    // lua_createtable(L, 0, 21);
    lauxh_pushint2tbl(L, "FIRST", MDBX_FIRST);
    lauxh_pushint2tbl(L, "FIRST_DUP", MDBX_FIRST_DUP);
    lauxh_pushint2tbl(L, "GET_BOTH", MDBX_GET_BOTH);
    lauxh_pushint2tbl(L, "GET_BOTH_RANGE", MDBX_GET_BOTH_RANGE);
    lauxh_pushint2tbl(L, "GET_CURRENT", MDBX_GET_CURRENT);
    lauxh_pushint2tbl(L, "GET_MULTIPLE", MDBX_GET_MULTIPLE);
    lauxh_pushint2tbl(L, "LAST", MDBX_LAST);
    lauxh_pushint2tbl(L, "LAST_DUP", MDBX_LAST_DUP);
    lauxh_pushint2tbl(L, "NEXT", MDBX_NEXT);
    lauxh_pushint2tbl(L, "NEXT_DUP", MDBX_NEXT_DUP);
    lauxh_pushint2tbl(L, "NEXT_MULTIPLE", MDBX_NEXT_MULTIPLE);
    lauxh_pushint2tbl(L, "NEXT_NODUP", MDBX_NEXT_NODUP);
    lauxh_pushint2tbl(L, "PREV", MDBX_PREV);
    lauxh_pushint2tbl(L, "PREV_DUP", MDBX_PREV_DUP);
    lauxh_pushint2tbl(L, "PREV_NODUP", MDBX_PREV_NODUP);
    lauxh_pushint2tbl(L, "SET", MDBX_SET);
    lauxh_pushint2tbl(L, "SET_KEY", MDBX_SET_KEY);
    lauxh_pushint2tbl(L, "SET_RANGE", MDBX_SET_RANGE);
    lauxh_pushint2tbl(L, "PREV_MULTIPLE", MDBX_PREV_MULTIPLE);
    lauxh_pushint2tbl(L, "SET_LOWERBOUND", MDBX_SET_LOWERBOUND);
    lauxh_pushint2tbl(L, "SET_UPPERBOUND", MDBX_SET_UPPERBOUND);
    // lua_setfield(L, -2, "cursor_op");

    // MDBX environment options
    // lua_createtable(L, 0, 13);
    lauxh_pushint2tbl(L, "opt_max_db", MDBX_opt_max_db);
    lauxh_pushint2tbl(L, "opt_max_readers", MDBX_opt_max_readers);
    lauxh_pushint2tbl(L, "opt_sync_bytes", MDBX_opt_sync_bytes);
    lauxh_pushint2tbl(L, "opt_sync_period", MDBX_opt_sync_period);
    lauxh_pushint2tbl(L, "opt_rp_augment_limit", MDBX_opt_rp_augment_limit);
    lauxh_pushint2tbl(L, "opt_loose_limit", MDBX_opt_loose_limit);
    lauxh_pushint2tbl(L, "opt_dp_reserve_limit", MDBX_opt_dp_reserve_limit);
    lauxh_pushint2tbl(L, "opt_txn_dp_limit", MDBX_opt_txn_dp_limit);
    lauxh_pushint2tbl(L, "opt_txn_dp_initial", MDBX_opt_txn_dp_initial);
    lauxh_pushint2tbl(L, "opt_spill_max_denominator",
                      MDBX_opt_spill_max_denominator);
    lauxh_pushint2tbl(L, "opt_spill_min_denominator",
                      MDBX_opt_spill_min_denominator);
    lauxh_pushint2tbl(L, "opt_spill_parent4child_denominator",
                      MDBX_opt_spill_parent4child_denominator);
    lauxh_pushint2tbl(L, "opt_merge_threshold_16dot16_percent",
                      MDBX_opt_merge_threshold_16dot16_percent);
    // lua_setfield(L, -2, "option");

    // Deletion modes for mdbx_env_delete().
    // lua_createtable(L, 0, 13);
    lauxh_pushint2tbl(L, "ENV_JUST_DELETE", MDBX_ENV_JUST_DELETE);
    lauxh_pushint2tbl(L, "ENV_ENSURE_UNUSED", MDBX_ENV_ENSURE_UNUSED);
    lauxh_pushint2tbl(L, "ENV_WAIT_FOR_UNUSED", MDBX_ENV_WAIT_FOR_UNUSED);
    // lua_setfield(L, -2, "env_delete_mode");

    // DBI state bits returted by mdbx_dbi_flags_ex()
    // lua_createtable(L, 0, 4);
    lauxh_pushint2tbl(L, "DBI_DIRTY", MDBX_DBI_DIRTY);
    lauxh_pushint2tbl(L, "DBI_STALE", MDBX_DBI_STALE);
    lauxh_pushint2tbl(L, "DBI_FRESH", MDBX_DBI_FRESH);
    lauxh_pushint2tbl(L, "DBI_CREAT", MDBX_DBI_CREAT);
    // lua_setfield(L, -2, "dbi_state");

    return 1;
}
