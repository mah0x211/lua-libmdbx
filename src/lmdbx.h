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

#ifndef lua_mdbx_h
#define lua_mdbx_h

#include "../deps/libmdbx/mdbx.h"
// #include "mdbx.h"
#include <lauxhlib.h>

static inline void lmdbx_pusherror(lua_State *L, int errnum)
{
    lua_pushstring(L, mdbx_strerror(errnum));
    lua_pushinteger(L, errnum);
}

static inline void lmdbx_register(lua_State *L, struct luaL_Reg *reg)
{
    while (reg->name) {
        lauxh_pushfn2tbl(L, reg->name, reg->func);
        reg++;
    }
}

static inline lua_Integer lmdbx_checkflags(lua_State *L, int idx)
{
    const int argc  = lua_gettop(L);
    lua_Integer flg = 0;

    if (argc >= idx) {
        for (; idx <= argc; idx++) {
            flg |= lauxh_checkinteger(L, idx);
        }
    }

    return flg;
}

static inline void lmdbx_pushstat(lua_State *L, MDBX_stat *stat)
{
    lua_createtable(L, 0, 7);
    // struct MDBX_stat {
    // Size of a database page. This is the same for all databases.
    lauxh_pushint2tbl(L, "psize", stat->ms_psize);
    // Depth (height) of the B-tree
    lauxh_pushint2tbl(L, "depth", stat->ms_depth);
    // Number of internal (non-leaf) pages
    lauxh_pushint2tbl(L, "branch_pages", stat->ms_branch_pages);
    // Number of leaf pages
    lauxh_pushint2tbl(L, "leaf_pages", stat->ms_leaf_pages);
    // Number of overflow pages
    lauxh_pushint2tbl(L, "overflow_pages", stat->ms_overflow_pages);
    // Number of data items
    lauxh_pushint2tbl(L, "entries", stat->ms_entries);
    // Transaction ID of committed last modification
    lauxh_pushint2tbl(L, "mod_txnid", stat->ms_mod_txnid);
    // };
}

static inline void lmdbx_pushenvinfo(lua_State *L, MDBX_envinfo *info)
{
    lua_createtable(L, 0, 7);
    // struct MDBX_envinfo {
    lua_createtable(L, 0, 5);
    // struct {
    /**< Lower limit for datafile size */
    lauxh_pushint2tbl(L, "lower", info->mi_geo.lower);
    /**< Upper limit for datafile size */
    lauxh_pushint2tbl(L, "upper", info->mi_geo.upper);
    /**< Current datafile size */
    lauxh_pushint2tbl(L, "current", info->mi_geo.current);
    /**< Shrink threshold for datafile */
    lauxh_pushint2tbl(L, "shrink", info->mi_geo.shrink);
    /**< Growth step for datafile */
    lauxh_pushint2tbl(L, "grow", info->mi_geo.grow);
    // } mi_geo;
    lua_setfield(L, -2, "mi_geo");

    /**< Size of the data memory map */
    lauxh_pushint2tbl(L, "mi_mapsize", info->mi_mapsize);
    /**< Number of the last used page */
    lauxh_pushint2tbl(L, "mi_last_pgno", info->mi_last_pgno);
    /**< ID of the last committed transaction */
    lauxh_pushint2tbl(L, "mi_recent_txnid", info->mi_recent_txnid);
    /**< ID of the last reader transaction */
    lauxh_pushint2tbl(L, "mi_latter_reader_txnid",
                      info->mi_latter_reader_txnid);
    /**< ID of the last reader transaction of caller process */
    lauxh_pushint2tbl(L, "mi_self_latter_reader_txnid",
                      info->mi_self_latter_reader_txnid);

    lauxh_pushint2tbl(L, "mi_meta0_txnid", info->mi_meta0_txnid);
    lauxh_pushint2tbl(L, "mi_meta0_sign", info->mi_meta0_sign);
    lauxh_pushint2tbl(L, "mi_meta1_txnid", info->mi_meta1_txnid);
    lauxh_pushint2tbl(L, "mi_meta1_sign", info->mi_meta1_sign);
    lauxh_pushint2tbl(L, "mi_meta2_txnid", info->mi_meta2_txnid);
    lauxh_pushint2tbl(L, "mi_meta2_sign", info->mi_meta2_sign);
    /**< Total reader slots in the environment */
    lauxh_pushint2tbl(L, "mi_maxreaders", info->mi_maxreaders);
    /**< Max reader slots used in the environment */
    lauxh_pushint2tbl(L, "mi_numreaders", info->mi_numreaders);
    /**< Database pagesize */
    lauxh_pushint2tbl(L, "mi_dxb_pagesize", info->mi_dxb_pagesize);
    /**< System pagesize */
    lauxh_pushint2tbl(L, "mi_sys_pagesize", info->mi_sys_pagesize);

    /** \brief A mostly unique ID that is regenerated on each boot.

     As such it can be used to identify the local machine's current boot.
     MDBX uses such when open the database to determine whether rollback
     required to the last steady sync point or not. I.e. if current bootid
     is differ from the value within a database then the system was rebooted
     and all changes since last steady sync must be reverted for data
     integrity. Zeros mean that no relevant information is available from
     the system. */
    // struct {
    lua_createtable(L, 0, 4);
    // struct {
    //     uint64_t x, y;
    // }
    lua_createtable(L, 0, 2);
    lauxh_pushint2tbl(L, "x", info->mi_bootid.current.x);
    lauxh_pushint2tbl(L, "y", info->mi_bootid.current.y);
    lua_setfield(L, -2, "current");
    lua_createtable(L, 0, 2);
    lauxh_pushint2tbl(L, "x", info->mi_bootid.meta0.x);
    lauxh_pushint2tbl(L, "y", info->mi_bootid.meta0.y);
    lua_setfield(L, -2, "meta0");
    lua_createtable(L, 0, 2);
    lauxh_pushint2tbl(L, "x", info->mi_bootid.meta1.x);
    lauxh_pushint2tbl(L, "y", info->mi_bootid.meta1.y);
    lua_setfield(L, -2, "meta1");
    lua_createtable(L, 0, 2);
    lauxh_pushint2tbl(L, "x", info->mi_bootid.meta2.x);
    lauxh_pushint2tbl(L, "y", info->mi_bootid.meta2.y);
    lua_setfield(L, -2, "meta2");
    // } mi_bootid;
    lua_setfield(L, -2, "mi_bootid");

    /** Bytes not explicitly synchronized to disk */
    lauxh_pushint2tbl(L, "mi_unsync_volume", info->mi_unsync_volume);
    /** Current auto-sync threshold, see \ref mdbx_env_set_syncbytes(). */
    lauxh_pushint2tbl(L, "mi_autosync_threshold", info->mi_autosync_threshold);
    /** Time since the last steady sync in 1/65536 of second */
    lauxh_pushint2tbl(L, "mi_since_sync_seconds16dot16",
                      info->mi_since_sync_seconds16dot16);
    /** Current auto-sync period in 1/65536 of second,
     * see \ref mdbx_env_set_syncperiod(). */
    lauxh_pushint2tbl(L, "mi_autosync_period_seconds16dot16",
                      info->mi_autosync_period_seconds16dot16);
    /** Time since the last readers check in 1/65536 of second,
     * see \ref mdbx_reader_check(). */
    lauxh_pushint2tbl(L, "mi_since_reader_check_seconds16dot16",
                      info->mi_since_reader_check_seconds16dot16);
    /** Current environment mode.
     * The same as \ref mdbx_env_get_flags() returns. */
    lauxh_pushint2tbl(L, "mi_mode", info->mi_mode);

    /** Statistics of page operations.
     * \details Overall statistics of page operations of all (running,
     * completed and aborted) transactions in the current multi-process
     * session (since the first process opened the database after everyone
     * had previously closed it).
     */
    // struct {
    lua_createtable(L, 0, 8);
    /**< Quantity of a new pages added */
    lauxh_pushint2tbl(L, "newly", info->mi_pgop_stat.newly);
    /**< Quantity of pages copied for update */
    lauxh_pushint2tbl(L, "cow", info->mi_pgop_stat.cow);
    /**< Quantity of parent's dirty pages clones for nested transactions */
    lauxh_pushint2tbl(L, "clone", info->mi_pgop_stat.clone);
    /**< Page splits */
    lauxh_pushint2tbl(L, "split", info->mi_pgop_stat.split);
    /**< Page merges */
    lauxh_pushint2tbl(L, "merge", info->mi_pgop_stat.merge);
    /**< Quantity of spilled dirty pages */
    lauxh_pushint2tbl(L, "spill", info->mi_pgop_stat.spill);
    /**< Quantity of unspilled/reloaded pages */
    lauxh_pushint2tbl(L, "unspill", info->mi_pgop_stat.unspill);
    /**< Number of explicit write operations (not a pages) to a disk */
    lauxh_pushint2tbl(L, "wops", info->mi_pgop_stat.wops);
    // } mi_pgop_stat;
    lua_setfield(L, -2, "mi_pgop_stat");
    // };
}

#define LMDBX_ERRNO_MT "libmdbx.errno"

void lmdbx_errno_init(lua_State *L);

void lmdbx_debug_init(lua_State *L);

#define LMDBX_ENV_MT "libmdbx.env"

typedef struct {
    pid_t pid;
    MDBX_env *env;
} lmdbx_env_t;

void lmdbx_env_init(lua_State *L);
int lmdbx_env_create_lua(lua_State *L);

#define LMDBX_TXN_MT "libmdbx.txn"

typedef struct {
    int env_ref;
    MDBX_dbi dbi;
    MDBX_txn *txn;
} lmdbx_txn_t;

void lmdbx_txn_init(lua_State *L);

#define LMDBX_CURSOR_MT "libmdbx.cursor"

typedef struct {
    int txn_ref;
    MDBX_cursor *cur;
} lmdbx_cursor_t;

void lmdbx_cursor_init(lua_State *L);

#endif
