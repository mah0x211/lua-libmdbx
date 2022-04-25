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

static int tostring_lua(lua_State *L)
{
    if (!lauxh_ismetatableof(L, 1, LMDBX_ERRNO_MT)) {
        lauxh_argerror(L, 1, LMDBX_ERRNO_MT " expected, got %s",
                       luaL_typename(L, 1));
    }
    lua_pushliteral(L, "message");
    lua_rawget(L, 1);
    return 1;
}

static void register_errno(lua_State *L, const char *name, int errnum)
{
    lua_createtable(L, 0, 3);
    lauxh_pushstr2tbl(L, "name", name);
    lauxh_pushint2tbl(L, "errno", errnum);
    lauxh_pushstr2tbl(L, "message", mdbx_strerror(errnum));
    lauxh_setmetatable(L, LMDBX_ERRNO_MT);
    lua_pushvalue(L, -1);
    lua_setfield(L, -3, name);
    lua_rawseti(L, -2, errnum);
}

void lmdbx_errno_init(lua_State *L)
{
    struct luaL_Reg mmethod[] = {
        {"__tostring", tostring_lua},
        {NULL,         NULL        }
    };

    // create metatable
    luaL_newmetatable(L, LMDBX_ERRNO_MT);
    // metamethods
    lmdbx_register(L, mmethod);
    lua_pop(L, 1);

    // Errors and return codes
    lua_newtable(L);
    register_errno(L, "SUCCESS", MDBX_SUCCESS);
    register_errno(L, "RESULT_FALSE", MDBX_RESULT_FALSE);
    register_errno(L, "RESULT_TRUE", MDBX_RESULT_TRUE);
    register_errno(L, "KEYEXIST", MDBX_KEYEXIST);
    register_errno(L, "FIRST_LMDB_ERRCODE", MDBX_FIRST_LMDB_ERRCODE);
    register_errno(L, "NOTFOUND", MDBX_NOTFOUND);
    register_errno(L, "PAGE_NOTFOUND", MDBX_PAGE_NOTFOUND);
    register_errno(L, "CORRUPTED", MDBX_CORRUPTED);
    register_errno(L, "PANIC", MDBX_PANIC);
    register_errno(L, "VERSION_MISMATCH", MDBX_VERSION_MISMATCH);
    register_errno(L, "INVALID", MDBX_INVALID);
    register_errno(L, "MAP_FULL", MDBX_MAP_FULL);
    register_errno(L, "DBS_FULL", MDBX_DBS_FULL);
    register_errno(L, "READERS_FULL", MDBX_READERS_FULL);
    register_errno(L, "TXN_FULL", MDBX_TXN_FULL);
    register_errno(L, "CURSOR_FULL", MDBX_CURSOR_FULL);
    register_errno(L, "PAGE_FULL", MDBX_PAGE_FULL);
    register_errno(L, "UNABLE_EXTEND_MAPSIZE", MDBX_UNABLE_EXTEND_MAPSIZE);
    register_errno(L, "INCOMPATIBLE", MDBX_INCOMPATIBLE);
    register_errno(L, "BAD_RSLOT", MDBX_BAD_RSLOT);
    register_errno(L, "BAD_TXN", MDBX_BAD_TXN);
    register_errno(L, "BAD_VALSIZE", MDBX_BAD_VALSIZE);
    register_errno(L, "BAD_DBI", MDBX_BAD_DBI);
    register_errno(L, "PROBLEM", MDBX_PROBLEM);
    register_errno(L, "LAST_LMDB_ERRCODE", MDBX_LAST_LMDB_ERRCODE);
    register_errno(L, "BUSY", MDBX_BUSY);
    register_errno(L, "FIRST_ADDED_ERRCODE", MDBX_FIRST_ADDED_ERRCODE);
    register_errno(L, "EMULTIVAL", MDBX_EMULTIVAL);
    register_errno(L, "EBADSIGN", MDBX_EBADSIGN);
    register_errno(L, "WANNA_RECOVERY", MDBX_WANNA_RECOVERY);
    register_errno(L, "EKEYMISMATCH", MDBX_EKEYMISMATCH);
    register_errno(L, "TOO_LARGE", MDBX_TOO_LARGE);
    register_errno(L, "THREAD_MISMATCH", MDBX_THREAD_MISMATCH);
    register_errno(L, "TXN_OVERLAPPING", MDBX_TXN_OVERLAPPING);
    register_errno(L, "LAST_ADDED_ERRCODE", MDBX_LAST_ADDED_ERRCODE);
#if defined(_WIN32) || defined(_WIN64)
    register_errno(L, "ENODATA", MDBX_ENODATA);
    register_errno(L, "EINVAL", MDBX_EINVAL);
    register_errno(L, "EACCESS", MDBX_EACCESS);
    register_errno(L, "ENOMEM", MDBX_ENOMEM);
    register_errno(L, "EROFS", MDBX_EROFS);
    register_errno(L, "ENOSYS", MDBX_ENOSYS);
    register_errno(L, "EIO", MDBX_EIO);
    register_errno(L, "EPERM", MDBX_EPERM);
    register_errno(L, "EINTR", MDBX_EINTR);
    register_errno(L, "ENOFILE", MDBX_ENOFILE);
    register_errno(L, "EREMOTE", MDBX_EREMOTE);
#else /* Windows */
# ifdef ENODATA
    register_errno(L, "ENODATA", MDBX_ENODATA);
# else
    // for compatibility with LLVM's C++ libraries/headers
    register_errno(L, "ENODATA", MDBX_ENODATA);
# endif /* ENODATA */
    register_errno(L, "EINVAL", MDBX_EINVAL);
    register_errno(L, "EACCESS", MDBX_EACCESS);
    register_errno(L, "ENOMEM", MDBX_ENOMEM);
    register_errno(L, "EROFS", MDBX_EROFS);
    register_errno(L, "ENOSYS", MDBX_ENOSYS);
    register_errno(L, "EIO", MDBX_EIO);
    register_errno(L, "EPERM", MDBX_EPERM);
    register_errno(L, "EINTR", MDBX_EINTR);
    register_errno(L, "ENOFILE", MDBX_ENOFILE);
    register_errno(L, "EREMOTE", MDBX_EREMOTE);
#endif  /* !Windows */
}
