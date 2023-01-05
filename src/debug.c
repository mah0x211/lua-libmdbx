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
#include <stdarg.h>
#include <stdio.h>

static lua_State *gL  = NULL;
static int gLoggerRef = LUA_NOREF;

static inline const char *loglevel2str(MDBX_log_level_t loglv)
{
    switch (loglv) {
    case MDBX_LOG_FATAL:
        return "LOG_FATAL";
    case MDBX_LOG_ERROR:
        return "LOG_ERROR";
    case MDBX_LOG_WARN:
        return "LOG_WARN";
    case MDBX_LOG_NOTICE:
        return "LOG_NOTICE";
    case MDBX_LOG_VERBOSE:
        return "LOG_VERBOSE";
    case MDBX_LOG_DEBUG:
        return "LOG_DEBUG";
    case MDBX_LOG_TRACE:
        return "LOG_TRACE";
    case MDBX_LOG_EXTRA:
        return "LOG_EXTRA";
    default:
        return NULL;
    }
}

static void debug_func(MDBX_log_level_t loglv, const char *function, int line,
                       const char *fmt, va_list args)
{
    char msg[BUFSIZ] = {0};
    int len          = vsnprintf(msg, BUFSIZ, fmt, args);
    if (len < 0) {
        len = snprintf(msg, BUFSIZ,
                       "failed to formating the debug message[%d]: %s", errno,
                       strerror(errno));
    }

    // call the debug function
    lauxh_pushref(gL, gLoggerRef);
    lua_pushstring(gL, loglevel2str(loglv));
    lua_pushstring(gL, function);
    lua_pushinteger(gL, line);
    lua_pushlstring(gL, msg, len);
    if (lua_pcall(gL, 4, 0, 0)) {
        fprintf(stderr, "failed to call the debug funciton: %s",
                lua_tostring(gL, -1));
    }
}

static int setup_lua(lua_State *L)
{
    int top                 = lua_gettop(L);
    lua_Integer loglv       = MDBX_LOG_DONTCHANGE;
    lua_Integer dbgflgs     = MDBX_DBG_DONTCHANGE;
    MDBX_debug_func *logger = MDBX_LOGGER_DONTCHANGE;
    int rv                  = 0;

    if (top > 0) {
        if (lua_isnoneornil(L, 1)) {
            gL         = NULL;
            gLoggerRef = lauxh_unref(L, gLoggerRef);
            logger     = NULL;
        } else {
            luaL_checktype(L, 1, LUA_TFUNCTION);
            gL         = L;
            gLoggerRef = lauxh_unref(L, gLoggerRef);
            gLoggerRef = lauxh_refat(L, 1);
            logger     = debug_func;
        }
    }
    if (!lua_isnoneornil(L, 2)) {
        loglv = lauxh_optinteger(L, 2, MDBX_LOG_DONTCHANGE);
    }
    if (top >= 3) {
        dbgflgs = lauxh_optflags(L, 3);
    }
    rv = mdbx_setup_debug(loglv, dbgflgs, logger);

    // previous log level
    loglv = (rv >> 16) & 0xFFFF;
    lua_pushstring(L, loglevel2str(loglv));

    // previous debug flags
    dbgflgs = rv & 0xFF;
    rv      = 1;
    switch (dbgflgs) {
    case MDBX_DBG_NONE:
        lua_pushliteral(L, "DBG_NONE");
        rv++;
        break;
    default:
        if (dbgflgs & MDBX_DBG_ASSERT) {
            lua_pushliteral(L, "DBG_ASSERT");
            rv++;
        }
        if (dbgflgs & MDBX_DBG_AUDIT) {
            lua_pushliteral(L, "DBG_AUDIT");
            rv++;
        }
        if (dbgflgs & MDBX_DBG_JITTER) {
            lua_pushliteral(L, "DBG_JITTER");
            rv++;
        }
        if (dbgflgs & MDBX_DBG_DUMP) {
            lua_pushliteral(L, "DBG_DUMP");
            rv++;
        }
        if (dbgflgs & MDBX_DBG_LEGACY_MULTIOPEN) {
            lua_pushliteral(L, "DBG_LEGACY_MULTIOPEN");
            rv++;
        }
        if (dbgflgs & MDBX_DBG_LEGACY_OVERLAP) {
            lua_pushliteral(L, "DBG_LEGACY_OVERLAP");
            rv++;
        }
        if (dbgflgs & MDBX_DBG_DONT_UPGRADE) {
            lua_pushliteral(L, "DBG_DONT_UPGRADE");
            rv++;
        }
    }
    return rv;
}

void lmdbx_debug_init(lua_State *L)
{
    struct luaL_Reg funcs[] = {
        {"setup", setup_lua},
        {NULL,    NULL     }
    };

    lua_newtable(L);
    lmdbx_register(L, funcs);
}
