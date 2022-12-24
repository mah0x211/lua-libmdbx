TARGET=libmdbx.$(LIB_EXTENSION)
SRCS=$(wildcard src/*.c)
OBJS=$(SRCS:.c=.o)
GCDAS=$(OBJS:.o=.gcda)
INSTALL?=install

ifdef MDBX_COVERAGE
COVFLAGS=--coverage
endif

.PHONY: all install clean

all: $(TARGET)

%.o: %.c
	$(CC) $(CFLAGS) $(WARNINGS) $(COVFLAGS) $(CPPFLAGS) -o $@ -c $<

$(TARGET): $(OBJS)
	$(CC) -o $@ $^ $(LIBFLAG) $(LIBS) $(COVFLAGS)

install:
	$(INSTALL) $(TARGET) $(INST_LIBDIR)
	rm -f ./src/*.o
	rm -f ./*.so

clean:
	rm -f $(OBJS) $(TARGET) $(GCDAS)
