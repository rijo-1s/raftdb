// Test-only LD_PRELOAD shim: makes fsync/fdatasync fail with EIO while the
// file named by $FSYNC_FAIL_FILE exists. Used by fsync_failure_check.sh.
// Linux only (LD_PRELOAD). Never ship or run this outside tests.
#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <stdlib.h>
#include <unistd.h>

static int should_fail(void) {
    const char *p = getenv("FSYNC_FAIL_FILE");
    return p && access(p, F_OK) == 0;
}
int fsync(int fd) {
    static int (*real)(int);
    if (!real) real = dlsym(RTLD_NEXT, "fsync");
    if (should_fail()) { errno = EIO; return -1; }
    return real(fd);
}
int fdatasync(int fd) {
    static int (*real)(int);
    if (!real) real = dlsym(RTLD_NEXT, "fdatasync");
    if (should_fail()) { errno = EIO; return -1; }
    return real(fd);
}
