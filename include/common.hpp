#include <stdexcept>

[[noreturn]] static inline void die(const char* msg) {
    perror(msg);
    exit(1);
}