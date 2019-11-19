#ifndef lock_server_cache_h
#define lock_server_cache_h

#include <string>

#include <map>
#include <queue>
#include "lock_protocol.h"
#include "rpc.h"
#include "lock_server.h"

class lock_server_cache {
private:
    struct lock_info {
        bool locked;
        std::string id;
        std::queue<std::string> waiting;
    };

    pthread_mutex_t mutex;
    int nacquire;
    std::map<lock_protocol::lockid_t, lock_info> lock;
    std::map<std::string, bool> conn;

public:
    lock_server_cache();
    ~lock_server_cache();
    lock_protocol::status stat(lock_protocol::lockid_t, int&);
    int acquire(lock_protocol::lockid_t, std::string, int&);
    int release(lock_protocol::lockid_t, std::string, int&);
    int flush(std::string, std::string, int&);
};

#endif
