// the caching lock server implementation

#include "lock_server_cache.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>
#include "lang/verify.h"
#include "handle.h"
#include "tprintf.h"

lock_server_cache::lock_server_cache() {
    pthread_mutex_init(&mutex, NULL);
    nacquire = 0;
}

lock_server_cache::~lock_server_cache() { pthread_mutex_destroy(&mutex); }

int lock_server_cache::acquire(lock_protocol::lockid_t lid, std::string id,
                               int&) {
    lock_protocol::status ret = lock_protocol::OK;
    pthread_mutex_lock(&mutex);
    if (lock[lid].locked) {
        std::string revokeid = lock[lid].id;
        if (!lock[lid].waiting.empty())
            revokeid = lock[lid].waiting.back();
        lock[lid].waiting.push(id);
        // fprintf(stderr, "%s => %X: acquire %d, revoke %s\n", id.c_str(),
        //         pthread_self(), lid, revokeid.c_str());
        pthread_mutex_unlock(&mutex);
        // revoke
        handle h(revokeid);
        rpcc* cl = h.safebind();
        int r;
        if (cl) {
            ret = cl->call(rlock_protocol::revoke, lid, r);
        } else {
            return lock_protocol::RPCERR;
        }
        return lock_protocol::RETRY;
    }

    // fprintf(stderr, "%s => %X: acquired %d\n", id.c_str(), pthread_self(),
    // lid);
    lock[lid].locked = true;
    lock[lid].id = id;
    ++nacquire;
    pthread_mutex_unlock(&mutex);
    return ret;
}

int lock_server_cache::release(lock_protocol::lockid_t lid, std::string id,
                               int& r) {
    lock_protocol::status ret = lock_protocol::OK;
    pthread_mutex_lock(&mutex);

    if (!lock[lid].waiting.empty()) {
        std::string lockid = lock[lid].waiting.front();
        lock[lid].id = lockid;
        lock[lid].waiting.pop();
        // fprintf(stderr, "%s => %X: release %d, next %s\n", id.c_str(),
        //         pthread_self(), lid, lockid.c_str());
        pthread_mutex_unlock(&mutex);

        handle h(lockid);
        rpcc* cl = h.safebind();
        int r;
        if (cl) {
            ret = cl->call(rlock_protocol::retry, lid, r);
        } else {
            return lock_protocol::RPCERR;
        }
        return ret;
    } else {
        // fprintf(stderr, "%s => %X: release %d\n", id.c_str(), pthread_self(),
        //         lid);
        if (lock[lid].locked)
            --nacquire;
        lock[lid].locked = false;
        lock[lid].id.clear();
    }

    pthread_mutex_unlock(&mutex);
    return ret;
}

lock_protocol::status lock_server_cache::stat(lock_protocol::lockid_t lid,
                                              int& r) {
    tprintf("stat request\n");
    r = nacquire;
    return lock_protocol::OK;
}
