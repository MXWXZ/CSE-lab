// RPC stubs for clients to talk to lock_server, and cache the locks
// see lock_client.cache.h for protocol details.

#include "lock_client_cache.h"
#include "rpc.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include "tprintf.h"

int lock_client_cache::last_port = 0;

lock_client_cache::lock_client_cache(std::string xdst,
                                     class lock_release_user* _lu)
    : lock_client(xdst), lu(_lu) {
    srand(time(NULL) ^ last_port);
    rlock_port = ((rand() % 32000) | (0x1 << 10));
    const char* hname;
    // VERIFY(gethostname(hname, 100) == 0);
    hname = "127.0.0.1";
    std::ostringstream host;
    host << hname << ":" << rlock_port;
    id = host.str();
    last_port = rlock_port;
    rpcs* rlsrpc = new rpcs(rlock_port);
    rlsrpc->reg(rlock_protocol::revoke, this,
                &lock_client_cache::revoke_handler);
    rlsrpc->reg(rlock_protocol::retry, this, &lock_client_cache::retry_handler);
    pthread_mutex_init(&mutex, NULL);
    pthread_cond_init(&cond, NULL);
}

lock_client_cache::~lock_client_cache() {
    pthread_mutex_destroy(&mutex);
    pthread_cond_destroy(&cond);
}

lock_protocol::status lock_client_cache::acquire(lock_protocol::lockid_t lid) {
    lock_protocol::status ret = lock_protocol::OK;
    int r;

    pthread_mutex_lock(&mutex);
    while (lock[lid].lock_status != FREE && lock[lid].lock_status != NONE)
        pthread_cond_wait(&cond, &mutex);
    if (lock[lid].lock_status == FREE) {
        lock[lid].lock_status = LOCKED;
        goto END;
    }
    lock[lid].lock_status = ACQUIRING;
    pthread_mutex_unlock(&mutex);
    ret = cl->call(lock_protocol::acquire, lid, id, r);
    pthread_mutex_lock(&mutex);
    while (ret != lock_protocol::OK && lock[lid].lock_status == ACQUIRING)
        pthread_cond_wait(&cond, &mutex);
    lock[lid].lock_status = LOCKED;

END:
    pthread_mutex_unlock(&mutex);
    return ret;
}

lock_protocol::status lock_client_cache::release(lock_protocol::lockid_t lid) {
    lock_protocol::status ret = lock_protocol::OK;
    // fprintf(stderr, "%s => %X: release %d status %d\n", id.c_str(),
    //         pthread_self(), lid, lock[lid].revoked);

    pthread_mutex_lock(&mutex);
    if (lock[lid].revoked) {
        int r;
        lock[lid].lock_status = RELEASING;
        pthread_mutex_unlock(&mutex);
        ret = cl->call(lock_protocol::release, lid, id, r);
        pthread_mutex_lock(&mutex);
        lock[lid].lock_status = NONE;
        lock[lid].revoked = false;
    } else {
        lock[lid].lock_status = FREE;
    }

    pthread_cond_signal(&cond);
    pthread_mutex_unlock(&mutex);
    return ret;
}

#include <unistd.h>
rlock_protocol::status lock_client_cache::revoke_handler(
    lock_protocol::lockid_t lid, int&) {
    usleep(100000);  // for least lock...
    pthread_mutex_lock(&mutex);
    // fprintf(stderr, "%s => %X: revoke %d, status %d\n", id.c_str(),
    //         pthread_self(), lid, lock[lid].lock_status);
    lock[lid].revoked = true;
    if (lock[lid].lock_status == FREE) {
        pthread_mutex_unlock(&mutex);
        release(lid);
    } else {
        pthread_mutex_unlock(&mutex);
    }
    return rlock_protocol::OK;
}

rlock_protocol::status lock_client_cache::retry_handler(
    lock_protocol::lockid_t lid, int&) {
    pthread_mutex_lock(&mutex);
    // fprintf(stderr, "%s => %X: retry %d\n", id.c_str(), pthread_self(), lid);
    lock[lid].lock_status = LOCKED;
    pthread_cond_signal(&cond);
    pthread_mutex_unlock(&mutex);
    return rlock_protocol::OK;
}
