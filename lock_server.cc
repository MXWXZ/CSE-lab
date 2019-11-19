// the lock server implementation

#include "lock_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>

lock_server::lock_server() : nacquire(0) {
    pthread_mutex_init(&mutex, NULL);
    pthread_cond_init(&cond, NULL);
}

lock_server::~lock_server() {
    pthread_mutex_destroy(&mutex);
    pthread_cond_destroy(&cond);
}

lock_protocol::status lock_server::stat(int clt, lock_protocol::lockid_t lid,
                                        int& r) {
    lock_protocol::status ret = lock_protocol::OK;
    printf("stat request from clt %d\n", clt);
    r = nacquire;
    return ret;
}

lock_protocol::status lock_server::acquire(int clt, lock_protocol::lockid_t lid,
                                           int& r) {
    lock_protocol::status ret = lock_protocol::OK;
    pthread_mutex_lock(&mutex);
    while (lock[lid])
        pthread_cond_wait(&cond, &mutex);

    lock[lid] = true;
    ++nacquire;
    pthread_mutex_unlock(&mutex);
    return ret;
}

lock_protocol::status lock_server::release(int clt, lock_protocol::lockid_t lid,
                                           int& r) {
    lock_protocol::status ret = lock_protocol::OK;
    pthread_mutex_lock(&mutex);

    if (lock[lid]) {
        --nacquire;
        lock[lid] = false;
    }

    pthread_cond_signal(&cond);
    pthread_mutex_unlock(&mutex);
    return ret;
}
