// yfs client.  implements FS operations using extent and lock server
#include "yfs_client.h"
#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <ctime>

yfs_client::yfs_client(std::string extent_dst, std::string lock_dst) {
    ec = new extent_client(extent_dst);
    lc = new lock_client_cache(lock_dst, this);
    if (ec_put(1, "") != extent_protocol::OK)
        printf("error init root dir\n");  // XYB: init root dir
}

yfs_client::inum yfs_client::n2i(std::string n) {
    std::istringstream ist(n);
    unsigned long long finum;
    ist >> finum;
    return finum;
}

std::string yfs_client::filename(inum inum) {
    std::ostringstream ost;
    ost << inum;
    return ost.str();
}

void yfs_client::buildlist(const char* buf, std::list<dirent>& list) {
    dirent dir;
    int size;
    while ((size = nextmap(buf, dir)) != 0) {
        list.push_back(dir);
        buf += size;
    }
}

int yfs_client::nextmap(const char* buf, dirent& dir) {
    std::string name;
    yfs_client::inum inum;
    const char* now = buf;

    while (*now != '\0') {
        name.push_back(*now);
        ++now;
    }
    ++now;
    if (name.empty())
        return 0;

    inum = *((uint32_t*)now);
    now += 4;

    dir.name = name;
    dir.inum = inum;
    return now - buf;
}

bool yfs_client::addmap(std::string& buf, const char* name, inum node) {
    if (buf.size() + strlen(name) + 5 >=
        (NDIRECT + NINDIRECT) * BLOCK_SIZE)  // last always 0
        return false;
    uint32_t tmp = node;
    buf.append(name);
    buf.append(1, 0);
    buf.append((const char*)&tmp, 4);
    return true;
}

void yfs_client::deletemap(std::string& buf, const char* name) {
    dirent tmp;
    const char* now = buf.c_str();
    int size;
    while ((size = nextmap(now, tmp)) != 0) {
        if (tmp.name == name) {
            buf.erase(now - buf.c_str(), size);
            break;
        }
        now += size;
    }
}

int yfs_client::checktype(inum inum) {
    extent_protocol::attr a;
    int ret = 0;
    lc->acquire(inum);

    if (ec_getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        goto RET;
    }
    ret = a.type;
RET:
    lc->release(inum);
    return ret;
}

bool yfs_client::isfile(inum inum) {
    return checktype(inum) == extent_protocol::T_FILE;
}

bool yfs_client::isdir(inum inum) {
    return checktype(inum) == extent_protocol::T_DIR;
}

bool yfs_client::issymlink(inum inum) {
    return checktype(inum) == extent_protocol::T_SYMLINK;
}

int yfs_client::getfile(inum inum, fileinfo& fin) {
    int r = OK;
    lc->acquire(inum);

    printf("getfile %016llx\n", inum);
    extent_protocol::attr a;
    if (ec_getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto RET;
    }

    fin.atime = a.atime;
    fin.mtime = a.mtime;
    fin.ctime = a.ctime;
    fin.size = a.size;
    printf("getfile %016llx -> sz %llu\n", inum, fin.size);

RET:
    lc->release(inum);
    return r;
}

int yfs_client::getdir(inum inum, dirinfo& din) {
    int r = OK;
    lc->acquire(inum);

    printf("getdir %016llx\n", inum);
    extent_protocol::attr a;
    if (ec_getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto RET;
    }
    din.atime = a.atime;
    din.mtime = a.mtime;
    din.ctime = a.ctime;

RET:
    lc->release(inum);
    return r;
}

#define EXT_RPC(xx)                                                \
    do {                                                           \
        if ((xx) != extent_protocol::OK) {                         \
            printf("EXT_RPC Error: %s:%d \n", __FILE__, __LINE__); \
            r = IOERR;                                             \
            goto release;                                          \
        }                                                          \
    } while (0)

// Only support set size of attr
int yfs_client::setattr(inum ino, size_t size) {
    int r = OK;
    lc->acquire(ino);

    extent_protocol::attr attr;
    std::string buf;
    if ((r = ec_getattr(ino, attr)) != extent_protocol::OK)
        goto RET;
    if (attr.size == size)
        goto RET;

    if ((r = ec_get(ino, buf)) != extent_protocol::OK)
        goto RET;

    if (attr.size > size)
        buf.erase(buf.begin() + size, buf.end());
    else
        buf.append(size - attr.size, 0);
    ec_put(ino, buf);

RET:
    lc->release(ino);
    return r;
}

/*
directory format
|  name  |  node(uint32)  |
*/

int yfs_client::createhelper(inum parent, const char* name, mode_t mode,
                             inum& ino_out, uint32_t type) {
    int r = OK;
    lc->acquire(parent);

    bool found = true;
    std::string buf;
    if ((r = lookup_nl(parent, name, found, ino_out)) != OK)
        goto RET;
    if (found) {
        r = EXIST;
        goto RET;
    }

    ec_create(type, ino_out);

    if ((r = ec_get(parent, buf)) != extent_protocol::OK)
        goto RET;

    if (!addmap(buf, name, ino_out)) {
        r = IOERR;
        goto RET;
    }

    if ((r = ec_put(parent, buf)) != extent_protocol::OK)
        goto RET;

RET:
    lc->release(parent);
    return r;
}

int yfs_client::create(inum parent, const char* name, mode_t mode,
                       inum& ino_out) {
    return createhelper(parent, name, mode, ino_out, extent_protocol::T_FILE);
}

int yfs_client::mkdir(inum parent, const char* name, mode_t mode,
                      inum& ino_out) {
    return createhelper(parent, name, mode, ino_out, extent_protocol::T_DIR);
}

int yfs_client::lookup_nl(inum parent, const char* name, bool& found,
                          inum& ino_out) {
    int r = OK;

    std::list<dirent> list;
    readdir_nl(parent, list);

    found = false;
    for (std::list<dirent>::iterator it = list.begin(); it != list.end();
         ++it) {
        if ((*it).name == name) {
            found = true;
            ino_out = (*it).inum;
            break;
        }
    }

    return r;
}

int yfs_client::readdir_nl(inum dir, std::list<dirent>& list) {
    int r = OK;

    std::string buf;
    if ((r = ec_get(dir, buf)) != extent_protocol::OK)
        return r;

    buildlist(buf.c_str(), list);

    return r;
}

int yfs_client::lookup(inum parent, const char* name, bool& found,
                       inum& ino_out) {
    lc->acquire(parent);
    int ret = lookup_nl(parent, name, found, ino_out);
    lc->release(parent);
    return ret;
}

int yfs_client::readdir(inum dir, std::list<dirent>& list) {
    lc->acquire(dir);
    int ret = readdir_nl(dir, list);
    lc->release(dir);
    return ret;
}

int yfs_client::read(inum ino, size_t size, off_t off, std::string& data) {
    int r = OK;
    lc->acquire(ino);

    std::string buf;
    if ((r = ec_get(ino, buf)) != extent_protocol::OK)
        goto RET;
    if (off >= (off_t)buf.size()) {
        r = IOERR;
        goto RET;
    }
    data = buf.substr(off, size);

RET:
    lc->release(ino);
    return r;
}

int yfs_client::write(inum ino, size_t size, off_t off, const char* data,
                      size_t& bytes_written) {
    int r = OK;
    lc->acquire(ino);

    bytes_written = size;
    std::string buf;
    if ((r = ec_get(ino, buf)) != extent_protocol::OK)
        goto RET;

    if (off > (off_t)buf.size()) {
        buf.append(off - buf.size(), 0);
        bytes_written += off - buf.size();
    }
    buf.replace(buf.begin() + off, buf.begin() + off + size, data, data + size);
    if ((r = ec_put(ino, buf)) != extent_protocol::OK)
        goto RET;

RET:
    lc->release(ino);
    return r;
}

int yfs_client::unlink(inum parent, const char* name) {
    int r = OK;
    lc->acquire(parent);

    inum ino;
    bool found = true;
    std::string buf;
    if ((r = lookup_nl(parent, name, found, ino)) != extent_protocol::OK)
        goto RET;
    if (!found) {
        r = NOENT;
        goto RET;
    }

    lc->acquire(ino);
    ec_remove(ino);
    lc->release(ino);

    if ((r = ec_get(parent, buf)) != extent_protocol::OK)
        goto RET;
    deletemap(buf, name);
    if ((r = ec_put(parent, buf)) != extent_protocol::OK)
        goto RET;

RET:
    lc->release(parent);
    return r;
}

int yfs_client::symlink(inum parent, const char* link, const char* name,
                        inum& ino_out) {
    int r = OK;

    if ((r = createhelper(parent, name, 777, ino_out,
                          extent_protocol::T_SYMLINK)) != OK)
        return r;

    size_t tmp;
    if ((r = write(ino_out, strlen(link), 0, link, tmp)) != OK)
        return r;

    return r;
}

int yfs_client::readlink(inum ino, std::string& buf) {
    int r = OK;

    if ((r = read(ino, 4096, 0, buf)) != OK)
        return r;

    return r;
}

yfs_client::cache_entry* yfs_client::find_cache(extent_protocol::extentid_t eid,
                                                cache_type type) {
    for (std::vector<cache_entry>::iterator it = cache.begin();
         it != cache.end(); ++it)
        if (it->eid == eid && it->type == type)
            return &(*it);
    return NULL;
}

extent_protocol::status yfs_client::ec_create(
    uint32_t type, extent_protocol::extentid_t& eid) {
    extent_protocol::status ret = ec->create(type, eid);
    if (ret == extent_protocol::OK) {
        extent_protocol::attr a;
        cache_entry newentry;
        a.type = type;
        int tm = std::time(0);
        a.mtime = tm;
        a.ctime = tm;
        a.atime = tm;
        newentry.eid = eid;
        newentry.type = CACHE_ATTR;
        newentry.attr = a;
        newentry.modified = false;
        cache.push_back(newentry);

        newentry.eid = eid;
        newentry.type = CACHE_DATA;
        newentry.data = "";
        newentry.modified = false;
        cache.push_back(newentry);
    }
    return ret;
}

extent_protocol::status yfs_client::ec_get(extent_protocol::extentid_t eid,
                                           std::string& buf) {
    cache_entry* entry = find_cache(eid, CACHE_DATA);
    if (entry) {
        buf = entry->data;
        return extent_protocol::OK;
    } else {
        extent_protocol::status ret = ec->get(eid, buf);
        if (ret == extent_protocol::OK) {
            cache_entry newentry;
            newentry.eid = eid;
            newentry.type = CACHE_DATA;
            newentry.data = buf;
            newentry.modified = false;
            cache.push_back(newentry);
        }
        return ret;
    }
}

extent_protocol::status yfs_client::ec_getattr(extent_protocol::extentid_t eid,
                                               extent_protocol::attr& a) {
    cache_entry* entry = find_cache(eid, CACHE_ATTR);
    if (entry) {
        a = entry->attr;
        return extent_protocol::OK;
    } else {
        extent_protocol::status ret = ec->getattr(eid, a);
        if (ret == extent_protocol::OK) {
            cache_entry newentry;
            newentry.eid = eid;
            newentry.type = CACHE_ATTR;
            newentry.attr = a;
            newentry.modified = false;
            cache.push_back(newentry);
        }
        return ret;
    }
}

extent_protocol::status yfs_client::ec_put(extent_protocol::extentid_t eid,
                                           std::string buf) {
    cache_entry* entry = find_cache(eid, CACHE_DATA);
    if (entry) {
        entry->data = buf;
        entry->modified = true;
    } else {
        cache_entry newentry;
        newentry.eid = eid;
        newentry.type = CACHE_DATA;
        newentry.data = buf;
        newentry.modified = true;
        cache.push_back(newentry);
    }
    entry = find_cache(eid, CACHE_ATTR);
    if (entry) {
        entry->attr.size = buf.size();
        int tm = std::time(0);
        entry->attr.mtime = tm;
        entry->attr.ctime = tm;
    }
    return extent_protocol::OK;
}

extent_protocol::status yfs_client::ec_remove(extent_protocol::extentid_t eid) {
    clear_cache(eid);
    deleted.push_back(eid);
    return ec->remove(eid);
}

void yfs_client::clear_cache(extent_protocol::extentid_t eid) {
    std::vector<cache_entry>::iterator it = cache.begin();
    while (it != cache.end())
        if (it->eid == eid)
            it = cache.erase(it);
        else
            ++it;
}

std::vector<extent_protocol::extentid_t> yfs_client::flush_cache(
    extent_protocol::extentid_t eid) {
    std::vector<extent_protocol::extentid_t> ret = deleted;
    std::vector<cache_entry>::iterator it = cache.begin();
    while (it != cache.end()) {
        if (it->eid == eid && it->type == CACHE_DATA && it->modified) {
            ec->put(eid, it->data);
            ret.push_back(eid);
            it = cache.erase(it);
        } else {
            ++it;
        }
    }
    deleted.clear();
    return ret;
}
