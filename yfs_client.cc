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

yfs_client::yfs_client(std::string extent_dst, std::string lock_dst) {
    ec = new extent_client(extent_dst);
    lc = new lock_client(lock_dst);
    if (ec->put(1, "") != extent_protocol::OK)
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

void yfs_client::addmap(std::string& buf, const char* name, inum node) {
    uint32_t tmp = node;
    buf.append(name);
    buf.append(1, 0);
    buf.append((const char*)&tmp, 4);
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

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return 0;
    }
    return a.type;
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

    printf("getfile %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    fin.atime = a.atime;
    fin.mtime = a.mtime;
    fin.ctime = a.ctime;
    fin.size = a.size;
    printf("getfile %016llx -> sz %llu\n", inum, fin.size);

release:
    return r;
}

int yfs_client::getdir(inum inum, dirinfo& din) {
    int r = OK;

    printf("getdir %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }
    din.atime = a.atime;
    din.mtime = a.mtime;
    din.ctime = a.ctime;

release:
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

    extent_protocol::attr attr;
    if ((r = ec->getattr(ino, attr)) != extent_protocol::OK)
        return r;
    if (attr.size == size)
        return r;

    std::string buf;
    if ((r = ec->get(ino, buf)) != extent_protocol::OK)
        return r;

    if (attr.size > size)
        buf.erase(buf.begin() + size, buf.end());
    else
        buf.append(size - attr.size, 0);
    ec->put(ino, buf);

    return r;
}

/*
directory format
|  name  |  node(uint32)  |
*/

int yfs_client::createhelper(inum parent, const char* name, mode_t mode,
                             inum& ino_out, uint32_t type) {
    int r = OK;

    bool found = true;
    if ((r = lookup(parent, name, found, ino_out)) != OK)
        return r;
    if (found)
        return EXIST;

    ec->create(type, ino_out);

    std::string buf;
    if ((r = ec->get(parent, buf)) != extent_protocol::OK)
        return r;
    addmap(buf, name, ino_out);
    if ((r = ec->put(parent, buf)) != extent_protocol::OK)
        return r;

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

int yfs_client::lookup(inum parent, const char* name, bool& found,
                       inum& ino_out) {
    int r = OK;

    std::string buf;
    if ((r = ec->get(parent, buf)) != extent_protocol::OK)
        return r;

    std::list<dirent> list;
    readdir(parent, list);

    found = false;
    for (std::list<dirent>::iterator it = list.begin(); it != list.end();
         ++it) {
        if ((*it).name.compare(name) == 0) {
            found = true;
            ino_out = (*it).inum;
            break;
        }
    }

    return r;
}

int yfs_client::readdir(inum dir, std::list<dirent>& list) {
    int r = OK;

    std::string buf;
    if ((r = ec->get(dir, buf)) != extent_protocol::OK)
        return r;

    buildlist(buf.c_str(), list);

    return r;
}

int yfs_client::read(inum ino, size_t size, off_t off, std::string& data) {
    int r = OK;

    std::string buf;
    if ((r = ec->get(ino, buf)) != extent_protocol::OK)
        return r;
    if (off >= (off_t)buf.size())
        return IOERR;
    data = buf.substr(off, size);
    return r;
}

int yfs_client::write(inum ino, size_t size, off_t off, const char* data,
                      size_t& bytes_written) {
    int r = OK;

    bytes_written = size;
    std::string buf;
    if ((r = ec->get(ino, buf)) != extent_protocol::OK)
        return r;
    if (off > (off_t)buf.size()) {
        buf.append(off - buf.size(), 0);
        bytes_written += off - buf.size();
    }
    buf.replace(buf.begin() + off, buf.begin() + off + size, data, data + size);
    if ((r = ec->put(ino, buf)) != extent_protocol::OK)
        return r;
    return r;
}

int yfs_client::unlink(inum parent, const char* name) {
    int r = OK;

    inum ino;
    bool found = true;
    if ((r = lookup(parent, name, found, ino)) != OK)
        return r;
    if (!found)
        return NOENT;

    ec->remove(ino);

    std::string buf;
    if ((r = ec->get(parent, buf)) != extent_protocol::OK)
        return r;
    deletemap(buf, name);
    if ((r = ec->put(parent, buf)) != extent_protocol::OK)
        return r;

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