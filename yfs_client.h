#ifndef yfs_client_h
#define yfs_client_h

#include <string>

#include "lock_protocol.h"
#include "lock_client_cache.h"

//#include "yfs_protocol.h"
#include "extent_client.h"
#include <vector>

class lock_client_cache;

class yfs_client {
    extent_client* ec;
    lock_client_cache* lc;

public:
    typedef unsigned long long inum;
    enum xxstatus { OK, RPCERR, NOENT, IOERR, EXIST };
    typedef int status;

    struct fileinfo {
        unsigned long long size;
        unsigned long atime;
        unsigned long mtime;
        unsigned long ctime;
    };
    struct dirinfo {
        unsigned long atime;
        unsigned long mtime;
        unsigned long ctime;
    };
    struct dirent {
        std::string name;
        yfs_client::inum inum;
    };

private:
    static std::string filename(inum);
    static inum n2i(std::string);
    static void buildlist(const char*, std::list<dirent>&);
    static int nextmap(const char*, dirent&);
    static bool addmap(std::string&, const char*, inum);
    static void deletemap(std::string&, const char*);
    int createhelper(inum, const char*, mode_t, inum&, uint32_t);
    int lookup_nl(inum, const char*, bool&, inum&);
    int readdir_nl(inum, std::list<dirent>&);

private:
    enum cache_type { CACHE_DATA, CACHE_ATTR };
    struct cache_entry {
        yfs_client::cache_type type;
        extent_protocol::extentid_t eid;
        extent_protocol::attr attr;
        std::string data;
        bool modified;
    };

    std::vector<cache_entry> cache;
    std::vector<extent_protocol::extentid_t> deleted;
    cache_entry* find_cache(extent_protocol::extentid_t eid, cache_type type);

    extent_protocol::status ec_create(uint32_t type,
                                      extent_protocol::extentid_t& eid);
    extent_protocol::status ec_get(extent_protocol::extentid_t eid,
                                   std::string& buf);
    extent_protocol::status ec_getattr(extent_protocol::extentid_t eid,
                                       extent_protocol::attr& a);
    extent_protocol::status ec_put(extent_protocol::extentid_t eid,
                                   std::string buf);
    extent_protocol::status ec_remove(extent_protocol::extentid_t eid);

public:
    void clear_cache(extent_protocol::extentid_t eid);
    std::vector<extent_protocol::extentid_t> flush_cache(
        extent_protocol::extentid_t eid);

public:
    yfs_client(std::string, std::string);

    int checktype(inum);
    bool isfile(inum);
    bool isdir(inum);
    bool issymlink(inum);

    int getfile(inum, fileinfo&);
    int getdir(inum, dirinfo&);

    int setattr(inum, size_t);
    int lookup(inum, const char*, bool&, inum&);
    int create(inum, const char*, mode_t, inum&);
    int readdir(inum, std::list<dirent>&);
    int write(inum, size_t, off_t, const char*, size_t&);
    int read(inum, size_t, off_t, std::string&);
    int unlink(inum, const char*);
    int mkdir(inum, const char*, mode_t, inum&);

    /** you may need to add symbolic link related methods here.*/
    int symlink(inum, const char*, const char*, inum&);
    int readlink(inum, std::string&);
};

#endif
