#include "inode_manager.h"

// disk layer -----------------------------------------

disk::disk() { bzero(blocks, sizeof(blocks)); }

void disk::read_block(blockid_t id, char* buf) {
    memcpy(buf, blocks[id], BLOCK_SIZE);
}

void disk::write_block(blockid_t id, const char* buf) {
    memcpy(blocks[id], buf, BLOCK_SIZE);
}

// block layer -----------------------------------------

// Allocate a free disk block.
blockid_t block_manager::alloc_block() {
    blockid_t data_start = sb.nblocks / BPB + INODE_NUM + 2;  // 1034
    static blockid_t id = data_start;
    while (using_blocks[id]) {
        id = (id + 1) % sb.nblocks;
        if (id < data_start)
            id = data_start;
    }
    using_blocks[id] = 1;

    return id;
}

void block_manager::free_block(uint32_t id) { using_blocks[id] = 0; }

// The layout of disk should be like this:
// |<-sb->|<-free block bitmap->|<-inode table->|<-data->|
// |  1   |       2-9           |    10-1033    | 1034-32767 |
block_manager::block_manager() {
    d = new disk();

    // format the disk
    sb.size = BLOCK_SIZE * BLOCK_NUM;
    sb.nblocks = BLOCK_NUM;
    sb.ninodes = INODE_NUM;
}

void block_manager::read_block(uint32_t id, char* buf) {
    d->read_block(id, buf);
}

void block_manager::write_block(uint32_t id, const char* buf) {
    d->write_block(id, buf);
}

// inode layer -----------------------------------------

inode_manager::inode_manager() {
    bm = new block_manager();
    uint32_t root_dir = alloc_inode(extent_protocol::T_DIR);
    if (root_dir != 1) {
        printf("\tim: error! alloc first inode %d, should be 1\n", root_dir);
        exit(0);
    }
}

/* Create a new file.
 * Return its inum. */
uint32_t inode_manager::alloc_inode(uint32_t type) {
    static uint32_t id = 1;
    while (using_inodes[id])
        id = id % INODE_NUM + 1;

    inode* ino = (inode*)malloc(sizeof(inode));
    memset(ino, 0, sizeof(inode));
    ino->type = type;
    put_inode(id, ino);
    using_inodes[id] = 1;
    free(ino);
    return id;
}

void inode_manager::free_inode(uint32_t inum) {
    if (using_inodes[inum]) {
        inode* ino = (inode*)malloc(sizeof(inode));
        memset(ino, 0, sizeof(inode));
        put_inode(inum, ino);
        free(ino);
        using_inodes[inum] = 0;
    }
}

/* Return an inode structure by inum, NULL otherwise.
 * Caller should release the memory. */
struct inode* inode_manager::get_inode(uint32_t inum) {
    struct inode *ino, *ino_disk;
    char buf[BLOCK_SIZE];

    printf("\tim: get_inode %d\n", inum);

    if (inum < 0 || inum >= INODE_NUM) {
        printf("\tim: inum out of range\n");
        return NULL;
    }

    bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
    // printf("%s:%d\n", __FILE__, __LINE__);

    ino_disk = (struct inode*)buf + inum % IPB;
    if (ino_disk->type == 0) {
        printf("\tim: inode not exist\n");
        return NULL;
    }

    ino = (struct inode*)malloc(sizeof(struct inode));
    *ino = *ino_disk;

    return ino;
}

void inode_manager::put_inode(uint32_t inum, struct inode* ino) {
    char buf[BLOCK_SIZE];
    struct inode* ino_disk;

    printf("\tim: put_inode %d\n", inum);
    if (ino == NULL)
        return;

    bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
    ino_disk = (struct inode*)buf + inum % IPB;
    *ino_disk = *ino;
    bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf);
}

#define MIN(a, b) ((a) < (b) ? (a) : (b))

/* Get all the data of a file by inum.
 * Return alloced data, should be freed by caller. */
void inode_manager::read_file(uint32_t inum, char** buf_out, int* size) {
    inode* ino = get_inode(inum);
    *size = ino->size;
    char* now = *buf_out = (char*)malloc(
        (*size) % BLOCK_SIZE
            ? ((unsigned int)(*size / BLOCK_SIZE) + 1) * BLOCK_SIZE
            : *size);
    for (int i = 0; i < NDIRECT; ++i)
        if (ino->blocks[i]) {
            bm->read_block(ino->blocks[i], now);
            now += BLOCK_SIZE;
        } else {
            break;
        }
    if (ino->blocks[NDIRECT]) {
        uint iblock[NINDIRECT];
        bm->read_block(ino->blocks[NDIRECT], (char*)iblock);
        for (int i = 0; i < (int)NINDIRECT; ++i)
            if (iblock[i]) {
                bm->read_block(iblock[i], now);
                now += BLOCK_SIZE;
            } else {
                break;
            }
    }
    free(ino);
}

void inode_manager::write_blockn(uint32_t id, const char* buf, int size) {
    if (size < BLOCK_SIZE) {
        char tmp[BLOCK_SIZE] = {0};
        memcpy(tmp, buf, size);
        bm->write_block(id, tmp);
    } else {
        bm->write_block(id, buf);
    }
}

/* alloc/free blocks if needed */
void inode_manager::write_file(uint32_t inum, const char* buf, int size) {
    inode* ino = get_inode(inum);
    int i = 0;
    ino->size = size;
    while (size > 0 && i < NDIRECT) {
        blockid_t bid = ino->blocks[i];
        if (bid == 0) {
            bid = bm->alloc_block();
            ino->blocks[i] = bid;
        }
        write_blockn(bid, buf, size);

        ++i, size -= BLOCK_SIZE, buf += BLOCK_SIZE;
    }

    if (size <= 0) {
        for (; i < NDIRECT; ++i) {
            if (ino->blocks[i])
                bm->free_block(ino->blocks[i]);
            ino->blocks[i] = 0;
        }
        if (ino->blocks[NDIRECT]) {
            remove_iblock(ino->blocks[NDIRECT]);
            ino->blocks[NDIRECT] = 0;
        }
    } else {
        blockid_t iid = ino->blocks[NDIRECT];
        uint iblock[NINDIRECT] = {0};
        if (iid == 0) {
            iid = bm->alloc_block();
            ino->blocks[NDIRECT] = iid;
        } else {
            bm->read_block(iid, (char*)iblock);
        }
        i = 0;
        while (size > 0 && i < (int)NINDIRECT) {
            blockid_t bid = iblock[i];
            if (bid == 0) {
                bid = bm->alloc_block();
                iblock[i] = bid;
            }
            write_blockn(bid, buf, size);

            ++i, size -= BLOCK_SIZE, buf += BLOCK_SIZE;
        }

        bm->write_block(iid, (char*)iblock);
    }
    put_inode(inum, ino);
    free(ino);
}

void inode_manager::getattr(uint32_t inum, extent_protocol::attr& a) {
    inode* ino = get_inode(inum);
    if (ino) {
        a.type = ino->type;
        a.size = ino->size;
        a.atime = ino->atime;
        a.ctime = ino->ctime;
        a.mtime = ino->mtime;
        free(ino);
    }
}

void inode_manager::remove_iblock(uint32_t inum) {
    uint iblock[NINDIRECT];
    bm->read_block(inum, (char*)iblock);
    for (int i = 0; i < (int)NINDIRECT; ++i)
        if (iblock[i])
            bm->free_block(iblock[i]);
        else
            break;
    bm->free_block(inum);
}

void inode_manager::remove_file(uint32_t inum) {
    inode* ino = get_inode(inum);
    for (int i = 0; i < NDIRECT; ++i)
        if (ino->blocks[i])
            bm->free_block(ino->blocks[i]);
        else
            break;
    if (ino->blocks[NDIRECT])
        remove_iblock(ino->blocks[NDIRECT]);
    free(ino);
    free_inode(inum);
}
