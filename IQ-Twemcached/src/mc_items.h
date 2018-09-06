/*
 * twemcache - Twitter memcached.
 * Copyright (c) 2012, Twitter, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * * Redistributions of source code must retain the above copyright
 *   notice, this list of conditions and the following disclaimer.
 * * Redistributions in binary form must reproduce the above copyright
 *   notice, this list of conditions and the following disclaimer in the
 *   documentation and/or other materials provided with the distribution.
 * * Neither the name of the Twitter nor the names of its contributors
 *   may be used to endorse or promote products derived from this software
 *   without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
 * THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#ifndef _MC_ITEMS_H_
#define _MC_ITEMS_H_

#include <mc_lease.h>
#include <mc_slabs.h>

#define DEFAULT_TOKEN 0

#define PREFIX_KEY_LEN 3
#define PREFIX_PARTITION_KEY_LEN 4

#define ALIVE 0
#define ABORT 1

typedef enum item_flags {
    ITEM_LINKED  = 1,  	/* item in lru q and hash */
    ITEM_CAS     = 2,  	/* item has cas */
    ITEM_SLABBED = 4,  	/* item in free q */
    ITEM_RALIGN  = 8,  	/* item data (payload) is right-aligned */
    ITEM_I_LEASE = 16,		/* item has an I lease */
    ITEM_Q_INV_LEASE = 32,	/* item has a Q lease for invalidate. no other lease may overwrite it */
    ITEM_Q_REF_LEASE = 64, /* item has a Q lease for refresh */
    ITEM_Q_INC_LEASE = 128	/* item has a Q lease for incremental updates */
} item_flags_t;

typedef enum item_coflags {
	C_LEASE = 1,
	O_LEASE_INV = 2,
	PTRANS = 4,
	HK = 8,
	SESS = 16,
	O_LEASE_REF = 32,
	PI_MEM = 64
} item_coflags_t;

typedef enum item_type {
	ITEM_NORMAL = 1,
	ITEM_APPEND = 2,
	ITEM_PREPEND = 4
}item_type_t;

typedef enum item_store_result {
    NOT_STORED,
    STORED,
    EXISTS,
    NOT_FOUND,
    STORE_ERROR
} item_store_result_t;

typedef enum item_iq_result {
	IQ_VALUE,
	IQ_NO_VALUE,
	IQ_LEASE,
	IQ_LEASE_NO_VALUE,
	IQ_NO_LEASE,
	IQ_ABORT,
	IQ_MISS,
	IQ_CLIENT_ERROR,
	IQ_SERVER_ERROR,
	IQ_OK,
	IQ_NOT_FOUND,
	IQ_NOT_STORED
} item_iq_result_t;

typedef enum item_co_result {
	CO_OK,
	CO_ABORT,
	CO_NOT_FOUND,
	CO_INVALID,
	CO_RETRY,
	CO_NOVALUE,
	CO_NOT_STORED
}item_co_result_t;

typedef enum item_delta_result {
    DELTA_OK,
    DELTA_NON_NUMERIC,
    DELTA_EOM,
    DELTA_NOT_FOUND,
} item_delta_result_t;

typedef enum item_lease_result {
	LEASE_OK,
	LEASE_DELTA,
	LEASE_ADJUST,
	LEASE_ERROR,
} item_lease_result_t;

/*
 * Every item chunk in the twemcache starts with an header (struct item)
 * followed by item data. An item is essentially a chunk of memory
 * carved out of a slab. Every item is owned by its parent slab
 *
 * Items are either linked or unlinked. When item is first allocated and
 * has no data, it is unlinked. When data is copied into an item, it is
 * linked into hash and lru q (ITEM_LINKED). When item is deleted either
 * explicitly or due to flush or expiry, it is moved in the free q
 * (ITEM_SLABBED). The flags ITEM_LINKED and ITEM_SLABBED are mutually
 * exclusive and when an item is unlinked it has neither of these flags
 *
 *   <-----------------------item size------------------>
 *   +---------------+----------------------------------+
 *   |               |                                  |
 *   |  item header  |          item payload            |
 *   | (struct item) |         ...      ...             |
 *   +---------------+-------+-------+------------------+
 *   ^               ^       ^       ^
 *   |               |       |       |
 *   |               |       |       |
 *   |               |       |       |
 *   |               |       |       \
 *   |               |       |       item_data()
 *   |               |       \
 *   \               |       item_key()
 *   item            \
 *                   item->end, (if enabled) item_cas()
 *
 * item->end is followed by:
 * - 8-byte cas, if ITEM_CAS flag is set
 * - key with terminating '\0', length = item->nkey + 1
 * - data with no terminating '\0'
 */
struct item {
    uint32_t          magic;      /* item magic (const) */
    TAILQ_ENTRY(item) i_tqe;      /* link in lru q or free q */
    SLIST_ENTRY(item) h_sle;      /* link in hash */
    rel_time_t        atime;      /* last access time in secs */
    rel_time_t        exptime;    /* expiry time in secs */
    uint32_t          nbyte;      /* date size */
    uint32_t          offset;     /* offset of item in slab */
    uint32_t          dataflags;  /* data flags opaque to the server */
    uint16_t          refcount;   /* # concurrent users of item */
    uint8_t           flags;      /* item flags */

    uint8_t			  coflags;	  /* item flags exclusively for CO leases */

    uint8_t			  p;		/* a flag to define whether the value is a pending value */
    uint8_t			  type;

    uint8_t           id;         /* slab class id */
    uint8_t           nkey;       /* key length */
    uint8_t 					sess_status;		/* status of a session */
    char              end[1];     /* item data */
};

SLIST_HEAD(item_slh, item);

TAILQ_HEAD(item_tqh, item);

#define ITEM_MAGIC      0xfeedface
#define ITEM_HDR_SIZE   offsetof(struct item, end)

/*
 * An item chunk is the portion of the memory carved out from the slab
 * for an item. An item chunk contains the item header followed by item
 * data.
 *
 * The smallest item data is actually a single byte key with a zero byte value
 * which internally is of sizeof("k"), as key is stored with terminating '\0'.
 * If cas is enabled, then item payload should have another 8-byte for cas.
 *
 * The largest item data is actually the room left in the slab_size()
 * slab, after the item header has been factored out
 */
#define ITEM_MIN_PAYLOAD_SIZE  (sizeof("k") + sizeof(uint64_t))
#define ITEM_MIN_CHUNK_SIZE \
    MC_ALIGN(ITEM_HDR_SIZE + ITEM_MIN_PAYLOAD_SIZE, MC_ALIGNMENT)

#define ITEM_PAYLOAD_SIZE      32
#define ITEM_CHUNK_SIZE     \
    MC_ALIGN(ITEM_HDR_SIZE + ITEM_PAYLOAD_SIZE, MC_ALIGNMENT)


#if __GNUC__ >= 4 && __GNUC_MINOR__ >= 2
#pragma GCC diagnostic ignored "-Wstrict-aliasing"
#endif

static size_t pimem_size;

static inline void
_item_incr_pimem_size(size_t size) {
  pimem_size += size;
}

static inline void
_item_decr_pimem_size(size_t size) {
  pimem_size -= size;
}

static inline bool
item_has_cas(struct item *it) {
    return (it->flags & ITEM_CAS);
}

static inline bool
item_is_linked(struct item *it) {
    return (it->flags & ITEM_LINKED);
}

static inline bool
item_is_slabbed(struct item *it) {
    return (it->flags & ITEM_SLABBED);
}

static inline bool
item_is_raligned(struct item *it) {
    return (it->flags & ITEM_RALIGN);
}

static inline bool
item_has_co_lease(struct item *it) {
	return ((it->coflags & C_LEASE) ||
			(it->coflags & O_LEASE_INV) ||
			(it->coflags & O_LEASE_REF));
}

static inline bool
item_has_c_lease(struct item *it) {
	return (it->coflags & C_LEASE);
}

static inline bool
item_has_o_lease(struct item *it) {
	return ((it->coflags & O_LEASE_INV) || (it->coflags & O_LEASE_REF));
}

static inline bool
item_has_o_lease_inv(struct item *it) {
	return it->coflags & O_LEASE_INV;
}

static inline bool
item_has_o_lease_ref(struct item *it) {
	return it->coflags & O_LEASE_REF;
}

static inline bool
item_has_i_lease(struct item *it) {
	return (it->flags & ITEM_I_LEASE);
}

static inline bool
item_has_q_ref_lease(struct item *it) {
	return (it->flags & ITEM_Q_REF_LEASE);
}

static inline bool
item_has_q_incr_lease(struct item *it) {
	return (it->flags & ITEM_Q_INC_LEASE);
}

static inline bool
item_has_q_inv_lease(struct item *it) {
	return (it->flags & ITEM_Q_INV_LEASE);
}

static inline bool
item_is_sess(struct item *it) {
    ASSERT(it->magic == ITEM_MAGIC);

	return (it->coflags & SESS);
}

static inline uint64_t
item_cas(struct item *it)
{
    ASSERT(it->magic == ITEM_MAGIC);

    if (item_has_cas(it)) {
        return *((uint64_t *)it->end);
    }

    return 0;
}

static inline void
item_set_ptrans(struct item *it) {
	ASSERT(it->magic == ITEM_MAGIC);

	it->coflags |= PTRANS;
}

static inline void
item_unset_ptrans(struct item *it) {
	ASSERT(it->magic == ITEM_MAGIC);

	it->coflags &= ~PTRANS;
}

static inline void
item_set_hotkeys(struct item *it) {
	ASSERT(it->magic == ITEM_MAGIC);

	it->coflags |= HK;
}

static inline void
item_unset_hotkeys(struct item *it) {
	ASSERT(it->magic == ITEM_MAGIC);

	it->coflags &= ~HK;
}

static inline void
item_set_type(struct item* it, uint8_t type) {
	it->type = type;
}

static inline uint8_t
item_get_type(struct item* it) {
	return it->type;
}

static inline void
item_set_c_lease(struct item *it) {
	ASSERT(it->magic == ITEM_MAGIC);

	it->coflags |= C_LEASE;
}

static inline void
item_unset_c_lease(struct item *it) {
	ASSERT(it->magic == ITEM_MAGIC);

	it->coflags &= ~C_LEASE;
}

static inline void
item_set_o_lease_inv(struct item *it) {
	ASSERT(it->magic == ITEM_MAGIC);

	it->coflags |= O_LEASE_INV;
}

static inline void
item_set_o_lease_ref(struct item *it) {
	ASSERT(it->magic == ITEM_MAGIC);

	it->coflags |= O_LEASE_REF;
}

static inline void
item_set_i_lease(struct item *it)
{
	ASSERT(it->magic == ITEM_MAGIC);

	it->flags |= ITEM_I_LEASE;
}

static inline void
item_unset_i_lease(struct item *it)
{
	ASSERT(it->magic == ITEM_MAGIC);

	it->flags &= ~ITEM_I_LEASE;
}

static inline void
item_set_q_inv_lease(struct item *it) {
	ASSERT(it->magic == ITEM_MAGIC);

	it->flags |= ITEM_Q_INV_LEASE;
}

static inline void
item_unset_q_inv_lease(struct item *it) {
	ASSERT(it->magic == ITEM_MAGIC);

	it->flags &= ~ITEM_Q_INV_LEASE;
}

static inline void
item_set_q_ref_lease(struct item *it) {
	ASSERT(it->magic == ITEM_MAGIC);

	it->flags |= ITEM_Q_REF_LEASE;
}

static inline void
item_unset_q_ref_lease(struct item *it) {
	ASSERT(it->magic == ITEM_MAGIC);

	it->flags &= ~ITEM_Q_REF_LEASE;
}

static inline void
item_set_q_inc_lease(struct item *it) {
	ASSERT(it->magic == ITEM_MAGIC);

	it->flags |= ITEM_Q_INC_LEASE;
}

static inline void
item_unset_q_inc_lease(struct item *it) {
	ASSERT(it->magic == ITEM_MAGIC);

	it->flags &= ~ITEM_Q_INC_LEASE;
}

static inline bool
item_is_lease_holder(struct item *it) {
	ASSERT(it->magic == ITEM_MAGIC);

	return (it->flags & ITEM_I_LEASE) || (it->flags & ITEM_Q_INV_LEASE) ||
			(it->flags & ITEM_Q_REF_LEASE) || (it->flags & ITEM_Q_INC_LEASE);
}

static inline bool
item_is_ptrans(struct item *it) {
	ASSERT(it->magic == ITEM_MAGIC);

	return (it->coflags & PTRANS);
}

static inline bool
item_is_hotkeys(struct item *it) {
	ASSERT(it->magic == ITEM_MAGIC);

	return (it->coflags & HK);
}

static inline bool
item_is_pimem(struct item *it) {
	ASSERT(it->magic == ITEM_MAGIC);

	return (it->coflags & PI_MEM);
}

static inline void
item_set_sess(struct item *it) {
	ASSERT(it->magic == ITEM_MAGIC);

	it->coflags |= SESS;
}

static inline bool
item_is_co_lease_holder(struct item *it) {
	ASSERT(it->magic == ITEM_MAGIC);

	return item_has_co_lease(it);
}

static inline bool
item_has_q_lease(struct item *it) {
	ASSERT(it->magic == ITEM_MAGIC);

	return (it->flags & ITEM_Q_INV_LEASE) ||
			(it->flags & ITEM_Q_REF_LEASE) || (it->flags & ITEM_Q_INC_LEASE);
}

static inline void
item_set_cas(struct item *it, uint64_t cas)
{
    ASSERT(it->magic == ITEM_MAGIC);

    if (item_has_cas(it)) {
        *((uint64_t *)it->end) = cas;
    }
}
#if __GNUC__ >= 4 && __GNUC_MINOR__ >= 6
#pragma GCC diagnostic pop
#endif

static inline char *
item_key(struct item *it)
{
    char *key;

    ASSERT(it->magic == ITEM_MAGIC);

    key = it->end;
    if (item_has_cas(it)) {
        key += sizeof(uint64_t);
    }

    return key;
}

static inline size_t
item_ntotal(uint8_t nkey, uint32_t nbyte, bool use_cas)
{
    size_t ntotal;

    ntotal = use_cas ? sizeof(uint64_t) : 0;
    ntotal += ITEM_HDR_SIZE + nkey + 1 + nbyte + CRLF_LEN;

    return ntotal;
}

static inline size_t
item_size(struct item *it)
{

    ASSERT(it->magic == ITEM_MAGIC);

    return item_ntotal(it->nkey, it->nbyte, item_has_cas(it));
}

void item_init(void);
void item_deinit(void);

char * item_data(struct item *it);
struct slab *item_2_slab(struct item *it);

void item_hdr_init(struct item *it, uint32_t offset, uint8_t id);

size_t item_get_pimem_size(void);
uint8_t item_slabid(uint8_t nkey, uint32_t nbyte);
struct item *item_alloc(uint8_t id, char *key, uint8_t nkey, uint32_t dataflags, rel_time_t exptime, uint32_t nbyte);

void item_reuse(struct item *it);

void item_delete(struct item *it);
void item_clean(uint8_t id, char* key, uint8_t nkey);

void item_remove(struct item *it);
void item_touch(struct item *it);
char *item_cache_dump(uint8_t id, uint32_t limit, uint32_t *bytes);

struct item *item_get(const char *key, size_t nkey);
void item_flush_expired(void);

void item_unset_pinned(struct item *it);
void item_set_pinned(struct item *it);
void item_set_pimem(struct item *it);
void item_unset_pimem(struct item *it);

item_store_result_t item_get_and_delete(char* key, uint8_t nkey, struct conn *c);
item_iq_result_t item_iqget(char *key, size_t nkey, lease_token_t lease_token,
		char* tid, size_t tid_size, struct conn *c, struct item** item, lease_token_t* new_lease_token);

void item_getprik(struct conn* c, struct item** it);
void item_ftrans(char*tid, size_t tid_size, char*key, size_t key_size, struct conn* c);
item_co_result_t item_ciget(char* sid, size_t nsid, char *key, size_t nkey,
		lease_token_t lease_token, struct conn *c, struct item** item, lease_token_t* new_lease_token);

rstatus_t item_get_and_unlease(char* key, uint8_t nkey, lease_token_t token_val, struct conn *c);

item_store_result_t item_store(struct item *it, req_type_t type, struct conn *c);
item_delta_result_t item_add_delta(struct conn *c, char *key, size_t nkey, int incr, int64_t delta, char *buf);

rstatus_t
item_quarantine_and_register(char* tid, size_t ntid, char* key,
		size_t nkey, u_int8_t* token_val,struct conn *c);

rstatus_t
item_delete_and_release(char* tid, u_int32_t ntid, struct conn *c);

rstatus_t item_quarantine_and_read(char* tid, size_t tid_size,
		char* key, uint8_t nkey, lease_token_t lease_token,
		lease_token_t *new_lease_token, struct conn *c, struct item** it, uint8_t *p);
item_store_result_t item_swap_and_release(struct item* it, struct conn *c);
item_store_result_t item_swap(struct item* it, struct conn *c);

item_co_result_t item_oqread(char* sid, size_t nsid, char* key, uint8_t nkey, struct conn *c, struct item ** it);
item_co_result_t item_oqswap(char* sid, size_t nsid, struct item* it, struct conn* c);
item_co_result_t item_oqwrite(char* sid, size_t nsid, struct item* it, struct conn* c);
item_co_result_t item_oqadd(char* sid, size_t nsid, struct item* it, struct conn* c);

item_co_result_t item_validate(char *sid, size_t nsid, struct conn *c);

item_iq_result_t item_commit(char* tid, u_int32_t ntid, struct conn *c, int32_t pending);
item_iq_result_t item_release(char* tid, u_int32_t ntid, struct conn *c);

void clean_session(char* sid, size_t nsid, struct conn* c);
void abort_sessions(struct conn* c, struct item* colease_it, char* sid, size_t nsid);

struct item*
_item_get_pending(char* key, size_t nkey);

item_iq_result_t
_item_add_delta_iq(struct conn* c, char*key, size_t nkey, long delta);

item_co_result_t
_item_add_delta_co(struct conn* c, char*key, size_t nkey, long delta);

void
_item_assoc_key_tid(struct conn* c, struct item* trans_it, char *key, size_t nkey, char *tid, size_t ntid);

void
_item_assoc_key_sid(struct conn* c, struct item* sess_it, char *key, size_t nkey, char *sid, size_t nsid);

item_iq_result_t
item_iqincr_iqdecr(struct conn *c, char *key, size_t nkey, bool incr, int64_t delta, char *tid, size_t ntid,
		uint8_t *pending, uint64_t *lease_token);

item_iq_result_t
_item_iqincr_iqdecr(struct conn *c, char *key, size_t nkey, bool incr, long delta, char *tid, size_t ntid,
		uint8_t *pending, uint64_t *lease_token);

item_iq_result_t
item_iqappend_iqprepend(struct conn *c, struct item* item, uint8_t *pending, uint64_t *lease_token);

item_iq_result_t
_item_append_prepend_iq(struct conn *c, struct item* it, char* data, size_t nbyte);
item_co_result_t
_item_append_prepend_co(struct conn *c, struct item* it);

item_co_result_t
item_oqincr_oqdecr(struct conn *c, char *key, size_t nkey, bool incr,
		int64_t delta, char *sid, size_t nsid, uint64_t *val);

item_co_result_t
item_oqappend_oqprepend(struct conn *c, struct item* item);
item_co_result_t
item_oqappend_oqprepend2(struct conn *c, struct item* item);

item_co_result_t _item_oqincr_oqdecr(struct conn *c, char *key, size_t nkey, bool incr,
		long delta, char *sid, size_t nsid);

struct item* _item_get_lease(char* key, size_t nkey);
struct item* _item_create_lease(char* key, size_t nkey, lease_token_t* token);

struct item* _item_get_ptrans(char* key, size_t nkey);
struct item* _item_create_ptrans(char* key, size_t nkey, int64_t token);

struct item* _item_get_pending_version(char* key, size_t nkey);
struct item* _item_create_pending_version(struct item* it);

struct item* _item_get_co_lease(char* key, size_t nkey);

void _item_assoc_sid_colease(struct conn* c, struct item* colease_it, char *key, size_t nkey, char *sid, size_t nsid, item_coflags_t type);
void _item_remove_sid_colease(struct conn* c, struct item* colease_it, char* key, size_t nkey, char *sid, size_t nsid);

void _item_assoc_key_hotkeys(struct conn* c, char* key, size_t nkey);
void _item_remove_key_hotkeys(struct conn* c, char* key, size_t nkey);

void _item_assoc_tid_lease(struct conn* c, struct item* lease_it,
		char* key, size_t nkey, char *tid, size_t ntid);
void _item_remove_tid_lease(struct conn* c, struct item* lease_it,
		char* key, size_t nkey, char *tid, size_t ntid);

void _item_assoc_entry_to_list(struct conn* c, struct item* it,
		char *entry_id, size_t entry_nid, char *key, size_t nkey);

void _item_remove_entry_from_list(struct conn *c, struct item* it,
		char* key, size_t nkey, char *entry_id, size_t entry_nid);

item_co_result_t
item_reg_c(char* sid, size_t nsid, char* key, size_t nkey, struct conn *c);

item_co_result_t
item_reg_o(char* sid, size_t nsid, char* key, size_t nkey, struct conn *c);

item_co_result_t
item_co_commit(char* sid, size_t nsid, struct conn *c);

item_co_result_t
item_oqreg(struct conn *c, char *sid, size_t nsid, char *key, size_t nkey);

item_co_result_t
item_dcommit(char *sid, size_t nsid, struct conn *c);

item_co_result_t
item_dabort(char *sid, size_t nsid, struct conn *c);

item_iq_result_t
item_co_unlease(char *sid, size_t nsid, struct conn *c);

lease_token_t _item_lease_value(struct item* it);

void
_item_assoc_tid_ptrans(struct conn* c, struct item* ptrans_it,
		char* key, size_t nkey, char *tid, size_t ntid);

void
_item_remove_tid_ptrans(struct conn* c, struct item* ptrans_it, char* key, size_t nkey, char *tid, size_t ntid);

#endif
