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

#include <stdio.h>
#include <stdlib.h>

#include <mc_core.h>
#include <mc_sqltrig.h>

extern struct settings settings;

/*
 * Parsing tokens:
 *
 * COMMAND  KEY   FLAGS   EXPIRY   VLEN
 * OLDset      <key> <flags> <expiry> <datalen> [noreply]\r\n<data>\r\n
 * add      <key> <flags> <expiry> <datalen> [noreply]\r\n<data>\r\n
 * replace  <key> <flags> <expiry> <datalen> [noreply]\r\n<data>\r\n
 * append   <key> <flags> <expiry> <datalen> [noreply]\r\n<data>\r\n
 * prepend  <key> <flags> <expiry> <datalen> [noreply]\r\n<data>\r\n
 *
 * oldCOMMAND	KEY	  FLAGS   EXPIRY   VLEN	     LEASE_TOKEN
 * oldset		<key> <flags> <expiry> <datalen> <token> [noreply]\r\n<data>\r\n
 *
 * COMMAND	KEY	  FLAGS   EXPIRY   VLEN	     LEASE_TOKEN 	VALUE_HASH
 * set		<key> <flags> <expiry> <datalen> <token> 	 	<value_hash> [noreply]\r\n<data>\r\n
 *
 * COMMAND  KEY   FLAGS   EXPIRY   VLEN      CAS
 * cas      <key> <flags> <expiry> <datalen> <cas> [noreply]\r\n<data>\r\n
 *
 * COMMAND  KEY
 * get      <key>\r\n
 * getnol	<key>\r\n
 * get      <key> [<key>]+\r\n
 * gets     <key>\r\n
 * gets     <key> [<key>]+\r\n
 * delete   <key> [noreply]\r\n
 *
 * COMMAND  KEY    DELTA
 * incr     <key> <value> [noreply]\r\n
 * decr     <key> <value> [noreply]\r\n
 *
 * COMMAND   SUBCOMMAND
 * quit\r\n
 * flush_all [<delay>] [noreply]\r\n
 * version\r\n
 * verbosity <num> [noreply]\r\n
 *
 * COMMAND   SUBCOMMAND CACHEDUMP_ID CACHEDUMP_LIMIT
 * stats\r\n
 * stats    <args>\r\n
 * stats    cachedump   <id>         <limit>\r\n
 *
 * COMMAND  SUBCOMMAND  AGGR_COMMAND
 * config   aggregate   <num>\r\n
 *
 * COMMAND  SUBCOMMAND  EVICT_COMMAND
 * config   evict       <num>\r\n
 *
 * COMMAND  SUBCOMMAND  KLOG_COMMAND  KLOG_SUBCOMMAND
 * config   klog        run           start\r\n
 * config   klog        run           stop\r\n
 * config   klog        interval      reset\r\n
 * config   klog        interval      <val>\r\n
 * config   klog        sampling      reset\r\n
 * config   klog        sampling      <val>\r\n
 */

#define TOKEN_COMMAND           0
#define TOKEN_KEY               1
#define TOKEN_FLAGS             2
#define TOKEN_EXPIRY            3
#define TOKEN_VLEN              4
#define TOKEN_CAS               6
#define TOKEN_LEASE_TOKEN				5
#define TOKEN_PI_MEM						6
#define TOKEN_DELTA             2
#define TOKEN_SUBCOMMAND        1
#define TOKEN_CACHEDUMP_ID      2
#define TOKEN_CACHEDUMP_LIMIT   3
#define TOKEN_AGGR_COMMAND      2
#define TOKEN_EVICT_COMMAND     2
#define TOKEN_KLOG_COMMAND      2
#define TOKEN_KLOG_SUBCOMMAND   3
#define TOKEN_MAX               16
#define TOKEN_LEASE_RELEASE		2
#define TOKEN_HASH_XLEASE		2
#define TRANS_ID				2

#define SUFFIX_MAX_LEN 44 /* =11+11+21+1 enough to hold " <uint32_t> <uint32_t> <uint64_t>\0" */

struct token {
	char *val; /* token value */
	size_t len; /* token length */
};

struct bound {
	struct {
		int min; /* min # token */
		int max; /* max # token */
	} b[2]; /* bound without and with noreply */
};

#define DEFINE_ACTION(_t, _min, _max, _nmin, _nmax) \
		{ {{ _min, _max }, { _nmin, _nmax }} },
static struct bound ntoken_bound[] = {
REQ_CODEC( DEFINE_ACTION ) };
#undef DEFINE_ACTION

#define strcrlf(m)                                                          \
		(*(m) == '\r' && *((m) + 1) == '\n')

#ifdef MC_LITTLE_ENDIAN

#define str4cmp(m, c0, c1, c2, c3)                                          \
		(*(uint32_t *) m == ((c3 << 24) | (c2 << 16) | (c1 << 8) | c0))

#define str5cmp(m, c0, c1, c2, c3, c4)                                      \
		(str4cmp(m, c0, c1, c2, c3) && (m[4] == c4))

#define str6cmp(m, c0, c1, c2, c3, c4, c5)                                  \
		(str4cmp(m, c0, c1, c2, c3) &&                                          \
				(((uint32_t *) m)[1] & 0xffff) == ((c5 << 8) | c4))

#define str7cmp(m, c0, c1, c2, c3, c4, c5, c6)                              \
		(str6cmp(m, c0, c1, c2, c3, c4, c5) && (m[6] == c6))

#define str8cmp(m, c0, c1, c2, c3, c4, c5, c6, c7)                          \
		(str4cmp(m, c0, c1, c2, c3) &&                                          \
				(((uint32_t *) m)[1] == ((c7 << 24) | (c6 << 16) | (c5 << 8) | c4)))

#define str9cmp(m, c0, c1, c2, c3, c4, c5, c6, c7, c8)                      \
		(str8cmp(m, c0, c1, c2, c3, c4, c5, c6, c7) && m[8] == c8)

#define str10cmp(m, c0, c1, c2, c3, c4, c5, c6, c7, c8, c9)                 \
		(str8cmp(m, c0, c1, c2, c3, c4, c5, c6, c7) &&                          \
				(((uint32_t *) m)[2] & 0xffff) == ((c9 << 8) | c8))

#define str11cmp(m, c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10)            \
		(str10cmp(m, c0, c1, c2, c3, c4, c5, c6, c7, c8, c9) && (m[10] == c10))

#define str12cmp(m, c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11)       \
		(str8cmp(m, c0, c1, c2, c3, c4, c5, c6, c7) &&                          \
				(((uint32_t *) m)[2] == ((c11 << 24) | (c10 << 16) | (c9 << 8) | c8)))

#else

#define str4cmp(m, c0, c1, c2, c3)                                          \
		(m[0] == c0 && m[1] == c1 && m[2] == c2 && m[3] == c3)

#define str5cmp(m, c0, c1, c2, c3, c4)                                      \
		(str4cmp(m, c0, c1, c2, c3) && (m[4] == c4))

#define str6cmp(m, c0, c1, c2, c3, c4, c5)                                  \
		(str5cmp(m, c0, c1, c2, c3, c4) && m[5] == c5)

#define str7cmp(m, c0, c1, c2, c3, c4, c5, c6)                              \
		(str6cmp(m, c0, c1, c2, c3, c4, c5) && m[6] == c6)

#define str8cmp(m, c0, c1, c2, c3, c4, c5, c6, c7)                          \
		(str7cmp(m, c0, c1, c2, c3, c4, c5, c6) && m[7] == c7)

#define str9cmp(m, c0, c1, c2, c3, c4, c5, c6, c7, c8)                      \
		(str8cmp(m, c0, c1, c2, c3, c4, c5, c6, c7) && m[8] == c8)

#define str10cmp(m, c0, c1, c2, c3, c4, c5, c6, c7, c8, c9)                 \
		(str9cmp(m, c0, c1, c2, c3, c4, c5, c6, c7, c8) && m[9] == c9)

#define str11cmp(m, c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10)            \
		(str10cmp(m, c0, c1, c2, c3, c4, c5, c6, c7, c8, c9) && m[10] == c10)

#define str12cmp(m, c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11)       \
		(str11cmp(m, c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10) && m[11] == c11)

#endif

/*
 * Returns true if ntoken is within the bounds for a given request
 * type, false otherwise.
 */
static bool asc_ntoken_valid(struct conn *c, int ntoken) {
	struct bound *t;
	int min, max;

	ASSERT(c->req_type > REQ_UNKNOWN && c->req_type < REQ_SENTINEL);

	t = &ntoken_bound[c->req_type];
	min = t->b[c->noreply].min;
	max = t->b[c->noreply].max;

	return (ntoken >= min && ntoken <= max) ? true : false;
}

/*
 * Tokenize the request header and update the token array token with
 * pointer to start of each token and length. Note that tokens are
 * not null terminated.
 *
 * Returns total number of tokens. The last valid token is the terminal
 * token (value points to the first unprocessed character of the string
 * and length zero).
 */
static size_t asc_tokenize(char *command, struct token *token, int ntoken_max) {
	char *s, *e; /* start and end marker */
	int ntoken; /* # tokens */

	ASSERT(command != NULL);ASSERT(token != NULL);ASSERT(ntoken_max > 1);

	for (s = e = command, ntoken = 0; ntoken < ntoken_max - 1; e++) {
		if (*e == ' ') {
			if (s != e) {
				/* save token */
				token[ntoken].val = s;
				token[ntoken].len = e - s;
				ntoken++;
			}
			s = e + 1;
		} else if (*e == '\0') {
			if (s != e) {
				/* save final token */
				token[ntoken].val = s;
				token[ntoken].len = e - s;
				ntoken++;
			}
			break;
		}
	}

	/*
	 * If we scanned the whole string, the terminal value pointer is NULL,
	 * otherwise it is the first unprocessed character.
	 */
	token[ntoken].val = (*e == '\0') ? NULL : e;
	token[ntoken].len = 0;
	ntoken++;

	return ntoken;
}

static void asc_write_string(struct conn *c, const char *str, size_t len) {
	log_debug(LOG_VVERB, "write on c %d noreply %d str '%.*s'", c->sd,
			c->noreply, len, str);

	if (c->noreply) {
		c->noreply = 0;
		conn_set_state(c, CONN_NEW_CMD);
		return;
	}

	if ((len + CRLF_LEN ) > c->wsize) {
		log_warn("server error on c %d for str '%.*s' because wbuf is not big "
				"enough", c->sd, len, str);

		stats_thread_incr(server_error);
		str = "SERVER_ERROR";
		len = sizeof("SERVER_ERROR") - 1;
	}

	memcpy(c->wbuf, str, len);
	memcpy(c->wbuf + len, CRLF, CRLF_LEN);
	c->wbytes = len + CRLF_LEN;
	c->wcurr = c->wbuf;

	conn_set_state(c, CONN_WRITE);
	c->write_and_go = CONN_NEW_CMD;
}

static void asc_write_abort(struct conn *c) {
	const char *str = "ABORT";
	size_t len = sizeof("ABORT") - 1;

	asc_write_string(c, str, len);

	klog_write(c->peer, c->req_type, c->req, c->req_len, RSP_ABORT, len);
}

static void asc_write_stored(struct conn *c) {
	const char *str = "STORED";
	size_t len = sizeof("STORED") - 1;

	asc_write_string(c, str, len);

	klog_write(c->peer, c->req_type, c->req, c->req_len, RSP_STORED, len);
}

static void asc_write_exists(struct conn *c) {
	const char *str = "EXISTS";
	size_t len = sizeof("EXISTS") - 1;

	asc_write_string(c, str, len);

	klog_write(c->peer, c->req_type, c->req, c->req_len, RSP_EXISTS, len);
}

static void asc_write_not_found(struct conn *c) {
	const char *str = "NOT_FOUND";
	size_t len = sizeof("NOT_FOUND") - 1;

	asc_write_string(c, str, len);

	klog_write(c->peer, c->req_type, c->req, c->req_len, RSP_NOT_FOUND, len);
}

static void asc_write_not_stored(struct conn *c) {
	const char *str = "NOT_STORED";
	size_t len = sizeof("NOT_STORED") - 1;

	asc_write_string(c, str, len);

	klog_write(c->peer, c->req_type, c->req, c->req_len, RSP_NOT_STORED, len);
}

static rstatus_t asc_create_suffix(struct conn *c, unsigned valid_key_iter,
		char **suffix) {
	if (valid_key_iter >= c->ssize) {
		char **new_suffix_list;

		new_suffix_list = mc_realloc(c->slist, sizeof(char *) * c->ssize * 2);
		if (new_suffix_list == NULL) {
			return MC_ENOMEM;
		}
		c->ssize *= 2;
		c->slist = new_suffix_list;
	}

	*suffix = cache_alloc(c->thread->suffix_cache);
	if (*suffix == NULL) {
		log_warn("server error on c %d for req of type %d with enomem on "
				"suffix cache", c->sd, c->req_type);

		asc_write_server_error(c);
		return MC_ENOMEM;
	}

	*(c->slist + valid_key_iter) = *suffix;
	return MC_OK;
}

static void asc_write_q_inv_leasehold(struct conn *c, u_int8_t markedVal) {
	char buf[SUFFIX_MAX_LEN];
	char *cptr = buf;
	size_t len = sizeof("LEASE") - 1;

	memcpy(cptr, "LEASE", len);
	cptr += len;

	int sz = mc_snprintf(cptr, SUFFIX_MAX_LEN - len, " %"PRIu8, markedVal);
	ASSERT(sz <= SUFFIX_SIZE);

	cptr += sz;
	len += sz;

	asc_write_string(c, buf, len);

	klog_write(c->peer, c->req_type, c->req, c->req_len, RSP_Q_INV_LEASE, len);
}

///* Respond with a
// * "LEASE " + lease_token
// */
//static void
//asc_write_leasehold(struct conn *c, lease_token_t lease_token) {
//	char buf[SUFFIX_MAX_LEN];
//	char *cptr = buf;
//	size_t len = sizeof("LEASE") - 1;
//
//	memcpy(cptr, "LEASE", len);
//	cptr += len;
//
//	int sz = mc_snprintf(cptr, SUFFIX_MAX_LEN - len, " %"PRIu64, lease_token);
//	ASSERT(sz <= SUFFIX_SIZE);
//
//	cptr += sz;
//	len += sz;
//
//	asc_write_string(c, buf, len);
//
//	klog_write(c->peer, c->req_type, c->req, c->req_len, RSP_LEASE, len);
//}

static void asc_write_deleted(struct conn *c) {
	const char *str = "DELETED";
	size_t len = sizeof("DELETED") - 1;

	asc_write_string(c, str, len);

	klog_write(c->peer, c->req_type, c->req, c->req_len, RSP_DELETED, len);
}

static void asc_write_invalid(struct conn *c) {
	const char *str = "INVALID";
	size_t len = sizeof("INVALID") - 1;

	asc_write_string(c, str, len);
}

static void asc_write_invalid_deleted(struct conn *c) {
	const char *str = "INVALID";
	size_t len = sizeof("INVALID") - 1;

	asc_write_string(c, str, len);

	klog_write(c->peer, c->req_type, c->req, c->req_len, RSP_DELETED, len);
}

static rstatus_t asc_write_novalue(struct conn *c) {
	rstatus_t status;
	char *suffix = NULL;
	int32_t total_len = 0;
	int sz;

	// lease token match and value does not exist
	// it is acceptable, the client should not back-off
	status = conn_add_iov(c, "NOVALUE ", sizeof("NOVALUE ") - 1);
	if (status != MC_OK)
		return status;
	total_len += sizeof("NOVALUE " - 1);

	status = asc_create_suffix(c, 0, &suffix);
	if (status != MC_OK) {
		return status;
	}

	sz = mc_snprintf(suffix, SUFFIX_MAX_LEN, " %"PRIu32, 0);
	ASSERT(sz <= SUFFIX_SIZE);
	status = conn_add_iov(c, suffix, sz);
	total_len += sz;

	status = conn_add_iov(c, CRLF, CRLF_LEN);
	if (status != MC_OK)
		return status;
	total_len += CRLF_LEN;

	log_debug(LOG_VVERB, ">%d END", c->sd);

	/*
	 * If the loop was terminated because of out-of-memory, it is not
	 * reliable to add END\r\n to the buffer, because it might not end
	 * in \r\n. So we send SERVER_ERROR instead.
	 */
	if (conn_add_iov(c, "END\r\n", 5) != MC_OK
			|| (c->udp && conn_build_udp_headers(c) != MC_OK)) {
		log_warn("server error on c %d for req of type %d with enomem", c->sd,
				c->req_type);

		asc_write_server_error(c);
	} else {
		conn_set_state(c, CONN_MWRITE);
		c->msg_curr = 0;
	}

	c->scurr = c->slist;
	c->sleft = 1;

	return status;
}

static rstatus_t asc_write_ciget_value(struct conn *c, struct item* it) {
	rstatus_t status;
	char *suffix = NULL;
	int32_t total_len = 0;
	int sz;
	unsigned int valid_key_iter = 0;

	if (valid_key_iter >= c->isize) {
		struct item **new_list;

		new_list = mc_realloc(c->ilist, sizeof(struct item *) * c->isize * 2);
		if (new_list != NULL) {
			c->isize *= 2;
			c->ilist = new_list;
		}
	}

	status = conn_add_iov(c, "VALUE ", sizeof("VALUE ") - 1);
	if (status != MC_OK)
		return status;
	total_len += sizeof("VALUE " - 1);

	status = conn_add_iov(c, item_key(it), it->nkey);
	if (status != MC_OK)
		return status;
	total_len += it->nkey;

	status = asc_create_suffix(c, valid_key_iter, &suffix);
	if (status != MC_OK) {
		return status;
	}

	sz = mc_snprintf(suffix, SUFFIX_MAX_LEN, " %"PRIu32" %"PRIu32,
			it->dataflags, it->nbyte);
	ASSERT(sz <= SUFFIX_SIZE);
	status = conn_add_iov(c, suffix, sz);
	total_len += sz;

	status = conn_add_iov(c, CRLF, CRLF_LEN);
	if (status != MC_OK)
		return status;
	total_len += CRLF_LEN;

	status = conn_add_iov(c, item_data(it), it->nbyte);
	if (status != MC_OK)
		return status;
	total_len += sz;

	status = conn_add_iov(c, CRLF, CRLF_LEN);
	if (status != MC_OK)
		return status;
	total_len += CRLF_LEN;

	if (it != NULL) {
		*(c->ilist + valid_key_iter) = it;
		valid_key_iter++;
	}

	c->icurr = c->ilist;
	c->ileft = valid_key_iter;

	c->scurr = c->slist;
	c->sleft = valid_key_iter;

	log_debug(LOG_VVERB, ">%d END", c->sd);

	/*
	 * If the loop was terminated because of out-of-memory, it is not
	 * reliable to add END\r\n to the buffer, because it might not end
	 * in \r\n. So we send SERVER_ERROR instead.
	 */
	if (conn_add_iov(c, "END\r\n", 5) != MC_OK
			|| (c->udp && conn_build_udp_headers(c) != MC_OK)) {
		log_warn("server error on c %d for req of type %d with enomem", c->sd,
				c->req_type);

		asc_write_server_error(c);
	} else {
		conn_set_state(c, CONN_MWRITE);
		c->msg_curr = 0;
	}

	return status;
}

static rstatus_t asc_write_iqget_value(struct conn *c, struct item* it) {
	rstatus_t status;
	char *suffix = NULL;
	int32_t total_len = 0;
	int sz;
	unsigned int valid_key_iter = 0;

	if (valid_key_iter >= c->isize) {
		struct item **new_list;

		new_list = mc_realloc(c->ilist, sizeof(struct item *) * c->isize * 2);
		if (new_list != NULL) {
			c->isize *= 2;
			c->ilist = new_list;
		}
	}

	status = conn_add_iov(c, "VALUE ", sizeof("VALUE ") - 1);
	if (status != MC_OK)
		return status;
	total_len += sizeof("VALUE " - 1);

	status = conn_add_iov(c, item_key(it), it->nkey);
	if (status != MC_OK)
		return status;
	total_len += it->nkey;

	status = asc_create_suffix(c, valid_key_iter, &suffix);
	if (status != MC_OK) {
		return status;
	}

	sz = mc_snprintf(suffix, SUFFIX_MAX_LEN, " %"PRIu32" %"PRIu8" %"PRIu32,
			it->dataflags, it->p, it->nbyte);
	ASSERT(sz <= SUFFIX_SIZE);
	status = conn_add_iov(c, suffix, sz);
	total_len += sz;

	status = conn_add_iov(c, CRLF, CRLF_LEN);
	if (status != MC_OK)
		return status;
	total_len += CRLF_LEN;

	status = conn_add_iov(c, item_data(it), it->nbyte);
	if (status != MC_OK)
		return status;
	total_len += sz;

	status = conn_add_iov(c, CRLF, CRLF_LEN);
	if (status != MC_OK)
		return status;
	total_len += CRLF_LEN;

	if (it != NULL) {
		*(c->ilist + valid_key_iter) = it;
		valid_key_iter++;
	}

	c->icurr = c->ilist;
	c->ileft = valid_key_iter;

	c->scurr = c->slist;
	c->sleft = valid_key_iter;

	log_debug(LOG_VVERB, ">%d END", c->sd);

	/*
	 * If the loop was terminated because of out-of-memory, it is not
	 * reliable to add END\r\n to the buffer, because it might not end
	 * in \r\n. So we send SERVER_ERROR instead.
	 */
	if (conn_add_iov(c, "END\r\n", 5) != MC_OK
			|| (c->udp && conn_build_udp_headers(c) != MC_OK)) {
		log_warn("server error on c %d for req of type %d with enomem", c->sd,
				c->req_type);

		asc_write_server_error(c);
	} else {
		conn_set_state(c, CONN_MWRITE);
		c->msg_curr = 0;
	}

	return status;
}

static rstatus_t asc_write_ciget_lease(struct conn *c, struct item* it,
		lease_token_t new_lease_token) {
	rstatus_t status;
	char *suffix = NULL;
	int32_t total_len = 0;
	int sz;
	unsigned int valid_key_iter = 0;

	status = conn_add_iov(c, "LVALUE ", sizeof("LVALUE ") - 1);
	if (status != MC_OK)
		return status;
	total_len += sizeof("LVALUE " - 1);

	status = conn_add_iov(c, item_key(it), it->nkey);
	if (status != MC_OK)
		return status;
	total_len += it->nkey;

	status = asc_create_suffix(c, valid_key_iter, &suffix);
	if (status != MC_OK) {
		return status;
	}

	uint32_t dataflags = it->dataflags;
	sz = mc_snprintf(suffix, SUFFIX_MAX_LEN, " %"PRIu32" %"PRIu64, dataflags,
			new_lease_token);
	ASSERT(sz <= SUFFIX_SIZE);
	status = conn_add_iov(c, suffix, sz);
	total_len += sz;

	status = conn_add_iov(c, CRLF, CRLF_LEN);
	if (status != MC_OK)
		return status;
	total_len += CRLF_LEN;

	log_debug(LOG_VVERB, ">%d END", c->sd);

	/*
	 * If the loop was terminated because of out-of-memory, it is not
	 * reliable to add END\r\n to the buffer, because it might not end
	 * in \r\n. So we send SERVER_ERROR instead.
	 */
	if (conn_add_iov(c, "END\r\n", 5) != MC_OK
			|| (c->udp && conn_build_udp_headers(c) != MC_OK)) {
		log_warn("server error on c %d for req of type %d with enomem", c->sd,
				c->req_type);

		asc_write_server_error(c);
	} else {
		conn_set_state(c, CONN_MWRITE);
		c->msg_curr = 0;
	}

	item_remove(it);

	return status;
}

static rstatus_t asc_write_iqget_lease(struct conn* c, struct item* it,
		lease_token_t new_lease_token) {
	rstatus_t status;
	char *suffix = NULL;
	int32_t total_len = 0;
	int sz;
	unsigned int valid_key_iter = 0;

	status = conn_add_iov(c, "LVALUE ", sizeof("LVALUE ") - 1);
	if (status != MC_OK)
		return status;
	total_len += sizeof("LVALUE " - 1);

	if (it != NULL) {
		status = conn_add_iov(c, item_key(it), it->nkey);
		if (status != MC_OK)
			return status;
		total_len += it->nkey;
	} else {
		status = conn_add_iov(c, "nokey", sizeof("nokey") - 1);
		if (status != MC_OK)
			return status;
		total_len += sizeof("nokey") - 1;
	}

	status = asc_create_suffix(c, valid_key_iter, &suffix);
	if (status != MC_OK) {
		return status;
	}

	uint32_t dataflags = it != NULL ? it->dataflags : 0;
	sz = mc_snprintf(suffix, SUFFIX_MAX_LEN, " %"PRIu32" %"PRIu8" %"PRIu64,
			dataflags, it->p, new_lease_token);
	ASSERT(sz <= SUFFIX_SIZE);
	status = conn_add_iov(c, suffix, sz);
	total_len += sz;

	status = conn_add_iov(c, CRLF, CRLF_LEN);
	if (status != MC_OK)
		return status;
	total_len += CRLF_LEN;

	log_debug(LOG_VVERB, ">%d END", c->sd);

	/*
	 * If the loop was terminated because of out-of-memory, it is not
	 * reliable to add END\r\n to the buffer, because it might not end
	 * in \r\n. So we send SERVER_ERROR instead.
	 */
	if (conn_add_iov(c, "END\r\n", 5) != MC_OK
			|| (c->udp && conn_build_udp_headers(c) != MC_OK)) {
		log_warn("server error on c %d for req of type %d with enomem", c->sd,
				c->req_type);

		asc_write_server_error(c);
	} else {
		conn_set_state(c, CONN_MWRITE);
		c->msg_curr = 0;

		c->scurr = c->slist;
		c->sleft = ++valid_key_iter;
	}

	return status;
}

static rstatus_t asc_write_qaread(struct conn *c, struct item* it, char *key,
		size_t nkey, lease_token_t lease_token, int8_t p) {
	char *suffix = NULL;
	rstatus_t status;
	int32_t total_len = 0;
	int sz;
	unsigned int valid_key_iter = 0;

	if (it == NULL) {
		status = conn_add_iov(c, "LVALUE ", sizeof("LVALUE ") - 1);
		if (status != MC_OK)
			return status;
		total_len += sizeof("LVALUE " - 1);
	} else {
		status = conn_add_iov(c, "LEASE ", sizeof("LEASE ") - 1);
		if (status != MC_OK)
			return status;
		total_len += sizeof("LEASE " - 1);
	}

	status = conn_add_iov(c, key, nkey);
	if (status != MC_OK)
		return status;
	total_len += nkey;

	status = asc_create_suffix(c, valid_key_iter, &suffix);
	if (status != MC_OK) {
		return status;
	}

	if (it == NULL) {
		sz = mc_snprintf(suffix, SUFFIX_MAX_LEN, " %"PRIu32" %"PRIu8" %"PRIu64,
				0, p, lease_token);
		ASSERT(sz <= SUFFIX_SIZE);
	} else {
		sz = mc_snprintf(suffix, SUFFIX_MAX_LEN,
				" %"PRIu32" %"PRIu8" %"PRIu64" %"PRIu32, it->dataflags, p,
				lease_token, it->nbyte);
		ASSERT(sz <= SUFFIX_SIZE);
	}

	status = conn_add_iov(c, suffix, sz);
	total_len += sz;

	status = conn_add_iov(c, CRLF, CRLF_LEN);
	if (status != MC_OK)
		return status;
	total_len += CRLF_LEN;

	if (it != NULL) {
		status = conn_add_iov(c, item_data(it), it->nbyte);
		if (status != MC_OK)
			return status;
		total_len += sz;
	}

	status = conn_add_iov(c, CRLF, CRLF_LEN);
	if (status != MC_OK)
		return status;
	total_len += CRLF_LEN;

	klog_write(c->peer, c->req_type, key, nkey, 0, total_len);

	if (it != NULL) {
		*(c->ilist + valid_key_iter) = it;
		valid_key_iter++;

		c->icurr = c->ilist;
		c->ileft = valid_key_iter;
	}

	c->scurr = c->slist;
	c->sleft = 1;

	if (conn_add_iov(c, "END\r\n", 5) != MC_OK
			|| (c->udp && conn_build_udp_headers(c) != MC_OK)) {
		log_warn("server error on c %d for req of type %d with enomem", c->sd,
				c->req_type);

		asc_write_server_error(c);
	} else {
		conn_set_state(c, CONN_MWRITE);
		c->msg_curr = 0;
	}

	return MC_OK;
}

static rstatus_t asc_write_oqread(struct conn *c, struct item* it, char *key,
		size_t nkey) {
	char *suffix = NULL;
	rstatus_t status;
	int32_t total_len = 0;
	int sz;
	unsigned int valid_key_iter = 0;

	if (it == NULL) {
		status = conn_add_iov(c, "NOVALUE ", sizeof("NOVALUE ") - 1);
		if (status != MC_OK)
			return status;
		total_len += sizeof("NOVALUE " - 1);
	} else {
		status = conn_add_iov(c, "VALUE ", sizeof("VALUE ") - 1);
		if (status != MC_OK)
			return status;
		total_len += sizeof("VALUE " - 1);
	}

	status = conn_add_iov(c, key, nkey);
	if (status != MC_OK)
		return status;
	total_len += nkey;

	status = asc_create_suffix(c, valid_key_iter, &suffix);
	if (status != MC_OK) {
		return status;
	}

	if (it == NULL) {
		sz = mc_snprintf(suffix, SUFFIX_MAX_LEN, " %"PRIu32, 0);
		ASSERT(sz <= SUFFIX_SIZE);
	} else {
		sz = mc_snprintf(suffix, SUFFIX_MAX_LEN, " %"PRIu32 " %"PRIu32,
				it->dataflags, it->nbyte);
		ASSERT(sz <= SUFFIX_SIZE);
	}

	status = conn_add_iov(c, suffix, sz);
	total_len += sz;

	status = conn_add_iov(c, CRLF, CRLF_LEN);
	if (status != MC_OK)
		return status;
	total_len += CRLF_LEN;

	if (it != NULL) {
		status = conn_add_iov(c, item_data(it), it->nbyte);
		if (status != MC_OK)
			return status;
		total_len += sz;
	}

	status = conn_add_iov(c, CRLF, CRLF_LEN);
	if (status != MC_OK)
		return status;
	total_len += CRLF_LEN;

	klog_write(c->peer, c->req_type, key, nkey, 0, total_len);

	*(c->ilist + valid_key_iter) = it;
	valid_key_iter++;

	c->icurr = c->ilist;
	c->ileft = valid_key_iter;

	c->scurr = c->slist;
	c->sleft = valid_key_iter;

	if (conn_add_iov(c, "END\r\n", 5) != MC_OK
			|| (c->udp && conn_build_udp_headers(c) != MC_OK)) {
		log_warn("server error on c %d for req of type %d with enomem", c->sd,
				c->req_type);

		asc_write_server_error(c);
	} else {
		conn_set_state(c, CONN_MWRITE);
		c->msg_curr = 0;
	}

	return MC_OK;
}

static rstatus_t asc_write_oqappends(struct conn* c, item_co_result_t* retco, int num_keys) {
	rstatus_t status;
	int i;

	for (i = 0; i < num_keys; ++i) {
		if (retco[i] == CO_OK) {
			status = conn_add_iov(c, STR_STORED, STORED_LEN);
			if (status != MC_OK)
				return status;
		} else if (retco[i] == CO_NOT_STORED) {
			status = conn_add_iov(c, STR_NOT_STORED, NOT_STORED_LEN);
			if (status != MC_OK)
				return status;
		}

		status = conn_add_iov(c, CRLF, CRLF_LEN);
		if (status != MC_OK)
			return status;
	}

	if (conn_add_iov(c, "END\r\n", 5) != MC_OK
			|| (c->udp && conn_build_udp_headers(c) != MC_OK)) {
		log_warn("server error on c %d for req of type %d with enomem", c->sd,
				c->req_type);

		asc_write_server_error(c);
	} else {
		conn_set_state(c, CONN_MWRITE);
		c->msg_curr = 0;
	}

	return MC_OK;
}

static void asc_write_client_error(struct conn *c) {
	const char *str = "CLIENT_ERROR";
	size_t len = sizeof("CLIENT_ERROR") - 1;

	stats_thread_incr(cmd_error);

	asc_write_string(c, str, len);
}

void asc_write_server_error(struct conn *c) {
	const char *str = "SERVER_ERROR";
	size_t len = sizeof("SERVER_ERROR") - 1;

	stats_thread_incr(server_error);

	asc_write_string(c, str, len);
}

static void asc_write_ok(struct conn *c) {
	const char *str = "OK";
	size_t len = sizeof("OK") - 1;

	asc_write_string(c, str, len);
}

static rstatus_t asc_write_ok_pending(struct conn *c, uint8_t pending,
		uint64_t lease_token) {
	rstatus_t status;
	char *suffix = NULL;
	int32_t total_len = 0;
	int sz;

	// lease token match and value does not exist
	// it is acceptable, the client should not back-off
	//	status = conn_add_iov(c, "OK", sizeof("OK") - 1);
	//	if (status != MC_OK)
	//		return status;
	//	total_len += sizeof("OK" - 1);

	status = asc_create_suffix(c, 0, &suffix);
	if (status != MC_OK) {
		return status;
	}

	sz = mc_snprintf(suffix, SUFFIX_MAX_LEN, "OK %"PRIu8" %"PRIu64, pending,
			lease_token);
	ASSERT(sz <= SUFFIX_SIZE);
	status = conn_add_iov(c, suffix, sz);
	if (status != MC_OK)
		return status;
	total_len += sz;

	status = conn_add_iov(c, CRLF, CRLF_LEN);
	if (status != MC_OK)
		return status;
	total_len += CRLF_LEN;

	log_debug(LOG_VVERB, ">%d END", c->sd);

//	if (conn_add_iov(c, "END\r\n", 5) != MC_OK ||
//			(c->udp && conn_build_udp_headers(c) != MC_OK)) {
//		log_warn("server error on c %d for req of type %d with enomem", c->sd,
//				c->req_type);
//
//		asc_write_server_error(c);
//	} else {
	conn_set_state(c, CONN_MWRITE);
	c->msg_curr = 0;
//	}

	c->scurr = c->slist;
	c->sleft = 1;

	return status;
}

static rstatus_t asc_write_ok_novalue_pending(struct conn *c, uint8_t pending,
		uint64_t lease_token) {
	rstatus_t status;
	char *suffix = NULL;
	int32_t total_len = 0;
	int sz;

	status = asc_create_suffix(c, 0, &suffix);
	if (status != MC_OK) {
		return status;
	}

	sz = mc_snprintf(suffix, SUFFIX_MAX_LEN, "OK_NOVALUE %"PRIu8" %"PRIu64,
			pending, lease_token);
	ASSERT(sz <= SUFFIX_SIZE);
	status = conn_add_iov(c, suffix, sz);
	if (status != MC_OK)
		return status;
	total_len += sz;

	status = conn_add_iov(c, CRLF, CRLF_LEN);
	if (status != MC_OK)
		return status;
	total_len += CRLF_LEN;

	log_debug(LOG_VVERB, ">%d END", c->sd);

//	if (conn_add_iov(c, "END\r\n", 5) != MC_OK ||
//			(c->udp && conn_build_udp_headers(c) != MC_OK)) {
//		log_warn("server error on c %d for req of type %d with enomem", c->sd,
//				c->req_type);
//
//		asc_write_server_error(c);
//	} else {
	conn_set_state(c, CONN_MWRITE);
	c->msg_curr = 0;
//	}

	c->scurr = c->slist;
	c->sleft = 1;

	return status;
}

static rstatus_t asc_write_ok_val(struct conn *c, uint64_t val) {
	rstatus_t status;
	char *suffix = NULL;
	int32_t total_len = 0;
	int sz;

	// lease token match and value does not exist
	// it is acceptable, the client should not back-off
	status = conn_add_iov(c, "VALUE", sizeof("VALUE") - 1);
	if (status != MC_OK)
		return status;
	total_len += sizeof("VALUE" - 1);

	status = asc_create_suffix(c, 0, &suffix);
	if (status != MC_OK) {
		return status;
	}

	sz = mc_snprintf(suffix, SUFFIX_MAX_LEN, " %"PRIu64, val);
	ASSERT(sz <= SUFFIX_SIZE);
	status = conn_add_iov(c, suffix, sz);
	total_len += sz;

	status = conn_add_iov(c, CRLF, CRLF_LEN);
	if (status != MC_OK)
		return status;
	total_len += CRLF_LEN;

	log_debug(LOG_VVERB, ">%d END", c->sd);

	/*
	 * If the loop was terminated because of out-of-memory, it is not
	 * reliable to add END\r\n to the buffer, because it might not end
	 * in \r\n. So we send SERVER_ERROR instead.
	 */
	if (conn_add_iov(c, "END\r\n", 5) != MC_OK
			|| (c->udp && conn_build_udp_headers(c) != MC_OK)) {
		log_warn("server error on c %d for req of type %d with enomem", c->sd,
				c->req_type);

		asc_write_server_error(c);
	} else {
		conn_set_state(c, CONN_MWRITE);
		c->msg_curr = 0;
	}

	return status;
}

//static void
//asc_write_ok_novalue(struct conn *c)
//{
//	const char *str = "OK_NOVALUE";
//	size_t len = sizeof("OK_NOVALUE") - 1;
//
//	asc_write_string(c, str, len);
//}

static void asc_write_retry(struct conn *c) {
	const char *str = "RETRY";
	size_t len = sizeof("RETRY") - 1;

	asc_write_string(c, str, len);
}

static void asc_write_version(struct conn *c) {
	const char *str = "VERSION "
	MC_VERSION_STRING;
	size_t
	len = sizeof("VERSION " MC_VERSION_STRING) - 1;

	asc_write_string(c, str, len);
}

void asc_complete_nreads(struct conn *c) {
	struct item *it;
	char *end;
	int i;

	int num_items = c->num_items;
	item_co_result_t* retco = malloc(sizeof(item_co_result_t)*num_items);
	bool abort = false;

	for (i = 0; i < num_items; ++i) {
		it = (c->items)[i];
		ASSERT (it != NULL);

		if (it->nbyte > 0) {
			end = item_data(it) + it->nbyte;

			if (!strcrlf(end)) {
				log_hexdump(LOG_NOTICE, c->req, c->req_len, "client error on c %d for "
						"req of type %d with missing crlf", c->sd, c->req_type);

				asc_write_client_error(c);
				abort = true;
				break;
			}

			if (c->req_type == REQ_OQAPPENDS) {
				retco[i] = item_oqappend_oqprepend(c, it);
			} else if (c->req_type == REQ_OQSWAPS) {
				retco[i] = item_oqswap(c->tid, c->ntid, it, c);
			}

			if (retco[i] == CO_ABORT) {
				asc_write_abort(c);
				abort = true;
				break;
			} else if (retco[i] == CO_INVALID) {
				asc_write_invalid(c);
				abort = true;
				break;
			}

		} else {
			if (c->req_type == REQ_OQSWAPS) {
				retco[i] = item_oqswap(c->tid, c->ntid, it, c);
				if (retco[i] == CO_ABORT) {
					asc_write_abort(c);
					abort = true;
					break;
				} else if (retco[i] == CO_INVALID) {
					asc_write_invalid(c);
					abort = true;
					break;
				}
			} else {
				asc_write_invalid(c);
				abort = true;
				break;
			}
		}
	}

	for (i = 0; i < num_items; ++i) {
		it = (c->items)[i];
		ASSERT (it != NULL);
		item_remove(it);
		(c->items)[i] = NULL;
	}

	if (abort == false) {
		asc_write_oqappends(c, retco, num_items);
//		size_t max_size = 100;
//		char* res = malloc(sizeof(char)*max_size);
//		size_t size = 0;
//		for (i = 0; i < num_items; ++i) {
//			if (retco[i] == CO_OK) {
//				asc_add_response(&res, &max_size, &size, STR_STORED, STORED_LEN);
//				asc_add_response(&res, &max_size, &size, CRLF, CRLF_LEN);
//			} else if (retco[i] == CO_NOT_STORED) {
//				asc_add_response(&res, &max_size, &size, STR_NOT_STORED, NOT_STORED_LEN);
//				asc_add_response(&res, &max_size, &size, CRLF, CRLF_LEN);
//			}
//		}
//		asc_add_response(&res, &max_size, &size, STR_END, END_LEN);
//		asc_write_string(c, res, size);
//		free(res);
	}

	free(retco);
	free(c->ritems);
	free(c->rrlbytes);
	free(c->items);
}

void asc_add_response(char** res, size_t *max_size, size_t *size, const char* str, size_t len) {
	if (*size + len > *max_size) {
		while (*size + len > *max_size) {
			*max_size = *max_size * 2;
		}
		*res = realloc(*res, *max_size);
	}
	memmove(*res+(*size), str, len);
	*size += len;
}

/*
 * We get here after reading the value in update commands. The command
 * is stored in c->req_type, and the item is ready in c->item.
 */
void asc_complete_nread(struct conn *c) {
	item_store_result_t ret;
	item_co_result_t retco;
	struct item *it;
	char *end;
	char* sid;
	size_t nsid;

	it = c->item;
	sid = c->tid;
	nsid = c->ntid;
	end = item_data(it) + it->nbyte;

	if (!strcrlf(end)) {
		log_hexdump(LOG_NOTICE, c->req, c->req_len, "client error on c %d for "
				"req of type %d with missing crlf", c->sd, c->req_type);

		asc_write_client_error(c);
	} else if (c->req_type == REQ_IQAPPEND || c->req_type == REQ_IQPREPEND) {
		uint8_t pending = 0;
		uint64_t new_lease_token = 0;
		item_iq_result_t retiq = item_iqappend_iqprepend(c, it, &pending,
				&new_lease_token);

		switch (retiq) {
		case IQ_LEASE:
			asc_write_ok_pending(c, pending, new_lease_token);
			break;
		case IQ_LEASE_NO_VALUE:
			asc_write_ok_novalue_pending(c, pending, new_lease_token);
			break;
		case IQ_ABORT:
			asc_write_abort(c);
			break;
		case IQ_SERVER_ERROR:
			asc_write_server_error(c);
			break;
		case IQ_CLIENT_ERROR:
			asc_write_client_error(c);
			break;
		case IQ_NOT_STORED:
			asc_write_not_stored(c);
			break;
		default:
			break;
		}
	} else if (c->req_type == REQ_OQAPPEND || c->req_type == REQ_OQPREPEND) {
		retco = item_oqappend_oqprepend(c, it);
		switch (retco) {
		case CO_RETRY:
			asc_write_retry(c);
			break;
		case CO_OK:
			asc_write_stored(c);
			break;
		case CO_NOT_STORED:
			asc_write_not_stored(c);
			break;
		case CO_ABORT:
			asc_write_abort(c);
			break;
		default:
			break;
		}
	} else if (c->req_type == REQ_OQAPPEND2) {
		retco = CO_OK; //item_oqappend_oqprepend2(c, it);
	} else if (c->req_type == REQ_SAR) {
		ret = item_swap_and_release(it, c);
		switch (ret) {
		case STORED:
			asc_write_stored(c);
			break;
		case NOT_STORED:
			asc_write_not_stored(c);
			break;
		default:
			asc_write_invalid_deleted(c);
			break;
		}
	} else if (c->req_type == REQ_OQSWAP) {
		retco = item_oqswap(sid, nsid, it, c);
		switch (retco) {
		case CO_OK:
			asc_write_stored(c);
			break;
		case CO_RETRY:
			asc_write_retry(c);
			break;
		case CO_ABORT:
			asc_write_abort(c);
			break;
		default:
			asc_write_invalid(c);
			break;
		}
	} else if (c->req_type == REQ_SWAP) {
		item_store_result_t retiq = item_swap(it, c);
		switch (retiq) {
		case STORED:
			asc_write_stored(c);
			break;
		case NOT_STORED:
			asc_write_abort(c);
			break;
		default:
			asc_write_invalid(c);
			break;
		}
	} else if (c->req_type == REQ_OQWRITE) {
		item_co_result_t retco = item_oqwrite(sid, nsid, it, c);
		switch (retco) {
		case CO_OK:
			asc_write_stored(c);
			break;
		case CO_RETRY:
			asc_write_retry(c);
			break;
		case CO_ABORT:
			asc_write_abort(c);
			break;
		default:
			asc_write_invalid(c);
			break;
		}
	} else if (c->req_type == REQ_OQADD) {
		item_co_result_t retco = item_oqadd(sid, nsid, it, c);
		switch (retco) {
		case CO_OK:
			asc_write_stored(c);
			break;
		case CO_RETRY:
			asc_write_retry(c);
			break;
		case CO_NOT_STORED:
			asc_write_not_stored(c);
			break;
		case CO_ABORT:
			asc_write_abort(c);
			break;
		default:
			asc_write_invalid(c);
			break;
		}
	} else {
		ret = item_store(it, c->req_type, c);
		switch (ret) {
		case STORED:
			asc_write_stored(c);
			break;

		case EXISTS:
			asc_write_exists(c);
			break;

		case NOT_FOUND:
			asc_write_not_found(c);
			break;

		case NOT_STORED:
			asc_write_not_stored(c);
			break;

		default:
			log_warn("server error on c %d for req of type %d with unknown "
					"store result %d", c->sd, c->req_type, ret);

			asc_write_server_error(c);
			break;
		}
	}

	item_remove(it);
	c->item = NULL;
}

static void asc_set_noreply_maybe(struct conn *c, struct token *token,
		int ntoken) {
	struct token *t;

	if (ntoken < 2) {
		return;
	}

	t = &token[ntoken - 2];

	if ((t->len == sizeof("noreply") - 1)
			&& str7cmp(t->val, 'n', 'o', 'r', 'e', 'p', 'l', 'y')) {
		c->noreply = 1;
	}
}

static rstatus_t asc_respond_cogets_retry(struct conn *c, char* key,
		size_t nkey) {
	rstatus_t status = conn_add_iov(c, "RETRY ", sizeof("RETRY ") - 1);
	if (status != MC_OK) {
		return status;
	}

	status = conn_add_iov(c, key, nkey);
	if (status != MC_OK) {
		return status;
	}

	status = conn_add_iov(c, CRLF, CRLF_LEN);
	if (status != MC_OK) {
		return status;
	}

	return status;
}

static rstatus_t asc_respond_cogets_novalue(struct conn *c, char* key,
		size_t nkey) {
	rstatus_t status = conn_add_iov(c, "NOVALUE ", sizeof("NOVALUE ") - 1);
	if (status != MC_OK) {
		return status;
	}

	status = conn_add_iov(c, key, nkey);
	if (status != MC_OK) {
		return status;
	}

	status = conn_add_iov(c, CRLF, CRLF_LEN);
	if (status != MC_OK) {
		return status;
	}

	return status;
}

static rstatus_t asc_respond_cogets_lease(struct conn *c,
		unsigned valid_key_iter, lease_token_t new_lease_token, char* key,
		size_t nkey) {
	char *suffix = NULL;
	int sz;

	rstatus_t status = conn_add_iov(c, "LEASE ", sizeof("LEASE ") - 1);
	if (status != MC_OK) {
		return status;
	}

	status = conn_add_iov(c, key, nkey);
	if (status != MC_OK) {
		return status;
	}

	status = asc_create_suffix(c, valid_key_iter, &suffix);
	if (status != MC_OK) {
		return status;
	}

	sz = mc_snprintf(suffix, SUFFIX_MAX_LEN, " %"PRIu64, new_lease_token);
	ASSERT(sz <= SUFFIX_SIZE);
	if (sz < 0) {
		return MC_ERROR;
	}

	status = conn_add_iov(c, suffix, sz);
	if (status != MC_OK) {
		return status;
	}

	status = conn_add_iov(c, CRLF, CRLF_LEN);
	if (status != MC_OK) {
		return status;
	}

	return status;
}

/*
 * Build the response. Each hit adds three elements to the outgoing
 * reponse vector, viz:
 *   "VALUE "
 *   key
 *   " " + flags + " " + data length + "\r\n" + data (with \r\n)
 */
static rstatus_t asc_respond_get(struct conn *c, unsigned valid_key_iter,
		struct item *it, char* key, size_t nkey,
		bool return_cas) {
	rstatus_t status;
	char *suffix = NULL;
	int sz;
	int total_len = 0;
	uint32_t nbyte = it->nbyte;
	char *data = item_data(it);

	status = conn_add_iov(c, VALUE, VALUE_LEN);
	if (status != MC_OK) {
		return status;
	}
	total_len += VALUE_LEN;

	status = conn_add_iov(c, key, nkey);
	if (status != MC_OK) {
		return status;
	}
	total_len += it->nkey;

	status = asc_create_suffix(c, valid_key_iter, &suffix);
	if (status != MC_OK) {
		return status;
	}
	if (return_cas) {
		sz = mc_snprintf(suffix, SUFFIX_MAX_LEN, " %"PRIu32" %"PRIu32" %"PRIu64,
				it->dataflags, nbyte, item_cas(it));
		ASSERT(sz <= SUFFIX_SIZE + CAS_SUFFIX_SIZE);
	} else {
		sz = mc_snprintf(suffix, SUFFIX_MAX_LEN, " %"PRIu32" %"PRIu32,
				it->dataflags, nbyte);
		ASSERT(sz <= SUFFIX_SIZE);
	}
	if (sz < 0) {
		return MC_ERROR;
	}

	status = conn_add_iov(c, suffix, sz);
	if (status != MC_OK) {
		return status;
	}
	total_len += sz;

	status = conn_add_iov(c, CRLF, CRLF_LEN);
	if (status != MC_OK) {
		return status;
	}
	total_len += CRLF_LEN;

	status = conn_add_iov(c, data, nbyte);
	if (status != MC_OK) {
		return status;
	}
	total_len += nbyte;

	status = conn_add_iov(c, CRLF, CRLF_LEN);
	if (status != MC_OK) {
		return status;
	}
	total_len += CRLF_LEN;

	klog_write(c->peer, c->req_type, key, nkey, 0, total_len);

	return MC_OK;
}

static inline void asc_process_read(struct conn *c, struct token *token,
		int ntoken) {
	rstatus_t status;
	char *key;
	size_t nkey;
	unsigned valid_key_iter = 0;
	struct item *it;
	struct token *key_token;
	bool return_cas;

	if (!asc_ntoken_valid(c, ntoken)) {
		log_hexdump(LOG_NOTICE, c->req, c->req_len, "client error on c %d for "
				"req of type %d with %d invalid tokens", c->sd,
				c->req_type, ntoken);

		asc_write_client_error(c);
		return;
	}

	return_cas = (c->req_type == REQ_GETS) ? true : false;
	key_token = &token[TOKEN_KEY];

	do {
		while (key_token->len != 0) {

			key = key_token->val;
			nkey = key_token->len;

			if (nkey > KEY_MAX_LEN) {
				log_debug(LOG_NOTICE, "client error on c %d for req of type %d "
						"and %d length key", c->sd, c->req_type, nkey);

				asc_write_client_error(c);
				return;
			}

			if (return_cas) {
				stats_thread_incr(gets);
			} else {
				stats_thread_incr(get);
			}

			it = item_get(key, nkey);

			if (it != NULL) {
				/* item found */
				if (return_cas) {
					stats_slab_incr(it->id, gets_hit);
				} else {
					stats_slab_incr(it->id, get_hit);
				}

				if (valid_key_iter >= c->isize) {
					struct item **new_list;

					new_list = mc_realloc(c->ilist,
							sizeof(struct item *) * c->isize * 2);
					if (new_list != NULL) {
						c->isize *= 2;
						c->ilist = new_list;
					} else {
						item_remove(it);
						break;
					}
				}

				status = asc_respond_get(c, valid_key_iter, it, key, nkey, return_cas);
				if (status != MC_OK) {
					log_debug(LOG_NOTICE, "client error on c %d for req of type "
							"%d with %d tokens", c->sd, c->req_type, ntoken);

					stats_thread_incr(cmd_error);
					item_remove(it);
					break;
				}

				log_debug(LOG_VVERB, ">%d sending key %.*s", c->sd, it->nkey,
						item_key(it));

				item_touch(it);
				*(c->ilist + valid_key_iter) = it;
				valid_key_iter++;
			}

			key_token++;
		}

		/*
		 * If the command string hasn't been fully processed, get the next set
		 * of token.
		 */
		if (key_token->val != NULL) {
			ntoken = asc_tokenize(key_token->val, token, TOKEN_MAX);
			/* ntoken is unused */
			key_token = token;
		}

	} while (key_token->val != NULL);

	c->icurr = c->ilist;
	c->ileft = valid_key_iter;

	c->scurr = c->slist;
	c->sleft = valid_key_iter;

	log_debug(LOG_VVERB, ">%d END", c->sd);

	/*
	 * If the loop was terminated because of out-of-memory, it is not
	 * reliable to add END\r\n to the buffer, because it might not end
	 * in \r\n. So we send SERVER_ERROR instead.
	 */
	if (key_token->val != NULL || conn_add_iov(c, "END\r\n", 5) != MC_OK
			|| (c->udp && conn_build_udp_headers(c) != MC_OK)) {
		log_warn("server error on c %d for req of type %d with enomem", c->sd,
				c->req_type);

		asc_write_server_error(c);
	} else {
		conn_set_state(c, CONN_MWRITE);
		c->msg_curr = 0;
	}
}

static inline void asc_process_cogets(struct conn *c, struct token *token,
		int ntoken) {
	rstatus_t status;
	char *key;
	uint8_t nkey;
	unsigned valid_key_iter = 0;
	struct token *key_token;
	char *sid;
	size_t sid_size = 0;
	lease_token_t lease_current_token = 0;
	lease_token_t new_lease_token = 0;
	struct item* it = NULL;

	if (!asc_ntoken_valid(c, ntoken)) {
		log_hexdump(LOG_NOTICE, c->req, c->req_len, "client error on c %d for "
				"req of type %d with %d invalid tokens", c->sd,
				c->req_type, ntoken);

		asc_write_client_error(c);
		return;
	}

	// get sid
	sid = token[1].val;
	sid_size = token[1].len;

	// get type of lease
	uint32_t lease_type = 0;
	if (!mc_strtoul(token[2].val, &lease_type)) {
		log_debug(LOG_NOTICE, "client error on c %d for req of type %d and "
				"invalid lease type '%.*s'", c->sd, c->req_type,
				token[2].len, token[2].val);

		asc_write_client_error(c);
		return;
	}
	if (lease_type != 0 && lease_type != 1) {
		log_debug(LOG_NOTICE, "client error on c %d for req of type %d and "
				"invalid lease type '%.*s'", c->sd, c->req_type,
				token[2].len, token[2].val);

		asc_write_client_error(c);
		return;
	}

	key_token = &token[3];
	int num_keys = 0;
	do {
		while (key_token->len != 0) {
			key_token++;
			num_keys++;
		}

		/*
		 * If the command string hasn't been fully processed, get the next set
		 * of token.
		 */
		if (key_token->val != NULL) {
			ntoken = asc_tokenize(key_token->val, token, TOKEN_MAX);
			/* ntoken is unused */
			key_token = token;
		}
	} while (key_token->val != NULL);

	struct item** its = malloc(num_keys * sizeof(struct item*));
	int i = 0;
	for (i = 0; i < num_keys; ++i) {
		its[i] = NULL;
	}
	lease_token_t* new_lease_tokens = malloc(num_keys * sizeof(lease_token_t));
	for (i = 0; i < num_keys; ++i) {
		new_lease_tokens[i] = 0;
	}

	item_co_result_t* results = malloc(num_keys * sizeof(item_co_result_t));
	char** keys = malloc(num_keys * sizeof(char*));
	size_t* key_sizes = malloc(num_keys * sizeof(size_t));

	asc_tokenize(c->req, token, TOKEN_MAX);
	key_token = &token[3];
	i = 0;
	bool abort = false;
	do {
		while (key_token->len != 0) {
			key = key_token->val;
			nkey = key_token->len;
			keys[i] = key;
			key_sizes[i] = nkey;

			if (nkey > KEY_MAX_LEN) {
				log_debug(LOG_NOTICE, "client error on c %d for req of type %d "
						"and %d length key", c->sd, c->req_type, nkey);
				asc_write_client_error(c);
				return;
			}

			if (lease_type == 1) {
				stats_thread_incr(oqread);
				results[i] = item_oqread(sid, sid_size, key, nkey, c,
						&(its[i]));
			} else {
				ASSERT(lease_type == 0);

				stats_thread_incr(ciget);
				results[i] = item_ciget(sid, sid_size, key, nkey,
						lease_current_token, c, &(its[i]),
						&(new_lease_tokens[i]));
			}

			if (results[i] == CO_ABORT) {
				abort = true;
				break;
			}

			key_token++;
			i++;
		}

		if (abort == true) {
			break;
		}

		/*
		 * If the command string hasn't been fully processed, get the next set
		 * of token.
		 */
		if (key_token->val != NULL) {
			ntoken = asc_tokenize(key_token->val, token, TOKEN_MAX);
			/* ntoken is unused */
			key_token = token;
		}

	} while (key_token->val != NULL);

	if (abort == true) {
		for (i = 0; i < num_keys; ++i) {
			it = its[i];
			if (it != NULL) {
				item_remove(it);
			}
		}
		asc_write_abort(c);
	} else {
		// produce the answer
		for (i = 0; i < num_keys; ++i) {
			item_co_result_t result = results[i];
			char* key = keys[i];
			size_t nkey = key_sizes[i];

			switch (result) {
			case CO_RETRY:
				asc_respond_cogets_retry(c, key, nkey);
				break;
			case CO_OK:
				it = its[i];
				new_lease_token = new_lease_tokens[i];

				if (new_lease_token != 0) {
					ASSERT(it != NULL);
					asc_respond_cogets_lease(c, valid_key_iter, new_lease_token,
							key, nkey);
					item_remove(it);
				} else if (it == NULL || item_data(it) == NULL) {
					asc_respond_cogets_novalue(c, key, nkey);
					if (it != NULL) {
						item_remove(it);
					}
				} else {
					/* item found */
					stats_slab_incr(it->id, get_hit);

					if (valid_key_iter >= c->isize) {
						struct item **new_list;

						new_list = mc_realloc(c->ilist,
								sizeof(struct item *) * c->isize * 2);
						if (new_list != NULL) {
							c->isize *= 2;
							c->ilist = new_list;
						} else {
							item_remove(it);
							break;
						}
					}

					status = asc_respond_get(c, valid_key_iter, it, key, nkey, false);
					if (status != MC_OK) {
						log_debug(LOG_NOTICE, "client error on c %d for req of type "
								"%d with %d tokens", c->sd, c->req_type, ntoken);

						stats_thread_incr(cmd_error);
						item_remove(it);
						break;
					}

					log_debug(LOG_VVERB, ">%d sending key %.*s", c->sd, it->nkey,
							item_key(it));

					item_touch(it);
					*(c->ilist + valid_key_iter) = it;
					valid_key_iter++;

					c->icurr = c->ilist;
					c->ileft = valid_key_iter;

					c->scurr = c->slist;
					c->sleft = valid_key_iter;
				}
				break;
			default:
				break;
			}
		}

		/*
		 * If the loop was terminated because of out-of-memory, it is not
		 * reliable to add END\r\n to the buffer, because it might not end
		 * in \r\n. So we send SERVER_ERROR instead.
		 */
		if (key_token->val != NULL || conn_add_iov(c, "END\r\n", 5) != MC_OK
				|| (c->udp && conn_build_udp_headers(c) != MC_OK)) {
			log_warn("server error on c %d for req of type %d with enomem", c->sd,
					c->req_type);

			asc_write_server_error(c);
		} else {
			conn_set_state(c, CONN_MWRITE);
			c->msg_curr = 0;
		}
	}

	free(keys);
	free(key_sizes);
	free(its);
	free(new_lease_tokens);
	free(results);
}

static void asc_process_update(struct conn *c, struct token *token, int ntoken) {
	char *key;
	int64_t lease_token;
	size_t keylen;
	uint8_t nkey;
	uint32_t flags, vlen;
	int32_t exptime_int;
	time_t exptime;
	uint64_t req_cas_id = 0;
	struct item *it;
	bool handle_cas;
	req_type_t type;
	uint8_t id;
	uint32_t pimem = 0;

	asc_set_noreply_maybe(c, token, ntoken);

	if (!asc_ntoken_valid(c, ntoken)) {
		log_hexdump(LOG_NOTICE, c->req, c->req_len, "client error on c %d for "
				"req of type %d with %d invalid tokens", c->sd,
				c->req_type, ntoken);

		asc_write_client_error(c);
		return;
	}

	type = c->req_type;
	handle_cas = (type == REQ_CAS) ? true : false;
	key = token[TOKEN_KEY].val;
	keylen = token[TOKEN_KEY].len;

	if (keylen > KEY_MAX_LEN) {
		log_debug(LOG_NOTICE, "client error on c %d for req of type %d and %d "
				"length key", c->sd, c->req_type, keylen);

		asc_write_client_error(c);
		return;
	} else {
		nkey = (uint8_t) keylen;
	}

	if (!mc_strtoul(token[TOKEN_FLAGS].val, &flags)) {
		log_debug(LOG_NOTICE, "client error on c %d for req of type %d and "
				"invalid flags '%.*s'", c->sd, c->req_type,
				token[TOKEN_FLAGS].len, token[TOKEN_FLAGS].val);

		asc_write_client_error(c);
		return;
	}

	if (!mc_strtol(token[TOKEN_EXPIRY].val, &exptime_int)) {
		log_debug(LOG_NOTICE, "client error on c %d for req of type %d and "
				"invalid expiry '%.*s'", c->sd, c->req_type,
				token[TOKEN_EXPIRY].len, token[TOKEN_EXPIRY].val);

		asc_write_client_error(c);
		return;
	}

	if (!mc_strtoul(token[TOKEN_VLEN].val, &vlen)) {
		log_debug(LOG_NOTICE, "client error on c %d for req of type %d and "
				"invalid vlen '%.*s'", c->sd, c->req_type,
				token[TOKEN_VLEN].len, token[TOKEN_VLEN].val);

		asc_write_client_error(c);
		return;
	}

	/* Obtain lease token */
	if (type == REQ_IQSET) {
		if (!mc_strtoll(token[TOKEN_LEASE_TOKEN].val, &lease_token)) {
			log_debug(LOG_NOTICE, "client error on c %d for req of type %d and "
					"invalid token '%.*s'", c->sd, c->req_type,
					token[TOKEN_LEASE_TOKEN].len, token[TOKEN_LEASE_TOKEN].val);

			asc_write_client_error(c);
			return;
		}
	} else {
		if (!mc_strtoul(token[5].val, &pimem)) {
			log_debug(LOG_NOTICE, "client error on c %d for req of type %d and "
					"invalid pimem", c->sd, c->req_type);

			asc_write_client_error(c);
			return;
		}
	}

	if (pimem == 1) {
		size_t size = item_ntotal(nkey, vlen, false);
		if (item_get_pimem_size() + size > settings.max_pimem_size) {
			asc_write_not_stored(c);
			c->write_and_go = CONN_SWALLOW;
			c->sbytes = vlen + CRLF_LEN;
			return;
		}
	}

	id = item_slabid(nkey, vlen);
	if (id == SLABCLASS_INVALID_ID) {
		log_debug(LOG_NOTICE, "client error on c %d for req of type %d and "
				"slab id out of range for key size %"PRIu8" and value size "
				"%"PRIu32, c->sd, c->req_type, nkey, vlen);

		asc_write_client_error(c);

		/* swallow the data line */
		c->write_and_go = CONN_SWALLOW;
		c->sbytes = vlen + CRLF_LEN;

		return;
	}

	exptime = (time_t) exptime_int;

	/* does cas value exist? */
	if (handle_cas) {
		if (!mc_strtoull(token[TOKEN_CAS].val, &req_cas_id)) {
			log_debug(LOG_NOTICE, "client error on c %d for req of type %d and "
					"invalid cas '%.*s'", c->sd, c->req_type,
					token[TOKEN_CAS].len, token[TOKEN_CAS].val);

			asc_write_client_error(c);
			return;
		}
	}

	it = item_alloc(id, key, nkey, flags, time_reltime(exptime), vlen);
	if (it == NULL) {
//		log_warn("server error on c %d for req of type %d because of oom in "
//				"storing item", c->sd, c->req_type);
		log_warn("could not set key(%d, %s) - value(%d) ", nkey, key, vlen);

		item_clean(id, key, nkey);

//		asc_write_server_error(c);
		asc_write_not_stored(c);

		/* swallow the data line */
		c->write_and_go = CONN_SWALLOW;
		c->sbytes = vlen + CRLF_LEN;

		/*
		 * Avoid stale data persisting in cache because we failed alloc.
		 * Unacceptable for SET. Anywhere else too?
		 *
		 * FIXME: either don't delete anything or should be unacceptable for
		 * all but add.
		 */
//		if (type == REQ_IQSET || type == REQ_SET) {
//			it = item_get(key, nkey);
//
//			if (it != NULL) {
//				item_delete(it);
//			}
//		}
		return;
	}

	item_set_cas(it, req_cas_id);

	if (type == REQ_IQSET) {
		c->lease_token = (uint64_t) lease_token;
	}

	if (pimem) {
		item_set_pimem(it);
	}

	c->item = it;
	c->ritem = item_data(it);
	c->rlbytes = it->nbyte + CRLF_LEN;
	conn_set_state(c, CONN_NREAD);
}

static void asc_process_arithmetic(struct conn *c, struct token *token,
		int ntoken) {
	item_delta_result_t res;
	char temp[INCR_MAX_STORAGE_LEN];
	uint64_t delta;
	char *key;
	size_t nkey;
	bool incr;

	asc_set_noreply_maybe(c, token, ntoken);

	if (!asc_ntoken_valid(c, ntoken)) {
		log_hexdump(LOG_NOTICE, c->req, c->req_len, "client error on c %d for "
				"req of type %d with %d invalid tokens", c->sd,
				c->req_type, ntoken);

		asc_write_client_error(c);
		return;
	}

	incr = (c->req_type == REQ_INCR) ? true : false;
	key = token[TOKEN_KEY].val;
	nkey = token[TOKEN_KEY].len;

	if (nkey > KEY_MAX_LEN) {
		log_debug(LOG_NOTICE, "client error on c %d for req of type %d and %d "
				"length key", c->sd, c->req_type, nkey);

		asc_write_client_error(c);
		return;
	}

	if (!mc_strtoull(token[TOKEN_DELTA].val, &delta)) {
		log_debug(LOG_NOTICE, "client error on c %d for req of type %d with "
				"invalid delta '%.*s'", c->sd, c->req_type,
				token[TOKEN_DELTA].len, token[TOKEN_DELTA].val);

		asc_write_client_error(c);
		return;
	}

	res = item_add_delta(c, key, nkey, incr, delta, temp);
	switch (res) {
	case DELTA_OK:
		asc_write_string(c, temp, strlen(temp));
		klog_write(c->peer, c->req_type, c->req, c->req_len, res, strlen(temp));
		break;

	case DELTA_NON_NUMERIC:
		log_debug(LOG_NOTICE, "client error on c %d for req of type %d with "
				"non-numeric value", c->sd, c->req_type);

		asc_write_client_error(c);
		break;

	case DELTA_EOM:
		log_warn("server error on c %d for req of type %d because of oom",
				c->sd, c->req_type);

		asc_write_server_error(c);
		break;

	case DELTA_NOT_FOUND:
		if (incr) {
			stats_thread_incr(incr_miss);
		} else {
			stats_thread_incr(decr_miss);
		}
		asc_write_not_found(c);
		break;

	default:
		NOT_REACHED();
		break;
	}
}

static item_store_result_t asc_delete(struct conn *c, char* key, size_t nkey) {
	return item_get_and_delete(key, nkey, c);
}

static void asc_process_delete(struct conn *c, struct token *token, int ntoken) {
	char *key; /* key to be deleted */
	size_t nkey; /* # key bytes */
	item_store_result_t res;

	asc_set_noreply_maybe(c, token, ntoken);

	if (!asc_ntoken_valid(c, ntoken)) {
		log_hexdump(LOG_NOTICE, c->req, c->req_len, "client error on c %d for "
				"req of type %d with %d invalid tokens", c->sd,
				c->req_type, ntoken);

		asc_write_client_error(c);
		return;
	}

	key = token[TOKEN_KEY].val;
	nkey = token[TOKEN_KEY].len;

	if (nkey > KEY_MAX_LEN) {
		log_debug(LOG_NOTICE, "client error on c %d for req of type %d and %d "
				"length key", c->sd, c->req_type, nkey);

		asc_write_client_error(c);
		return;
	}

	res = asc_delete(c, key, nkey);
	if (res == EXISTS || res == STORED) {
		asc_write_deleted(c);
	} else if (res == NOT_FOUND) {
		asc_write_not_found(c);
	} else {
		asc_write_server_error(c);
	}
}

static void asc_process_unlease(struct conn *c, struct token *token, int ntoken) {
	char *key; /* key to be released */
	size_t nkey; /* # key bytes */
	int64_t lease_token;

	asc_set_noreply_maybe(c, token, ntoken);

	if (!asc_ntoken_valid(c, ntoken)) {
		log_hexdump(LOG_NOTICE, c->req, c->req_len, "client error on c %d for "
				"req of type %d with %d invalid tokens", c->sd,
				c->req_type, ntoken);

		asc_write_client_error(c);
		return;
	}

	key = token[TOKEN_KEY].val;
	nkey = token[TOKEN_KEY].len;

	if (nkey > KEY_MAX_LEN) {
		log_debug(LOG_NOTICE, "client error on c %d for req of type %d and %d "
				"length key", c->sd, c->req_type, nkey);

		asc_write_client_error(c);
		return;
	}

	/* Obtain lease token */
	if (!mc_strtoll(token[TOKEN_LEASE_RELEASE].val, &lease_token)) {
		log_debug(LOG_NOTICE, "client error on c %d for req of type %d and "
				"invalid token '%.*s'", c->sd, c->req_type,
				token[TOKEN_LEASE_RELEASE].len, token[TOKEN_LEASE_RELEASE].val);

		asc_write_client_error(c);
		return;
	}

	if (item_get_and_unlease(key, nkey, lease_token, c) == MC_OK) {
		asc_write_deleted(c);
	} else {
		asc_write_server_error(c);
	}
}

static void asc_process_dcommit(struct conn *c, struct token *token, int ntoken) {
	char *sid = NULL;
	size_t sid_size; /* # key bytes */

	// check whether this request need reply or not
	asc_set_noreply_maybe(c, token, ntoken);

	if (!asc_ntoken_valid(c, ntoken)) {
		log_hexdump(LOG_NOTICE, c->req, c->req_len, "client error on c %d for "
				"req of type %d with %d invalid tokens", c->sd,
				c->req_type, ntoken);

		asc_write_client_error(c);
		return;
	}

	// get sid
	sid = token[1].val;
	sid_size = token[1].len;

	if (sid_size > KEY_MAX_LEN) {
		log_debug(LOG_NOTICE, "client error on c %d for req of type %d and %d "
				"length key", c->sd, c->req_type, sid_size);

		asc_write_client_error(c);
		return;
	}

	item_co_result_t exc = item_dcommit(sid, sid_size, c);

	if (exc == CO_OK) {
		asc_write_ok(c);
	} else if (exc == CO_INVALID) {
		asc_write_invalid(c);
	} else if (exc == CO_NOT_FOUND) {
		asc_write_not_found(c);
	} else if (exc == CO_ABORT) {
		asc_write_abort(c);
	} else {
		asc_write_server_error(c);
	}
}

static void asc_process_dabort(struct conn *c, struct token *token, int ntoken) {
	char *sid = NULL;
	size_t sid_size; /* # key bytes */

	// check whether this request need reply or not
	asc_set_noreply_maybe(c, token, ntoken);

	if (!asc_ntoken_valid(c, ntoken)) {
		log_hexdump(LOG_NOTICE, c->req, c->req_len, "client error on c %d for "
				"req of type %d with %d invalid tokens", c->sd,
				c->req_type, ntoken);

		asc_write_client_error(c);
		return;
	}

	// get sid
	sid = token[1].val;
	sid_size = token[1].len;

	if (sid_size > KEY_MAX_LEN) {
		log_debug(LOG_NOTICE, "client error on c %d for req of type %d and %d "
				"length key", c->sd, c->req_type, sid_size);

		asc_write_client_error(c);
		return;
	}

	item_co_result_t exc = item_dabort(sid, sid_size, c);

	if (exc == CO_OK) {
		asc_write_ok(c);
	} else if (exc == CO_INVALID) {
		asc_write_invalid(c);
	} else if (exc == CO_NOT_FOUND) {
		asc_write_not_found(c);
	} else if (exc == CO_ABORT) {
		asc_write_abort(c);
	} else {
		asc_write_server_error(c);
	}
}

static void asc_process_co_unlease(struct conn *c, struct token *token,
		int ntoken) {
	char *sid;
	size_t sid_size; /* # key bytes */

	// check whether this request need reply or not
	asc_set_noreply_maybe(c, token, ntoken);

	if (!asc_ntoken_valid(c, ntoken)) {
		log_hexdump(LOG_NOTICE, c->req, c->req_len, "client error on c %d for "
				"req of type %d with %d invalid tokens", c->sd,
				c->req_type, ntoken);

		asc_write_client_error(c);
		return;
	}

	// get sid
	sid = token[1].val;
	sid_size = token[1].len;

	if (sid_size > KEY_MAX_LEN) {
		log_debug(LOG_NOTICE, "client error on c %d for req of type %d and %d "
				"length key", c->sd, c->req_type, sid_size);

		asc_write_client_error(c);
		return;
	}

	item_co_result_t exc = item_co_unlease(sid, sid_size, c);

	if (exc == CO_OK) {
		asc_write_ok(c);
	} else if (exc == CO_INVALID) {
		asc_write_invalid(c);
	} else if (exc == CO_NOT_FOUND) {
		asc_write_not_found(c);
	} else {
		asc_write_server_error(c);
	}
}

static void asc_process_oqreg(struct conn *c, struct token *token, int ntoken) {
	char *sid;
	char *key; /* key to be released */
	size_t sid_size, nkey; /* # key bytes */

	// check whether this request need reply or not
	asc_set_noreply_maybe(c, token, ntoken);

	if (!asc_ntoken_valid(c, ntoken)) {
		log_hexdump(LOG_NOTICE, c->req, c->req_len, "client error on c %d for "
				"req of type %d with %d invalid tokens", c->sd,
				c->req_type, ntoken);

		asc_write_client_error(c);
		return;
	}

	// get sid
	sid = token[1].val;
	sid_size = token[1].len;

	if (sid_size > KEY_MAX_LEN) {
		log_debug(LOG_NOTICE, "client error on c %d for req of type %d and %d "
				"length key", c->sd, c->req_type, sid_size);

		asc_write_client_error(c);
		return;
	}

	// get key
	key = token[2].val;
	nkey = token[2].len;
	if (nkey > TID_MAX_LEN) {
		log_debug(LOG_NOTICE, "client error on c %d for req of type %d and %d "
				"length key", c->sd, c->req_type, nkey);

		asc_write_client_error(c);
		return;
	}

	item_co_result_t exc = item_oqreg(c, sid, sid_size, key, nkey);

	if (exc == CO_OK) {
		asc_write_ok(c);
	} else if (exc == CO_INVALID) {
		asc_write_invalid(c);
	} else if (exc == CO_NOT_FOUND) {
		asc_write_not_found(c);
	} else if (exc == CO_ABORT) {
		asc_write_abort(c);
	} else {
		asc_write_server_error(c);
	}
}

static void asc_process_qareg(struct conn *c, struct token *token, int ntoken) {
	char *tid;
	char *key; /* key to be released */
	size_t tid_size, nkey; /* # key bytes */
	u_int8_t markedVal;

	// check whether this request need reply or not
	asc_set_noreply_maybe(c, token, ntoken);

	if (!asc_ntoken_valid(c, ntoken)) {
		log_hexdump(LOG_NOTICE, c->req, c->req_len, "client error on c %d for "
				"req of type %d with %d invalid tokens", c->sd,
				c->req_type, ntoken);

		asc_write_client_error(c);
		return;
	}

	// get key
	key = token[TOKEN_KEY].val;
	nkey = token[TOKEN_KEY].len;

	if (nkey > KEY_MAX_LEN) {
		log_debug(LOG_NOTICE, "client error on c %d for req of type %d and %d "
				"length key", c->sd, c->req_type, nkey);

		asc_write_client_error(c);
		return;
	}

	// get transaction id
	tid = token[TRANS_ID].val;
	tid_size = token[TRANS_ID].len;
	if (tid_size > TID_MAX_LEN) {
		log_debug(LOG_NOTICE, "client error on c %d for req of type %d and %d "
				"length key", c->sd, c->req_type, nkey);

		asc_write_client_error(c);
		return;
	}

	// process memcached to create and maintain the list
	if (item_quarantine_and_register(tid, tid_size, key, nkey, &markedVal,
			c) == MC_OK) {
		// Respond to the client with the lease token. If lease could not be granted, LEASE_HOTMISS is returned.
		asc_write_q_inv_leasehold(c, markedVal);
	} else {
		asc_write_server_error(c);
	}
}

static void asc_process_dar(struct conn *c, struct token *token, int ntoken) {
	char *tid; /* key to be released */
	size_t tid_size; /* # key bytes */

	asc_set_noreply_maybe(c, token, ntoken);

	if (!asc_ntoken_valid(c, ntoken)) {
		log_hexdump(LOG_NOTICE, c->req, c->req_len, "client error on c %d for "
				"req of type %d with %d invalid tokens", c->sd,
				c->req_type, ntoken);

		asc_write_client_error(c);
		return;
	}

	tid = token[1].val;
	tid_size = token[1].len;

	if (tid_size > TID_MAX_LEN) {
		log_debug(LOG_NOTICE, "client error on c %d for req of type %d and %d "
				"length key", c->sd, c->req_type, tid_size);

		asc_write_client_error(c);
		return;
	}

	int exc = item_delete_and_release(tid, tid_size, c);

	// process memcached to create and maintain the list
	if (exc == MC_OK) {
		// Respond to the client with the lease token. If lease could not be granted, LEASE_HOTMISS is returned.
		asc_write_deleted(c);
	} else if (exc == MC_INVALID) {
		asc_write_invalid_deleted(c);
	} else {
		asc_write_server_error(c);
	}
}

// Command: raq key [noreply]
static void asc_process_qaread(struct conn *c, struct token *token, int ntoken) {
	char *key; /* key need to be granted lease */
	size_t key_size; /* # key bytes */
	struct item* it = NULL;
	int64_t lease_token = DEFAULT_TOKEN;
	char *tid = NULL;
	size_t tid_size = 0;

	asc_set_noreply_maybe(c, token, ntoken);

	if (!asc_ntoken_valid(c, ntoken)) {
		log_hexdump(LOG_NOTICE, c->req, c->req_len, "client error on c %d for "
				"req of type %d with %d invalid tokens", c->sd,
				c->req_type, ntoken);

		asc_write_client_error(c);
		return;
	}

	key = token[1].val;
	key_size = token[1].len;

	if (key_size > KEY_MAX_LEN) {
		log_debug(LOG_NOTICE, "client error on c %d for req of type %d and %d "
				"length key", c->sd, c->req_type, key_size);

		asc_write_client_error(c);
		return;
	}

	if (ntoken > 3) {
		if (!mc_strtoll(token[2].val, &lease_token)) {
			log_debug(LOG_NOTICE, "client error on c %d for req of type %d and "
					"invalid token '%.*s'", c->sd, c->req_type,
					token[2].len, token[2].val);

			asc_write_client_error(c);
			return;
		}
	}

	if (ntoken > 4) {
		tid = token[3].val;
		tid_size = token[3].len;
	}

	lease_token_t new_lease_token;
	uint8_t p = 0;
	rstatus_t exc = item_quarantine_and_read(tid, tid_size, key, key_size,
			(lease_token_t) lease_token, &new_lease_token, c, &it, &p);
	if (exc == MC_OK) {
		// Respond to the client with the lease token.
		// If lease could not be granted, LEASE_HOTMISS is returned.
		exc = asc_write_qaread(c, it, key, key_size, new_lease_token, p);

		if (exc != MC_OK) {
			if (it != NULL)
				item_remove(it);
		}
	} else if (exc == MC_INVALID) {
		asc_write_invalid_deleted(c);
	} else {
		asc_write_server_error(c);
	}
}

// Command: sar key flags expiry vlen lease val [noreply]
static void asc_process_sar(struct conn *c, struct token *token, int ntoken) {
	char *key;
	int64_t lease_token;
	size_t keylen;
	uint8_t nkey;
	uint32_t flags, vlen;
	int32_t exptime_int;
	time_t exptime;
	//    uint64_t req_cas_id = 0;
	struct item *it;
	//    bool handle_cas;
	//    req_type_t type;
	uint8_t id;
	//    char *value_hash;
	//    uint8_t value_hash_len;

	asc_set_noreply_maybe(c, token, ntoken);

	if (!asc_ntoken_valid(c, ntoken)) {
		log_hexdump(LOG_NOTICE, c->req, c->req_len, "client error on c %d for "
				"req of type %d with %d invalid tokens", c->sd,
				c->req_type, ntoken);

		asc_write_client_error(c);
		return;
	}

	// get key
	key = token[TOKEN_KEY].val;
	keylen = token[TOKEN_KEY].len;
	if (keylen > KEY_MAX_LEN) {
		log_debug(LOG_NOTICE, "client error on c %d for req of type %d and %d "
				"length key", c->sd, c->req_type, keylen);

		asc_write_client_error(c);
		return;
	} else {
		nkey = (uint8_t) keylen;
	}

	// get flags
	if (!mc_strtoul(token[TOKEN_FLAGS].val, &flags)) {
		log_debug(LOG_NOTICE, "client error on c %d for req of type %d and "
				"invalid flags '%.*s'", c->sd, c->req_type,
				token[TOKEN_FLAGS].len, token[TOKEN_FLAGS].val);

		asc_write_client_error(c);
		return;
	}

	// get expired time
	if (!mc_strtol(token[TOKEN_EXPIRY].val, &exptime_int)) {
		log_debug(LOG_NOTICE, "client error on c %d for req of type %d and "
				"invalid expiry '%.*s'", c->sd, c->req_type,
				token[TOKEN_EXPIRY].len, token[TOKEN_EXPIRY].val);

		asc_write_client_error(c);
		return;
	}
	exptime = (time_t) exptime_int;

	// get value length
	if (!mc_strtoul(token[TOKEN_VLEN].val, &vlen)) {
		log_debug(LOG_NOTICE, "client error on c %d for req of type %d and "
				"invalid vlen '%.*s'", c->sd, c->req_type,
				token[TOKEN_VLEN].len, token[TOKEN_VLEN].val);

		asc_write_client_error(c);
		return;
	}

	/* Obtain lease token */
	if (!mc_strtoll(token[TOKEN_LEASE_TOKEN].val, &lease_token)) {
		log_debug(LOG_NOTICE, "client error on c %d for req of type %d and "
				"invalid token '%.*s'", c->sd, c->req_type,
				token[TOKEN_LEASE_TOKEN].len, token[TOKEN_LEASE_TOKEN].val);

		asc_write_client_error(c);
		return;
	}

	// calculate the suitable slab id
	id = item_slabid(nkey, vlen);
	if (id == SLABCLASS_INVALID_ID) {
		log_debug(LOG_NOTICE, "client error on c %d for req of type %d and "
				"slab id out of range for key size %"PRIu8" and value size "
				"%"PRIu32, c->sd, c->req_type, nkey, vlen);

		asc_write_client_error(c);
		return;
	}

	// allocate new item in normal space
	it = item_alloc(id, key, nkey, flags, time_reltime(exptime), vlen);
	if (it == NULL) {
		log_warn("server error on c %d for req of type %d because of oom in "
				"storing item", c->sd, c->req_type);
		log_warn("could not set key(%d, %s) - value(%d) ", nkey, key, vlen);

		asc_write_server_error(c);

		/* swallow the data line */
		c->write_and_go = CONN_SWALLOW;
		c->sbytes = vlen + CRLF_LEN;

		/*
		 * Avoid stale data persisting in cache because we failed alloc.
		 * Unacceptable for SET. Anywhere else too?
		 *
		 * FIXME: either don't delete anything or should be unacceptable for
		 * all but add.
		 */
		it = item_get(key, nkey);

		if (it != NULL) {
			item_delete(it);
		}

		return;
	}

	//    item_set_cas(it, req_cas_id);

	// temporarily set lease token for the newly item
	c->lease_token = (uint64_t) lease_token;

	c->item = it;
	c->ritem = item_data(it);
	c->rlbytes = it->nbyte + CRLF_LEN;
	conn_set_state(c, CONN_NREAD);
}

static void asc_process_iqincr_iqdecr(struct conn *c, struct token *token,
		int ntoken) {
	item_iq_result_t res;
	uint64_t delta;
	char *key;
	size_t nkey;
	bool incr;
	char *tid = NULL;
	size_t tid_size = 0;

	asc_set_noreply_maybe(c, token, ntoken);

	if (!asc_ntoken_valid(c, ntoken)) {
		log_hexdump(LOG_NOTICE, c->req, c->req_len, "client error on c %d for "
				"req of type %d with %d invalid tokens", c->sd,
				c->req_type, ntoken);

		asc_write_client_error(c);
		return;
	}

	incr = (c->req_type == REQ_IQINCR) ? true : false;
	key = token[TOKEN_KEY].val;
	nkey = token[TOKEN_KEY].len;

	if (nkey > KEY_MAX_LEN) {
		log_debug(LOG_NOTICE, "client error on c %d for req of type %d and %d "
				"length key", c->sd, c->req_type, nkey);

		asc_write_client_error(c);
		return;
	}

	if (!mc_strtoull(token[TOKEN_DELTA].val, &delta)) {
		log_debug(LOG_NOTICE, "client error on c %d for req of type %d with "
				"invalid delta '%.*s'", c->sd, c->req_type,
				token[TOKEN_DELTA].len, token[TOKEN_DELTA].val);

		asc_write_client_error(c);
		return;
	}

	if (ntoken > 4) {
		tid = token[3].val;
		tid_size = token[3].len;
	}

	uint8_t pending = 0;
	uint64_t new_lease_token = 0;
	res = item_iqincr_iqdecr(c, key, nkey, incr, delta, tid, tid_size, &pending,
			&new_lease_token);

	switch (res) {
	case IQ_LEASE:
		asc_write_ok_pending(c, pending, new_lease_token);
		break;

	case IQ_LEASE_NO_VALUE:
		asc_write_ok_novalue_pending(c, pending, new_lease_token);
		break;

	case IQ_CLIENT_ERROR:
		log_debug(LOG_NOTICE, "client error on c %d for req of type %d with "
				"non-numeric value", c->sd, c->req_type);

		asc_write_client_error(c);
		break;

	case IQ_SERVER_ERROR:
		log_warn("server error on c %d for req of type %d because of oom",
				c->sd, c->req_type);

		asc_write_server_error(c);
		break;

	case IQ_ABORT:
		if (incr) {
			stats_thread_incr(incr_miss);
		} else {
			stats_thread_incr(decr_miss);
		}
		asc_write_abort(c);
		break;

	default:
		NOT_REACHED();
		break;
	}
}

static void asc_process_iqappend_iqprepend(struct conn *c, struct token *token,
		int ntoken) {
	char *key;
	size_t key_size;
	struct item* it = NULL;
	char *tid;
	size_t tid_size = 0;

	asc_set_noreply_maybe(c, token, ntoken);

	uint32_t vlen;

	if (!asc_ntoken_valid(c, ntoken)) {
		log_hexdump(LOG_NOTICE, c->req, c->req_len, "client error on c %d for "
				"req of type %d with %d invalid tokens", c->sd,
				c->req_type, ntoken);

		asc_write_client_error(c);
		return;
	}

	key = token[1].val;
	key_size = token[1].len;

	if (key_size > KEY_MAX_LEN) {
		log_debug(LOG_NOTICE, "client error on c %d for req of type %d and %d "
				"length key", c->sd, c->req_type, key_size);

		asc_write_client_error(c);
		return;
	}

	// get value length
	if (!mc_strtoul(token[2].val, &vlen)) {
		log_debug(LOG_NOTICE, "client error on c %d for req of type %d and "
				"invalid vlen '%.*s'", c->sd, c->req_type,
				token[2].len, token[2].val);

		asc_write_client_error(c);
		return;
	}

	tid = token[3].val;
	tid_size = token[3].len;

	it = item_alloc(item_slabid(key_size, vlen), key, key_size, 0, 0, vlen);
	if (it == NULL) {
		log_warn("server error on c %d for req of type %d because of oom in "
				"storing item", c->sd, c->req_type);
		log_warn("could not set key(%d, %s) - value(%d) ", key_size, key, vlen);

		asc_write_not_stored(c);

		/* swallow the data line */
		c->write_and_go = CONN_SWALLOW;
		c->sbytes = vlen + CRLF_LEN;

		return;
	}

	// because we only need the data and not try to assign it to any particular item
	// at the beginning, set the c->item to NULL
	c->tid = tid;
	c->ntid = tid_size;

	c->item = it;
	c->ritem = item_data(it);
	c->rlbytes = vlen + CRLF_LEN;
	conn_set_state(c, CONN_NREAD);
}

static void asc_process_oqincr_oqdecr(struct conn *c, struct token *token,
		int ntoken) {
	item_co_result_t res;
	uint64_t delta;
	char *key;
	size_t nkey;
	bool incr;
	char *sid = NULL;
	size_t sid_size = 0;

	asc_set_noreply_maybe(c, token, ntoken);

	if (!asc_ntoken_valid(c, ntoken)) {
		log_hexdump(LOG_NOTICE, c->req, c->req_len, "client error on c %d for "
				"req of type %d with %d invalid tokens", c->sd,
				c->req_type, ntoken);

		asc_write_client_error(c);
		return;
	}

	incr = (c->req_type == REQ_OQINCR) ? true : false;

	sid = token[1].val;
	sid_size = token[1].len;

	key = token[2].val;
	nkey = token[2].len;

	if (nkey > KEY_MAX_LEN) {
		log_debug(LOG_NOTICE, "client error on c %d for req of type %d and %d "
				"length key", c->sd, c->req_type, nkey);
		asc_write_client_error(c);
		return;
	}

	if (!mc_strtoull(token[3].val, &delta)) {
		log_debug(LOG_NOTICE, "client error on c %d for req of type %d with "
				"invalid delta '%.*s'", c->sd, c->req_type,
				token[3].len, token[3].val);

		asc_write_client_error(c);
		return;
	}

	uint64_t val;
	res = item_oqincr_oqdecr(c, key, nkey, incr, delta, sid, sid_size, &val);

	switch (res) {
	case CO_OK:
		asc_write_ok_val(c, val);
		break;

	case CO_NOVALUE:
		asc_write_novalue(c);
		break;

	case CO_ABORT:
		asc_write_abort(c);
		break;
	default:
		NOT_REACHED();
		break;
	}
}

static void asc_process_oqappend_oqprepend(struct conn *c, struct token *token,
		int ntoken) {
	char *key;
	size_t key_size;
	struct item* it = NULL;
	char *tid;
	size_t tid_size = 0;

	asc_set_noreply_maybe(c, token, ntoken);

	uint32_t vlen;

	if (!asc_ntoken_valid(c, ntoken)) {
		log_hexdump(LOG_NOTICE, c->req, c->req_len, "client error on c %d for "
				"req of type %d with %d invalid tokens", c->sd,
				c->req_type, ntoken);

		asc_write_client_error(c);
		return;
	}

	tid = token[1].val;
	tid_size = token[1].len;

	key = token[2].val;
	key_size = token[2].len;

	if (key_size > KEY_MAX_LEN) {
		log_debug(LOG_NOTICE, "client error on c %d for req of type %d and %d "
				"length key", c->sd, c->req_type, key_size);

		asc_write_client_error(c);
		return;
	}

	// get value length
	if (!mc_strtoul(token[3].val, &vlen)) {
		log_debug(LOG_NOTICE, "client error on c %d for req of type %d and "
				"invalid vlen '%.*s'", c->sd, c->req_type,
				token[3].len, token[3].val);

		asc_write_client_error(c);
		return;
	}

	it = item_alloc(item_slabid(key_size, vlen), key, key_size, 0, 0, vlen);
	if (it == NULL) {
		log_warn("server error on c %d for req of type %d because of oom in "
				"storing item", c->sd, c->req_type);
		log_warn("could not set key(%d, %s) - value(%d) ", key_size, key, vlen);

		asc_write_server_error(c);

		/* swallow the data line */
		c->write_and_go = CONN_SWALLOW;
		c->sbytes = vlen + CRLF_LEN;

		return;
	}

	// because we only need the data and not try to assign it to any particular item
	// at the beginning, set the c->item to NULL
	c->tid = tid;
	c->ntid = tid_size;

	c->item = it;
	c->ritem = item_data(it);
	c->rlbytes = vlen + CRLF_LEN;
	conn_set_state(c, CONN_NREAD);
}

static void asc_process_oqappend2(struct conn *c, struct token *token,
		int ntoken) {
	char *key;
	size_t key_size;
	struct item* it = NULL;
	char *tid;
	size_t tid_size = 0;

	asc_set_noreply_maybe(c, token, ntoken);

	uint32_t vlen;

	if (!asc_ntoken_valid(c, ntoken)) {
		log_hexdump(LOG_NOTICE, c->req, c->req_len, "client error on c %d for "
				"req of type %d with %d invalid tokens", c->sd,
				c->req_type, ntoken);

		asc_write_client_error(c);
		return;
	}

	tid = token[1].val;
	tid_size = token[1].len;

	key = token[2].val;
	key_size = token[2].len;

	if (key_size > KEY_MAX_LEN) {
		log_debug(LOG_NOTICE, "client error on c %d for req of type %d and %d "
				"length key", c->sd, c->req_type, key_size);

		asc_write_client_error(c);
		return;
	}

	// get value length
	if (!mc_strtoul(token[3].val, &vlen)) {
		log_debug(LOG_NOTICE, "client error on c %d for req of type %d and "
				"invalid vlen '%.*s'", c->sd, c->req_type,
				token[3].len, token[3].val);

		asc_write_client_error(c);
		return;
	}

	it = item_alloc(item_slabid(key_size, vlen), key, key_size, 0, 0, vlen);
	if (it == NULL) {
		log_warn("server error on c %d for req of type %d because of oom in "
				"storing item", c->sd, c->req_type);
		log_warn("could not set key(%d, %s) - value(%d) ", key_size, key, vlen);

		asc_write_server_error(c);

		/* swallow the data line */
		c->write_and_go = CONN_SWALLOW;
		c->sbytes = vlen + CRLF_LEN;

		return;
	}

	// because we only need the data and not try to assign it to any particular item
	// at the beginning, set the c->item to NULL
	c->tid = tid;
	c->ntid = tid_size;

	c->item = it;
	c->ritem = item_data(it);
	c->rlbytes = vlen + CRLF_LEN;
	conn_set_state(c, CONN_NREAD);
}

static void asc_process_commit(struct conn *c, struct token *token, int ntoken) {
	char *tid; /* key to be released */
	size_t tid_size; /* # key bytes */
	int32_t pending;

	asc_set_noreply_maybe(c, token, ntoken);

	if (!asc_ntoken_valid(c, ntoken)) {
		log_hexdump(LOG_NOTICE, c->req, c->req_len, "client error on c %d for "
				"req of type %d with %d invalid tokens", c->sd,
				c->req_type, ntoken);

		asc_write_client_error(c);
		return;
	}

	tid = token[1].val;
	tid_size = token[1].len;

	if (tid_size > TID_MAX_LEN) {
		log_debug(LOG_NOTICE, "client error on c %d for req of type %d and %d "
				"length key", c->sd, c->req_type, tid_size);

		asc_write_client_error(c);
		return;
	}

	if (!mc_strtol(token[2].val, &pending)) {
		log_debug(LOG_NOTICE, "client error on c %d for req of type %d with "
				"invalid delta '%.*s'", c->sd, c->req_type,
				token[3].len, token[3].val);

		asc_write_client_error(c);
		return;
	}

	item_iq_result_t exc = item_commit(tid, tid_size, c, pending);

	// process memcached to create and maintain the list
	if (exc == IQ_OK) {
		// Respond to the client with the lease token. If lease could not be granted, LEASE_HOTMISS is returned.
		asc_write_ok(c);
	} else if (exc == IQ_NOT_FOUND) {
		asc_write_not_found(c);
	} else {
		asc_write_server_error(c);
	}
}

static void asc_process_release(struct conn *c, struct token *token, int ntoken) {
	char *tid; /* key to be released */
	size_t tid_size; /* # key bytes */

	asc_set_noreply_maybe(c, token, ntoken);

	if (!asc_ntoken_valid(c, ntoken)) {
		log_hexdump(LOG_NOTICE, c->req, c->req_len, "client error on c %d for "
				"req of type %d with %d invalid tokens", c->sd,
				c->req_type, ntoken);

		asc_write_client_error(c);
		return;
	}

	tid = token[1].val;
	tid_size = token[1].len;

	if (tid_size > TID_MAX_LEN) {
		log_debug(LOG_NOTICE, "client error on c %d for req of type %d and %d "
				"length key", c->sd, c->req_type, tid_size);

		asc_write_client_error(c);
		return;
	}

	item_iq_result_t exc = item_release(tid, tid_size, c);

	// process memcached to create and maintain the list
	if (exc == IQ_OK) {
		// Respond to the client with the lease token. If lease could not be granted, LEASE_HOTMISS is returned.
		asc_write_ok(c);
	} else if (exc == IQ_NOT_FOUND) {
		asc_write_not_found(c);
	}
}

static void asc_process_check(struct conn *c, struct token *token, int ntoken) {
	int64_t num_keys;
	char *key;
	size_t key_size;
	struct item* it;

	if (!asc_ntoken_valid(c, ntoken)) {
		log_hexdump(LOG_NOTICE, c->req, c->req_len, "client error on c %d for "
				"req of type %d with %d invalid tokens", c->sd,
				c->req_type, ntoken);

		asc_write_client_error(c);
		return;
	}

	if (!mc_strtoll(token[1].val, &num_keys)) {
		log_debug(LOG_NOTICE, "client error on c %d for req of type %d and "
				"invalid token '%.*s'", c->sd, c->req_type,
				token[2].len, token[2].val);

		asc_write_client_error(c);
		return;
	}

	char* buf = mc_alloc(1024);
	size_t len = 0;
	memcpy(buf, "OK ", sizeof("OK ") - 1);
	len += sizeof("OK ") - 1;

	int i = 1;
	for (; i <= num_keys; i++) {
		key = token[1 + i].val;
		key_size = token[1 + i].len;
		it = item_get(key, key_size);
		if (it == NULL) {
			// add keys to return
			memcpy(buf + len, key, key_size);
			len += key_size;
			memcpy(buf + len, " ", 1);
			len += 1;
		} else {
			item_remove(it);
		}
	}
	asc_write_string(c, buf, len);

	mc_free(buf);
	return;
}

static void asc_process_ftrans(struct conn *c, struct token *token, int ntoken) {
	char *tid = NULL;
	char *key = NULL;
	size_t tid_size = 0;
	size_t key_size = 0;
	int64_t num_keys;

	asc_set_noreply_maybe(c, token, ntoken);

	if (!asc_ntoken_valid(c, ntoken)) {
		log_hexdump(LOG_NOTICE, c->req, c->req_len, "client error on c %d for "
				"req of type %d with %d invalid tokens", c->sd,
				c->req_type, ntoken);

		asc_write_client_error(c);
		return;
	}

	tid = token[1].val;
	tid_size = token[1].len;

	if (!mc_strtoll(token[2].val, &num_keys)) {
		log_debug(LOG_NOTICE, "client error on c %d for req of type %d and "
				"invalid token '%.*s'", c->sd, c->req_type,
				token[2].len, token[2].val);

		asc_write_client_error(c);
		return;
	}

	int i = 0;
	for (; i < num_keys; i++) {
		key = token[3 + i].val;
		key_size = token[3 + i].len;
		item_ftrans(tid, tid_size, key, key_size, c);
	}

	asc_write_ok(c);
	return;
}

static void asc_process_oqread(struct conn *c, struct token *token, int ntoken) {
	char *key; /* key need to be granted lease */
	size_t key_size; /* # key bytes */
	struct item* it = NULL;
	char *sid = NULL;
	size_t sid_size = 0;

	asc_set_noreply_maybe(c, token, ntoken);

	if (!asc_ntoken_valid(c, ntoken)) {
		log_hexdump(LOG_NOTICE, c->req, c->req_len, "client error on c %d for "
				"req of type %d with %d invalid tokens", c->sd,
				c->req_type, ntoken);

		asc_write_client_error(c);
		return;
	}

	sid = token[1].val;
	sid_size = token[1].len;

	key = token[2].val;
	key_size = token[2].len;

	if (key_size > KEY_MAX_LEN) {
		log_debug(LOG_NOTICE, "client error on c %d for req of type %d and %d "
				"length key", c->sd, c->req_type, key_size);

		asc_write_client_error(c);
		return;
	}

	item_co_result_t exc = item_oqread(sid, sid_size, key, key_size, c, &it);
	if (exc == CO_OK) {
		// Respond to the client with the lease token.
		// If lease could not be granted, LEASE_HOTMISS is returned.
		rstatus_t status = asc_write_oqread(c, it, key, key_size);

		if (status != MC_OK) {
			if (it != NULL)
				item_remove(it);
		}
	} else if (exc == CO_ABORT) {
		asc_write_abort(c);
	} else if (exc == CO_INVALID) {
		asc_write_invalid(c);
	} else if (exc == CO_RETRY) {
		asc_write_retry(c);
	}
}

static void asc_process_oqwrite(struct conn *c, struct token *token, int ntoken) {
	char *key;
	size_t keylen;
	uint8_t nkey;
	uint32_t flags, vlen;
	int32_t exptime_int;
	time_t exptime;
	struct item *it;
	uint8_t id;
	char* sid;
	size_t nsid;

	asc_set_noreply_maybe(c, token, ntoken);

	if (!asc_ntoken_valid(c, ntoken)) {
		log_hexdump(LOG_NOTICE, c->req, c->req_len, "client error on c %d for "
				"req of type %d with %d invalid tokens", c->sd,
				c->req_type, ntoken);

		asc_write_client_error(c);
		return;
	}

	sid = token[1].val;
	nsid = token[1].len;

	// get key
	key = token[2].val;
	keylen = token[2].len;
	if (keylen > KEY_MAX_LEN) {
		log_debug(LOG_NOTICE, "client error on c %d for req of type %d and %d "
				"length key", c->sd, c->req_type, keylen);

		asc_write_client_error(c);
		return;
	} else {
		nkey = (uint8_t) keylen;
	}

	// get flags
	if (!mc_strtoul(token[3].val, &flags)) {
		log_debug(LOG_NOTICE, "client error on c %d for req of type %d and "
				"invalid flags '%.*s'", c->sd, c->req_type,
				token[3].len, token[3].val);

		asc_write_client_error(c);
		return;
	}

	// get expired time
	if (!mc_strtol(token[4].val, &exptime_int)) {
		log_debug(LOG_NOTICE, "client error on c %d for req of type %d and "
				"invalid expiry '%.*s'", c->sd, c->req_type,
				token[4].len, token[4].val);

		asc_write_client_error(c);
		return;
	}
	exptime = (time_t) exptime_int;

	// get value length
	if (!mc_strtoul(token[5].val, &vlen)) {
		log_debug(LOG_NOTICE, "client error on c %d for req of type %d and "
				"invalid vlen '%.*s'", c->sd, c->req_type,
				token[5].len, token[5].val);

		asc_write_client_error(c);
		return;
	}

	// calculate the suitable slab id
	id = item_slabid(nkey, vlen);
	if (id == SLABCLASS_INVALID_ID) {
		log_debug(LOG_NOTICE, "client error on c %d for req of type %d and "
				"slab id out of range for key size %"PRIu8" and value size "
				"%"PRIu32, c->sd, c->req_type, nkey, vlen);

		asc_write_client_error(c);
		return;
	}

	// allocate new item in normal space
	it = item_alloc(id, key, nkey, flags, time_reltime(exptime), vlen);
	if (it == NULL) {
		log_warn("server error on c %d for req of type %d because of oom in "
				"storing item", c->sd, c->req_type);
		log_warn("could not set key(%d, %s) - value(%d) ", nkey, key, vlen);

		asc_write_server_error(c);

		/* swallow the data line */
		c->write_and_go = CONN_SWALLOW;
		c->sbytes = vlen + CRLF_LEN;

		it = item_get(key, nkey);

		if (it != NULL) {
			item_delete(it);
		}

		return;
	}

	c->item = it;
	c->ritem = item_data(it);
	c->tid = sid;
	c->ntid = nsid;
	c->rlbytes = it->nbyte + CRLF_LEN;
	conn_set_state(c, CONN_NREAD);
}

static void asc_process_oqadd(struct conn *c, struct token *token, int ntoken) {
	char *key;
	size_t keylen;
	uint8_t nkey;
	uint32_t flags, vlen;
	int32_t exptime_int;
	time_t exptime;
	struct item *it;
	uint8_t id;
	char* sid;
	size_t nsid;

	asc_set_noreply_maybe(c, token, ntoken);

	if (!asc_ntoken_valid(c, ntoken)) {
		log_hexdump(LOG_NOTICE, c->req, c->req_len, "client error on c %d for "
				"req of type %d with %d invalid tokens", c->sd,
				c->req_type, ntoken);

		asc_write_client_error(c);
		return;
	}

	sid = token[1].val;
	nsid = token[1].len;

	// get key
	key = token[2].val;
	keylen = token[2].len;
	if (keylen > KEY_MAX_LEN) {
		log_debug(LOG_NOTICE, "client error on c %d for req of type %d and %d "
				"length key", c->sd, c->req_type, keylen);

		asc_write_client_error(c);
		return;
	} else {
		nkey = (uint8_t) keylen;
	}

	// get flags
	if (!mc_strtoul(token[3].val, &flags)) {
		log_debug(LOG_NOTICE, "client error on c %d for req of type %d and "
				"invalid flags '%.*s'", c->sd, c->req_type,
				token[3].len, token[3].val);

		asc_write_client_error(c);
		return;
	}

	// get expired time
	if (!mc_strtol(token[4].val, &exptime_int)) {
		log_debug(LOG_NOTICE, "client error on c %d for req of type %d and "
				"invalid expiry '%.*s'", c->sd, c->req_type,
				token[4].len, token[4].val);

		asc_write_client_error(c);
		return;
	}
	exptime = (time_t) exptime_int;

	// get value length
	if (!mc_strtoul(token[5].val, &vlen)) {
		log_debug(LOG_NOTICE, "client error on c %d for req of type %d and "
				"invalid vlen '%.*s'", c->sd, c->req_type,
				token[5].len, token[5].val);

		asc_write_client_error(c);
		return;
	}

	// calculate the suitable slab id
	id = item_slabid(nkey, vlen);
	if (id == SLABCLASS_INVALID_ID) {
		log_debug(LOG_NOTICE, "client error on c %d for req of type %d and "
				"slab id out of range for key size %"PRIu8" and value size "
				"%"PRIu32, c->sd, c->req_type, nkey, vlen);

		asc_write_client_error(c);
		return;
	}

	// allocate new item in normal space
	it = item_alloc(id, key, nkey, flags, time_reltime(exptime), vlen);
	if (it == NULL) {
		log_warn("server error on c %d for req of type %d because of oom in "
				"storing item", c->sd, c->req_type);
		log_warn("could not set key(%d, %s) - value(%d) ", nkey, key, vlen);

		asc_write_server_error(c);

		/* swallow the data line */
		c->write_and_go = CONN_SWALLOW;
		c->sbytes = vlen + CRLF_LEN;

		it = item_get(key, nkey);

		if (it != NULL) {
			item_delete(it);
		}

		return;
	}

	c->item = it;
	c->ritem = item_data(it);
	c->tid = sid;
	c->ntid = nsid;
	c->rlbytes = it->nbyte + CRLF_LEN;
	conn_set_state(c, CONN_NREAD);
}

static void asc_process_oqswap(struct conn *c, struct token *token, int ntoken) {
	char *key;
	size_t keylen;
	uint8_t nkey;
	uint32_t flags, vlen;
	int32_t exptime_int;
	time_t exptime;
	struct item *it;
	uint8_t id;
	char* sid;
	size_t nsid;

	asc_set_noreply_maybe(c, token, ntoken);

	if (!asc_ntoken_valid(c, ntoken)) {
		log_hexdump(LOG_NOTICE, c->req, c->req_len, "client error on c %d for "
				"req of type %d with %d invalid tokens", c->sd,
				c->req_type, ntoken);

		asc_write_client_error(c);
		return;
	}

	sid = token[1].val;
	nsid = token[1].len;

	// get key
	key = token[2].val;
	keylen = token[2].len;
	if (keylen > KEY_MAX_LEN) {
		log_debug(LOG_NOTICE, "client error on c %d for req of type %d and %d "
				"length key", c->sd, c->req_type, keylen);

		asc_write_client_error(c);
		return;
	} else {
		nkey = (uint8_t) keylen;
	}

	// get flags
	if (!mc_strtoul(token[3].val, &flags)) {
		log_debug(LOG_NOTICE, "client error on c %d for req of type %d and "
				"invalid flags '%.*s'", c->sd, c->req_type,
				token[3].len, token[3].val);

		asc_write_client_error(c);
		return;
	}

	// get expired time
	if (!mc_strtol(token[4].val, &exptime_int)) {
		log_debug(LOG_NOTICE, "client error on c %d for req of type %d and "
				"invalid expiry '%.*s'", c->sd, c->req_type,
				token[4].len, token[4].val);

		asc_write_client_error(c);
		return;
	}
	exptime = (time_t) exptime_int;

	// get value length
	if (!mc_strtoul(token[5].val, &vlen)) {
		log_debug(LOG_NOTICE, "client error on c %d for req of type %d and "
				"invalid vlen '%.*s'", c->sd, c->req_type,
				token[5].len, token[5].val);

		asc_write_client_error(c);
		return;
	}

	// calculate the suitable slab id
	id = item_slabid(nkey, vlen);
	if (id == SLABCLASS_INVALID_ID) {
		log_debug(LOG_NOTICE, "client error on c %d for req of type %d and "
				"slab id out of range for key size %"PRIu8" and value size "
				"%"PRIu32, c->sd, c->req_type, nkey, vlen);

		asc_write_client_error(c);
		return;
	}

	// allocate new item in normal space
	it = item_alloc(id, key, nkey, flags, time_reltime(exptime), vlen);
	if (it == NULL) {
		log_warn("server error on c %d for req of type %d because of oom in "
				"storing item", c->sd, c->req_type);
		log_warn("could not set key(%d, %s) - value(%d) ", nkey, key, vlen);

		asc_write_server_error(c);

		/* swallow the data line */
		c->write_and_go = CONN_SWALLOW;
		c->sbytes = vlen + CRLF_LEN;

		it = item_get(key, nkey);

		if (it != NULL) {
			item_delete(it);
		}

		return;
	}

	c->item = it;
	c->ritem = item_data(it);
	c->tid = sid;
	c->ntid = nsid;
	c->rlbytes = it->nbyte + CRLF_LEN;
	conn_set_state(c, CONN_NREAD);
}

static void asc_process_stores(struct conn *c, struct token *token,
		int ntoken) {
	char *key = NULL;
	uint8_t nkey = 0;
	uint32_t flags, vlen;
	int32_t exptime_int;
	time_t exptime = 0;
	uint8_t id;
	char* sid;
	size_t nsid;
	struct token *key_token;

	asc_set_noreply_maybe(c, token, ntoken);

	if (!asc_ntoken_valid(c, ntoken)) {
		log_hexdump(LOG_NOTICE, c->req, c->req_len, "client error on c %d for "
				"req of type %d with %d invalid tokens", c->sd,
				c->req_type, ntoken);

		asc_write_client_error(c);
		return;
	}

	sid = token[1].val;
	nsid = token[1].len;

	key_token = &token[2];
	int num_keys = 0;
	int idx = 0;
	do {
		while (key_token->len != 0) {
			key_token++;
			idx++;
			if (idx == 4) {
				num_keys++;
				idx = 0;
			}
		}

		/*
		 * If the command string hasn't been fully processed, get the next set
		 * of token.
		 */
		if (key_token->val != NULL) {
			ntoken = asc_tokenize(key_token->val, token, TOKEN_MAX);
			/* ntoken is unused */
			key_token = token;
		}
	} while (key_token->val != NULL);

	c->items = malloc(sizeof(struct item*) * num_keys);
	c->ritems = malloc(sizeof(char*) * num_keys);
	c->rrlbytes = malloc(sizeof(int) * num_keys);
	c->num_items = num_keys;

	asc_tokenize(c->req, token, TOKEN_MAX);
	key_token = &token[2];
	int i = 0;
	c->rlbytes = 0;
	idx = 0;
	do {
		while (key_token->len != 0) {
			if (idx == 0) {
				key = key_token->val;
				nkey = key_token->len;

				if (nkey > KEY_MAX_LEN) {
					log_debug(LOG_NOTICE, "client error on c %d for req of type %d "
							"and %d length key", c->sd, c->req_type, nkey);
					asc_write_client_error(c);
					return;
				}
			} else if (idx == 1) {
				// get flags
				if (!mc_strtoul(key_token->val, &flags)) {
					log_debug(LOG_NOTICE, "client error on c %d for req of type %d and "
							"invalid flags '%.*s'", c->sd, c->req_type,
							key_token->len, key_token->val);

					asc_write_client_error(c);
					return;
				}
			} else if (idx == 2) {
				// get expired time
				if (!mc_strtol(key_token->val, &exptime_int)) {
					log_debug(LOG_NOTICE, "client error on c %d for req of type %d and "
							"invalid expiry '%.*s'", c->sd, c->req_type,
							key_token->len, key_token->val);

					asc_write_client_error(c);
					return;
				}
				exptime = (time_t) exptime_int;
			} else if (idx == 3) {
				// get value length
				if (!mc_strtoul(key_token->val, &vlen)) {
					log_debug(LOG_NOTICE, "client error on c %d for req of type %d and "
							"invalid vlen '%.*s'", c->sd, c->req_type,
							key_token->len, key_token->val);

					asc_write_client_error(c);
					return;
				}

				// calculate the suitable slab id
				id = item_slabid(nkey, vlen);
				if (id == SLABCLASS_INVALID_ID) {
					log_debug(LOG_NOTICE, "client error on c %d for req of type %d and "
							"slab id out of range for key size %"PRIu8" and value size "
							"%"PRIu32, c->sd, c->req_type, nkey, vlen);

					asc_write_client_error(c);
					return;
				}

				// allocate new item in normal space
				struct item *it = item_alloc(id, key, nkey, flags, time_reltime(exptime), vlen);
				if (it == NULL) {
					log_warn(
							"server error on c %d for req of type %d because of oom in "
									"storing item", c->sd, c->req_type);
					log_warn("could not set key(%d, %s) - value(%d) ", nkey,
							key, vlen);

					asc_write_server_error(c);

					/* swallow the data line */
					c->write_and_go = CONN_SWALLOW;
					c->sbytes = vlen + CRLF_LEN;

					it = item_get(key, nkey);

					if (it != NULL) {
						item_delete(it);
					}

					return;
				}

				(c->items)[i] = it;
				(c->ritems)[i] = item_data(it);
				if (it->nbyte > 0) {
					(c->rrlbytes)[i] = it->nbyte + CRLF_LEN;
					c->rlbytes += it->nbyte + CRLF_LEN;
				} else {
					(c->rrlbytes)[i] = 0;
				}

				i++;
				idx = -1;
			}

			key_token++;
			idx++;
		}

		/*
		 * If the command string hasn't been fully processed, get the next set
		 * of token.
		 */
		if (key_token->val != NULL) {
			ntoken = asc_tokenize(key_token->val, token, TOKEN_MAX);
			/* ntoken is unused */
			key_token = token;
		}
	} while (key_token->val != NULL);

	c->tid = sid;
	c->ntid = nsid;

	conn_set_state(c, CONN_NREADS);
}

// Command: swap key flags expiry vlen lease val [noreply]
static void asc_process_swap(struct conn *c, struct token *token, int ntoken) {
	char *key;
	int64_t lease_token;
	size_t keylen;
	uint8_t nkey;
	uint32_t flags, vlen;
	int32_t exptime_int;
	time_t exptime;
	struct item *it;
	uint8_t id;
	int64_t pi_mem = 0;

	asc_set_noreply_maybe(c, token, ntoken);

	if (!asc_ntoken_valid(c, ntoken)) {
		log_hexdump(LOG_NOTICE, c->req, c->req_len, "client error on c %d for "
				"req of type %d with %d invalid tokens", c->sd,
				c->req_type, ntoken);

		asc_write_client_error(c);
		return;
	}

	// get key
	key = token[TOKEN_KEY].val;
	keylen = token[TOKEN_KEY].len;
	if (keylen > KEY_MAX_LEN) {
		log_debug(LOG_NOTICE, "client error on c %d for req of type %d and %d "
				"length key", c->sd, c->req_type, keylen);

		asc_write_client_error(c);
		return;
	} else {
		nkey = (uint8_t) keylen;
	}

	// get flags
	if (!mc_strtoul(token[TOKEN_FLAGS].val, &flags)) {
		log_debug(LOG_NOTICE, "client error on c %d for req of type %d and "
				"invalid flags '%.*s'", c->sd, c->req_type,
				token[TOKEN_FLAGS].len, token[TOKEN_FLAGS].val);

		asc_write_client_error(c);
		return;
	}

	// get expired time
	if (!mc_strtol(token[TOKEN_EXPIRY].val, &exptime_int)) {
		log_debug(LOG_NOTICE, "client error on c %d for req of type %d and "
				"invalid expiry '%.*s'", c->sd, c->req_type,
				token[TOKEN_EXPIRY].len, token[TOKEN_EXPIRY].val);

		asc_write_client_error(c);
		return;
	}
	exptime = (time_t) exptime_int;

	// get value length
	if (!mc_strtoul(token[TOKEN_VLEN].val, &vlen)) {
		log_debug(LOG_NOTICE, "client error on c %d for req of type %d and "
				"invalid vlen '%.*s'", c->sd, c->req_type,
				token[TOKEN_VLEN].len, token[TOKEN_VLEN].val);

		asc_write_client_error(c);
		return;
	}

	/* Obtain lease token */
	if (!mc_strtoll(token[TOKEN_LEASE_TOKEN].val, &lease_token)) {
		log_debug(LOG_NOTICE, "client error on c %d for req of type %d and "
				"invalid token '%.*s'", c->sd, c->req_type,
				token[TOKEN_LEASE_TOKEN].len, token[TOKEN_LEASE_TOKEN].val);

		asc_write_client_error(c);
		return;
	}

	if (ntoken > TOKEN_PI_MEM) {
		if (!mc_strtoll(token[TOKEN_PI_MEM].val, &pi_mem)) {
			log_debug(LOG_NOTICE, "client error on c %d for req of type %d and "
					"invalid token '%.*s'", c->sd, c->req_type,
					token[TOKEN_PI_MEM].len, token[TOKEN_PI_MEM].val);

			asc_write_client_error(c);
			return;
		}
	}

	if (pi_mem == 1) {
		size_t size = item_ntotal(nkey, vlen, false);
		if (item_get_pimem_size() + size > settings.max_pimem_size) {
			asc_write_not_stored(c);
			c->write_and_go = CONN_SWALLOW;
			c->sbytes = vlen + CRLF_LEN;
			return;
		}
	}

	// calculate the suitable slab id
	id = item_slabid(nkey, vlen);
	if (id == SLABCLASS_INVALID_ID) {
		log_debug(LOG_NOTICE, "client error on c %d for req of type %d and "
				"slab id out of range for key size %"PRIu8" and value size "
				"%"PRIu32, c->sd, c->req_type, nkey, vlen);

		asc_write_client_error(c);
		return;
	}

	// allocate new item in normal space
	it = item_alloc(id, key, nkey, flags, time_reltime(exptime), vlen);
	if (it == NULL) {
		log_warn("server error on c %d for req of type %d because of oom in "
				"storing item", c->sd, c->req_type);
		log_warn("could not set key(%d, %s) - value(%d) ", nkey, key, vlen);

		asc_write_server_error(c);

		/* swallow the data line */
		c->write_and_go = CONN_SWALLOW;
		c->sbytes = vlen + CRLF_LEN;

		/*
		 * Avoid stale data persisting in cache because we failed alloc.
		 * Unacceptable for SET. Anywhere else too?
		 *
		 * FIXME: either don't delete anything or should be unacceptable for
		 * all but add.
		 */
		it = item_get(key, nkey);

		if (it != NULL) {
			item_delete(it);
		}

		return;
	}

	// set this item to be in PiMem space
	if (pi_mem) {
		item_set_pimem(it);
	}

	// temporarily set lease token for the newly item
	c->lease_token = (uint64_t) lease_token;

	c->item = it;
	c->ritem = item_data(it);
	c->rlbytes = it->nbyte + CRLF_LEN;
	conn_set_state(c, CONN_NREAD);
}

static void asc_process_validate(struct conn *c, struct token *token,
		int ntoken) {
	char *sid = NULL;
	size_t sid_size = 0;
	item_co_result_t exc;

	asc_set_noreply_maybe(c, token, ntoken);

	if (!asc_ntoken_valid(c, ntoken)) {
		log_hexdump(LOG_NOTICE, c->req, c->req_len, "client error on c %d for "
				"req of type %d with %d invalid tokens", c->sd,
				c->req_type, ntoken);

		asc_write_client_error(c);
		return;
	}

	sid = token[1].val;
	sid_size = token[1].len;

	exc = item_validate(sid, sid_size, c);
	switch (exc) {
	case CO_OK:
		asc_write_ok(c);
		break;
	case CO_ABORT:
		asc_write_abort(c);
		break;
	case CO_INVALID:
		asc_write_invalid(c);
		break;
	default:
		asc_write_invalid(c);
		break;
	}
}

static void asc_process_ciget(struct conn *c, struct token *token, int ntoken) {
	char *key;
	size_t key_size;
	int64_t lease_token = 0;
	lease_token_t new_lease_token = 0;
	struct item* it = NULL;
	item_co_result_t exc;

	char *sid = NULL;
	size_t sid_size = 0;

	asc_set_noreply_maybe(c, token, ntoken);

	if (!asc_ntoken_valid(c, ntoken)) {
		log_hexdump(LOG_NOTICE, c->req, c->req_len, "client error on c %d for "
				"req of type %d with %d invalid tokens", c->sd,
				c->req_type, ntoken);

		asc_write_client_error(c);
		return;
	}

	sid = token[1].val;
	sid_size = token[1].len;

	key = token[2].val;
	key_size = token[2].len;

	if (key_size > KEY_MAX_LEN) {
		log_debug(LOG_NOTICE, "client error on c %d for req of type %d and %d "
				"length key", c->sd, c->req_type, key_size);

		asc_write_client_error(c);
		return;
	}

	if (!mc_strtoll(token[3].val, &lease_token)) {
		log_debug(LOG_NOTICE, "client error on c %d for req of type %d and "
				"invalid token '%.*s'", c->sd, c->req_type,
				token[2].len, token[2].val);

		asc_write_client_error(c);
		return;
	}

	exc = item_ciget(sid, sid_size, key, key_size, (lease_token_t) lease_token,
			c, &it, &new_lease_token);

	switch (exc) {
	case CO_OK:
		if (new_lease_token != 0) {
			asc_write_ciget_lease(c, it, new_lease_token);
		} else if (it == NULL || item_data(it) == NULL) {
			asc_write_novalue(c);
		} else {
			asc_write_ciget_value(c, it);
		}

		// prevent _item_remove because the connection will use it to send data to the client and then decr
		// refcount later
		return;
		break;
	case CO_RETRY:
		asc_write_retry(c);
		break;
	case CO_ABORT:
		asc_write_abort(c);
		break;
	case CO_INVALID:
		asc_write_invalid(c);
		break;
	default:
		break;
	}

	if (it != NULL)
		item_remove(it);
}

static void asc_process_iqget(struct conn *c, struct token *token, int ntoken) {
	char *key; /* key need to be granted lease */
	size_t key_size; /* # key bytes */
	int64_t lease_token = 0;
	lease_token_t new_lease_token = 0;	// storing the new lease value
	struct item* it = NULL;
	item_iq_result_t exc;

	char *tid = NULL;
	size_t tid_size = 0;

	asc_set_noreply_maybe(c, token, ntoken);

	if (!asc_ntoken_valid(c, ntoken)) {
		log_hexdump(LOG_NOTICE, c->req, c->req_len, "client error on c %d for "
				"req of type %d with %d invalid tokens", c->sd,
				c->req_type, ntoken);

		asc_write_client_error(c);
		return;
	}

	key = token[1].val;
	key_size = token[1].len;

	if (key_size > KEY_MAX_LEN) {
		log_debug(LOG_NOTICE, "client error on c %d for req of type %d and %d "
				"length key", c->sd, c->req_type, key_size);

		asc_write_client_error(c);
		return;
	}

	if (!mc_strtoll(token[2].val, &lease_token)) {
		log_debug(LOG_NOTICE, "client error on c %d for req of type %d and "
				"invalid token '%.*s'", c->sd, c->req_type,
				token[2].len, token[2].val);

		asc_write_client_error(c);
		return;
	}

	if (ntoken > 4) {
		tid = token[3].val;
		tid_size = token[3].len;
	}

	exc = item_iqget(key, key_size, (lease_token_t) lease_token, tid, tid_size,
			c, &it, &new_lease_token);

	switch (exc) {
	case IQ_VALUE:
		asc_write_iqget_value(c, it);

		// prevent _item_remove because the connection will use it to send data to the client and then decr
		// refcount later
		return;
		break;
	case IQ_NO_VALUE:
		asc_write_novalue(c);
		break;
	case IQ_MISS:
	case IQ_LEASE:
		asc_write_iqget_lease(c, it, new_lease_token);
		break;
	case IQ_SERVER_ERROR:
		asc_write_server_error(c);
		break;
	default:
		break;
	}

	if (it != NULL)
		item_remove(it);
}

static void asc_process_stats(struct conn *c, struct token *token, int ntoken) {
	struct token *t = &token[TOKEN_SUBCOMMAND];

	if (!stats_enabled()) {
		log_warn("server error on c %d for req of type %d because stats is "
				"disabled", c->sd, c->req_type);

		asc_write_server_error(c);
		return;
	}

	if (!asc_ntoken_valid(c, ntoken)) {
		log_hexdump(LOG_NOTICE, c->req, c->req_len, "client error on c %d for "
				"req of type %d with %d invalid tokens", c->sd,
				c->req_type, ntoken);

		asc_write_client_error(c);
		return;
	}

	if (ntoken == 2) {
		stats_default(c);
	} else if (strncmp(t->val, "reset", t->len) == 0) {
		log_warn("server error on c %d for req of type %d because stats reset "
				"is not supported", c->sd, c->req_type);
		asc_write_server_error(c);
		return;
	} else if (strncmp(t->val, "settings", t->len) == 0) {
		stats_settings(c);
	} else if (strncmp(t->val, "cachedump", t->len) == 0) {
		char *buf;
		unsigned int bytes, id, limit = 0;

		if (ntoken < 5) {
			log_hexdump(LOG_NOTICE, c->req, c->req_len, "client error on c %d "
					"for req of type %d with %d invalid tokens", c->sd,
					c->req_type, ntoken);

			asc_write_client_error(c);
			return;
		}

		if (!mc_strtoul(token[TOKEN_CACHEDUMP_ID].val, &id)
				|| !mc_strtoul(token[TOKEN_CACHEDUMP_LIMIT].val, &limit)) {
			log_debug(LOG_NOTICE, "client error on c %d for req of type %d "
					"because either id '%.*s' or limit '%.*s' is invalid",
					c->sd, c->req_type, token[TOKEN_CACHEDUMP_ID].len,
					token[TOKEN_CACHEDUMP_ID].val, token[TOKEN_CACHEDUMP_LIMIT].len,
					token[TOKEN_CACHEDUMP_LIMIT].val);

			asc_write_client_error(c);
			return;
		}

		if (id < SLABCLASS_MIN_ID || id > SLABCLASS_MAX_ID) {
			log_debug(LOG_NOTICE, "client error on c %d for req of type %d "
					"because %d is an illegal slab id", c->sd, c->req_type,
					id);

			asc_write_client_error(c);
			return;
		}

		buf = item_cache_dump(id, limit, &bytes);
		core_write_and_free(c, buf, bytes);
		return;
	} else {
		/*
		 * Getting here means that the sub command is either engine specific
		 * or is invalid. query the engine and see
		 */
		if (strncmp(t->val, "slabs", t->len) == 0) {
			stats_slabs(c);
		} else if (strncmp(t->val, "sizes", t->len) == 0) {
			stats_sizes(c);
		} else {
			log_debug(LOG_NOTICE, "client error on c %d for req of type %d with "
					"invalid stats subcommand '%.*s", c->sd, c->req_type,
					t->len, t->val);

			asc_write_client_error(c);
			return;
		}

		if (c->stats.buffer == NULL) {
			log_warn("server error on c %d for req of type %d because of oom "
					"writing stats", c->sd, c->req_type);

			asc_write_server_error(c);
		} else {
			core_write_and_free(c, c->stats.buffer, c->stats.offset);
			c->stats.buffer = NULL;
		}

		return;
	}

	/* append terminator and start the transfer */
	stats_append(c, NULL, 0, NULL, 0);

	if (c->stats.buffer == NULL) {
		log_warn("server error on c %d for req of type %d because of oom "
				"writing stats", c->sd, c->req_type);

		asc_write_server_error(c);
	} else {
		core_write_and_free(c, c->stats.buffer, c->stats.offset);
		c->stats.buffer = NULL;
	}
}

static void asc_process_klog(struct conn *c, struct token *token, int ntoken) {
	struct token *t;

	if (!asc_ntoken_valid(c, ntoken)) {
		log_hexdump(LOG_NOTICE, c->req, c->req_len, "client error on c %d for "
				"req of type %d with %d invalid tokens", c->sd,
				c->req_type, ntoken);

		asc_write_client_error(c);
		return;
	}

	t = &token[TOKEN_KLOG_COMMAND];

	if (strncmp(t->val, "run", t->len) == 0) {
		if (settings.klog_name == NULL) {
			log_debug(LOG_NOTICE, "client error on c %d for req of type %d "
					"with klog filename not set", c->sd, c->req_type);

			asc_write_client_error(c);
			return;
		}

		t = &token[TOKEN_KLOG_SUBCOMMAND];
		if (strncmp(t->val, "start", t->len) == 0) {
			log_debug(LOG_NOTICE, "klog start at epoch %u", time_now());
			settings.klog_running = true;
			asc_write_ok(c);
		} else if (strncmp(t->val, "stop", t->len) == 0) {
			log_debug(LOG_NOTICE, "klog stops at epoch %u", time_now());
			settings.klog_running = false;
			asc_write_ok(c);
		} else {
			log_debug(LOG_NOTICE, "client error on c %d for req of type %d "
					"with invalid klog run subcommand '%.*s'", c->sd,
					c->req_type, t->len, t->val);

			asc_write_client_error(c);
		}
	} else if (strncmp(t->val, "interval", t->len) == 0) {
		t = &token[TOKEN_KLOG_SUBCOMMAND];
		if (strncmp(t->val, "reset", t->len) == 0) {
			stats_set_interval(STATS_DEFAULT_INTVL);
			asc_write_ok(c);
		} else {
			int32_t interval;

			if (!mc_strtol(t->val, &interval)) {
				log_debug(LOG_NOTICE, "client error on c %d for req of type %d "
						"with invalid klog interval '%.*s'", c->sd,
						c->req_type, t->len, t->val);

				asc_write_client_error(c);
			} else if (interval < KLOG_MIN_INTVL) {
				log_debug(LOG_NOTICE, "client error on c %d for req of type %d "
						"with invalid klog interval %"PRId32"", c->sd,
						c->req_type, interval);

				asc_write_client_error(c);
			} else {
				stats_set_interval(interval);
				asc_write_ok(c);
			}
		}
	} else if (strncmp(t->val, "sampling", t->len) == 0) {
		t = &token[TOKEN_KLOG_SUBCOMMAND];
		if (strncmp(t->val, "reset", t->len) == 0) {
			settings.klog_sampling_rate = KLOG_DEFAULT_SMP_RATE;
			asc_write_ok(c);
		} else {
			int32_t sampling;

			if (!mc_strtol(t->val, &sampling)) {
				log_debug(LOG_NOTICE, "client error on c %d for req of type %d "
						"with invalid klog sampling '%.*s'", c->sd,
						c->req_type, t->len, t->val);

				asc_write_client_error(c);
			} else if (sampling <= 0) {
				log_debug(LOG_NOTICE, "client error on c %d for req of type %d "
						"with invalid klog sampling %"PRId32"", c->sd,
						c->req_type, sampling);

				asc_write_client_error(c);
			} else {
				settings.klog_sampling_rate = sampling;
				asc_write_ok(c);
			}
		}
	} else {
		log_debug(LOG_NOTICE, "client error on c %d for req of type %d with "
				"invalid klog subcommand '%.*s'", c->sd, c->req_type,
				t->len, t->val);

		asc_write_client_error(c);
	}
}

static void asc_process_verbosity(struct conn *c, struct token *token,
		int ntoken) {
	uint32_t level;

	asc_set_noreply_maybe(c, token, ntoken);

	if (ntoken != 3 && ntoken != 4) {
		log_hexdump(LOG_NOTICE, c->req, c->req_len, "client error on c %d for "
				"req of type %d with %d invalid tokens", c->sd,
				c->req_type, ntoken);

		asc_write_client_error(c);
		return;
	}

	if (!mc_strtoul(token[TOKEN_SUBCOMMAND].val, &level)) {
		log_debug(LOG_NOTICE, "client error on c %d for req of type %d with "
				"invalid level '%.*s'", c->sd, c->req_type,
				token[TOKEN_SUBCOMMAND].len, token[TOKEN_SUBCOMMAND].val);

		asc_write_client_error(c);
		return;
	}

	log_level_set(level);

	asc_write_ok(c);
}

static void asc_process_aggregate(struct conn *c, struct token *token,
		int ntoken) {
	int32_t interval;

	if (ntoken != 4) {
		log_hexdump(LOG_NOTICE, c->req, c->req_len, "client error on c %d for "
				"req of type %d with %d invalid tokens", c->sd,
				c->req_type, ntoken);

		asc_write_client_error(c);
		return;
	}

	if (!mc_strtol(token[TOKEN_AGGR_COMMAND].val, &interval)) {
		log_debug(LOG_NOTICE, "client error on c %d for req of type %d with "
				"invalid option '%.*s'", c->sd, c->req_type,
				token[TOKEN_AGGR_COMMAND].len, token[TOKEN_AGGR_COMMAND].val);

		asc_write_client_error(c);
		return;
	}

	if (interval > 0) {
		stats_set_interval(interval);
		asc_write_ok(c);
	} else if (interval == 0) {
		stats_set_interval(STATS_DEFAULT_INTVL);
		asc_write_ok(c);
	} else {
		stats_set_interval(-1000000);
		asc_write_ok(c);
	}
}

static void asc_process_evict(struct conn *c, struct token *token, int ntoken) {
	int32_t option;

	if (ntoken != 4) {
		log_hexdump(LOG_NOTICE, c->req, c->req_len, "client error on c %d for "
				"req of type %d with %d invalid tokens", c->sd,
				c->req_type, ntoken);

		asc_write_client_error(c);
		return;
	}

	if (!mc_strtol(token[TOKEN_EVICT_COMMAND].val, &option)) {
		log_debug(LOG_NOTICE, "client error on c %d for req of type %d with "
				"invalid option '%.*s'", c->sd, c->req_type,
				token[TOKEN_EVICT_COMMAND].len,token[TOKEN_EVICT_COMMAND].val);

		asc_write_client_error(c);
		return;
	}

	if (option >= EVICT_NONE && option < EVICT_INVALID) {
		settings.evict_opt = option;
		asc_write_ok(c);
		return;
	}

	log_debug(LOG_NOTICE, "client error on c %d for req of type %d with "
			"invalid option %"PRId32"", c->sd, c->req_type, option);

	asc_write_client_error(c);
}

static void asc_process_config(struct conn *c, struct token *token, int ntoken) {
	struct token *t = &token[TOKEN_SUBCOMMAND];

	if (strncmp(t->val, "aggregate", t->len) == 0) {
		asc_process_aggregate(c, token, ntoken);
	} else if (strncmp(t->val, "klog", t->len) == 0) {
		asc_process_klog(c, token, ntoken);
	} else if (strncmp(t->val, "evict", t->len) == 0) {
		asc_process_evict(c, token, ntoken);
	}
}

static void asc_process_flushall(struct conn *c, struct token *token,
		int ntoken) {
	struct bound *t = &ntoken_bound[REQ_FLUSHALL];
	int32_t exptime_int;
	time_t exptime;

	time_update();

	asc_set_noreply_maybe(c, token, ntoken);

	if (!asc_ntoken_valid(c, ntoken)) {
		log_hexdump(LOG_NOTICE, c->req, c->req_len, "client error on c %d for "
				"req of type %d with %d invalid tokens", c->sd,
				c->req_type, ntoken);

		asc_write_client_error(c);
		return;
	}

	if (ntoken == t->b[c->noreply].min) {
		settings.oldest_live = time_now() - 1;
		item_flush_expired();
		asc_write_ok(c);
		return;
	}

	if (!mc_strtol(token[TOKEN_SUBCOMMAND].val, &exptime_int)) {
		log_debug(LOG_NOTICE, "client error on c %d for req of type %d with "
				"invalid numeric value '%.*s'", c->sd, c->req_type,
				token[TOKEN_SUBCOMMAND].len, token[TOKEN_SUBCOMMAND].val);

		asc_write_client_error(c);
		return;
	}

	exptime = (time_t) exptime_int;

	/*
	 * If exptime is zero time_reltime() would return zero too, and
	 * time_reltime(exptime) - 1 would overflow to the max unsigned value.
	 * So we process exptime == 0 the same way we do when no delay is
	 * given at all.
	 */
	if (exptime > 0) {
		settings.oldest_live = time_reltime(exptime) - 1;
	} else {
		/* exptime == 0 */
		settings.oldest_live = time_now() - 1;
	}

	item_flush_expired();
	asc_write_ok(c);
}

static req_type_t asc_parse_type(struct conn *c, struct token *token,
		int ntoken) {
	char *tval; /* token value */
	size_t tlen; /* token length */
	req_type_t type; /* request type */

	if (ntoken < 2) {
		return REQ_UNKNOWN;
	}

	tval = token[TOKEN_COMMAND].val;
	tlen = token[TOKEN_COMMAND].len;

	type = REQ_UNKNOWN;

	switch (tlen) {
	case 3:
		if (str4cmp(tval, 'g', 'e', 't', ' ')) {
			type = REQ_GET;
		} else if (str4cmp(tval, 's', 'e', 't', ' ')) {
			type = REQ_SET;
		} else if (str4cmp(tval, 'a', 'd', 'd', ' ')) {
			type = REQ_ADD;
		} else if (str4cmp(tval, 'c', 'a', 's', ' ')) {
			type = REQ_CAS;
		} else if (str4cmp(tval, 'd', 'a', 'r', ' ')) {
			type = REQ_DAR;
		} else if (str4cmp(tval, 's', 'a', 'r', ' ')) {
			type = REQ_SAR;
		}

		break;

	case 4:
		if (str4cmp(tval, 'g', 'e', 't', 's')) {
			type = REQ_GETS;
		} else if (str4cmp(tval, 'i', 'n', 'c', 'r')) {
			type = REQ_INCR;
		} else if (str4cmp(tval, 'd', 'e', 'c', 'r')) {
			type = REQ_DECR;
		} else if (str4cmp(tval, 'q', 'u', 'i', 't')) {
			type = REQ_QUIT;
		} else if (str4cmp(tval, 's', 'w', 'a', 'p')) {
			type = REQ_SWAP;
		}

		break;

	case 5:
		if (str5cmp(tval, 'i', 'q', 'g', 'e', 't')) {
			type = REQ_IQGET;
		} else if (str5cmp(tval, 'i', 'q', 's', 'e', 't')) {
			type = REQ_IQSET;
		} else if (str5cmp(tval, 's', 't', 'a', 't', 's')) {
			type = REQ_STATS;
		} else if (str5cmp(tval, 'q', 'a', 'r', 'e', 'g')) {
			type = REQ_QAREG;
		} else if (str5cmp(tval, 'c', 'i', 'g', 'e', 't')) {
			type = REQ_CIGET;
		} else if (str5cmp(tval, 'o', 'q', 'r', 'e', 'g')) {
			type = REQ_OQREG;
		} else if (str5cmp(tval, 'c', 'h', 'e', 'c', 'k')) {
			type = REQ_CHECK;
		} else if (str5cmp(tval, 'o', 'q', 'a', 'd', 'd')) {
			type = REQ_OQADD;
		}

		break;

	case 6:
		if (str6cmp(tval, 'a', 'p', 'p', 'e', 'n', 'd')) {
			type = REQ_APPEND;
		} else if (str6cmp(tval, 'd', 'e', 'l', 'e', 't', 'e')) {
			type = REQ_DELETE;
		} else if (str6cmp(tval, 'c', 'o', 'n', 'f', 'i', 'g')) {
			type = REQ_CONFIG;
		} else if (str6cmp(tval, 'q', 'a', 'r', 'e', 'a', 'd')) {
			type = REQ_QAREAD;
		} else if (str6cmp(tval, 'i', 'q', 'i', 'n', 'c', 'r')) {
			type = REQ_IQINCR;
		} else if (str6cmp(tval, 'i', 'q', 'd', 'e', 'c', 'r')) {
			type = REQ_IQDECR;
		} else if (str6cmp(tval, 'c', 'o', 'm', 'm', 'i', 't')) {
			type = REQ_COMMIT;
		} else if (str6cmp(tval, 'f', 't', 'r', 'a', 'n', 's')) {
			type = REQ_FTRANS;
		} else if (str6cmp(tval, 'o', 'q', 'r', 'e', 'a', 'd')) {
			type = REQ_OQREAD;
		} else if (str6cmp(tval, 'o', 'q', 's', 'w', 'a', 'p')) {
			type = REQ_OQSWAP;
		} else if (str6cmp(tval, 'o', 'q', 'i', 'n', 'c', 'r')) {
			type = REQ_OQINCR;
		} else if (str6cmp(tval, 'o', 'q', 'd', 'e', 'c', 'r')) {
			type = REQ_OQDECR;
		} else if (str6cmp(tval, 'd', 'a', 'b', 'o', 'r', 't')) {
			type = REQ_DABORT;
		} else if (str6cmp(tval, 'c', 'o', 'g', 'e', 't', 's')) {
			type = REQ_COGETS;
		}

		break;

	case 7:
		if (str8cmp(tval, 'r', 'e', 'p', 'l', 'a', 'c', 'e', ' ')) {
			type = REQ_REPLACE;
		} else if (str8cmp(tval, 'p', 'r', 'e', 'p', 'e', 'n', 'd', ' ')) {
			type = REQ_PREPEND;
		} else if (str7cmp(tval, 'v', 'e', 'r', 's', 'i', 'o', 'n')) {
			type = REQ_VERSION;
		} else if (str7cmp(tval, 'u', 'n', 'l', 'e', 'a', 's', 'e')) {
			type = REQ_UNLEASE;
		} else if (str7cmp(tval, 'r', 'e', 'l', 'e', 'a', 's', 'e')) {
			type = REQ_RELEASE;
		} else if (str7cmp(tval, 'd', 'c', 'o', 'm', 'm', 'i', 't')) {
			type = REQ_DCOMMIT;
		} else if (str7cmp(tval, 'g', 'e', 't', 'p', 'r', 'i', 'k')) {
			type = REQ_GETPRIK;
		} else if (str7cmp(tval, 'o', 'q', 'w', 'r', 'i', 't', 'e')) {
			type = REQ_OQWRITE;
		} else if (str7cmp(tval, 'o', 'q', 's', 'w', 'a', 'p', 's')) {
			type = REQ_OQSWAPS;
		}

		break;

	case 8:
		if (str8cmp(tval, 'i', 'q', 'a', 'p', 'p', 'e', 'n', 'd')) {
			type = REQ_IQAPPEND;
		} else if (str8cmp(tval, 'v', 'a', 'l', 'i', 'd', 'a', 't', 'e')) {
			type = REQ_VALIDATE;
		} else if (str8cmp(tval, 'o', 'q', 'a', 'p', 'p', 'e', 'n', 'd')) {
			type = REQ_OQAPPEND;
		}

		break;

	case 9:
		if (str9cmp(tval, 'f', 'l', 'u', 's', 'h', '_', 'a', 'l', 'l')) {
			type = REQ_FLUSHALL;
		} else if (str9cmp(tval, 'v', 'e', 'r', 'b', 'o', 's', 'i', 't', 'y')) {
			type = REQ_VERBOSITY;
		} else if (str9cmp(tval, 'i', 'q', 'p', 'r', 'e', 'p', 'e', 'n', 'd')) {
			type = REQ_IQPREPEND;
		} else if (str9cmp(tval, 'c', 'o', 'u', 'n', 'l', 'e', 'a', 's', 'e')) {
			type = REQ_COUNLEASE;
		} else if (str9cmp(tval, 'o', 'q', 'p', 'r', 'e', 'p', 'e', 'n', 'd')) {
			type = REQ_OQPREPEND;
		} else if (str9cmp(tval, 'o', 'q', 'a', 'p', 'p', 'e', 'n', 'd', '2')) {
			type = REQ_OQAPPEND2;
		} else if (str9cmp(tval, 'o', 'q', 'a', 'p', 'p', 'e', 'n', 'd', 's')) {
			type = REQ_OQAPPENDS;
		}

		break;

	default:
		type = REQ_UNKNOWN;
		break;
	}

	return type;
}

static void asc_dispatch(struct conn *c) {
	rstatus_t status;
	struct token token[TOKEN_MAX];
	int ntoken;

	/*
	 * For commands set, add, or replace, we build an item and read the data
	 * directly into it, then continue in asc_complete_nread().
	 */

	c->msg_curr = 0;
	c->msg_used = 0;
	c->iov_used = 0;
	status = conn_add_msghdr(c);
	if (status != MC_OK) {
		log_warn("server error on c %d for req of type %d because of oom in "
				"preparing response", c->sd, c->req_type);

		asc_write_server_error(c);
		return;
	}

	ntoken = asc_tokenize(c->req, token, TOKEN_MAX);

	c->req_type = asc_parse_type(c, token, ntoken);
	switch (c->req_type) {
	case REQ_GET:
	case REQ_GETS:
		/* we do not update stats metrics here because of multi-get */
		asc_process_read(c, token, ntoken);
		break;
	case REQ_COGETS:
		asc_process_cogets(c, token, ntoken);
		break;

	case REQ_CIGET:
		asc_process_ciget(c, token, ntoken);
		break;

	case REQ_IQGET:
		asc_process_iqget(c, token, ntoken);
		break;

	case REQ_IQSET:
		asc_process_update(c, token, ntoken);
		break;
	case REQ_SET:
		stats_thread_incr(set);
		asc_process_update(c, token, ntoken);
		break;

	case REQ_ADD:
		stats_thread_incr(add);
		asc_process_update(c, token, ntoken);
		break;

	case REQ_REPLACE:
		stats_thread_incr(replace);
		asc_process_update(c, token, ntoken);
		break;

	case REQ_APPEND:
		stats_thread_incr(append);
		asc_process_update(c, token, ntoken);
		break;

	case REQ_PREPEND:
		stats_thread_incr(prepend);
		asc_process_update(c, token, ntoken);
		break;

	case REQ_CAS:
		stats_thread_incr(cas);
		asc_process_update(c, token, ntoken);
		break;

	case REQ_INCR:
		stats_thread_incr(incr);
		asc_process_arithmetic(c, token, ntoken);
		break;

	case REQ_DECR:
		stats_thread_incr(decr);
		asc_process_arithmetic(c, token, ntoken);
		break;

	case REQ_DELETE:
		stats_thread_incr(delete);
		asc_process_delete(c, token, ntoken);
		break;

	case REQ_OQREAD:
		stats_thread_incr(oqread);
		asc_process_oqread(c, token, ntoken);
		break;

	case REQ_OQSWAP:
		stats_thread_incr(oqswap);
		asc_process_oqswap(c, token, ntoken);
		break;

	case REQ_SWAP:
		stats_thread_incr(swap);
		asc_process_swap(c, token, ntoken);
		break;

	case REQ_OQWRITE:
		stats_thread_incr(oqwrite);
		asc_process_oqwrite(c, token, ntoken);
		break;

	case REQ_OQADD:
		stats_thread_incr(oqadd);
		asc_process_oqadd(c, token, ntoken);
		break;

	case REQ_VALIDATE:
		stats_thread_incr(validate);
		asc_process_validate(c, token, ntoken);
		break;

	case REQ_STATS:
		stats_thread_incr(stats);
		asc_process_stats(c, token, ntoken);
		break;

	case REQ_FLUSHALL:
		stats_thread_incr(flush);
		asc_process_flushall(c, token, ntoken);
		break;

	case REQ_VERSION:
		asc_write_version(c);
		break;

	case REQ_QUIT:
		conn_set_state(c, CONN_CLOSE);
		break;

	case REQ_VERBOSITY:
		asc_process_verbosity(c, token, ntoken);
		break;

	case REQ_CONFIG:
		asc_process_config(c, token, ntoken);
		break;

	case REQ_UNLEASE:
		stats_thread_incr(unlease);
		asc_process_unlease(c, token, ntoken);
		break;

	case REQ_COUNLEASE:
		asc_process_co_unlease(c, token, ntoken);
		break;

	case REQ_OQREG:
		asc_process_oqreg(c, token, ntoken);
		break;

	case REQ_DCOMMIT:
		asc_process_dcommit(c, token, ntoken);
		break;
	case REQ_DABORT:
		asc_process_dabort(c, token, ntoken);
		break;

	case REQ_QAREG:
		//TODO consider implementing statistic-function here
		asc_process_qareg(c, token, ntoken);
		break;

	case REQ_DAR:
		asc_process_dar(c, token, ntoken);
		break;

	case REQ_QAREAD:
		asc_process_qaread(c, token, ntoken);
		break;

	case REQ_SAR:
		asc_process_sar(c, token, ntoken);
		break;

	case REQ_IQAPPEND:
	case REQ_IQPREPEND:
		asc_process_iqappend_iqprepend(c, token, ntoken);
		break;
	case REQ_IQINCR:
	case REQ_IQDECR:
		asc_process_iqincr_iqdecr(c, token, ntoken);
		break;

	case REQ_OQAPPEND:
	case REQ_OQPREPEND:
		asc_process_oqappend_oqprepend(c, token, ntoken);
		break;

	case REQ_OQAPPEND2:
		asc_process_oqappend2(c, token, ntoken);
		break;

	case REQ_OQINCR:
	case REQ_OQDECR:
		asc_process_oqincr_oqdecr(c, token, ntoken);
		break;

	case REQ_COMMIT:
		asc_process_commit(c, token, ntoken);
		break;

	case REQ_RELEASE:
		asc_process_release(c, token, ntoken);
		break;

	case REQ_CHECK:
		asc_process_check(c, token, ntoken);
		break;

	case REQ_FTRANS:
		stats_thread_incr(ftrans);
		asc_process_ftrans(c, token, ntoken);
		break;

	case REQ_OQAPPENDS:
		asc_process_stores(c, token, ntoken);
		break;
	case REQ_OQSWAPS:
		asc_process_stores(c, token, ntoken);
		break;

	case REQ_UNKNOWN:
	default:
		log_hexdump(LOG_INFO, c->req, c->req_len, "req on c %d with %d "
				"invalid tokens", c->sd, ntoken);
		asc_write_client_error(c);
		break;
	}
}

rstatus_t asc_parse(struct conn *c) {
	char *el, *cont; /* eol marker, continue marker */

	ASSERT(c->rcurr <= c->rbuf + c->rsize);

	if (c->rbytes == 0) {
		return MC_EAGAIN;
	}

	el = memchr(c->rcurr, '\n', c->rbytes);
	if (el == NULL) {
		if (c->rbytes > 1024) {
			char *ptr = c->rcurr;

			/*
			 * We didn't have a '\n' in the first k. This _has_ to be a
			 * large multiget, if not we should just nuke the connection.
			 */

			/* ignore leading whitespaces */
			while (*ptr == ' ') {
				++ptr;
			}

			if (ptr - c->rcurr > 100
					|| (strncmp(ptr, "get ", 4) && strncmp(ptr, "gets ", 5) &&
							strncmp(ptr, "cogets ", 7) && strncmp(ptr, "oqappends ", 10) && strncmp(ptr, "oqswaps ", 8))) {
				conn_set_state(c, CONN_CLOSE);
				return MC_ERROR;
			}
		}

		return MC_EAGAIN;
	}

	cont = el + 1;
	if ((el - c->rcurr) > 1 && *(el - 1) == '\r') {
		el--;
	}
	*el = '\0';

	log_hexdump(LOG_VERB, c->rcurr, el - c->rcurr, "recv on c %d req with "
			"%d bytes", c->sd, el - c->rcurr);

	ASSERT(cont <= c->rbuf + c->rsize);ASSERT(cont <= c->rcurr + c->rbytes);

	c->req = c->rcurr;
	c->req_len = (uint16_t) (el - c->rcurr);

	asc_dispatch(c);

	/* update the read marker to point to continue marker */
	c->rbytes -= (cont - c->rcurr);
	c->rcurr = cont;

	return MC_OK;
}

void asc_append_stats(struct conn *c, const char *key, uint16_t klen,
		const char *val, uint32_t vlen) {
	char *pos;
	uint32_t nbyte;
	int remaining, room;

	pos = c->stats.buffer + c->stats.offset;
	remaining = c->stats.size - c->stats.offset;
	room = remaining - 1;

	if (klen == 0 && vlen == 0) {
		nbyte = snprintf(pos, room, "END\r\n");
	} else if (vlen == 0) {
		nbyte = snprintf(pos, room, "STAT %s\r\n", key);
	} else {
		nbyte = snprintf(pos, room, "STAT %s %s\r\n", key, val);
	}

	c->stats.offset += nbyte;
}
