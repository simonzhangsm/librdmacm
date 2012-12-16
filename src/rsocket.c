/*
 * Copyright (c) 2008-2012 Intel Corporation.  All rights reserved.
 *
 * This software is available to you under a choice of one of two
 * licenses.  You may choose to be licensed under the terms of the GNU
 * General Public License (GPL) Version 2, available from the file
 * COPYING in the main directory of this source tree, or the
 * OpenIB.org BSD license below:
 *
 *     Redistribution and use in source and binary forms, with or
 *     without modification, are permitted provided that the following
 *     conditions are met:
 *
 *      - Redistributions of source code must retain the above
 *        copyright notice, this list of conditions and the following
 *        disclaimer.
 *
 *      - Redistributions in binary form must reproduce the above
 *        copyright notice, this list of conditions and the following
 *        disclaimer in the documentation and/or other materials
 *        provided with the distribution.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
 * ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */

#if HAVE_CONFIG_H
#  include <config.h>
#endif /* HAVE_CONFIG_H */

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <stdarg.h>
#include <netdb.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdio.h>
#include <stddef.h>
#include <string.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/epoll.h>
#include <search.h>

#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>
#include <rdma/rsocket.h>
#include "cma.h"
#include "indexer.h"

#define RS_OLAP_START_SIZE 2048
#define RS_MAX_TRANSFER 65536
#define RS_SNDLOWAT 2048
#define RS_QP_MAX_SIZE 0xFFFE
#define RS_QP_CTRL_SIZE 4
#define RS_CONN_RETRIES 6
#define RS_SGL_SIZE 2
static struct index_map idm;
static pthread_mutex_t mut = PTHREAD_MUTEX_INITIALIZER;

enum {
	RS_SVC_INSERT,
	RS_SVC_REMOVE
};

struct rsocket;


#define PRINTADDR(a) \
printf("%s port %x ip %x\n", __func__, \
	((struct sockaddr_in *)a)->sin_port, \
	((struct sockaddr_in *)a)->sin_addr.s_addr)



struct rs_svc_msg {
	uint32_t op;
	uint32_t status;
	struct rsocket *rs;
};

static pthread_t svc_id;
static int svc_sock[2];
static int svc_cnt;
static int svc_size;
static struct rsocket **svc_rss;
static struct pollfd *svc_fds;
static uint8_t svc_buf[RS_SNDLOWAT];
static void *rs_svc_run(void *arg);

static uint16_t def_iomap_size = 0;
static uint16_t def_inline = 64;
static uint16_t def_sqsize = 384;
static uint16_t def_rqsize = 384;
static uint32_t def_mem = (1 << 17);
static uint32_t def_wmem = (1 << 17);
static uint32_t polling_time = 10;

/*
 * Immediate data format is determined by the upper bits
 * bit 31: message type, 0 - data, 1 - control
 * bit 30: buffers updated, 0 - target, 1 - direct-receive
 * bit 29: more data, 0 - end of transfer, 1 - more data available
 *
 * for data transfers:
 * bits [28:0]: bytes transferred
 * for control messages:
 * SGL, CTRL
 * bits [28-0]: receive credits granted
 * IOMAP_SGL
 * bits [28-16]: reserved, bits [15-0]: index
 */

enum {
	RS_OP_DATA,
	RS_OP_RSVD_DATA_MORE,
	RS_OP_WRITE, /* opcode is not transmitted over the network */
	RS_OP_RSVD_DRA_MORE,
	RS_OP_SGL,
	RS_OP_RSVD,
	RS_OP_IOMAP_SGL,
	RS_OP_CTRL
};
#define rs_msg_set(op, data)  ((op << 29) | (uint32_t) (data))
#define rs_msg_op(imm_data)   (imm_data >> 29)
#define rs_msg_data(imm_data) (imm_data & 0x1FFFFFFF)
#define RS_RECV_WR_ID (~((uint64_t) 0))

#define DS_WR_RECV 0xFFFFFFFF
#define ds_send_wr_id(offset, length) (((uint64_t) (offset)) << 32 | (uint64_t) length)
#define ds_recv_wr_id(offset) (((uint64_t) (offset)) << 32 | (uint64_t) DS_WR_RECV)
#define ds_wr_offset(wr_id) ((uint32_t) (wr_id >> 32))
#define ds_wr_length(wr_id) ((uint32_t) wr_id)
#define ds_wr_is_recv(wr_id) (ds_wr_length(wr_id) == DS_WR_RECV)

enum {
	RS_CTRL_DISCONNECT,
	RS_CTRL_SHUTDOWN
};

struct rs_msg {
	uint32_t op;
	uint32_t data;
};

struct ds_qp;

struct ds_rmsg {
	struct ds_qp	*qp;
	uint32_t	offset;
	uint32_t	length;
};

struct ds_smsg {
	struct ds_smsg	*next;
};

struct rs_sge {
	uint64_t addr;
	uint32_t key;
	uint32_t length;
};

struct rs_iomap {
	uint64_t offset;
	struct rs_sge sge;
};

struct rs_iomap_mr {
	uint64_t offset;
	struct ibv_mr *mr;
	dlist_entry entry;
	atomic_t refcnt;
	int index;	/* -1 if mapping is local and not in iomap_list */
};

#define RS_MIN_INLINE      (sizeof(struct rs_sge))
#define rs_host_is_net()   (1 == htonl(1))
#define RS_CONN_FLAG_NET   (1 << 0)
#define RS_CONN_FLAG_IOMAP (1 << 1)

struct rs_conn_data {
	uint8_t		  version;
	uint8_t		  flags;
	uint16_t	  credits;
	uint8_t		  reserved[3];
	uint8_t		  target_iomap_size;
	struct rs_sge	  target_sgl;
	struct rs_sge	  data_buf;
};

/*
 * rsocket states are ordered as passive, connecting, connected, disconnected.
 */
enum rs_state {
	rs_init,
	rs_bound	   =		    0x0001,
	rs_listening	   =		    0x0002,
	rs_opening	   =		    0x0004,
	rs_resolving_addr  = rs_opening |   0x0010,
	rs_resolving_route = rs_opening |   0x0020,
	rs_connecting      = rs_opening |   0x0040,
	rs_accepting       = rs_opening |   0x0080,
	rs_connected	   =		    0x0100,
	rs_writable 	   =		    0x0200,
	rs_readable	   =		    0x0400,
	rs_connect_rdwr    = rs_connected | rs_readable | rs_writable,
	rs_connect_error   =		    0x0800,
	rs_disconnected	   =		    0x1000,
	rs_error	   =		    0x2000,
};

#define RS_OPT_SWAP_SGL 1

union socket_addr {
	struct sockaddr		sa;
	struct sockaddr_in	sin;
	struct sockaddr_in6	sin6;
};

struct ds_header {
	uint8_t		  version;
	uint8_t		  length;
	uint16_t	  port;
	union {
		uint32_t  ipv4;
		struct {
			uint32_t flowinfo;
			uint8_t  addr[16];
		} ipv6;
	} addr;
};

#define DS_IPV4_HDR_LEN  8
#define DS_IPV6_HDR_LEN 24

struct ds_dest {
	union socket_addr addr;	/* must be first */
	struct ds_qp	  *qp;
	struct ibv_ah	  *ah;
	uint32_t	   qpn;
};

struct ds_qp {
	dlist_entry	  list;
	struct rsocket	  *rs;
	struct rdma_cm_id *cm_id;
	struct ds_header  hdr;
	struct ds_dest	  dest;

	struct ibv_mr	  *smr;
	struct ibv_mr	  *rmr;
	uint8_t		  *rbuf;

	int		  cq_armed;
};

struct rsocket {
	int		  type;
	int		  index;
	fastlock_t	  slock;
	fastlock_t	  rlock;
	fastlock_t	  cq_lock;
	fastlock_t	  cq_wait_lock;
	fastlock_t	  map_lock; /* acquire slock first if needed */

	union {
		/* data stream */
		struct {
			struct rdma_cm_id *cm_id;
			uint64_t	  tcp_opts;

			int		  ctrl_avail;
			uint16_t	  sseq_no;
			uint16_t	  sseq_comp;
			uint16_t	  rseq_no;
			uint16_t	  rseq_comp;

			int		  remote_sge;
			struct rs_sge	  remote_sgl;
			struct rs_sge	  remote_iomap;

			struct ibv_mr	  *target_mr;
			int		  target_sge;
			int		  target_iomap_size;
			void		  *target_buffer_list;
			volatile struct rs_sge	  *target_sgl;
			struct rs_iomap   *target_iomap;

			int		  rbuf_bytes_avail;
			int		  rbuf_free_offset;
			int		  rbuf_offset;
			struct ibv_mr	  *rmr;
			uint8_t		  *rbuf;

			int		  sbuf_bytes_avail;
			struct ibv_mr	  *smr;
			struct ibv_sge	  ssgl[2];
		};
		/* datagram */
		struct {
			struct ds_qp	  *qp_list;
			void		  *dest_map;
			struct ds_dest    *conn_dest;

			int		  udp_sock;
			int		  epfd;
			int		  rqe_avail;
			struct ds_smsg	  *smsg_free;
		};
	};

	int		  opts;
	long		  fd_flags;
	uint64_t	  so_opts;
	uint64_t	  ipv6_opts;
	int		  state;
	int		  cq_armed;
	int		  retries;
	int		  err;

	int		  sqe_avail;
	uint32_t	  sbuf_size;
	uint16_t	  sq_size;
	uint16_t	  sq_inline;

	uint32_t	  rbuf_size;
	uint16_t	  rq_size;
	int		  rmsg_head;
	int		  rmsg_tail;
	union {
		struct rs_msg	  *rmsg;
		struct ds_rmsg	  *dmsg;
	};

	uint8_t		  *sbuf;
	struct rs_iomap_mr *remote_iomappings;
	dlist_entry	  iomap_list;
	dlist_entry	  iomap_queue;
	int		  iomap_pending;
};

#define DS_UDP_TAG 0x55555555

struct ds_udp_header {
	uint32_t	  tag;
	uint8_t		  version;
	uint8_t		  op;
	uint8_t		  length;
	uint8_t		  reserved;
	uint32_t	  qpn;  /* lower 8-bits reserved */
	union {
		uint32_t ipv4;
		uint8_t  ipv6[16];
	} addr;
};

#define DS_UDP_IPV4_HDR_LEN 16
#define DS_UDP_IPV6_HDR_LEN 28

#define ds_next_qp(qp) container_of((qp)->list.next, struct ds_qp, list)

static void ds_insert_qp(struct rsocket *rs, struct ds_qp *qp)
{
	if (!rs->qp_list)
		dlist_init(&qp->list);
	else
		dlist_insert_head(&qp->list, &rs->qp_list->list);
	rs->qp_list = qp;
}

static void ds_remove_qp(struct rsocket *rs, struct ds_qp *qp)
{
	if (qp->list.next != &qp->list) {
		rs->qp_list = ds_next_qp(qp);
		dlist_remove(&qp->list);
	} else {
		rs->qp_list = NULL;
	}
}

static int rs_add_to_svc(struct rsocket *rs)
{
	struct rs_svc_msg msg;
	int ret;

	pthread_mutex_lock(&mut);
	if (!svc_cnt) {
		ret = socketpair(AF_UNIX, SOCK_STREAM, 0, svc_sock);
		if (ret)
			goto err1;

		ret = pthread_create(&svc_id, NULL, rs_svc_run, NULL);
		if (ret) {
			ret = ERR(ret);
			goto err2;
		}
	}

	msg.op = RS_SVC_INSERT;
	msg.status = EINVAL;
	msg.rs = rs;
	write(svc_sock[0], &msg, sizeof msg);
	read(svc_sock[0], &msg, sizeof msg);
	ret = rdma_seterrno(msg.status);
	if (ret && !svc_cnt)
		goto err3;

	pthread_mutex_unlock(&mut);
	return ret;

err3:
	pthread_join(svc_id, NULL);
err2:
	close(svc_sock[0]);
	close(svc_sock[1]);
err1:
	pthread_mutex_unlock(&mut);
	return ret;
}

static int rs_remove_from_svc(struct rsocket *rs)
{
	struct rs_svc_msg msg;
	int ret;

	pthread_mutex_lock(&mut);
	msg.op = RS_SVC_REMOVE;
	msg.status = EINVAL;
	msg.rs = rs;
	write(svc_sock[0], &msg, sizeof msg);
	read(svc_sock[0], &msg, sizeof msg);
	ret = ERR(msg.status);
	if (!svc_cnt) {
		pthread_join(svc_id, NULL);
		close(svc_sock[0]);
		close(svc_sock[1]);
	}

	pthread_mutex_unlock(&mut);
	return ret;
}

static int ds_compare_addr(const void *dst1, const void *dst2)
{
	const struct sockaddr *sa1, *sa2;
	size_t len;

	sa1 = (const struct sockaddr *) dst1;
	sa2 = (const struct sockaddr *) dst2;

	len = (sa1->sa_family == AF_INET6 && sa2->sa_family == AF_INET6) ?
	      sizeof(struct sockaddr_in6) : sizeof(struct sockaddr_in);
//	printf("%s len %d sizeof sin %d memcmp %d\n", __func__, len, sizeof(struct sockaddr_in), memcmp(dst1,dst2,len));
	return memcmp(dst1, dst2, len);
}

static int rs_value_to_scale(int value, int bits)
{
	return value <= (1 << (bits - 1)) ?
	       value : (1 << (bits - 1)) | (value >> bits);
}

static int rs_scale_to_value(int value, int bits)
{
	return value <= (1 << (bits - 1)) ?
	       value : (value & ~(1 << (bits - 1))) << bits;
}

void rs_configure(void)
{
	FILE *f;
	static int init;

	if (init)
		return;

	pthread_mutex_lock(&mut);
	if (init)
		goto out;

	if ((f = fopen(RS_CONF_DIR "/polling_time", "r"))) {
		(void) fscanf(f, "%u", &polling_time);
		fclose(f);
	}

	if ((f = fopen(RS_CONF_DIR "/inline_default", "r"))) {
		(void) fscanf(f, "%hu", &def_inline);
		fclose(f);

		if (def_inline < RS_MIN_INLINE)
			def_inline = RS_MIN_INLINE;
	}

	if ((f = fopen(RS_CONF_DIR "/sqsize_default", "r"))) {
		(void) fscanf(f, "%hu", &def_sqsize);
		fclose(f);
	}

	if ((f = fopen(RS_CONF_DIR "/rqsize_default", "r"))) {
		(void) fscanf(f, "%hu", &def_rqsize);
		fclose(f);
	}

	if ((f = fopen(RS_CONF_DIR "/mem_default", "r"))) {
		(void) fscanf(f, "%u", &def_mem);
		fclose(f);

		if (def_mem < 1)
			def_mem = 1;
	}

	if ((f = fopen(RS_CONF_DIR "/wmem_default", "r"))) {
		(void) fscanf(f, "%u", &def_wmem);
		fclose(f);
		if (def_wmem < RS_SNDLOWAT)
			def_wmem = RS_SNDLOWAT << 1;
	}

	if ((f = fopen(RS_CONF_DIR "/iomap_size", "r"))) {
		(void) fscanf(f, "%hu", &def_iomap_size);
		fclose(f);

		/* round to supported values */
		def_iomap_size = (uint8_t) rs_value_to_scale(
			(uint16_t) rs_scale_to_value(def_iomap_size, 8), 8);
	}
	init = 1;
out:
	pthread_mutex_unlock(&mut);
}

static int rs_insert(struct rsocket *rs, int index)
{
	pthread_mutex_lock(&mut);
	rs->index = idm_set(&idm, index, rs);
	pthread_mutex_unlock(&mut);
	return rs->index;
}

static void rs_remove(struct rsocket *rs)
{
	pthread_mutex_lock(&mut);
	idm_clear(&idm, rs->index);
	pthread_mutex_unlock(&mut);
}

static struct rsocket *rs_alloc(struct rsocket *inherited_rs, int type)
{
	struct rsocket *rs;

	rs = calloc(1, sizeof *rs);
	if (!rs)
		return NULL;

	rs->type = type;
	rs->index = -1;
	if (type == SOCK_DGRAM) {
		rs->udp_sock = -1;
		rs->epfd = -1;
	}

	if (inherited_rs) {
		rs->sbuf_size = inherited_rs->sbuf_size;
		rs->rbuf_size = inherited_rs->rbuf_size;
		rs->sq_inline = inherited_rs->sq_inline;
		rs->sq_size = inherited_rs->sq_size;
		rs->rq_size = inherited_rs->rq_size;
		if (type == SOCK_STREAM) {
			rs->ctrl_avail = inherited_rs->ctrl_avail;
			rs->target_iomap_size = inherited_rs->target_iomap_size;
		}
	} else {
		rs->sbuf_size = def_wmem;
		rs->rbuf_size = def_mem;
		rs->sq_inline = def_inline;
		rs->sq_size = def_sqsize;
		rs->rq_size = def_rqsize;
		if (type == SOCK_STREAM) {
			rs->ctrl_avail = RS_QP_CTRL_SIZE;
			rs->target_iomap_size = def_iomap_size;
		}
	}
	fastlock_init(&rs->slock);
	fastlock_init(&rs->rlock);
	fastlock_init(&rs->cq_lock);
	fastlock_init(&rs->cq_wait_lock);
	fastlock_init(&rs->map_lock);
	dlist_init(&rs->iomap_list);
	dlist_init(&rs->iomap_queue);
	return rs;
}

static int rs_set_nonblocking(struct rsocket *rs, long arg)
{
	struct ds_qp *qp;
	int ret = 0;

	if (rs->type == SOCK_STREAM) {
		if (rs->cm_id->recv_cq_channel)
			ret = fcntl(rs->cm_id->recv_cq_channel->fd, F_SETFL, arg);

		if (!ret && rs->state < rs_connected)
			ret = fcntl(rs->cm_id->channel->fd, F_SETFL, arg);
	} else {
		ret = fcntl(rs->epfd, F_SETFL, arg);
		if (!ret && rs->qp_list) {
			qp = rs->qp_list;
			do {
				ret = fcntl(qp->cm_id->recv_cq_channel->fd,
					    F_SETFL, arg);
				qp = ds_next_qp(qp);
			} while (qp != rs->qp_list && !ret);
		}
	}

	return ret;
}

static void rs_set_qp_size(struct rsocket *rs)
{
	uint16_t max_size;

	max_size = min(ucma_max_qpsize(rs->cm_id), RS_QP_MAX_SIZE);

	if (rs->sq_size > max_size)
		rs->sq_size = max_size;
	else if (rs->sq_size < 2)
		rs->sq_size = 2;
	if (rs->sq_size <= (RS_QP_CTRL_SIZE << 2))
		rs->ctrl_avail = 1;

	if (rs->rq_size > max_size)
		rs->rq_size = max_size;
	else if (rs->rq_size < 2)
		rs->rq_size = 2;
}

static void ds_set_qp_size(struct rsocket *rs)
{
	uint16_t max_size;

	max_size = min(ucma_max_qpsize(NULL), RS_QP_MAX_SIZE);

	if (rs->sq_size > max_size)
		rs->sq_size = max_size;
	if (rs->rq_size > max_size)
		rs->rq_size = max_size;

	if (rs->rq_size > (rs->rbuf_size / RS_SNDLOWAT))
		rs->rq_size = rs->rbuf_size / RS_SNDLOWAT;
	else
		rs->rbuf_size = rs->rq_size * RS_SNDLOWAT;

	if (rs->sq_size > (rs->sbuf_size / RS_SNDLOWAT))
		rs->sq_size = rs->sbuf_size / RS_SNDLOWAT;
	else
		rs->sbuf_size = rs->sq_size * RS_SNDLOWAT;
}

static int rs_init_bufs(struct rsocket *rs)
{
	size_t len;

	rs->rmsg = calloc(rs->rq_size + 1, sizeof(*rs->rmsg));
	if (!rs->rmsg)
		return ERR(ENOMEM);

	rs->sbuf = calloc(rs->sbuf_size, sizeof(*rs->sbuf));
	if (!rs->sbuf)
		return ERR(ENOMEM);

	rs->smr = rdma_reg_msgs(rs->cm_id, rs->sbuf, rs->sbuf_size);
	if (!rs->smr)
		return -1;

	len = sizeof(*rs->target_sgl) * RS_SGL_SIZE +
	      sizeof(*rs->target_iomap) * rs->target_iomap_size;
	rs->target_buffer_list = malloc(len);
	if (!rs->target_buffer_list)
		return ERR(ENOMEM);

	rs->target_mr = rdma_reg_write(rs->cm_id, rs->target_buffer_list, len);
	if (!rs->target_mr)
		return -1;

	memset(rs->target_buffer_list, 0, len);
	rs->target_sgl = rs->target_buffer_list;
	if (rs->target_iomap_size)
		rs->target_iomap = (struct rs_iomap *) (rs->target_sgl + RS_SGL_SIZE);

	rs->rbuf = calloc(rs->rbuf_size, sizeof(*rs->rbuf));
	if (!rs->rbuf)
		return ERR(ENOMEM);

	rs->rmr = rdma_reg_write(rs->cm_id, rs->rbuf, rs->rbuf_size);
	if (!rs->rmr)
		return -1;

	rs->ssgl[0].addr = rs->ssgl[1].addr = (uintptr_t) rs->sbuf;
	rs->sbuf_bytes_avail = rs->sbuf_size;
	rs->ssgl[0].lkey = rs->ssgl[1].lkey = rs->smr->lkey;

	rs->rbuf_free_offset = rs->rbuf_size >> 1;
	rs->rbuf_bytes_avail = rs->rbuf_size >> 1;
	rs->sqe_avail = rs->sq_size - rs->ctrl_avail;
	rs->rseq_comp = rs->rq_size >> 1;
	return 0;
}

static int ds_init_bufs(struct ds_qp *qp)
{
	qp->rbuf = calloc(qp->rs->rbuf_size + sizeof(struct ibv_grh),
			  sizeof(*qp->rbuf));
	if (!qp->rbuf)
		return ERR(ENOMEM);

	qp->smr = rdma_reg_msgs(qp->cm_id, qp->rs->sbuf, qp->rs->sbuf_size);
	if (!qp->smr)
		return -1;

	qp->rmr = rdma_reg_msgs(qp->cm_id, qp->rbuf, qp->rs->rbuf_size +
						     sizeof(struct ibv_grh));
	if (!qp->rmr)
		return -1;

	return 0;
}

static int rs_create_cq(struct rsocket *rs, struct rdma_cm_id *cm_id)
{
	cm_id->recv_cq_channel = ibv_create_comp_channel(cm_id->verbs);
	if (!cm_id->recv_cq_channel)
		return -1;

	cm_id->recv_cq = ibv_create_cq(cm_id->verbs, rs->sq_size + rs->rq_size,
				       cm_id, cm_id->recv_cq_channel, 0);
	if (!cm_id->recv_cq)
		goto err1;

	if (rs->fd_flags & O_NONBLOCK) {
		if (fcntl(cm_id->recv_cq_channel->fd, F_SETFL, O_NONBLOCK))
			goto err2;
	} else {
		ibv_req_notify_cq(cm_id->recv_cq, 0);
	}

	cm_id->send_cq_channel = cm_id->recv_cq_channel;
	cm_id->send_cq = cm_id->recv_cq;
	return 0;

err2:
	ibv_destroy_cq(cm_id->recv_cq);
	cm_id->recv_cq = NULL;
err1:
	ibv_destroy_comp_channel(cm_id->recv_cq_channel);
	cm_id->recv_cq_channel = NULL;
	return -1;
}

static inline int rs_post_recv(struct rsocket *rs)
{
	struct ibv_recv_wr wr, *bad;

	wr.wr_id = RS_RECV_WR_ID;
	wr.next = NULL;
	wr.sg_list = NULL;
	wr.num_sge = 0;

	return rdma_seterrno(ibv_post_recv(rs->cm_id->qp, &wr, &bad));
}

static inline int ds_post_recv(struct rsocket *rs, struct ds_qp *qp, uint32_t offset)
{
	struct ibv_recv_wr wr, *bad;
	struct ibv_sge sge[2];

	sge[0].addr = (uintptr_t) qp->rbuf + rs->rbuf_size;
	sge[0].length = sizeof(struct ibv_grh);
	sge[0].lkey = qp->rmr->lkey;
	sge[1].addr = (uintptr_t) qp->rbuf + offset;
	sge[1].length = RS_SNDLOWAT;
	sge[1].lkey = qp->rmr->lkey;

	wr.wr_id = ds_recv_wr_id(offset);
	wr.next = NULL;
	wr.sg_list = sge;
	wr.num_sge = 2;

	return rdma_seterrno(ibv_post_recv(qp->cm_id->qp, &wr, &bad));
}

static int rs_create_ep(struct rsocket *rs)
{
	struct ibv_qp_init_attr qp_attr;
	int i, ret;

	rs_set_qp_size(rs);
	ret = rs_init_bufs(rs);
	if (ret)
		return ret;

	ret = rs_create_cq(rs, rs->cm_id);
	if (ret)
		return ret;

	memset(&qp_attr, 0, sizeof qp_attr);
	qp_attr.qp_context = rs;
	qp_attr.send_cq = rs->cm_id->send_cq;
	qp_attr.recv_cq = rs->cm_id->recv_cq;
	qp_attr.qp_type = IBV_QPT_RC;
	qp_attr.sq_sig_all = 1;
	qp_attr.cap.max_send_wr = rs->sq_size;
	qp_attr.cap.max_recv_wr = rs->rq_size;
	qp_attr.cap.max_send_sge = 2;
	qp_attr.cap.max_recv_sge = 1;
	qp_attr.cap.max_inline_data = rs->sq_inline;

	ret = rdma_create_qp(rs->cm_id, NULL, &qp_attr);
	if (ret)
		return ret;

	for (i = 0; i < rs->rq_size; i++) {
		ret = rs_post_recv(rs);
		if (ret)
			return ret;
	}
	return 0;
}

static void rs_release_iomap_mr(struct rs_iomap_mr *iomr)
{
	if (atomic_dec(&iomr->refcnt))
		return;

	dlist_remove(&iomr->entry);
	ibv_dereg_mr(iomr->mr);
	if (iomr->index >= 0)
		iomr->mr = NULL;
	else
		free(iomr);
}

static void rs_free_iomappings(struct rsocket *rs)
{
	struct rs_iomap_mr *iomr;

	while (!dlist_empty(&rs->iomap_list)) {
		iomr = container_of(rs->iomap_list.next,
				    struct rs_iomap_mr, entry);
		riounmap(rs->index, iomr->mr->addr, iomr->mr->length);
	}
	while (!dlist_empty(&rs->iomap_queue)) {
		iomr = container_of(rs->iomap_queue.next,
				    struct rs_iomap_mr, entry);
		riounmap(rs->index, iomr->mr->addr, iomr->mr->length);
	}
}

static void ds_free_qp(struct ds_qp *qp)
{
	if (qp->smr)
		rdma_dereg_mr(qp->smr);

	if (qp->rbuf) {
		if (qp->rmr)
			rdma_dereg_mr(qp->rmr);
		free(qp->rbuf);
	}

	if (qp->cm_id) {
		if (qp->cm_id->qp) {
			tdelete(&qp->dest.addr, &qp->rs->dest_map, ds_compare_addr);
			epoll_ctl(qp->rs->epfd, EPOLL_CTL_DEL,
				  qp->cm_id->recv_cq_channel->fd, NULL);
			rdma_destroy_qp(qp->cm_id);
		}
		rdma_destroy_id(qp->cm_id);
	}

	free(qp);
}

static void ds_free(struct rsocket *rs)
{
	struct ds_qp *qp;

	if (rs->state & (rs_readable | rs_writable))
		rs_remove_from_svc(rs);

	if (rs->udp_sock >= 0)
		close(rs->udp_sock);

	if (rs->index >= 0)
		rs_remove(rs);

	if (rs->dmsg)
		free(rs->dmsg);

	while ((qp = rs->qp_list)) {
		ds_remove_qp(rs, qp);
		ds_free_qp(qp);
	}

	if (rs->epfd >= 0)
		close(rs->epfd);

	if (rs->sbuf)
		free(rs->sbuf);

	tdestroy(rs->dest_map, free);
	fastlock_destroy(&rs->map_lock);
	fastlock_destroy(&rs->cq_wait_lock);
	fastlock_destroy(&rs->cq_lock);
	fastlock_destroy(&rs->rlock);
	fastlock_destroy(&rs->slock);
	free(rs);
}

static void rs_free(struct rsocket *rs)
{
	if (rs->type == SOCK_DGRAM) {
		ds_free(rs);
		return;
	}

	if (rs->index >= 0)
		rs_remove(rs);

	if (rs->rmsg)
		free(rs->rmsg);

	if (rs->sbuf) {
		if (rs->smr)
			rdma_dereg_mr(rs->smr);
		free(rs->sbuf);
	}

	if (rs->rbuf) {
		if (rs->rmr)
			rdma_dereg_mr(rs->rmr);
		free(rs->rbuf);
	}

	if (rs->target_buffer_list) {
		if (rs->target_mr)
			rdma_dereg_mr(rs->target_mr);
		free(rs->target_buffer_list);
	}

	if (rs->cm_id) {
		rs_free_iomappings(rs);
		if (rs->cm_id->qp)
			rdma_destroy_qp(rs->cm_id);
		rdma_destroy_id(rs->cm_id);
	}

	fastlock_destroy(&rs->map_lock);
	fastlock_destroy(&rs->cq_wait_lock);
	fastlock_destroy(&rs->cq_lock);
	fastlock_destroy(&rs->rlock);
	fastlock_destroy(&rs->slock);
	free(rs);
}

static void rs_set_conn_data(struct rsocket *rs, struct rdma_conn_param *param,
			     struct rs_conn_data *conn)
{
	conn->version = 1;
	conn->flags = RS_CONN_FLAG_IOMAP |
		      (rs_host_is_net() ? RS_CONN_FLAG_NET : 0);
	conn->credits = htons(rs->rq_size);
	memset(conn->reserved, 0, sizeof conn->reserved);
	conn->target_iomap_size = (uint8_t) rs_value_to_scale(rs->target_iomap_size, 8);

	conn->target_sgl.addr = htonll((uintptr_t) rs->target_sgl);
	conn->target_sgl.length = htonl(RS_SGL_SIZE);
	conn->target_sgl.key = htonl(rs->target_mr->rkey);

	conn->data_buf.addr = htonll((uintptr_t) rs->rbuf);
	conn->data_buf.length = htonl(rs->rbuf_size >> 1);
	conn->data_buf.key = htonl(rs->rmr->rkey);

	param->private_data = conn;
	param->private_data_len = sizeof *conn;
}

static void rs_save_conn_data(struct rsocket *rs, struct rs_conn_data *conn)
{
	rs->remote_sgl.addr = ntohll(conn->target_sgl.addr);
	rs->remote_sgl.length = ntohl(conn->target_sgl.length);
	rs->remote_sgl.key = ntohl(conn->target_sgl.key);
	rs->remote_sge = 1;
	if ((rs_host_is_net() && !(conn->flags & RS_CONN_FLAG_NET)) ||
	    (!rs_host_is_net() && (conn->flags & RS_CONN_FLAG_NET)))
		rs->opts = RS_OPT_SWAP_SGL;

	if (conn->flags & RS_CONN_FLAG_IOMAP) {
		rs->remote_iomap.addr = rs->remote_sgl.addr +
					sizeof(rs->remote_sgl) * rs->remote_sgl.length;
		rs->remote_iomap.length = rs_scale_to_value(conn->target_iomap_size, 8);
		rs->remote_iomap.key = rs->remote_sgl.key;
	}

	rs->target_sgl[0].addr = ntohll(conn->data_buf.addr);
	rs->target_sgl[0].length = ntohl(conn->data_buf.length);
	rs->target_sgl[0].key = ntohl(conn->data_buf.key);

	rs->sseq_comp = ntohs(conn->credits);
}

static int ds_init(struct rsocket *rs, int domain)
{
	rs->udp_sock = socket(domain, SOCK_DGRAM, 0);
	if (rs->udp_sock < 0)
		return rs->udp_sock;

	rs->epfd = epoll_create(2);
	if (rs->epfd < 0)
		return rs->epfd;

	return 0;
}

static int ds_init_ep(struct rsocket *rs)
{
	struct ds_smsg *msg;
	int i, ret;

	ds_set_qp_size(rs);

	rs->sbuf = calloc(rs->sq_size, RS_SNDLOWAT);
	if (!rs->sbuf)
		return ERR(ENOMEM);

	rs->dmsg = calloc(rs->rq_size + 1, sizeof(*rs->dmsg));
	if (!rs->dmsg)
		return ERR(ENOMEM);

	rs->sqe_avail = rs->sq_size;
	rs->rqe_avail = rs->rq_size;

	rs->smsg_free = (struct ds_smsg *) rs->sbuf;
	msg = rs->smsg_free;
	for (i = 0; i < rs->sq_size - 1; i++) {
		msg->next = (void *) msg + RS_SNDLOWAT;
		msg = msg->next;
	}
	msg->next = NULL;

	ret = rs_add_to_svc(rs);
	if (ret)
		return ret;

	rs->state = rs_readable | rs_writable;
	return 0;
}

int rsocket(int domain, int type, int protocol)
{
	struct rsocket *rs;
	int index, ret;

	if ((domain != PF_INET && domain != PF_INET6) ||
	    ((type != SOCK_STREAM) && (type != SOCK_DGRAM)) ||
	    (type == SOCK_STREAM && protocol && protocol != IPPROTO_TCP) ||
	    (type == SOCK_DGRAM && protocol && protocol != IPPROTO_UDP))
		return ERR(ENOTSUP);

	rs_configure();
	rs = rs_alloc(NULL, type);
	if (!rs)
		return ERR(ENOMEM);

	if (type == SOCK_STREAM) {
		ret = rdma_create_id(NULL, &rs->cm_id, rs, RDMA_PS_TCP);
		if (ret)
			goto err;

		rs->cm_id->route.addr.src_addr.sa_family = domain;
		index = rs->cm_id->channel->fd;
	} else {
		ret = ds_init(rs, domain);
		if (ret)
			goto err;

		index = rs->udp_sock;
	}

	ret = rs_insert(rs, index);
	if (ret < 0)
		goto err;

	return rs->index;

err:
	rs_free(rs);
	return ret;
}

int rbind(int socket, const struct sockaddr *addr, socklen_t addrlen)
{
	struct rsocket *rs;
	int ret;

	rs = idm_at(&idm, socket);
	if (rs->type == SOCK_STREAM) {
		ret = rdma_bind_addr(rs->cm_id, (struct sockaddr *) addr);
		if (!ret)
			rs->state = rs_bound;
	} else {
		if (rs->state == rs_init) {
			ret = ds_init_ep(rs);
			if (ret)
				return ret;
		}
		ret = bind(rs->udp_sock, addr, addrlen);
	}
	return ret;
}

int rlisten(int socket, int backlog)
{
	struct rsocket *rs;
	int ret;

	rs = idm_at(&idm, socket);
	ret = rdma_listen(rs->cm_id, backlog);
	if (!ret)
		rs->state = rs_listening;
	return ret;
}

/*
 * Nonblocking is usually not inherited between sockets, but we need to
 * inherit it here to establish the connection only.  This is needed to
 * prevent rdma_accept from blocking until the remote side finishes
 * establishing the connection.  If we were to allow rdma_accept to block,
 * then a single thread cannot establish a connection with itself, or
 * two threads which try to connect to each other can deadlock trying to
 * form a connection.
 *
 * Data transfers on the new socket remain blocking unless the user
 * specifies otherwise through rfcntl.
 */
int raccept(int socket, struct sockaddr *addr, socklen_t *addrlen)
{
	struct rsocket *rs, *new_rs;
	struct rdma_conn_param param;
	struct rs_conn_data *creq, cresp;
	int ret;

	rs = idm_at(&idm, socket);
	new_rs = rs_alloc(rs, rs->type);
	if (!new_rs)
		return ERR(ENOMEM);

	ret = rdma_get_request(rs->cm_id, &new_rs->cm_id);
	if (ret)
		goto err;

	ret = rs_insert(new_rs, new_rs->cm_id->channel->fd);
	if (ret < 0)
		goto err;

	creq = (struct rs_conn_data *) new_rs->cm_id->event->param.conn.private_data;
	if (creq->version != 1) {
		ret = ERR(ENOTSUP);
		goto err;
	}

	if (rs->fd_flags & O_NONBLOCK)
		fcntl(new_rs->cm_id->channel->fd, F_SETFL, O_NONBLOCK);

	ret = rs_create_ep(new_rs);
	if (ret)
		goto err;

	rs_save_conn_data(new_rs, creq);
	param = new_rs->cm_id->event->param.conn;
	rs_set_conn_data(new_rs, &param, &cresp);
	ret = rdma_accept(new_rs->cm_id, &param);
	if (!ret)
		new_rs->state = rs_connect_rdwr;
	else if (errno == EAGAIN || errno == EWOULDBLOCK)
		new_rs->state = rs_accepting;
	else
		goto err;

	if (addr && addrlen)
		rgetpeername(new_rs->index, addr, addrlen);
	return new_rs->index;

err:
	rs_free(new_rs);
	return ret;
}

static int rs_do_connect(struct rsocket *rs)
{
	struct rdma_conn_param param;
	struct rs_conn_data creq, *cresp;
	int to, ret;

	switch (rs->state) {
	case rs_init:
	case rs_bound:
resolve_addr:
		to = 1000 << rs->retries++;
		ret = rdma_resolve_addr(rs->cm_id, NULL,
					&rs->cm_id->route.addr.dst_addr, to);
		if (!ret)
			goto resolve_route;
		if (errno == EAGAIN || errno == EWOULDBLOCK)
			rs->state = rs_resolving_addr;
		break;
	case rs_resolving_addr:
		ret = ucma_complete(rs->cm_id);
		if (ret) {
			if (errno == ETIMEDOUT && rs->retries <= RS_CONN_RETRIES)
				goto resolve_addr;
			break;
		}

		rs->retries = 0;
resolve_route:
		to = 1000 << rs->retries++;
		ret = rdma_resolve_route(rs->cm_id, to);
		if (!ret)
			goto do_connect;
		if (errno == EAGAIN || errno == EWOULDBLOCK)
			rs->state = rs_resolving_route;
		break;
	case rs_resolving_route:
		ret = ucma_complete(rs->cm_id);
		if (ret) {
			if (errno == ETIMEDOUT && rs->retries <= RS_CONN_RETRIES)
				goto resolve_route;
			break;
		}
do_connect:
		ret = rs_create_ep(rs);
		if (ret)
			break;

		memset(&param, 0, sizeof param);
		rs_set_conn_data(rs, &param, &creq);
		param.flow_control = 1;
		param.retry_count = 7;
		param.rnr_retry_count = 7;
		rs->retries = 0;

		ret = rdma_connect(rs->cm_id, &param);
		if (!ret)
			goto connected;
		if (errno == EAGAIN || errno == EWOULDBLOCK)
			rs->state = rs_connecting;
		break;
	case rs_connecting:
		ret = ucma_complete(rs->cm_id);
		if (ret)
			break;
connected:
		cresp = (struct rs_conn_data *) rs->cm_id->event->param.conn.private_data;
		if (cresp->version != 1) {
			ret = ERR(ENOTSUP);
			break;
		}

		rs_save_conn_data(rs, cresp);
		rs->state = rs_connect_rdwr;
		break;
	case rs_accepting:
		if (!(rs->fd_flags & O_NONBLOCK))
			fcntl(rs->cm_id->channel->fd, F_SETFL, 0);

		ret = ucma_complete(rs->cm_id);
		if (ret)
			break;

		rs->state = rs_connect_rdwr;
		break;
	default:
		ret = ERR(EINVAL);
		break;
	}

	if (ret) {
		if (errno == EAGAIN || errno == EWOULDBLOCK) {
			errno = EINPROGRESS;
		} else {
			rs->state = rs_connect_error;
			rs->err = errno;
		}
	}
	return ret;
}

static int rs_any_addr(const union socket_addr *addr)
{
	if (addr->sa.sa_family == AF_INET) {
		return (addr->sin.sin_addr.s_addr == INADDR_ANY ||
			addr->sin.sin_addr.s_addr == INADDR_LOOPBACK);
	} else {
		return (!memcmp(&addr->sin6.sin6_addr, &in6addr_any, 16) ||
			!memcmp(&addr->sin6.sin6_addr, &in6addr_loopback, 16));
	}
}

static int ds_get_src_addr(struct rsocket *rs,
			   const struct sockaddr *dest_addr, socklen_t dest_len,
			   union socket_addr *src_addr, socklen_t *src_len)
{
	int sock, ret;
	uint16_t port;

//	printf("dest: "); PRINTADDR(dest_addr);
	*src_len = sizeof src_addr;
	ret = getsockname(rs->udp_sock, &src_addr->sa, src_len);
//	printf("src: "); PRINTADDR(src_addr);
	if (ret || !rs_any_addr(src_addr))
		return ret;

	port = src_addr->sin.sin_port;
	sock = socket(dest_addr->sa_family, SOCK_DGRAM, 0);
	if (sock < 0)
		return sock;

	ret = connect(sock, dest_addr, dest_len);
	if (ret)
		goto out;

	*src_len = sizeof src_addr;
	ret = getsockname(sock, &src_addr->sa, src_len);
	src_addr->sin.sin_port = port;
//	printf("selected src: ");
out:
	close(sock);
	return ret;
}

static void ds_format_hdr(struct ds_header *hdr, union socket_addr *addr)
{
	if (addr->sa.sa_family == AF_INET) {
//		PRINTADDR(addr);
		hdr->version = 4;
		hdr->length = DS_IPV4_HDR_LEN;
		hdr->port = addr->sin.sin_port;
		hdr->addr.ipv4 = addr->sin.sin_addr.s_addr;
	} else {
		hdr->version = 6;
		hdr->length = DS_IPV6_HDR_LEN;
		hdr->port = addr->sin6.sin6_port;
		hdr->addr.ipv6.flowinfo= addr->sin6.sin6_flowinfo;
		memcpy(&hdr->addr.ipv6.addr, &addr->sin6.sin6_addr, 16);
	}
}

static int ds_add_qp_dest(struct ds_qp *qp, union socket_addr *addr,
			  socklen_t addrlen)
{
	struct ibv_port_attr port_attr;
	struct ibv_ah_attr attr;
	int ret;

//	printf("%s\n", __func__);
	memcpy(&qp->dest.addr, addr, addrlen);
	qp->dest.qp = qp;
	qp->dest.qpn = qp->cm_id->qp->qp_num;

	ret = ibv_query_port(qp->cm_id->verbs, qp->cm_id->port_num, &port_attr);
	if (ret)
		return ret;

	memset(&attr, 0, sizeof attr);
	attr.dlid = port_attr.lid;
	attr.port_num = qp->cm_id->port_num;
	qp->dest.ah = ibv_create_ah(qp->cm_id->pd, &attr);
//	printf("%s ah %p lid %x port %d qpn %x\n", __func__, qp->dest.ah, attr.dlid,
//		attr.port_num, qp->dest.qpn);
	if (!qp->dest.ah)
		return ERR(ENOMEM);

	tsearch(&qp->dest.addr, &qp->rs->dest_map, ds_compare_addr);
	return 0;
}

static int ds_create_qp(struct rsocket *rs, union socket_addr *src_addr,
			socklen_t addrlen, struct ds_qp **new_qp)
{
	struct ds_qp *qp;
	struct ibv_qp_init_attr qp_attr;
	struct epoll_event event;
	int i, ret;

//	PRINTADDR(src_addr);
	qp = calloc(1, sizeof(*qp));
	if (!qp)
		return ERR(ENOMEM);

	qp->rs = rs;
	ret = rdma_create_id(NULL, &qp->cm_id, qp, RDMA_PS_UDP);
	if (ret)
		goto err;

	ds_format_hdr(&qp->hdr, src_addr);
	ret = rdma_bind_addr(qp->cm_id, &src_addr->sa);
	if (ret)
		goto err;

	ret = ds_init_bufs(qp);
	if (ret)
		goto err;

	ret = rs_create_cq(rs, qp->cm_id);
	if (ret)
		goto err;

	memset(&qp_attr, 0, sizeof qp_attr);
	qp_attr.qp_context = qp;
	qp_attr.send_cq = qp->cm_id->send_cq;
	qp_attr.recv_cq = qp->cm_id->recv_cq;
	qp_attr.qp_type = IBV_QPT_UD;
	qp_attr.sq_sig_all = 1;
	qp_attr.cap.max_send_wr = rs->sq_size;
	qp_attr.cap.max_recv_wr = rs->rq_size;
	qp_attr.cap.max_send_sge = 1;
	qp_attr.cap.max_recv_sge = 2;
	qp_attr.cap.max_inline_data = rs->sq_inline;
	ret = rdma_create_qp(qp->cm_id, NULL, &qp_attr);
	if (ret)
		goto err;

	ret = ds_add_qp_dest(qp, src_addr, addrlen);
	if (ret)
		goto err;

	event.events = EPOLLIN;
	event.data.ptr = qp;
	ret = epoll_ctl(rs->epfd,  EPOLL_CTL_ADD,
			qp->cm_id->recv_cq_channel->fd, &event);
	if (ret)
		goto err;

	for (i = 0; i < rs->rq_size; i++) {
		ret = ds_post_recv(rs, qp, i * RS_SNDLOWAT);
		if (ret)
			goto err;
	}

	ds_insert_qp(rs, qp);
	*new_qp = qp;
	return 0;
err:
	ds_free_qp(qp);
	return ret;
}

static int ds_get_qp(struct rsocket *rs, union socket_addr *src_addr,
		     socklen_t addrlen, struct ds_qp **qp)
{
	if (rs->qp_list) {
		*qp = rs->qp_list;
		do {
			if (!ds_compare_addr(rdma_get_local_addr((*qp)->cm_id),
					     src_addr))
				return 0;

			*qp = ds_next_qp(*qp);
		} while (*qp != rs->qp_list);
	}

	return ds_create_qp(rs, src_addr, addrlen, qp);
}

static int ds_get_dest(struct rsocket *rs, const struct sockaddr *addr,
		       socklen_t addrlen, struct ds_dest **dest)
{
	union socket_addr src_addr;
	socklen_t src_len;
	struct ds_qp *qp;
	struct ds_dest **tdest, *new_dest;
	int ret = 0;

//	PRINTADDR(addr);
	fastlock_acquire(&rs->map_lock);
	tdest = tfind(addr, &rs->dest_map, ds_compare_addr);
	if (tdest)
		goto found;

	ret = ds_get_src_addr(rs, addr, addrlen, &src_addr, &src_len);
//	printf("get src: "); PRINTADDR(&src_addr);
	if (ret)
		goto out;

	ret = ds_get_qp(rs, &src_addr, src_len, &qp);
	if (ret)
		goto out;

	tdest = tfind(addr, &rs->dest_map, ds_compare_addr);
	if (!tdest) {
		new_dest = calloc(1, sizeof(*new_dest));
		if (!new_dest) {
			ret = ERR(ENOMEM);
			goto out;
		}

		memcpy(&new_dest->addr, addr, addrlen);
		new_dest->qp = qp;
		tdest = tsearch(&new_dest->addr, &rs->dest_map, ds_compare_addr);
	}

found:
	*dest = *tdest;
out:
	fastlock_release(&rs->map_lock);
	return ret;
}

int rconnect(int socket, const struct sockaddr *addr, socklen_t addrlen)
{
	struct rsocket *rs;
	int ret;

	rs = idm_at(&idm, socket);
	if (rs->type == SOCK_STREAM) {
		memcpy(&rs->cm_id->route.addr.dst_addr, addr, addrlen);
		ret = rs_do_connect(rs);
	} else {
		if (rs->state == rs_init) {
			ret = ds_init_ep(rs);
			if (ret)
				return ret;
		}

		fastlock_acquire(&rs->slock);
//		PRINTADDR(addr);
		ret = connect(rs->udp_sock, addr, addrlen);
		if (!ret)
			ret = ds_get_dest(rs, addr, addrlen, &rs->conn_dest);
		fastlock_release(&rs->slock);
	}
	return ret;
}

static int rs_post_write_msg(struct rsocket *rs,
			 struct ibv_sge *sgl, int nsge,
			 uint32_t imm_data, int flags,
			 uint64_t addr, uint32_t rkey)
{
	struct ibv_send_wr wr, *bad;

	wr.wr_id = (uint64_t) imm_data;
	wr.next = NULL;
	wr.sg_list = sgl;
	wr.num_sge = nsge;
	wr.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
	wr.send_flags = flags;
	wr.imm_data = htonl(imm_data);
	wr.wr.rdma.remote_addr = addr;
	wr.wr.rdma.rkey = rkey;

	return rdma_seterrno(ibv_post_send(rs->cm_id->qp, &wr, &bad));
}

static int rs_post_write(struct rsocket *rs,
			 struct ibv_sge *sgl, int nsge,
			 uint64_t wr_id, int flags,
			 uint64_t addr, uint32_t rkey)
{
	struct ibv_send_wr wr, *bad;

	wr.wr_id = wr_id;
	wr.next = NULL;
	wr.sg_list = sgl;
	wr.num_sge = nsge;
	wr.opcode = IBV_WR_RDMA_WRITE;
	wr.send_flags = flags;
	wr.wr.rdma.remote_addr = addr;
	wr.wr.rdma.rkey = rkey;

	return rdma_seterrno(ibv_post_send(rs->cm_id->qp, &wr, &bad));
}

static int ds_post_send(struct rsocket *rs, struct ibv_sge *sge,
			uint64_t wr_id)
{
	struct ibv_send_wr wr, *bad;

	wr.wr_id = wr_id;
	wr.next = NULL;
	wr.sg_list = sge;
	wr.num_sge = 1;
	wr.opcode = IBV_WR_SEND;
	wr.send_flags = (sge->length <= rs->sq_inline) ? IBV_SEND_INLINE : 0;
	wr.wr.ud.ah = rs->conn_dest->ah;
	wr.wr.ud.remote_qpn = rs->conn_dest->qpn;
	wr.wr.ud.remote_qkey = RDMA_UDP_QKEY;
//	printf("%s ah %p qpn %x\n", __func__, rs->conn_dest->ah,
//		rs->conn_dest->qpn);

	return rdma_seterrno(ibv_post_send(rs->conn_dest->qp->cm_id->qp, &wr, &bad));
}

/*
 * Update target SGE before sending data.  Otherwise the remote side may
 * update the entry before we do.
 */
static int rs_write_data(struct rsocket *rs,
			 struct ibv_sge *sgl, int nsge,
			 uint32_t length, int flags)
{
	uint64_t addr;
	uint32_t rkey;

	rs->sseq_no++;
	rs->sqe_avail--;
	rs->sbuf_bytes_avail -= length;

	addr = rs->target_sgl[rs->target_sge].addr;
	rkey = rs->target_sgl[rs->target_sge].key;

	rs->target_sgl[rs->target_sge].addr += length;
	rs->target_sgl[rs->target_sge].length -= length;

	if (!rs->target_sgl[rs->target_sge].length) {
		if (++rs->target_sge == RS_SGL_SIZE)
			rs->target_sge = 0;
	}

	return rs_post_write_msg(rs, sgl, nsge, rs_msg_set(RS_OP_DATA, length),
				 flags, addr, rkey);
}

static int rs_write_direct(struct rsocket *rs, struct rs_iomap *iom, uint64_t offset,
			   struct ibv_sge *sgl, int nsge, uint32_t length, int flags)
{
	uint64_t addr;

	rs->sqe_avail--;
	rs->sbuf_bytes_avail -= length;

	addr = iom->sge.addr + offset - iom->offset;
	return rs_post_write(rs, sgl, nsge, rs_msg_set(RS_OP_WRITE, length),
			     flags, addr, iom->sge.key);
}

static int rs_write_iomap(struct rsocket *rs, struct rs_iomap_mr *iomr,
			  struct ibv_sge *sgl, int nsge, int flags)
{
	uint64_t addr;

	rs->sseq_no++;
	rs->sqe_avail--;
	rs->sbuf_bytes_avail -= sizeof(struct rs_iomap);

	addr = rs->remote_iomap.addr + iomr->index * sizeof(struct rs_iomap);
	return rs_post_write_msg(rs, sgl, nsge, rs_msg_set(RS_OP_IOMAP_SGL, iomr->index),
			         flags, addr, rs->remote_iomap.key);
}

static uint32_t rs_sbuf_left(struct rsocket *rs)
{
	return (uint32_t) (((uint64_t) (uintptr_t) &rs->sbuf[rs->sbuf_size]) -
			   rs->ssgl[0].addr);
}

static void rs_send_credits(struct rsocket *rs)
{
	struct ibv_sge ibsge;
	struct rs_sge sge;

	rs->ctrl_avail--;
	rs->rseq_comp = rs->rseq_no + (rs->rq_size >> 1);
	if (rs->rbuf_bytes_avail >= (rs->rbuf_size >> 1)) {
		if (!(rs->opts & RS_OPT_SWAP_SGL)) {
			sge.addr = (uintptr_t) &rs->rbuf[rs->rbuf_free_offset];
			sge.key = rs->rmr->rkey;
			sge.length = rs->rbuf_size >> 1;
		} else {
			sge.addr = bswap_64((uintptr_t) &rs->rbuf[rs->rbuf_free_offset]);
			sge.key = bswap_32(rs->rmr->rkey);
			sge.length = bswap_32(rs->rbuf_size >> 1);
		}

		ibsge.addr = (uintptr_t) &sge;
		ibsge.lkey = 0;
		ibsge.length = sizeof(sge);

		rs_post_write_msg(rs, &ibsge, 1,
				  rs_msg_set(RS_OP_SGL, rs->rseq_no + rs->rq_size),
				  IBV_SEND_INLINE,
				  rs->remote_sgl.addr +
				  rs->remote_sge * sizeof(struct rs_sge),
				  rs->remote_sgl.key);

		rs->rbuf_bytes_avail -= rs->rbuf_size >> 1;
		rs->rbuf_free_offset += rs->rbuf_size >> 1;
		if (rs->rbuf_free_offset >= rs->rbuf_size)
			rs->rbuf_free_offset = 0;
		if (++rs->remote_sge == rs->remote_sgl.length)
			rs->remote_sge = 0;
	} else {
		rs_post_write_msg(rs, NULL, 0,
				  rs_msg_set(RS_OP_SGL, rs->rseq_no + rs->rq_size),
				  0, 0, 0);
	}
}

static int rs_give_credits(struct rsocket *rs)
{
	return ((rs->rbuf_bytes_avail >= (rs->rbuf_size >> 1)) ||
	        ((short) ((short) rs->rseq_no - (short) rs->rseq_comp) >= 0)) &&
	       rs->ctrl_avail && (rs->state & rs_connected);
}

static void rs_update_credits(struct rsocket *rs)
{
	if (rs_give_credits(rs))
		rs_send_credits(rs);
}

static int rs_poll_cq(struct rsocket *rs)
{
	struct ibv_wc wc;
	uint32_t imm_data;
	int ret, rcnt = 0;

	while ((ret = ibv_poll_cq(rs->cm_id->recv_cq, 1, &wc)) > 0) {
		if (wc.wr_id == RS_RECV_WR_ID) {
			if (wc.status != IBV_WC_SUCCESS)
				continue;
			rcnt++;

			imm_data = ntohl(wc.imm_data);
			switch (rs_msg_op(imm_data)) {
			case RS_OP_SGL:
				rs->sseq_comp = (uint16_t) rs_msg_data(imm_data);
				break;
			case RS_OP_IOMAP_SGL:
				/* The iomap was updated, that's nice to know. */
				break;
			case RS_OP_CTRL:
				if (rs_msg_data(imm_data) == RS_CTRL_DISCONNECT) {
					rs->state = rs_disconnected;
					return 0;
				} else if (rs_msg_data(imm_data) == RS_CTRL_SHUTDOWN) {
					rs->state &= ~rs_readable;
				}
				break;
			case RS_OP_WRITE:
				/* We really shouldn't be here. */
				break;
			default:
				rs->rmsg[rs->rmsg_tail].op = rs_msg_op(imm_data);
				rs->rmsg[rs->rmsg_tail].data = rs_msg_data(imm_data);
				if (++rs->rmsg_tail == rs->rq_size + 1)
					rs->rmsg_tail = 0;
				break;
			}
		} else {
			switch  (rs_msg_op((uint32_t) wc.wr_id)) {
			case RS_OP_SGL:
				rs->ctrl_avail++;
				break;
			case RS_OP_CTRL:
				rs->ctrl_avail++;
				if (rs_msg_data((uint32_t) wc.wr_id) == RS_CTRL_DISCONNECT)
					rs->state = rs_disconnected;
				break;
			case RS_OP_IOMAP_SGL:
				rs->sqe_avail++;
				rs->sbuf_bytes_avail += sizeof(struct rs_iomap);
				break;
			default:
				rs->sqe_avail++;
				rs->sbuf_bytes_avail += rs_msg_data((uint32_t) wc.wr_id);
				break;
			}
			if (wc.status != IBV_WC_SUCCESS && (rs->state & rs_connected)) {
				rs->state = rs_error;
				rs->err = EIO;
			}
		}
	}

	if (rs->state & rs_connected) {
		while (!ret && rcnt--)
			ret = rs_post_recv(rs);

		if (ret) {
			rs->state = rs_error;
			rs->err = errno;
		}
	}
	return ret;
}

static int rs_get_cq_event(struct rsocket *rs)
{
	struct ibv_cq *cq;
	void *context;
	int ret;

	if (!rs->cq_armed)
		return 0;

	ret = ibv_get_cq_event(rs->cm_id->recv_cq_channel, &cq, &context);
	if (!ret) {
		ibv_ack_cq_events(rs->cm_id->recv_cq, 1);
		rs->cq_armed = 0;
	} else if (errno != EAGAIN) {
		rs->state = rs_error;
	}

	return ret;
}

/*
 * Although we serialize rsend and rrecv calls with respect to themselves,
 * both calls may run simultaneously and need to poll the CQ for completions.
 * We need to serialize access to the CQ, but rsend and rrecv need to
 * allow each other to make forward progress.
 *
 * For example, rsend may need to wait for credits from the remote side,
 * which could be stalled until the remote process calls rrecv.  This should
 * not block rrecv from receiving data from the remote side however.
 *
 * We handle this by using two locks.  The cq_lock protects against polling
 * the CQ and processing completions.  The cq_wait_lock serializes access to
 * waiting on the CQ.
 */
static int rs_process_cq(struct rsocket *rs, int nonblock, int (*test)(struct rsocket *rs))
{
	int ret;

	fastlock_acquire(&rs->cq_lock);
	do {
		rs_update_credits(rs);
		ret = rs_poll_cq(rs);
		if (test(rs)) {
			ret = 0;
			break;
		} else if (ret) {
			break;
		} else if (nonblock) {
			ret = ERR(EWOULDBLOCK);
		} else if (!rs->cq_armed) {
			ibv_req_notify_cq(rs->cm_id->recv_cq, 0);
			rs->cq_armed = 1;
		} else {
			rs_update_credits(rs);
			fastlock_acquire(&rs->cq_wait_lock);
			fastlock_release(&rs->cq_lock);

			ret = rs_get_cq_event(rs);
			fastlock_release(&rs->cq_wait_lock);
			fastlock_acquire(&rs->cq_lock);
		}
	} while (!ret);

	rs_update_credits(rs);
	fastlock_release(&rs->cq_lock);
	return ret;
}

static int rs_get_comp(struct rsocket *rs, int nonblock, int (*test)(struct rsocket *rs))
{
	struct timeval s, e;
	uint32_t poll_time = 0;
	int ret;

	do {
		ret = rs_process_cq(rs, 1, test);
		if (!ret || nonblock || errno != EWOULDBLOCK)
			return ret;

		if (!poll_time)
			gettimeofday(&s, NULL);

		gettimeofday(&e, NULL);
		poll_time = (e.tv_sec - s.tv_sec) * 1000000 +
			    (e.tv_usec - s.tv_usec) + 1;
	} while (poll_time <= polling_time);

	ret = rs_process_cq(rs, 0, test);
	return ret;
}

static int ds_valid_recv(struct ds_qp *qp, struct ibv_wc *wc)
{
	struct ds_header *hdr;

	hdr = (struct ds_header *) (qp->rbuf + ds_wr_offset(wc->wr_id));
	return ((wc->byte_len >= sizeof(struct ibv_grh) + DS_IPV4_HDR_LEN) &&
		((hdr->version == 4 && hdr->length == DS_IPV4_HDR_LEN) ||
		 (hdr->version == 6 && hdr->length == DS_IPV6_HDR_LEN)));
}

/*
 * Poll all CQs associated with a datagram rsocket.  We need to drop any
 * received messages that we do not have room to store.  To limit drops,
 * we only poll if we have room to store the receive or we need a send
 * buffer.  To ensure fairness, we poll the CQs round robin, remembering
 * where we left off.
 */
static void ds_poll_cqs(struct rsocket *rs)
{
	struct ds_qp *qp;
	struct ds_smsg *smsg;
	struct ds_rmsg *rmsg;
	struct ibv_wc wc;
	int ret, cnt;

	if (!(qp = rs->qp_list))
		return;

	do {
		cnt = 0;
		do {
			ret = ibv_poll_cq(qp->cm_id->recv_cq, 1, &wc);
			if (ret <= 0) {
				qp = ds_next_qp(qp);
				continue;
			}

			if (ds_wr_is_recv(wc.wr_id)) {
				if (rs->rqe_avail && wc.status == IBV_WC_SUCCESS &&
				    ds_valid_recv(qp, &wc)) {
//					printf("%s recv over QP\n", __func__);
					rs->rqe_avail--;
					rmsg = &rs->dmsg[rs->rmsg_tail];
					rmsg->qp = qp;
					rmsg->offset = ds_wr_offset(wc.wr_id);
					rmsg->length = wc.byte_len - sizeof(struct ibv_grh);
					if (++rs->rmsg_tail == rs->rq_size + 1)
						rs->rmsg_tail = 0;
				} else {
//					printf("%s invalid recv\n", __func__);
					ds_post_recv(rs, qp, ds_wr_offset(wc.wr_id));
				}
			} else {
				smsg = (struct ds_smsg *)
				       (rs->sbuf + ds_wr_offset(wc.wr_id));
//				printf("%s send smsg %p free %p\n", __func__, smsg, rs->smsg_free);
				smsg->next = rs->smsg_free;
				rs->smsg_free = smsg;
				rs->sqe_avail++;
			}

			qp = ds_next_qp(qp);
			if (!rs->rqe_avail && rs->sqe_avail) {
				rs->qp_list = qp;
				return;
			}
			cnt++;
		} while (qp != rs->qp_list);
	} while (cnt);
}

static void ds_req_notify_cqs(struct rsocket *rs)
{
	struct ds_qp *qp;

	if (!(qp = rs->qp_list))
		return;

	do {
		if (!qp->cq_armed) {
			ibv_req_notify_cq(qp->cm_id->recv_cq, 0);
			qp->cq_armed = 1;
		}
		qp = ds_next_qp(qp);
	} while (qp != rs->qp_list);
}

static int ds_get_cq_event(struct rsocket *rs)
{
	struct epoll_event event;
	struct ds_qp *qp;
	struct ibv_cq *cq;
	void *context;
	int ret;

	if (!rs->cq_armed)
		return 0;

//	printf("wait for epoll event\n");
	ret = epoll_wait(rs->epfd, &event, 1, -1);
//	printf("%s epoll wait ret %d errno %s\n", __func__, ret, strerror(errno));
	if (ret <= 0)
		return ret;

	qp = event.data.ptr;
	ret = ibv_get_cq_event(qp->cm_id->recv_cq_channel, &cq, &context);
	if (!ret) {
		ibv_ack_cq_events(qp->cm_id->recv_cq, 1);
		qp->cq_armed = 0;
		rs->cq_armed = 0;
	}

	return ret;
}

static int ds_process_cqs(struct rsocket *rs, int nonblock, int (*test)(struct rsocket *rs))
{
	int ret = 0;

	fastlock_acquire(&rs->cq_lock);
	do {
		ds_poll_cqs(rs);
		if (test(rs)) {
//			printf("%s test succeeded\n", __func__);
			ret = 0;
			break;
		} else if (nonblock) {
			ret = ERR(EWOULDBLOCK);
//			printf("%s nonblocking \n", __func__);
		} else if (!rs->cq_armed) {
//			printf("%s req notify \n", __func__);
			ds_req_notify_cqs(rs);
			rs->cq_armed = 1;
		} else {
			fastlock_acquire(&rs->cq_wait_lock);
			fastlock_release(&rs->cq_lock);

			ret = ds_get_cq_event(rs);
//			printf("%s get event ret %d %s\n", __func__, ret, strerror(errno));
			fastlock_release(&rs->cq_wait_lock);
			fastlock_acquire(&rs->cq_lock);
		}
	} while (!ret);

	fastlock_release(&rs->cq_lock);
//	printf("%s ret %d errno %s\n", __func__, ret, strerror(errno));
	return ret;
}

static int ds_get_comp(struct rsocket *rs, int nonblock, int (*test)(struct rsocket *rs))
{
	struct timeval s, e;
	uint32_t poll_time = 0;
	int ret;

	do {
		ret = ds_process_cqs(rs, 1, test);
//		printf("%s ret %d errno %s\n", __func__, ret, strerror(errno));
		if (!ret || nonblock || errno != EWOULDBLOCK)
			return ret;

		if (!poll_time)
			gettimeofday(&s, NULL);

		gettimeofday(&e, NULL);
		poll_time = (e.tv_sec - s.tv_sec) * 1000000 +
			    (e.tv_usec - s.tv_usec) + 1;
	} while (poll_time <= polling_time);

	ret = ds_process_cqs(rs, 0, test);
	return ret;
}

static int rs_nonblocking(struct rsocket *rs, int flags)
{
	return (rs->fd_flags & O_NONBLOCK) || (flags & MSG_DONTWAIT);
}

static int rs_is_cq_armed(struct rsocket *rs)
{
	return rs->cq_armed;
}

static int rs_poll_all(struct rsocket *rs)
{
	return 1;
}

/*
 * We use hardware flow control to prevent over running the remote
 * receive queue.  However, data transfers still require space in
 * the remote rmsg queue, or we risk losing notification that data
 * has been transfered.
 *
 * Be careful with race conditions in the check below.  The target SGL
 * may be updated by a remote RDMA write.
 */
static int rs_can_send(struct rsocket *rs)
{
	return rs->sqe_avail && (rs->sbuf_bytes_avail >= RS_SNDLOWAT) &&
	       (rs->sseq_no != rs->sseq_comp) &&
	       (rs->target_sgl[rs->target_sge].length != 0);
}

static int ds_can_send(struct rsocket *rs)
{
	return rs->sqe_avail;
}

static int ds_all_sends_done(struct rsocket *rs)
{
	return rs->sqe_avail == rs->sq_size;
}

static int rs_conn_can_send(struct rsocket *rs)
{
	return rs_can_send(rs) || !(rs->state & rs_writable);
}

static int rs_conn_can_send_ctrl(struct rsocket *rs)
{
	return rs->ctrl_avail || !(rs->state & rs_connected);
}

static int rs_have_rdata(struct rsocket *rs)
{
	return (rs->rmsg_head != rs->rmsg_tail);
}

static int rs_conn_have_rdata(struct rsocket *rs)
{
	return rs_have_rdata(rs) || !(rs->state & rs_readable);
}

static int rs_conn_all_sends_done(struct rsocket *rs)
{
	return ((rs->sqe_avail + rs->ctrl_avail) == rs->sq_size) ||
	       !(rs->state & rs_connected);
}

static void ds_set_src(struct sockaddr *addr, socklen_t *addrlen,
		       struct ds_header *hdr)
{
	union socket_addr sa;

	memset(&sa, 0, sizeof sa);
	if (hdr->version == 4) {
		if (*addrlen > sizeof(sa.sin))
			*addrlen = sizeof(sa.sin);

		sa.sin.sin_family = AF_INET;
		sa.sin.sin_port = hdr->port;
		sa.sin.sin_addr.s_addr =  hdr->addr.ipv4;
	} else {
		if (*addrlen > sizeof(sa.sin6))
			*addrlen = sizeof(sa.sin6);

		sa.sin6.sin6_family = AF_INET6;
		sa.sin6.sin6_port = hdr->port;
		sa.sin6.sin6_flowinfo = hdr->addr.ipv6.flowinfo;
		memcpy(&sa.sin6.sin6_addr, &hdr->addr.ipv6.addr, 16);
	}
	memcpy(addr, &sa, *addrlen);
}

static ssize_t ds_recvfrom(struct rsocket *rs, void *buf, size_t len, int flags,
			   struct sockaddr *src_addr, socklen_t *addrlen)
{
	struct ds_rmsg *rmsg;
	struct ds_header *hdr;
	int ret;

//	printf("%s \n", __func__);
	if (!(rs->state & rs_readable))
		return ERR(EINVAL);

	if (!rs_have_rdata(rs)) {
//		printf("%s need rdata \n", __func__);
		ret = ds_get_comp(rs, rs_nonblocking(rs, flags),
				  rs_have_rdata);
//		printf("%s ret %d errno %s\n", __func__, ret, strerror(errno));
		if (ret)
			return ret;
	}

	rmsg = &rs->dmsg[rs->rmsg_head];
	hdr = (struct ds_header *) (rmsg->qp->rbuf + rmsg->offset);
	if (len > rmsg->length - hdr->length)
		len = rmsg->length - hdr->length;

	memcpy(buf, (void *) hdr + hdr->length, len);
	if (addrlen)
{
		ds_set_src(src_addr, addrlen, hdr);
//PRINTADDR(src_addr);
}

	if (!(flags & MSG_PEEK)) {
		ds_post_recv(rs, rmsg->qp, rmsg->offset);
		if (++rs->rmsg_head == rs->rq_size + 1)
			rs->rmsg_head = 0;
	}

//	printf("%s ret %d errno %s\n", __func__, ret, strerror(errno));
	return len;
}

static ssize_t rs_peek(struct rsocket *rs, void *buf, size_t len)
{
	size_t left = len;
	uint32_t end_size, rsize;
	int rmsg_head, rbuf_offset;

	rmsg_head = rs->rmsg_head;
	rbuf_offset = rs->rbuf_offset;

	for (; left && (rmsg_head != rs->rmsg_tail); left -= rsize) {
		if (left < rs->rmsg[rmsg_head].data) {
			rsize = left;
		} else {
			rsize = rs->rmsg[rmsg_head].data;
			if (++rmsg_head == rs->rq_size + 1)
				rmsg_head = 0;
		}

		end_size = rs->rbuf_size - rbuf_offset;
		if (rsize > end_size) {
			memcpy(buf, &rs->rbuf[rbuf_offset], end_size);
			rbuf_offset = 0;
			buf += end_size;
			rsize -= end_size;
			left -= end_size;
		}
		memcpy(buf, &rs->rbuf[rbuf_offset], rsize);
		rbuf_offset += rsize;
		buf += rsize;
	}

	return len - left;
}

/*
 * Continue to receive any queued data even if the remote side has disconnected.
 */
ssize_t rrecv(int socket, void *buf, size_t len, int flags)
{
	struct rsocket *rs;
	size_t left = len;
	uint32_t end_size, rsize;
	int ret;

	rs = idm_at(&idm, socket);
	if (rs->type == SOCK_DGRAM) {
		fastlock_acquire(&rs->rlock);
		ret = ds_recvfrom(rs, buf, len, flags, NULL, 0);
		fastlock_release(&rs->rlock);
		return ret;
	}

	if (rs->state & rs_opening) {
		ret = rs_do_connect(rs);
		if (ret) {
			if (errno == EINPROGRESS)
				errno = EAGAIN;
			return ret;
		}
	}
	fastlock_acquire(&rs->rlock);
	do {
		if (!rs_have_rdata(rs)) {
			ret = rs_get_comp(rs, rs_nonblocking(rs, flags),
					  rs_conn_have_rdata);
			if (ret)
				break;
		}

		ret = 0;
		if (flags & MSG_PEEK) {
			left = len - rs_peek(rs, buf, left);
			break;
		}

		for (; left && rs_have_rdata(rs); left -= rsize) {
			if (left < rs->rmsg[rs->rmsg_head].data) {
				rsize = left;
				rs->rmsg[rs->rmsg_head].data -= left;
			} else {
				rs->rseq_no++;
				rsize = rs->rmsg[rs->rmsg_head].data;
				if (++rs->rmsg_head == rs->rq_size + 1)
					rs->rmsg_head = 0;
			}

			end_size = rs->rbuf_size - rs->rbuf_offset;
			if (rsize > end_size) {
				memcpy(buf, &rs->rbuf[rs->rbuf_offset], end_size);
				rs->rbuf_offset = 0;
				buf += end_size;
				rsize -= end_size;
				left -= end_size;
				rs->rbuf_bytes_avail += end_size;
			}
			memcpy(buf, &rs->rbuf[rs->rbuf_offset], rsize);
			rs->rbuf_offset += rsize;
			buf += rsize;
			rs->rbuf_bytes_avail += rsize;
		}

	} while (left && (flags & MSG_WAITALL) && (rs->state & rs_readable));

	fastlock_release(&rs->rlock);
	return ret ? ret : len - left;
}

ssize_t rrecvfrom(int socket, void *buf, size_t len, int flags,
		  struct sockaddr *src_addr, socklen_t *addrlen)
{
	struct rsocket *rs;
	int ret;

	rs = idm_at(&idm, socket);
	if (rs->type == SOCK_DGRAM) {
		fastlock_acquire(&rs->rlock);
		ret = ds_recvfrom(rs, buf, len, flags, src_addr, addrlen);
		fastlock_release(&rs->rlock);
		return ret;
	}

	ret = rrecv(socket, buf, len, flags);
	if (ret > 0 && src_addr)
		rgetpeername(socket, src_addr, addrlen);

	return ret;
}

/*
 * Simple, straightforward implementation for now that only tries to fill
 * in the first vector.
 */
static ssize_t rrecvv(int socket, const struct iovec *iov, int iovcnt, int flags)
{
	return rrecv(socket, iov[0].iov_base, iov[0].iov_len, flags);
}

ssize_t rrecvmsg(int socket, struct msghdr *msg, int flags)
{
	if (msg->msg_control && msg->msg_controllen)
		return ERR(ENOTSUP);

	return rrecvv(socket, msg->msg_iov, (int) msg->msg_iovlen, msg->msg_flags);
}

ssize_t rread(int socket, void *buf, size_t count)
{
	return rrecv(socket, buf, count, 0);
}

ssize_t rreadv(int socket, const struct iovec *iov, int iovcnt)
{
	return rrecvv(socket, iov, iovcnt, 0);
}

static int rs_send_iomaps(struct rsocket *rs, int flags)
{
	struct rs_iomap_mr *iomr;
	struct ibv_sge sge;
	struct rs_iomap iom;
	int ret;

	fastlock_acquire(&rs->map_lock);
	while (!dlist_empty(&rs->iomap_queue)) {
		if (!rs_can_send(rs)) {
			ret = rs_get_comp(rs, rs_nonblocking(rs, flags),
					  rs_conn_can_send);
			if (ret)
				break;
			if (!(rs->state & rs_writable)) {
				ret = ERR(ECONNRESET);
				break;
			}
		}

		iomr = container_of(rs->iomap_queue.next, struct rs_iomap_mr, entry);
		if (!(rs->opts & RS_OPT_SWAP_SGL)) {
			iom.offset = iomr->offset;
			iom.sge.addr = (uintptr_t) iomr->mr->addr;
			iom.sge.length = iomr->mr->length;
			iom.sge.key = iomr->mr->rkey;
		} else {
			iom.offset = bswap_64(iomr->offset);
			iom.sge.addr = bswap_64((uintptr_t) iomr->mr->addr);
			iom.sge.length = bswap_32(iomr->mr->length);
			iom.sge.key = bswap_32(iomr->mr->rkey);
		}

		if (rs->sq_inline >= sizeof iom) {
			sge.addr = (uintptr_t) &iom;
			sge.length = sizeof iom;
			sge.lkey = 0;
			ret = rs_write_iomap(rs, iomr, &sge, 1, IBV_SEND_INLINE);
		} else if (rs_sbuf_left(rs) >= sizeof iom) {
			memcpy((void *) (uintptr_t) rs->ssgl[0].addr, &iom, sizeof iom);
			rs->ssgl[0].length = sizeof iom;
			ret = rs_write_iomap(rs, iomr, rs->ssgl, 1, 0);
			if (rs_sbuf_left(rs) > sizeof iom)
				rs->ssgl[0].addr += sizeof iom;
			else
				rs->ssgl[0].addr = (uintptr_t) rs->sbuf;
		} else {
			rs->ssgl[0].length = rs_sbuf_left(rs);
			memcpy((void *) (uintptr_t) rs->ssgl[0].addr, &iom,
				rs->ssgl[0].length);
			rs->ssgl[1].length = sizeof iom - rs->ssgl[0].length;
			memcpy(rs->sbuf, ((void *) &iom) + rs->ssgl[0].length,
			       rs->ssgl[1].length);
			ret = rs_write_iomap(rs, iomr, rs->ssgl, 2, 0);
			rs->ssgl[0].addr = (uintptr_t) rs->sbuf + rs->ssgl[1].length;
		}
		dlist_remove(&iomr->entry);
		dlist_insert_tail(&iomr->entry, &rs->iomap_list);
		if (ret)
			break;
	}

	rs->iomap_pending = !dlist_empty(&rs->iomap_queue);
	fastlock_release(&rs->map_lock);
	return ret;
}

static ssize_t ds_sendv_udp(struct rsocket *rs, const struct iovec *iov,
			    int iovcnt, int flags, uint8_t op)
{
	struct ds_udp_header hdr;
	struct msghdr msg;
	struct iovec miov[8];
	ssize_t ret;

//	printf("%s\n", __func__);
	if (iovcnt > 8)
		return ERR(ENOTSUP);

	hdr.tag = htonl(DS_UDP_TAG);
	hdr.version = rs->conn_dest->qp->hdr.version;
	hdr.op = op;
	hdr.reserved = 0;
	hdr.qpn = htonl(rs->conn_dest->qp->cm_id->qp->qp_num & 0xFFFFFF);
	if (rs->conn_dest->qp->hdr.version == 4) {
		hdr.length = DS_UDP_IPV4_HDR_LEN;
		hdr.addr.ipv4 = rs->conn_dest->qp->hdr.addr.ipv4;
	} else {
		hdr.length = DS_UDP_IPV6_HDR_LEN;
		memcpy(hdr.addr.ipv6, &rs->conn_dest->qp->hdr.addr.ipv6, 16);
	}

	miov[0].iov_base = &hdr;
	miov[0].iov_len = hdr.length;
	if (iov && iovcnt)
		memcpy(&miov[1], iov, sizeof *iov * iovcnt);

	memset(&msg, 0, sizeof msg);
	msg.msg_name = &rs->conn_dest->addr;
	msg.msg_namelen = ucma_addrlen(&rs->conn_dest->addr.sa);
	msg.msg_iov = miov;
	msg.msg_iovlen = iovcnt + 1;
//	printf("%s iov cnt %d\n", __func__, msg.msg_iovlen);
	ret = sendmsg(rs->udp_sock, &msg, flags);
	return ret > 0 ? ret - hdr.length : ret;
}

static ssize_t ds_send_udp(struct rsocket *rs, const void *buf, size_t len,
			   int flags, uint8_t op)
{
	struct iovec iov;
//	printf("%s\n", __func__);
	if (buf && len) {
//		printf("%s have buffer\n", __func__);
		iov.iov_base = (void *) buf;
		iov.iov_len = len;
		return ds_sendv_udp(rs, &iov, 1, flags, op);
	} else {
//		printf("%s no buffer\n", __func__);
		return ds_sendv_udp(rs, NULL, 0, flags, op);
	}
}

static ssize_t dsend(struct rsocket *rs, const void *buf, size_t len, int flags)
{
	struct ds_smsg *msg;
	struct ibv_sge sge;
	uint64_t offset;
	int ret = 0;

//	printf("%s\n", __func__);
	if (!rs->conn_dest->ah)
		return ds_send_udp(rs, buf, len, flags, RS_OP_DATA);

	if (!ds_can_send(rs)) {
		ret = ds_get_comp(rs, rs_nonblocking(rs, flags), ds_can_send);
		if (ret)
			return ret;
	}

	msg = rs->smsg_free;
	rs->smsg_free = msg->next;
	rs->sqe_avail--;

	memcpy((void *) msg, &rs->conn_dest->qp->hdr, rs->conn_dest->qp->hdr.length);
	memcpy((void *) msg + rs->conn_dest->qp->hdr.length, buf, len);
	sge.addr = (uintptr_t) msg;
	sge.length = rs->conn_dest->qp->hdr.length + len;
	sge.lkey = rs->conn_dest->qp->smr->lkey;
	offset = (uint8_t *) msg - rs->sbuf;

//	printf("%s - sending over QP\n", __func__);
	ret = ds_post_send(rs, &sge, ds_send_wr_id(offset, sge.length));
//	printf("%s - ds_post_send %d %s\n", __func__, ret, strerror(errno));
	return ret ? ret : len;
}

/*
 * We overlap sending the data, by posting a small work request immediately,
 * then increasing the size of the send on each iteration.
 */
ssize_t rsend(int socket, const void *buf, size_t len, int flags)
{
	struct rsocket *rs;
	struct ibv_sge sge;
	size_t left = len;
	uint32_t xfer_size, olen = RS_OLAP_START_SIZE;
	int ret = 0;

	rs = idm_at(&idm, socket);
	if (rs->type == SOCK_DGRAM) {
		fastlock_acquire(&rs->slock);
		ret = dsend(rs, buf, len, flags);
		fastlock_release(&rs->slock);
		return ret;
	}

	if (rs->state & rs_opening) {
		ret = rs_do_connect(rs);
		if (ret) {
			if (errno == EINPROGRESS)
				errno = EAGAIN;
			return ret;
		}
	}

	fastlock_acquire(&rs->slock);
	if (rs->iomap_pending) {
		ret = rs_send_iomaps(rs, flags);
		if (ret)
			goto out;
	}
	for (; left; left -= xfer_size, buf += xfer_size) {
		if (!rs_can_send(rs)) {
			ret = rs_get_comp(rs, rs_nonblocking(rs, flags),
					  rs_conn_can_send);
			if (ret)
				break;
			if (!(rs->state & rs_writable)) {
				ret = ERR(ECONNRESET);
				break;
			}
		}

		if (olen < left) {
			xfer_size = olen;
			if (olen < RS_MAX_TRANSFER)
				olen <<= 1;
		} else {
			xfer_size = left;
		}

		if (xfer_size > rs->sbuf_bytes_avail)
			xfer_size = rs->sbuf_bytes_avail;
		if (xfer_size > rs->target_sgl[rs->target_sge].length)
			xfer_size = rs->target_sgl[rs->target_sge].length;

		if (xfer_size <= rs->sq_inline) {
			sge.addr = (uintptr_t) buf;
			sge.length = xfer_size;
			sge.lkey = 0;
			ret = rs_write_data(rs, &sge, 1, xfer_size, IBV_SEND_INLINE);
		} else if (xfer_size <= rs_sbuf_left(rs)) {
			memcpy((void *) (uintptr_t) rs->ssgl[0].addr, buf, xfer_size);
			rs->ssgl[0].length = xfer_size;
			ret = rs_write_data(rs, rs->ssgl, 1, xfer_size, 0);
			if (xfer_size < rs_sbuf_left(rs))
				rs->ssgl[0].addr += xfer_size;
			else
				rs->ssgl[0].addr = (uintptr_t) rs->sbuf;
		} else {
			rs->ssgl[0].length = rs_sbuf_left(rs);
			memcpy((void *) (uintptr_t) rs->ssgl[0].addr, buf,
				rs->ssgl[0].length);
			rs->ssgl[1].length = xfer_size - rs->ssgl[0].length;
			memcpy(rs->sbuf, buf + rs->ssgl[0].length, rs->ssgl[1].length);
			ret = rs_write_data(rs, rs->ssgl, 2, xfer_size, 0);
			rs->ssgl[0].addr = (uintptr_t) rs->sbuf + rs->ssgl[1].length;
		}
		if (ret)
			break;
	}
out:
	fastlock_release(&rs->slock);

	return (ret && left == len) ? ret : len - left;
}

ssize_t rsendto(int socket, const void *buf, size_t len, int flags,
		const struct sockaddr *dest_addr, socklen_t addrlen)
{
	struct rsocket *rs;
	int ret;

//	PRINTADDR(dest_addr);
//	printf("%s sendto data 0x%x\n", __func__, *((uint32_t*)buf));
	rs = idm_at(&idm, socket);
	if (rs->type == SOCK_STREAM) {
		if (dest_addr || addrlen)
			return ERR(EISCONN);

		return rsend(socket, buf, len, flags);
	}

	if (rs->state == rs_init) {
		ret = ds_init_ep(rs);
		if (ret)
			return ret;
	}

	fastlock_acquire(&rs->slock);
//	printf("%s - checking conn dest %p\n", __func__, rs->conn_dest);
//	printf("dest addr: af %d", dest_addr->sa_family); PRINTADDR(dest_addr);
//	if (rs->conn_dest) {
//		int i;
//		printf("conn addr: af %d", rs->conn_dest->addr.sa.sa_family); PRINTADDR(&rs->conn_dest->addr);
//		for (i=0;i<16;i++)
//			printf("%x", ((uint8_t *)dest_addr)[i]);
//		printf("\n");
//		for (i=0;i<16;i++)
//			printf("%x", ((uint8_t *)&rs->conn_dest->addr)[i]);
//		printf("\n");
//	}
	if (!rs->conn_dest || ds_compare_addr(dest_addr, &rs->conn_dest->addr)) {
//		printf("%s - getting conn dest\n", __func__);
		ret = ds_get_dest(rs, dest_addr, addrlen, &rs->conn_dest);
//		printf("%s - get conn dest %d %s\n", __func__, ret, strerror(errno));
		if (ret)
			goto out;
	}

	ret = dsend(rs, buf, len, flags);
//	printf("%s - dsend %d %s\n", __func__, ret, strerror(errno));
out:
	fastlock_release(&rs->slock);
	return ret;
}

static void rs_copy_iov(void *dst, const struct iovec **iov, size_t *offset, size_t len)
{
	size_t size;

	while (len) {
		size = (*iov)->iov_len - *offset;
		if (size > len) {
			memcpy (dst, (*iov)->iov_base + *offset, len);
			*offset += len;
			break;
		}

		memcpy(dst, (*iov)->iov_base + *offset, size);
		len -= size;
		dst += size;
		(*iov)++;
		*offset = 0;
	}
}

static ssize_t rsendv(int socket, const struct iovec *iov, int iovcnt, int flags)
{
	struct rsocket *rs;
	const struct iovec *cur_iov;
	size_t left, len, offset = 0;
	uint32_t xfer_size, olen = RS_OLAP_START_SIZE;
	int i, ret = 0;

	rs = idm_at(&idm, socket);
	if (rs->state & rs_opening) {
		ret = rs_do_connect(rs);
		if (ret) {
			if (errno == EINPROGRESS)
				errno = EAGAIN;
			return ret;
		}
	}

	cur_iov = iov;
	len = iov[0].iov_len;
	for (i = 1; i < iovcnt; i++)
		len += iov[i].iov_len;
	left = len;

	fastlock_acquire(&rs->slock);
	if (rs->iomap_pending) {
		ret = rs_send_iomaps(rs, flags);
		if (ret)
			goto out;
	}
	for (; left; left -= xfer_size) {
		if (!rs_can_send(rs)) {
			ret = rs_get_comp(rs, rs_nonblocking(rs, flags),
					  rs_conn_can_send);
			if (ret)
				break;
			if (!(rs->state & rs_writable)) {
				ret = ERR(ECONNRESET);
				break;
			}
		}

		if (olen < left) {
			xfer_size = olen;
			if (olen < RS_MAX_TRANSFER)
				olen <<= 1;
		} else {
			xfer_size = left;
		}

		if (xfer_size > rs->sbuf_bytes_avail)
			xfer_size = rs->sbuf_bytes_avail;
		if (xfer_size > rs->target_sgl[rs->target_sge].length)
			xfer_size = rs->target_sgl[rs->target_sge].length;

		if (xfer_size <= rs_sbuf_left(rs)) {
			rs_copy_iov((void *) (uintptr_t) rs->ssgl[0].addr,
				    &cur_iov, &offset, xfer_size);
			rs->ssgl[0].length = xfer_size;
			ret = rs_write_data(rs, rs->ssgl, 1, xfer_size,
					    xfer_size <= rs->sq_inline ? IBV_SEND_INLINE : 0);
			if (xfer_size < rs_sbuf_left(rs))
				rs->ssgl[0].addr += xfer_size;
			else
				rs->ssgl[0].addr = (uintptr_t) rs->sbuf;
		} else {
			rs->ssgl[0].length = rs_sbuf_left(rs);
			rs_copy_iov((void *) (uintptr_t) rs->ssgl[0].addr, &cur_iov,
				    &offset, rs->ssgl[0].length);
			rs->ssgl[1].length = xfer_size - rs->ssgl[0].length;
			rs_copy_iov(rs->sbuf, &cur_iov, &offset, rs->ssgl[1].length);
			ret = rs_write_data(rs, rs->ssgl, 2, xfer_size,
					    xfer_size <= rs->sq_inline ? IBV_SEND_INLINE : 0);
			rs->ssgl[0].addr = (uintptr_t) rs->sbuf + rs->ssgl[1].length;
		}
		if (ret)
			break;
	}
out:
	fastlock_release(&rs->slock);

	return (ret && left == len) ? ret : len - left;
}

ssize_t rsendmsg(int socket, const struct msghdr *msg, int flags)
{
	if (msg->msg_control && msg->msg_controllen)
		return ERR(ENOTSUP);

	return rsendv(socket, msg->msg_iov, (int) msg->msg_iovlen, flags);
}

ssize_t rwrite(int socket, const void *buf, size_t count)
{
	return rsend(socket, buf, count, 0);
}

ssize_t rwritev(int socket, const struct iovec *iov, int iovcnt)
{
	return rsendv(socket, iov, iovcnt, 0);
}

static struct pollfd *rs_fds_alloc(nfds_t nfds)
{
	static __thread struct pollfd *rfds;
	static __thread nfds_t rnfds;

	if (nfds > rnfds) {
		if (rfds)
			free(rfds);

		rfds = malloc(sizeof *rfds * nfds);
		rnfds = rfds ? nfds : 0;
	}

	return rfds;
}

static int rs_poll_rs(struct rsocket *rs, int events,
		      int nonblock, int (*test)(struct rsocket *rs))
{
	struct pollfd fds;
	short revents;
	int ret;

check_cq:
	if ((rs->type == SOCK_STREAM) && ((rs->state & rs_connected) ||
	     (rs->state == rs_disconnected) || (rs->state & rs_error))) {
		rs_process_cq(rs, nonblock, test);

		revents = 0;
		if ((events & POLLIN) && rs_conn_have_rdata(rs))
			revents |= POLLIN;
		if ((events & POLLOUT) && rs_can_send(rs))
			revents |= POLLOUT;
		if (!(rs->state & rs_connected)) {
			if (rs->state == rs_disconnected)
				revents |= POLLHUP;
			else
				revents |= POLLERR;
		}

		return revents;
	} else if (rs->type == SOCK_DGRAM) {
		ds_process_cqs(rs, nonblock, test);

		revents = 0;
		if ((events & POLLIN) && rs_have_rdata(rs))
			revents |= POLLIN;
		if ((events & POLLOUT) && ds_can_send(rs))
			revents |= POLLOUT;

		return revents;
	}

	if (rs->state == rs_listening) {
		fds.fd = rs->cm_id->channel->fd;
		fds.events = events;
		fds.revents = 0;
		poll(&fds, 1, 0);
		return fds.revents;
	}

	if (rs->state & rs_opening) {
		ret = rs_do_connect(rs);
		if (ret) {
			if (errno == EINPROGRESS) {
				errno = 0;
				return 0;
			} else {
				return POLLOUT;
			}
		}
		goto check_cq;
	}

	if (rs->state == rs_connect_error)
		return (rs->err && events & POLLOUT) ? POLLOUT : 0;

	return 0;
}

static int rs_poll_check(struct pollfd *fds, nfds_t nfds)
{
	struct rsocket *rs;
	int i, cnt = 0;

	for (i = 0; i < nfds; i++) {
		rs = idm_lookup(&idm, fds[i].fd);
		if (rs)
			fds[i].revents = rs_poll_rs(rs, fds[i].events, 1, rs_poll_all);
		else
			poll(&fds[i], 1, 0);

		if (fds[i].revents)
			cnt++;
	}
	return cnt;
}

static int rs_poll_arm(struct pollfd *rfds, struct pollfd *fds, nfds_t nfds)
{
	struct rsocket *rs;
	int i;

	for (i = 0; i < nfds; i++) {
		rs = idm_lookup(&idm, fds[i].fd);
		if (rs) {
			fds[i].revents = rs_poll_rs(rs, fds[i].events, 0, rs_is_cq_armed);
			if (fds[i].revents)
				return 1;

			if (rs->type == SOCK_STREAM) {
				if (rs->state >= rs_connected)
					rfds[i].fd = rs->cm_id->recv_cq_channel->fd;
				else
					rfds[i].fd = rs->cm_id->channel->fd;
			} else {
				rfds[i].fd = rs->epfd;
			}
			rfds[i].events = POLLIN;
		} else {
			rfds[i].fd = fds[i].fd;
			rfds[i].events = fds[i].events;
		}
		rfds[i].revents = 0;

	}
	return 0;
}

static int rs_poll_events(struct pollfd *rfds, struct pollfd *fds, nfds_t nfds)
{
	struct rsocket *rs;
	int i, cnt = 0;

	for (i = 0; i < nfds; i++) {
		if (!rfds[i].revents)
			continue;

		rs = idm_lookup(&idm, fds[i].fd);
		if (rs) {
			if (rs->type == SOCK_STREAM)
				rs_get_cq_event(rs);
			else
				ds_get_cq_event(rs);
			fds[i].revents = rs_poll_rs(rs, fds[i].events, 1, rs_poll_all);
		} else {
			fds[i].revents = rfds[i].revents;
		}
		if (fds[i].revents)
			cnt++;
	}
	return cnt;
}

/*
 * We need to poll *all* fd's that the user specifies at least once.
 * Note that we may receive events on an rsocket that may not be reported
 * to the user (e.g. connection events or credit updates).  Process those
 * events, then return to polling until we find ones of interest.
 */
int rpoll(struct pollfd *fds, nfds_t nfds, int timeout)
{
	struct timeval s, e;
	struct pollfd *rfds;
	uint32_t poll_time = 0;
	int ret;

	do {
		ret = rs_poll_check(fds, nfds);
		if (ret || !timeout)
			return ret;

		if (!poll_time)
			gettimeofday(&s, NULL);

		gettimeofday(&e, NULL);
		poll_time = (e.tv_sec - s.tv_sec) * 1000000 +
			    (e.tv_usec - s.tv_usec) + 1;
	} while (poll_time <= polling_time);

	rfds = rs_fds_alloc(nfds);
	if (!rfds)
		return ERR(ENOMEM);

	do {
		ret = rs_poll_arm(rfds, fds, nfds);
		if (ret)
			break;

		ret = poll(rfds, nfds, timeout);
		if (ret <= 0)
			break;

		ret = rs_poll_events(rfds, fds, nfds);
	} while (!ret);

	return ret;
}

static struct pollfd *
rs_select_to_poll(int *nfds, fd_set *readfds, fd_set *writefds, fd_set *exceptfds)
{
	struct pollfd *fds;
	int fd, i = 0;

	fds = calloc(*nfds, sizeof *fds);
	if (!fds)
		return NULL;

	for (fd = 0; fd < *nfds; fd++) {
		if (readfds && FD_ISSET(fd, readfds)) {
			fds[i].fd = fd;
			fds[i].events = POLLIN;
		}

		if (writefds && FD_ISSET(fd, writefds)) {
			fds[i].fd = fd;
			fds[i].events |= POLLOUT;
		}

		if (exceptfds && FD_ISSET(fd, exceptfds))
			fds[i].fd = fd;

		if (fds[i].fd)
			i++;
	}

	*nfds = i;
	return fds;
}

static int
rs_poll_to_select(int nfds, struct pollfd *fds, fd_set *readfds,
		  fd_set *writefds, fd_set *exceptfds)
{
	int i, cnt = 0;

	for (i = 0; i < nfds; i++) {
		if (readfds && (fds[i].revents & (POLLIN | POLLHUP))) {
			FD_SET(fds[i].fd, readfds);
			cnt++;
		}

		if (writefds && (fds[i].revents & POLLOUT)) {
			FD_SET(fds[i].fd, writefds);
			cnt++;
		}

		if (exceptfds && (fds[i].revents & ~(POLLIN | POLLOUT))) {
			FD_SET(fds[i].fd, exceptfds);
			cnt++;
		}
	}
	return cnt;
}

static int rs_convert_timeout(struct timeval *timeout)
{
	return !timeout ? -1 :
		timeout->tv_sec * 1000 + timeout->tv_usec / 1000;
}

int rselect(int nfds, fd_set *readfds, fd_set *writefds,
	    fd_set *exceptfds, struct timeval *timeout)
{
	struct pollfd *fds;
	int ret;

	fds = rs_select_to_poll(&nfds, readfds, writefds, exceptfds);
	if (!fds)
		return ERR(ENOMEM);

	ret = rpoll(fds, nfds, rs_convert_timeout(timeout));

	if (readfds)
		FD_ZERO(readfds);
	if (writefds)
		FD_ZERO(writefds);
	if (exceptfds)
		FD_ZERO(exceptfds);

	if (ret > 0)
		ret = rs_poll_to_select(nfds, fds, readfds, writefds, exceptfds);

	free(fds);
	return ret;
}

/*
 * For graceful disconnect, notify the remote side that we're
 * disconnecting and wait until all outstanding sends complete.
 */
int rshutdown(int socket, int how)
{
	struct rsocket *rs;
	int ctrl, ret = 0;

	rs = idm_at(&idm, socket);
	if (how == SHUT_RD) {
		rs->state &= ~rs_readable;
		return 0;
	}

	if (rs->fd_flags & O_NONBLOCK)
		rs_set_nonblocking(rs, 0);

	if (rs->state & rs_connected) {
		if (how == SHUT_RDWR) {
			ctrl = RS_CTRL_DISCONNECT;
			rs->state &= ~(rs_readable | rs_writable);
		} else {
			rs->state &= ~rs_writable;
			ctrl = (rs->state & rs_readable) ?
				RS_CTRL_SHUTDOWN : RS_CTRL_DISCONNECT;
		}
		if (!rs->ctrl_avail) {
			ret = rs_process_cq(rs, 0, rs_conn_can_send_ctrl);
			if (ret)
				return ret;
		}

		if ((rs->state & rs_connected) && rs->ctrl_avail) {
			rs->ctrl_avail--;
			ret = rs_post_write_msg(rs, NULL, 0,
						rs_msg_set(RS_OP_CTRL, ctrl), 0, 0, 0);
		}
	}

	if (rs->state & rs_connected)
		rs_process_cq(rs, 0, rs_conn_all_sends_done);

	if ((rs->fd_flags & O_NONBLOCK) && (rs->state & rs_connected))
		rs_set_nonblocking(rs, 1);

	return 0;
}

static void ds_shutdown(struct rsocket *rs)
{
	if (rs->fd_flags & O_NONBLOCK)
		rs_set_nonblocking(rs, 0);

	rs->state &= ~(rs_readable | rs_writable);
	ds_process_cqs(rs, 0, ds_all_sends_done);

	if (rs->fd_flags & O_NONBLOCK)
		rs_set_nonblocking(rs, 1);
}

int rclose(int socket)
{
	struct rsocket *rs;

	rs = idm_at(&idm, socket);
	if (rs->type == SOCK_STREAM) {
		if (rs->state & rs_connected)
			rshutdown(socket, SHUT_RDWR);
	} else {
		ds_shutdown(rs);
	}

	rs_free(rs);
	return 0;
}

static void rs_copy_addr(struct sockaddr *dst, struct sockaddr *src, socklen_t *len)
{
	socklen_t size;

	if (src->sa_family == AF_INET) {
		size = min(*len, sizeof(struct sockaddr_in));
		*len = sizeof(struct sockaddr_in);
	} else {
		size = min(*len, sizeof(struct sockaddr_in6));
		*len = sizeof(struct sockaddr_in6);
	}
	memcpy(dst, src, size);
}

int rgetpeername(int socket, struct sockaddr *addr, socklen_t *addrlen)
{
	struct rsocket *rs;

	rs = idm_at(&idm, socket);
	if (rs->type == SOCK_STREAM) {
		rs_copy_addr(addr, rdma_get_peer_addr(rs->cm_id), addrlen);
		return 0;
	} else {
		return getpeername(rs->udp_sock, addr, addrlen);
	}
}

int rgetsockname(int socket, struct sockaddr *addr, socklen_t *addrlen)
{
	struct rsocket *rs;

	rs = idm_at(&idm, socket);
	if (rs->type == SOCK_STREAM) {
		rs_copy_addr(addr, rdma_get_local_addr(rs->cm_id), addrlen);
		return 0;
	} else {
		return getsockname(rs->udp_sock, addr, addrlen);
	}
}

int rsetsockopt(int socket, int level, int optname,
		const void *optval, socklen_t optlen)
{
	struct rsocket *rs;
	int ret, opt_on = 0;
	uint64_t *opts = NULL;

	ret = ERR(ENOTSUP);
	rs = idm_at(&idm, socket);
	if (rs->type == SOCK_DGRAM && level != SOL_RDMA) {
		ret = setsockopt(rs->udp_sock, level, optname, optval, optlen);
		if (ret)
			return ret;
	}

	switch (level) {
	case SOL_SOCKET:
		opts = &rs->so_opts;
		switch (optname) {
		case SO_REUSEADDR:
			if (rs->type == SOCK_STREAM) {
				ret = rdma_set_option(rs->cm_id, RDMA_OPTION_ID,
						      RDMA_OPTION_ID_REUSEADDR,
						      (void *) optval, optlen);
				if (ret && ((errno == ENOSYS) || ((rs->state != rs_init) &&
				    rs->cm_id->context &&
				    (rs->cm_id->verbs->device->transport_type == IBV_TRANSPORT_IB))))
					ret = 0;
			}
			opt_on = *(int *) optval;
			break;
		case SO_RCVBUF:
			if ((rs->type == SOCK_STREAM && !rs->rbuf) ||
			    (rs->type == SOCK_DGRAM && !rs->qp_list))
				rs->rbuf_size = (*(uint32_t *) optval) << 1;
			ret = 0;
			break;
		case SO_SNDBUF:
			if (!rs->sbuf)
				rs->sbuf_size = (*(uint32_t *) optval) << 1;
			if (rs->sbuf_size < RS_SNDLOWAT)
				rs->sbuf_size = RS_SNDLOWAT << 1;
			ret = 0;
			break;
		case SO_LINGER:
			/* Invert value so default so_opt = 0 is on */
			opt_on =  !((struct linger *) optval)->l_onoff;
			ret = 0;
			break;
		case SO_KEEPALIVE:
			opt_on = *(int *) optval;
			ret = 0;
			break;
		case SO_OOBINLINE:
			opt_on = *(int *) optval;
			ret = 0;
			break;
		default:
			break;
		}
		break;
	case IPPROTO_TCP:
		opts = &rs->tcp_opts;
		switch (optname) {
		case TCP_NODELAY:
			opt_on = *(int *) optval;
			ret = 0;
			break;
		case TCP_MAXSEG:
			ret = 0;
			break;
		default:
			break;
		}
		break;
	case IPPROTO_IPV6:
		opts = &rs->ipv6_opts;
		switch (optname) {
		case IPV6_V6ONLY:
			if (rs->type == SOCK_STREAM) {
				ret = rdma_set_option(rs->cm_id, RDMA_OPTION_ID,
						      RDMA_OPTION_ID_AFONLY,
						      (void *) optval, optlen);
			}
			opt_on = *(int *) optval;
			break;
		default:
			break;
		}
		break;
	case SOL_RDMA:
		if (rs->state >= rs_opening) {
			ret = ERR(EINVAL);
			break;
		}

		switch (optname) {
		case RDMA_SQSIZE:
			rs->sq_size = min((*(uint32_t *) optval), RS_QP_MAX_SIZE);
			break;
		case RDMA_RQSIZE:
			rs->rq_size = min((*(uint32_t *) optval), RS_QP_MAX_SIZE);
			break;
		case RDMA_INLINE:
			rs->sq_inline = min(*(uint32_t *) optval, RS_QP_MAX_SIZE);
			if (rs->sq_inline < RS_MIN_INLINE)
				rs->sq_inline = RS_MIN_INLINE;
			break;
		case RDMA_IOMAPSIZE:
			rs->target_iomap_size = (uint16_t) rs_scale_to_value(
				(uint8_t) rs_value_to_scale(*(int *) optval, 8), 8);
			break;
		default:
			break;
		}
		break;
	default:
		break;
	}

	if (!ret && opts) {
		if (opt_on)
			*opts |= (1 << optname);
		else
			*opts &= ~(1 << optname);
	}

	return ret;
}

int rgetsockopt(int socket, int level, int optname,
		void *optval, socklen_t *optlen)
{
	struct rsocket *rs;
	int ret = 0;

	rs = idm_at(&idm, socket);
	switch (level) {
	case SOL_SOCKET:
		switch (optname) {
		case SO_REUSEADDR:
		case SO_KEEPALIVE:
		case SO_OOBINLINE:
			*((int *) optval) = !!(rs->so_opts & (1 << optname));
			*optlen = sizeof(int);
			break;
		case SO_RCVBUF:
			*((int *) optval) = rs->rbuf_size;
			*optlen = sizeof(int);
			break;
		case SO_SNDBUF:
			*((int *) optval) = rs->sbuf_size;
			*optlen = sizeof(int);
			break;
		case SO_LINGER:
			/* Value is inverted so default so_opt = 0 is on */
			((struct linger *) optval)->l_onoff =
					!(rs->so_opts & (1 << optname));
			((struct linger *) optval)->l_linger = 0;
			*optlen = sizeof(struct linger);
			break;
		case SO_ERROR:
			*((int *) optval) = rs->err;
			*optlen = sizeof(int);
			rs->err = 0;
			break;
		default:
			ret = ENOTSUP;
			break;
		}
		break;
	case IPPROTO_TCP:
		switch (optname) {
		case TCP_NODELAY:
			*((int *) optval) = !!(rs->tcp_opts & (1 << optname));
			*optlen = sizeof(int);
			break;
		case TCP_MAXSEG:
			*((int *) optval) = (rs->cm_id && rs->cm_id->route.num_paths) ?
					    1 << (7 + rs->cm_id->route.path_rec->mtu) :
					    2048;
			*optlen = sizeof(int);
			break;
		default:
			ret = ENOTSUP;
			break;
		}
		break;
	case IPPROTO_IPV6:
		switch (optname) {
		case IPV6_V6ONLY:
			*((int *) optval) = !!(rs->ipv6_opts & (1 << optname));
			*optlen = sizeof(int);
			break;
		default:
			ret = ENOTSUP;
			break;
		}
		break;
	case SOL_RDMA:
		switch (optname) {
		case RDMA_SQSIZE:
			*((int *) optval) = rs->sq_size;
			*optlen = sizeof(int);
			break;
		case RDMA_RQSIZE:
			*((int *) optval) = rs->rq_size;
			*optlen = sizeof(int);
			break;
		case RDMA_INLINE:
			*((int *) optval) = rs->sq_inline;
			*optlen = sizeof(int);
			break;
		case RDMA_IOMAPSIZE:
			*((int *) optval) = rs->target_iomap_size;
			*optlen = sizeof(int);
			break;
		default:
			ret = ENOTSUP;
			break;
		}
		break;
	default:
		ret = ENOTSUP;
		break;
	}

	return rdma_seterrno(ret);
}

int rfcntl(int socket, int cmd, ... /* arg */ )
{
	struct rsocket *rs;
	va_list args;
	long param;
	int ret = 0;

	rs = idm_at(&idm, socket);
	va_start(args, cmd);
	switch (cmd) {
	case F_GETFL:
		ret = (int) rs->fd_flags;
		break;
	case F_SETFL:
		param = va_arg(args, long);
		if (param & O_NONBLOCK)
			ret = rs_set_nonblocking(rs, O_NONBLOCK);

		if (!ret)
			rs->fd_flags |= param;
		break;
	default:
		ret = ERR(ENOTSUP);
		break;
	}
	va_end(args);
	return ret;
}

static struct rs_iomap_mr *rs_get_iomap_mr(struct rsocket *rs)
{
	int i;

	if (!rs->remote_iomappings) {
		rs->remote_iomappings = calloc(rs->remote_iomap.length,
					       sizeof(*rs->remote_iomappings));
		if (!rs->remote_iomappings)
			return NULL;

		for (i = 0; i < rs->remote_iomap.length; i++)
			rs->remote_iomappings[i].index = i;
	}

	for (i = 0; i < rs->remote_iomap.length; i++) {
		if (!rs->remote_iomappings[i].mr)
			return &rs->remote_iomappings[i];
	}
	return NULL;
}

/*
 * If an offset is given, we map to it.  If offset is -1, then we map the
 * offset to the address of buf.  We do not check for conflicts, which must
 * be fixed at some point.
 */
off_t riomap(int socket, void *buf, size_t len, int prot, int flags, off_t offset)
{
	struct rsocket *rs;
	struct rs_iomap_mr *iomr;
	int access = IBV_ACCESS_LOCAL_WRITE;

	rs = idm_at(&idm, socket);
	if (!rs->cm_id->pd || (prot & ~(PROT_WRITE | PROT_NONE)))
		return ERR(EINVAL);

	fastlock_acquire(&rs->map_lock);
	if (prot & PROT_WRITE) {
		iomr = rs_get_iomap_mr(rs);
		access |= IBV_ACCESS_REMOTE_WRITE;
	} else {
		iomr = calloc(1, sizeof *iomr);
		iomr->index = -1;
	}
	if (!iomr) {
		offset = ERR(ENOMEM);
		goto out;
	}

	iomr->mr = ibv_reg_mr(rs->cm_id->pd, buf, len, access);
	if (!iomr->mr) {
		if (iomr->index < 0)
			free(iomr);
		offset = -1;
		goto out;
	}

	if (offset == -1)
		offset = (uintptr_t) buf;
	iomr->offset = offset;
	atomic_init(&iomr->refcnt);
	atomic_set(&iomr->refcnt, 1);

	if (iomr->index >= 0) {
		dlist_insert_tail(&iomr->entry, &rs->iomap_queue);
		rs->iomap_pending = 1;
	} else {
		dlist_insert_tail(&iomr->entry, &rs->iomap_list);
	}
out:
	fastlock_release(&rs->map_lock);
	return offset;
}

int riounmap(int socket, void *buf, size_t len)
{
	struct rsocket *rs;
	struct rs_iomap_mr *iomr;
	dlist_entry *entry;
	int ret = 0;

	rs = idm_at(&idm, socket);
	fastlock_acquire(&rs->map_lock);

	for (entry = rs->iomap_list.next; entry != &rs->iomap_list;
	     entry = entry->next) {
		iomr = container_of(entry, struct rs_iomap_mr, entry);
		if (iomr->mr->addr == buf && iomr->mr->length == len) {
			rs_release_iomap_mr(iomr);
			goto out;
		}
	}

	for (entry = rs->iomap_queue.next; entry != &rs->iomap_queue;
	     entry = entry->next) {
		iomr = container_of(entry, struct rs_iomap_mr, entry);
		if (iomr->mr->addr == buf && iomr->mr->length == len) {
			rs_release_iomap_mr(iomr);
			goto out;
		}
	}
	ret = ERR(EINVAL);
out:
	fastlock_release(&rs->map_lock);
	return ret;
}

static struct rs_iomap *rs_find_iomap(struct rsocket *rs, off_t offset)
{
	int i;

	for (i = 0; i < rs->target_iomap_size; i++) {
		if (offset >= rs->target_iomap[i].offset &&
		    offset < rs->target_iomap[i].offset + rs->target_iomap[i].sge.length)
			return &rs->target_iomap[i];
	}
	return NULL;
}

size_t riowrite(int socket, const void *buf, size_t count, off_t offset, int flags)
{
	struct rsocket *rs;
	struct rs_iomap *iom = NULL;
	struct ibv_sge sge;
	size_t left = count;
	uint32_t xfer_size, olen = RS_OLAP_START_SIZE;
	int ret = 0;

	rs = idm_at(&idm, socket);
	fastlock_acquire(&rs->slock);
	if (rs->iomap_pending) {
		ret = rs_send_iomaps(rs, flags);
		if (ret)
			goto out;
	}
	for (; left; left -= xfer_size, buf += xfer_size, offset += xfer_size) {
		if (!iom || offset > iom->offset + iom->sge.length) {
			iom = rs_find_iomap(rs, offset);
			if (!iom)
				break;
		}

		if (!rs_can_send(rs)) {
			ret = rs_get_comp(rs, rs_nonblocking(rs, flags),
					  rs_conn_can_send);
			if (ret)
				break;
			if (!(rs->state & rs_writable)) {
				ret = ERR(ECONNRESET);
				break;
			}
		}

		if (olen < left) {
			xfer_size = olen;
			if (olen < RS_MAX_TRANSFER)
				olen <<= 1;
		} else {
			xfer_size = left;
		}

		if (xfer_size > rs->sbuf_bytes_avail)
			xfer_size = rs->sbuf_bytes_avail;
		if (xfer_size > iom->offset + iom->sge.length - offset)
			xfer_size = iom->offset + iom->sge.length - offset;

		if (xfer_size <= rs->sq_inline) {
			sge.addr = (uintptr_t) buf;
			sge.length = xfer_size;
			sge.lkey = 0;
			ret = rs_write_direct(rs, iom, offset, &sge, 1,
					      xfer_size, IBV_SEND_INLINE);
		} else if (xfer_size <= rs_sbuf_left(rs)) {
			memcpy((void *) (uintptr_t) rs->ssgl[0].addr, buf, xfer_size);
			rs->ssgl[0].length = xfer_size;
			ret = rs_write_direct(rs, iom, offset, rs->ssgl, 1, xfer_size, 0);
			if (xfer_size < rs_sbuf_left(rs))
				rs->ssgl[0].addr += xfer_size;
			else
				rs->ssgl[0].addr = (uintptr_t) rs->sbuf;
		} else {
			rs->ssgl[0].length = rs_sbuf_left(rs);
			memcpy((void *) (uintptr_t) rs->ssgl[0].addr, buf,
				rs->ssgl[0].length);
			rs->ssgl[1].length = xfer_size - rs->ssgl[0].length;
			memcpy(rs->sbuf, buf + rs->ssgl[0].length, rs->ssgl[1].length);
			ret = rs_write_direct(rs, iom, offset, rs->ssgl, 2, xfer_size, 0);
			rs->ssgl[0].addr = (uintptr_t) rs->sbuf + rs->ssgl[1].length;
		}
		if (ret)
			break;
	}
out:
	fastlock_release(&rs->slock);

	return (ret && left == count) ? ret : count - left;
}

static int rs_svc_grow_sets(void)
{
	struct rsocket **rss;
	struct pollfd *fds;
	void *set;

	set = calloc(svc_size + 2, sizeof(*rss) + sizeof(*fds));
	if (!set)
		return ENOMEM;

	svc_size += 2;
	rss = set;
	fds = set + sizeof(*rss) * svc_size;
	if (svc_cnt) {
		memcpy(rss, svc_rss, sizeof(*rss) * svc_cnt);
		memcpy(fds, svc_fds, sizeof(*fds) * svc_cnt);
	}

	free(svc_rss);
	free(svc_fds);
	svc_rss = rss;
	svc_fds = fds;
	return 0;
}

/*
 * Index 0 is reserved for the service's communication socket.
 */
static int rs_svc_add_rs(struct rsocket *rs)
{
	int ret;

	if (svc_cnt >= svc_size - 1) {
		ret = rs_svc_grow_sets();
		if (ret)
			return ret;
	}

	svc_rss[++svc_cnt] = rs;
	svc_fds[svc_cnt].fd = rs->udp_sock;
	svc_fds[svc_cnt].events = POLLIN;
	svc_fds[svc_cnt].revents = 0;
	return 0;
}

static int rs_svc_rm_rs(struct rsocket *rs)
{
	int i;

	for (i = 1; i <= svc_cnt; i++) {
		if (svc_rss[i] == rs) {
			svc_cnt--;
			svc_rss[i] = svc_rss[svc_cnt];
			svc_fds[i] = svc_fds[svc_cnt];
			return 0;
		}
	}
	return EBADF;
}

static void rs_svc_process_sock(void)
{
	struct rs_svc_msg msg;

	read(svc_sock[1], &msg, sizeof msg);
	switch (msg.op) {
	case RS_SVC_INSERT:
		msg.status = rs_svc_add_rs(msg.rs);
		break;
	case RS_SVC_REMOVE:
		msg.status = rs_svc_rm_rs(msg.rs);
		break;
	default:
		msg.status = ENOTSUP;
		break;
	}
	write(svc_sock[1], &msg, sizeof msg);
}

static uint8_t rs_svc_sgid_index(struct ds_dest *dest, union ibv_gid *sgid)
{
	union ibv_gid gid;
	int i, ret;

	for (i = 0; i < 16; i++) {
		ret = ibv_query_gid(dest->qp->cm_id->verbs, dest->qp->cm_id->port_num,
				    i, &gid);
		if (!memcmp(sgid, &gid, sizeof gid))
			return i;
	}
	return 0;
}

static uint8_t rs_svc_path_bits(struct ds_dest *dest)
{
	struct ibv_port_attr attr;

	if (!ibv_query_port(dest->qp->cm_id->verbs, dest->qp->cm_id->port_num, &attr))
		return (uint8_t) ((1 << attr.lmc) - 1);
	return 0x7f;
}

static void rs_svc_create_ah(struct rsocket *rs, struct ds_dest *dest, uint32_t qpn)
{
	union socket_addr saddr;
	struct rdma_cm_id *id;
	struct ibv_ah_attr attr;
	int ret;

	if (dest->ah) {
		fastlock_acquire(&rs->slock);
		ibv_destroy_ah(dest->ah);
		dest->ah = NULL;
		fastlock_release(&rs->slock);
	}

	ret = rdma_create_id(NULL, &id, NULL, dest->qp->cm_id->ps);
	if  (ret)
		return;

	memcpy(&saddr, rdma_get_local_addr(dest->qp->cm_id),
	       ucma_addrlen(rdma_get_local_addr(dest->qp->cm_id)));
	if (saddr.sa.sa_family == AF_INET)
		saddr.sin.sin_port = 0;
	else
		saddr.sin6.sin6_port = 0;
	ret = rdma_resolve_addr(id, &saddr.sa, &dest->addr.sa, 2000);
	if (ret)
		goto out;

	ret = rdma_resolve_route(id, 2000);
	if (ret)
		goto out;

	memset(&attr, 0, sizeof attr);
	if (id->route.path_rec->hop_limit > 1) {
		attr.is_global = 1;
		attr.grh.dgid = id->route.path_rec->dgid;
		attr.grh.flow_label = ntohl(id->route.path_rec->flow_label);
		attr.grh.sgid_index = rs_svc_sgid_index(dest, &id->route.path_rec->sgid);
		attr.grh.hop_limit = id->route.path_rec->hop_limit;
		attr.grh.traffic_class = id->route.path_rec->traffic_class;
	}
	attr.dlid = ntohs(id->route.path_rec->dlid);
	attr.sl = id->route.path_rec->sl;
	attr.src_path_bits = id->route.path_rec->slid & rs_svc_path_bits(dest);
	attr.static_rate = id->route.path_rec->rate;
	attr.port_num  = id->port_num;

	fastlock_acquire(&rs->slock);
	dest->qpn = qpn;
	dest->ah = ibv_create_ah(dest->qp->cm_id->pd, &attr);
	fastlock_release(&rs->slock);
out:
	rdma_destroy_id(id);
}

static int rs_svc_valid_udp_hdr(struct ds_udp_header *udp_hdr,
				union socket_addr *addr)
{
	return (udp_hdr->tag == ntohl(DS_UDP_TAG)) &&
		((udp_hdr->version == 4 && addr->sa.sa_family == AF_INET &&
		  udp_hdr->length == DS_UDP_IPV4_HDR_LEN) ||
		 (udp_hdr->version == 6 && addr->sa.sa_family == AF_INET6 &&
		  udp_hdr->length == DS_UDP_IPV6_HDR_LEN));
}

static void rs_svc_forward(struct rsocket *rs, void *buf, size_t len,
			   union socket_addr *src)
{
	struct ds_header hdr;
	struct ds_smsg *msg;
	struct ibv_sge sge;
	uint64_t offset;

//	PRINTADDR(src);
	if (!ds_can_send(rs)) {
		if (ds_get_comp(rs, 0, ds_can_send))
			return;
	}

	msg = rs->smsg_free;
	rs->smsg_free = msg->next;
	rs->sqe_avail--;

	ds_format_hdr(&hdr, src);
//	printf("%s hdr ver %d length %d port %x\n", __func__, hdr.version,
//			hdr.length, hdr.port);
	memcpy((void *) msg, &hdr, hdr.length);
	memcpy((void *) msg + hdr.length, buf, len);
//	printf("%s received data 0x%x\n", __func__, *((uint32_t*)buf));
	sge.addr = (uintptr_t) msg;
	sge.length = hdr.length + len;
	sge.lkey = rs->conn_dest->qp->smr->lkey;
	offset = (uint8_t *) msg - rs->sbuf;

//	printf("%s ver %d length %d port %x\n", __func__, ((struct ds_header *) msg)->version,
//			((struct ds_header *) msg)->length, ((struct ds_header *) msg)->port);
	ds_post_send(rs, &sge, ds_send_wr_id(offset, sge.length));
}

static void rs_svc_process_rs(struct rsocket *rs)
{
	struct ds_dest *dest, *cur_dest;
	struct ds_udp_header *udp_hdr;
	union socket_addr addr;
	socklen_t addrlen = sizeof addr;
	int len, ret;

	ret = recvfrom(rs->udp_sock, svc_buf, sizeof svc_buf, 0, &addr.sa, &addrlen);
//	PRINTADDR(&addr);
//	printf("%s received data 0x%x\n", __func__, *((uint32_t*)&svc_buf[8]));
	if (ret < DS_UDP_IPV4_HDR_LEN)
		return;

	udp_hdr = (struct ds_udp_header *) svc_buf;
	if (!rs_svc_valid_udp_hdr(udp_hdr, &addr))
		return;

	len = ret - udp_hdr->length;
	udp_hdr->tag = ntohl(udp_hdr->tag);
	udp_hdr->qpn = ntohl(udp_hdr->qpn) & 0xFFFFFF;
	ret = ds_get_dest(rs, &addr.sa, addrlen, &dest);
	if (ret)
		return;

	if (!dest->ah || (dest->qpn != udp_hdr->qpn))
		rs_svc_create_ah(rs, dest, udp_hdr->qpn);

	/* to do: handle when dest local ip address doesn't match udp ip */
	if (udp_hdr->op != RS_OP_DATA)
		return;

	fastlock_acquire(&rs->slock);
	cur_dest = rs->conn_dest;
	rs->conn_dest = &dest->qp->dest;
	rs_svc_forward(rs, svc_buf + udp_hdr->length, len, &addr);

	rs->conn_dest = dest;
	ds_send_udp(rs, NULL, 0, 0, RS_OP_CTRL);
	rs->conn_dest = cur_dest;
	fastlock_release(&rs->slock);
}

static void *rs_svc_run(void *arg)
{
	struct rs_svc_msg msg;
	int i, ret;

	ret = rs_svc_grow_sets();
	if (ret) {
		msg.status = ret;
		write(svc_sock[1], &msg, sizeof msg);
		return (void *) (uintptr_t) ret;
	}

	svc_fds[0].fd = svc_sock[1];
	svc_fds[0].events = POLLIN;
	do {
		for (i = 0; i <= svc_cnt; i++)
			svc_fds[i].revents = 0;

		poll(svc_fds, svc_cnt + 1, -1);
		if (svc_fds[0].revents)
			rs_svc_process_sock();

		for (i = 1; i <= svc_cnt; i++) {
			if (svc_fds[i].revents)
				rs_svc_process_rs(svc_rss[i]);
		}
	} while (svc_cnt >= 1);

	return NULL;
}
