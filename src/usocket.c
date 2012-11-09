/*
 * Copyright (c) 2012 Intel Corporation.  All rights reserved.
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
#include <string.h>
#include <netinet/in.h>

#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>
#include <rdma/usocket.h>
#include "cma.h"
#include "indexer.h"

//#define RS_SNDLOWAT 64
//#define RS_QP_MAX_SIZE 0xFFFE
//#define RS_SGL_SIZE 2
//static struct index_map idm;
//static pthread_mutex_t mut = PTHREAD_MUTEX_INITIALIZER;

//static uint16_t def_inline = 64;
//static uint16_t def_sqsize = 384;
//static uint16_t def_rqsize = 384;
//static uint32_t def_mem = (1 << 17);
//static uint32_t def_wmem = (1 << 17);
//static uint32_t polling_time = 10;

//enum {
//	RS_OP_DATA,
//	RS_OP_RSVD_DATA_MORE,
//	RS_OP_WRITE, /* opcode is not transmitted over the network */
//	RS_OP_RSVD_DRA_MORE,
//	RS_OP_SGL,
//	RS_OP_RSVD,
//	RS_OP_IOMAP_SGL,
//	RS_OP_CTRL
//};
//#define rs_msg_set(op, data)  ((op << 29) | (uint32_t) (data))
//#define rs_msg_op(imm_data)   (imm_data >> 29)
//#define rs_msg_data(imm_data) (imm_data & 0x1FFFFFFF)

struct rs_msg {
	uint32_t op;
	uint32_t data;
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

#define RS_RECV_WR_ID (~((uint64_t) 0))

/*
 * usocket states are ordered as passive, connecting, connected, disconnected.
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
	rs_connect_wr 	   =		    0x0200,
	rs_connect_rd	   =		    0x0400,
	rs_connect_rdwr    = rs_connected | rs_connect_rd | rs_connect_wr,
	rs_connect_error   =		    0x0800,
	rs_disconnected	   =		    0x1000,
	rs_error	   =		    0x2000,
};

#define RS_OPT_SWAP_SGL 1

struct usocket {
	struct rdma_cm_id *cm_id;
	fastlock_t	  slock;
	fastlock_t	  rlock;
	fastlock_t	  cq_lock;
	fastlock_t	  cq_wait_lock;
	fastlock_t	  iomap_lock;

	int		  opts;
	long		  fd_flags;
	uint64_t	  so_opts;
	uint64_t	  tcp_opts;
	uint64_t	  ipv6_opts;
	int		  state;
	int		  cq_armed;
	int		  retries;
	int		  err;
	int		  index;
	int		  ctrl_avail;
	int		  sqe_avail;
	int		  sbuf_bytes_avail;
	uint16_t	  sseq_no;
	uint16_t	  sseq_comp;
	uint16_t	  sq_size;
	uint16_t	  sq_inline;

	uint16_t	  rq_size;
	uint16_t	  rseq_no;
	uint16_t	  rseq_comp;
	int		  rbuf_bytes_avail;
	int		  rbuf_free_offset;
	int		  rbuf_offset;
	int		  rmsg_head;
	int		  rmsg_tail;
	struct rs_msg	  *rmsg;

	int		  remote_sge;
	struct rs_sge	  remote_sgl;
	struct rs_sge	  remote_iomap;

	struct rs_iomap_mr *remote_iomappings;
	dlist_entry	  iomap_list;
	dlist_entry	  iomap_queue;
	int		  iomap_pending;

	struct ibv_mr	 *target_mr;
	int		  target_sge;
	int		  target_iomap_size;
	void		 *target_buffer_list;
	volatile struct rs_sge	  *target_sgl;
	struct rs_iomap  *target_iomap;

	uint32_t	  rbuf_size;
	struct ibv_mr	 *rmr;
	uint8_t		  *rbuf;

	uint32_t	  sbuf_size;
	struct ibv_mr	 *smr;
	struct ibv_sge	  ssgl[2];
	uint8_t		  *sbuf;
};

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

static int rs_insert(struct usocket *us)
{
	pthread_mutex_lock(&mut);
	us->index = idm_set(&idm, us->cm_id->channel->fd, us);
	pthread_mutex_unlock(&mut);
	return us->index;
}

static void rs_remove(struct usocket *us)
{
	pthread_mutex_lock(&mut);
	idm_clear(&idm, us->index);
	pthread_mutex_unlock(&mut);
}

static struct usocket *rs_alloc(struct usocket *inherited_rs)
{
	struct usocket *us;

	us = calloc(1, sizeof *us);
	if (!us)
		return NULL;

	us->index = -1;
	if (inherited_rs) {
		us->sbuf_size = inherited_rs->sbuf_size;
		us->rbuf_size = inherited_rs->rbuf_size;
		us->sq_inline = inherited_rs->sq_inline;
		us->sq_size = inherited_rs->sq_size;
		us->rq_size = inherited_rs->rq_size;
		us->ctrl_avail = inherited_rs->ctrl_avail;
		us->target_iomap_size = inherited_rs->target_iomap_size;
	} else {
		us->sbuf_size = def_wmem;
		us->rbuf_size = def_mem;
		us->sq_inline = def_inline;
		us->sq_size = def_sqsize;
		us->rq_size = def_rqsize;
		us->ctrl_avail = RS_QP_CTRL_SIZE;
		us->target_iomap_size = def_iomap_size;
	}
	fastlock_init(&us->slock);
	fastlock_init(&us->rlock);
	fastlock_init(&us->cq_lock);
	fastlock_init(&us->cq_wait_lock);
	fastlock_init(&us->iomap_lock);
	dlist_init(&us->iomap_list);
	dlist_init(&us->iomap_queue);
	return us;
}

static int rs_set_nonblocking(struct usocket *us, long arg)
{
	int ret = 0;

	if (us->cm_id->recv_cq_channel)
		ret = fcntl(us->cm_id->recv_cq_channel->fd, F_SETFL, arg);

	if (!ret && us->state < rs_connected)
		ret = fcntl(us->cm_id->channel->fd, F_SETFL, arg);

	return ret;
}

static void rs_set_qp_size(struct usocket *us)
{
	uint16_t max_size;

	max_size = min(ucma_max_qpsize(us->cm_id), RS_QP_MAX_SIZE);

	if (us->sq_size > max_size)
		us->sq_size = max_size;
	else if (us->sq_size < 2)
		us->sq_size = 2;
	if (us->sq_size <= (RS_QP_CTRL_SIZE << 2))
		us->ctrl_avail = 1;

	if (us->rq_size > max_size)
		us->rq_size = max_size;
	else if (us->rq_size < 2)
		us->rq_size = 2;
}

static int rs_init_bufs(struct usocket *us)
{
	size_t len;

	us->rmsg = calloc(us->rq_size + 1, sizeof(*us->rmsg));
	if (!us->rmsg)
		return -1;

	us->sbuf = calloc(us->sbuf_size, sizeof(*us->sbuf));
	if (!us->sbuf)
		return -1;

	us->smr = rdma_reg_msgs(us->cm_id, us->sbuf, us->sbuf_size);
	if (!us->smr)
		return -1;

	len = sizeof(*us->target_sgl) * RS_SGL_SIZE +
	      sizeof(*us->target_iomap) * us->target_iomap_size;
	us->target_buffer_list = malloc(len);
	if (!us->target_buffer_list)
		return -1;

	us->target_mr = rdma_reg_write(us->cm_id, us->target_buffer_list, len);
	if (!us->target_mr)
		return -1;

	memset(us->target_buffer_list, 0, len);
	us->target_sgl = us->target_buffer_list;
	if (us->target_iomap_size)
		us->target_iomap = (struct rs_iomap *) (us->target_sgl + RS_SGL_SIZE);

	us->rbuf = calloc(us->rbuf_size, sizeof(*us->rbuf));
	if (!us->rbuf)
		return -1;

	us->rmr = rdma_reg_write(us->cm_id, us->rbuf, us->rbuf_size);
	if (!us->rmr)
		return -1;

	us->ssgl[0].addr = us->ssgl[1].addr = (uintptr_t) us->sbuf;
	us->sbuf_bytes_avail = us->sbuf_size;
	us->ssgl[0].lkey = us->ssgl[1].lkey = us->smr->lkey;

	us->rbuf_free_offset = us->rbuf_size >> 1;
	us->rbuf_bytes_avail = us->rbuf_size >> 1;
	us->sqe_avail = us->sq_size - us->ctrl_avail;
	us->rseq_comp = us->rq_size >> 1;
	return 0;
}

static int rs_create_cq(struct usocket *us)
{
	us->cm_id->recv_cq_channel = ibv_create_comp_channel(us->cm_id->verbs);
	if (!us->cm_id->recv_cq_channel)
		return -1;

	us->cm_id->recv_cq = ibv_create_cq(us->cm_id->verbs, us->sq_size + us->rq_size,
					   us->cm_id, us->cm_id->recv_cq_channel, 0);
	if (!us->cm_id->recv_cq)
		goto err1;

	if (us->fd_flags & O_NONBLOCK) {
		if (rs_set_nonblocking(us, O_NONBLOCK))
			goto err2;
	}

	us->cm_id->send_cq_channel = us->cm_id->recv_cq_channel;
	us->cm_id->send_cq = us->cm_id->recv_cq;
	return 0;

err2:
	ibv_destroy_cq(us->cm_id->recv_cq);
	us->cm_id->recv_cq = NULL;
err1:
	ibv_destroy_comp_channel(us->cm_id->recv_cq_channel);
	us->cm_id->recv_cq_channel = NULL;
	return -1;
}

static inline int
rs_post_recv(struct usocket *us)
{
	struct ibv_recv_wr wr, *bad;

	wr.wr_id = RS_RECV_WR_ID;
	wr.next = NULL;
	wr.sg_list = NULL;
	wr.num_sge = 0;

	return rdma_seterrno(ibv_post_recv(us->cm_id->qp, &wr, &bad));
}

static int rs_create_ep(struct usocket *us)
{
	struct ibv_qp_init_attr qp_attr;
	int i, ret;

	rs_set_qp_size(us);
	ret = rs_init_bufs(us);
	if (ret)
		return ret;

	ret = rs_create_cq(us);
	if (ret)
		return ret;

	memset(&qp_attr, 0, sizeof qp_attr);
	qp_attr.qp_context = us;
	qp_attr.send_cq = us->cm_id->send_cq;
	qp_attr.recv_cq = us->cm_id->recv_cq;
	qp_attr.qp_type = IBV_QPT_RC;
	qp_attr.sq_sig_all = 1;
	qp_attr.cap.max_send_wr = us->sq_size;
	qp_attr.cap.max_recv_wr = us->rq_size;
	qp_attr.cap.max_send_sge = 2;
	qp_attr.cap.max_recv_sge = 1;
	qp_attr.cap.max_inline_data = us->sq_inline;

	ret = rdma_create_qp(us->cm_id, NULL, &qp_attr);
	if (ret)
		return ret;

	for (i = 0; i < us->rq_size; i++) {
		ret = rs_post_recv(us);
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

static void rs_free_iomappings(struct usocket *us)
{
	struct rs_iomap_mr *iomr;

	while (!dlist_empty(&us->iomap_list)) {
		iomr = container_of(us->iomap_list.next,
				    struct rs_iomap_mr, entry);
		riounmap(us->index, iomr->mr->addr, iomr->mr->length);
	}
	while (!dlist_empty(&us->iomap_queue)) {
		iomr = container_of(us->iomap_queue.next,
				    struct rs_iomap_mr, entry);
		riounmap(us->index, iomr->mr->addr, iomr->mr->length);
	}
}

static void rs_free(struct usocket *us)
{
	if (us->index >= 0)
		rs_remove(us);

	if (us->rmsg)
		free(us->rmsg);

	if (us->sbuf) {
		if (us->smr)
			rdma_dereg_mr(us->smr);
		free(us->sbuf);
	}

	if (us->rbuf) {
		if (us->rmr)
			rdma_dereg_mr(us->rmr);
		free(us->rbuf);
	}

	if (us->target_buffer_list) {
		if (us->target_mr)
			rdma_dereg_mr(us->target_mr);
		free(us->target_buffer_list);
	}

	if (us->cm_id) {
		rs_free_iomappings(us);
		if (us->cm_id->qp)
			rdma_destroy_qp(us->cm_id);
		rdma_destroy_id(us->cm_id);
	}

	fastlock_destroy(&us->iomap_lock);
	fastlock_destroy(&us->cq_wait_lock);
	fastlock_destroy(&us->cq_lock);
	fastlock_destroy(&us->rlock);
	fastlock_destroy(&us->slock);
	free(us);
}

static void rs_set_conn_data(struct usocket *us, struct rdma_conn_param *param,
			     struct rs_conn_data *conn)
{
	conn->version = 1;
	conn->flags = RS_CONN_FLAG_IOMAP |
		      (rs_host_is_net() ? RS_CONN_FLAG_NET : 0);
	conn->credits = htons(us->rq_size);
	memset(conn->reserved, 0, sizeof conn->reserved);
	conn->target_iomap_size = (uint8_t) rs_value_to_scale(us->target_iomap_size, 8);

	conn->target_sgl.addr = htonll((uintptr_t) us->target_sgl);
	conn->target_sgl.length = htonl(RS_SGL_SIZE);
	conn->target_sgl.key = htonl(us->target_mr->rkey);

	conn->data_buf.addr = htonll((uintptr_t) us->rbuf);
	conn->data_buf.length = htonl(us->rbuf_size >> 1);
	conn->data_buf.key = htonl(us->rmr->rkey);

	param->private_data = conn;
	param->private_data_len = sizeof *conn;
}

static void rs_save_conn_data(struct usocket *us, struct rs_conn_data *conn)
{
	us->remote_sgl.addr = ntohll(conn->target_sgl.addr);
	us->remote_sgl.length = ntohl(conn->target_sgl.length);
	us->remote_sgl.key = ntohl(conn->target_sgl.key);
	us->remote_sge = 1;
	if ((rs_host_is_net() && !(conn->flags & RS_CONN_FLAG_NET)) ||
	    (!rs_host_is_net() && (conn->flags & RS_CONN_FLAG_NET)))
		us->opts = RS_OPT_SWAP_SGL;

	if (conn->flags & RS_CONN_FLAG_IOMAP) {
		us->remote_iomap.addr = us->remote_sgl.addr +
					sizeof(us->remote_sgl) * us->remote_sgl.length;
		us->remote_iomap.length = rs_scale_to_value(conn->target_iomap_size, 8);
		us->remote_iomap.key = us->remote_sgl.key;
	}

	us->target_sgl[0].addr = ntohll(conn->data_buf.addr);
	us->target_sgl[0].length = ntohl(conn->data_buf.length);
	us->target_sgl[0].key = ntohl(conn->data_buf.key);

	us->sseq_comp = ntohs(conn->credits);
}

int usocket(int domain, int type, int protocol)
{
	struct usocket *us;
	int ret;

	if ((domain != PF_INET && domain != PF_INET6) ||
	    (type != SOCK_STREAM) || (protocol && protocol != IPPROTO_TCP))
		return ERR(ENOTSUP);

	rs_configure();
	us = rs_alloc(NULL);
	if (!us)
		return ERR(ENOMEM);

	ret = rdma_create_id(NULL, &us->cm_id, us, RDMA_PS_TCP);
	if (ret)
		goto err;

	ret = rs_insert(us);
	if (ret < 0)
		goto err;

	us->cm_id->route.addr.src_addr.sa_family = domain;
	return us->index;

err:
	rs_free(us);
	return ret;
}

int rbind(int socket, const struct sockaddr *addr, socklen_t addrlen)
{
	struct usocket *us;
	int ret;

	us = idm_at(&idm, socket);
	ret = rdma_bind_addr(us->cm_id, (struct sockaddr *) addr);
	if (!ret)
		us->state = rs_bound;
	return ret;
}

int rlisten(int socket, int backlog)
{
	struct usocket *us;
	int ret;

	us = idm_at(&idm, socket);
	ret = rdma_listen(us->cm_id, backlog);
	if (!ret)
		us->state = rs_listening;
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
	struct usocket *us, *new_rs;
	struct rdma_conn_param param;
	struct rs_conn_data *creq, cresp;
	int ret;

	us = idm_at(&idm, socket);
	new_rs = rs_alloc(us);
	if (!new_rs)
		return ERR(ENOMEM);

	ret = rdma_get_request(us->cm_id, &new_rs->cm_id);
	if (ret)
		goto err;

	ret = rs_insert(new_rs);
	if (ret < 0)
		goto err;

	creq = (struct rs_conn_data *) new_rs->cm_id->event->param.conn.private_data;
	if (creq->version != 1) {
		ret = ERR(ENOTSUP);
		goto err;
	}

	if (us->fd_flags & O_NONBLOCK)
		rs_set_nonblocking(new_rs, O_NONBLOCK);

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

static int rs_do_connect(struct usocket *us)
{
	struct rdma_conn_param param;
	struct rs_conn_data creq, *cresp;
	int to, ret;

	switch (us->state) {
	case rs_init:
	case rs_bound:
resolve_addr:
		to = 1000 << us->retries++;
		ret = rdma_resolve_addr(us->cm_id, NULL,
					&us->cm_id->route.addr.dst_addr, to);
		if (!ret)
			goto resolve_route;
		if (errno == EAGAIN || errno == EWOULDBLOCK)
			us->state = rs_resolving_addr;
		break;
	case rs_resolving_addr:
		ret = ucma_complete(us->cm_id);
		if (ret) {
			if (errno == ETIMEDOUT && us->retries <= RS_CONN_RETRIES)
				goto resolve_addr;
			break;
		}

		us->retries = 0;
resolve_route:
		to = 1000 << us->retries++;
		ret = rdma_resolve_route(us->cm_id, to);
		if (!ret)
			goto do_connect;
		if (errno == EAGAIN || errno == EWOULDBLOCK)
			us->state = rs_resolving_route;
		break;
	case rs_resolving_route:
		ret = ucma_complete(us->cm_id);
		if (ret) {
			if (errno == ETIMEDOUT && us->retries <= RS_CONN_RETRIES)
				goto resolve_route;
			break;
		}
do_connect:
		ret = rs_create_ep(us);
		if (ret)
			break;

		memset(&param, 0, sizeof param);
		rs_set_conn_data(us, &param, &creq);
		param.flow_control = 1;
		param.retry_count = 7;
		param.rnr_retry_count = 7;
		us->retries = 0;

		ret = rdma_connect(us->cm_id, &param);
		if (!ret)
			goto connected;
		if (errno == EAGAIN || errno == EWOULDBLOCK)
			us->state = rs_connecting;
		break;
	case rs_connecting:
		ret = ucma_complete(us->cm_id);
		if (ret)
			break;
connected:
		cresp = (struct rs_conn_data *) us->cm_id->event->param.conn.private_data;
		if (cresp->version != 1) {
			ret = ERR(ENOTSUP);
			break;
		}

		rs_save_conn_data(us, cresp);
		us->state = rs_connect_rdwr;
		break;
	case rs_accepting:
		if (!(us->fd_flags & O_NONBLOCK))
			rs_set_nonblocking(us, 0);

		ret = ucma_complete(us->cm_id);
		if (ret)
			break;

		us->state = rs_connect_rdwr;
		break;
	default:
		ret = ERR(EINVAL);
		break;
	}

	if (ret) {
		if (errno == EAGAIN || errno == EWOULDBLOCK) {
			errno = EINPROGRESS;
		} else {
			us->state = rs_connect_error;
			us->err = errno;
		}
	}
	return ret;
}

int rconnect(int socket, const struct sockaddr *addr, socklen_t addrlen)
{
	struct usocket *us;

	us = idm_at(&idm, socket);
	memcpy(&us->cm_id->route.addr.dst_addr, addr, addrlen);
	return rs_do_connect(us);
}

static int rs_post_write_msg(struct usocket *us,
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

	return rdma_seterrno(ibv_post_send(us->cm_id->qp, &wr, &bad));
}

static int rs_post_write(struct usocket *us,
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

	return rdma_seterrno(ibv_post_send(us->cm_id->qp, &wr, &bad));
}

/*
 * Update target SGE before sending data.  Otherwise the remote side may
 * update the entry before we do.
 */
static int rs_write_data(struct usocket *us,
			 struct ibv_sge *sgl, int nsge,
			 uint32_t length, int flags)
{
	uint64_t addr;
	uint32_t rkey;

	us->sseq_no++;
	us->sqe_avail--;
	us->sbuf_bytes_avail -= length;

	addr = us->target_sgl[us->target_sge].addr;
	rkey = us->target_sgl[us->target_sge].key;

	us->target_sgl[us->target_sge].addr += length;
	us->target_sgl[us->target_sge].length -= length;

	if (!us->target_sgl[us->target_sge].length) {
		if (++us->target_sge == RS_SGL_SIZE)
			us->target_sge = 0;
	}

	return rs_post_write_msg(us, sgl, nsge, rs_msg_set(RS_OP_DATA, length),
				 flags, addr, rkey);
}

static int rs_write_direct(struct usocket *us, struct rs_iomap *iom, uint64_t offset,
			   struct ibv_sge *sgl, int nsge, uint32_t length, int flags)
{
	uint64_t addr;

	us->sqe_avail--;
	us->sbuf_bytes_avail -= length;

	addr = iom->sge.addr + offset - iom->offset;
	return rs_post_write(us, sgl, nsge, rs_msg_set(RS_OP_WRITE, length),
			     flags, addr, iom->sge.key);
}

static int rs_write_iomap(struct usocket *us, struct rs_iomap_mr *iomr,
			  struct ibv_sge *sgl, int nsge, int flags)
{
	uint64_t addr;

	us->sseq_no++;
	us->sqe_avail--;
	us->sbuf_bytes_avail -= sizeof(struct rs_iomap);

	addr = us->remote_iomap.addr + iomr->index * sizeof(struct rs_iomap);
	return rs_post_write_msg(us, sgl, nsge, rs_msg_set(RS_OP_IOMAP_SGL, iomr->index),
			         flags, addr, us->remote_iomap.key);
}

static uint32_t rs_sbuf_left(struct usocket *us)
{
	return (uint32_t) (((uint64_t) (uintptr_t) &us->sbuf[us->sbuf_size]) -
			   us->ssgl[0].addr);
}

static void rs_send_credits(struct usocket *us)
{
	struct ibv_sge ibsge;
	struct rs_sge sge;

	us->ctrl_avail--;
	us->rseq_comp = us->rseq_no + (us->rq_size >> 1);
	if (us->rbuf_bytes_avail >= (us->rbuf_size >> 1)) {
		if (!(us->opts & RS_OPT_SWAP_SGL)) {
			sge.addr = (uintptr_t) &us->rbuf[us->rbuf_free_offset];
			sge.key = us->rmr->rkey;
			sge.length = us->rbuf_size >> 1;
		} else {
			sge.addr = bswap_64((uintptr_t) &us->rbuf[us->rbuf_free_offset]);
			sge.key = bswap_32(us->rmr->rkey);
			sge.length = bswap_32(us->rbuf_size >> 1);
		}

		ibsge.addr = (uintptr_t) &sge;
		ibsge.lkey = 0;
		ibsge.length = sizeof(sge);

		rs_post_write_msg(us, &ibsge, 1,
				  rs_msg_set(RS_OP_SGL, us->rseq_no + us->rq_size),
				  IBV_SEND_INLINE,
				  us->remote_sgl.addr +
				  us->remote_sge * sizeof(struct rs_sge),
				  us->remote_sgl.key);

		us->rbuf_bytes_avail -= us->rbuf_size >> 1;
		us->rbuf_free_offset += us->rbuf_size >> 1;
		if (us->rbuf_free_offset >= us->rbuf_size)
			us->rbuf_free_offset = 0;
		if (++us->remote_sge == us->remote_sgl.length)
			us->remote_sge = 0;
	} else {
		rs_post_write_msg(us, NULL, 0,
				  rs_msg_set(RS_OP_SGL, us->rseq_no + us->rq_size),
				  0, 0, 0);
	}
}

static int rs_give_credits(struct usocket *us)
{
	return ((us->rbuf_bytes_avail >= (us->rbuf_size >> 1)) ||
	        ((short) ((short) us->rseq_no - (short) us->rseq_comp) >= 0)) &&
	       us->ctrl_avail && (us->state & rs_connected);
}

static void rs_update_credits(struct usocket *us)
{
	if (rs_give_credits(us))
		rs_send_credits(us);
}

static int rs_poll_cq(struct usocket *us)
{
	struct ibv_wc wc;
	uint32_t imm_data;
	int ret, rcnt = 0;

	while ((ret = ibv_poll_cq(us->cm_id->recv_cq, 1, &wc)) > 0) {
		if (wc.wr_id == RS_RECV_WR_ID) {
			if (wc.status != IBV_WC_SUCCESS)
				continue;
			rcnt++;

			imm_data = ntohl(wc.imm_data);
			switch (rs_msg_op(imm_data)) {
			case RS_OP_SGL:
				us->sseq_comp = (uint16_t) rs_msg_data(imm_data);
				break;
			case RS_OP_IOMAP_SGL:
				/* The iomap was updated, that's nice to know. */
				break;
			case RS_OP_CTRL:
				if (rs_msg_data(imm_data) == RS_CTRL_DISCONNECT) {
					us->state = rs_disconnected;
					return 0;
				} else if (rs_msg_data(imm_data) == RS_CTRL_SHUTDOWN) {
					us->state &= ~rs_connect_rd;
				}
				break;
			case RS_OP_WRITE:
				/* We really shouldn't be here. */
				break;
			default:
				us->rmsg[us->rmsg_tail].op = rs_msg_op(imm_data);
				us->rmsg[us->rmsg_tail].data = rs_msg_data(imm_data);
				if (++us->rmsg_tail == us->rq_size + 1)
					us->rmsg_tail = 0;
				break;
			}
		} else {
			switch  (rs_msg_op((uint32_t) wc.wr_id)) {
			case RS_OP_SGL:
				us->ctrl_avail++;
				break;
			case RS_OP_CTRL:
				us->ctrl_avail++;
				if (rs_msg_data((uint32_t) wc.wr_id) == RS_CTRL_DISCONNECT)
					us->state = rs_disconnected;
				break;
			case RS_OP_IOMAP_SGL:
				us->sqe_avail++;
				us->sbuf_bytes_avail += sizeof(struct rs_iomap);
				break;
			default:
				us->sqe_avail++;
				us->sbuf_bytes_avail += rs_msg_data((uint32_t) wc.wr_id);
				break;
			}
			if (wc.status != IBV_WC_SUCCESS && (us->state & rs_connected)) {
				us->state = rs_error;
				us->err = EIO;
			}
		}
	}

	if (us->state & rs_connected) {
		while (!ret && rcnt--)
			ret = rs_post_recv(us);

		if (ret) {
			us->state = rs_error;
			us->err = errno;
		}
	}
	return ret;
}

static int rs_get_cq_event(struct usocket *us)
{
	struct ibv_cq *cq;
	void *context;
	int ret;

	if (!us->cq_armed)
		return 0;

	ret = ibv_get_cq_event(us->cm_id->recv_cq_channel, &cq, &context);
	if (!ret) {
		ibv_ack_cq_events(us->cm_id->recv_cq, 1);
		us->cq_armed = 0;
	} else if (errno != EAGAIN) {
		us->state = rs_error;
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
static int rs_process_cq(struct usocket *us, int nonblock, int (*test)(struct usocket *us))
{
	int ret;

	fastlock_acquire(&us->cq_lock);
	do {
		rs_update_credits(us);
		ret = rs_poll_cq(us);
		if (test(us)) {
			ret = 0;
			break;
		} else if (ret) {
			break;
		} else if (nonblock) {
			ret = ERR(EWOULDBLOCK);
		} else if (!us->cq_armed) {
			ibv_req_notify_cq(us->cm_id->recv_cq, 0);
			us->cq_armed = 1;
		} else {
			rs_update_credits(us);
			fastlock_acquire(&us->cq_wait_lock);
			fastlock_release(&us->cq_lock);

			ret = rs_get_cq_event(us);
			fastlock_release(&us->cq_wait_lock);
			fastlock_acquire(&us->cq_lock);
		}
	} while (!ret);

	rs_update_credits(us);
	fastlock_release(&us->cq_lock);
	return ret;
}

static int rs_get_comp(struct usocket *us, int nonblock, int (*test)(struct usocket *us))
{
	struct timeval s, e;
	uint32_t poll_time = 0;
	int ret;

	do {
		ret = rs_process_cq(us, 1, test);
		if (!ret || nonblock || errno != EWOULDBLOCK)
			return ret;

		if (!poll_time)
			gettimeofday(&s, NULL);

		gettimeofday(&e, NULL);
		poll_time = (e.tv_sec - s.tv_sec) * 1000000 +
			    (e.tv_usec - s.tv_usec) + 1;
	} while (poll_time <= polling_time);

	ret = rs_process_cq(us, 0, test);
	return ret;
}

static int rs_nonblocking(struct usocket *us, int flags)
{
	return (us->fd_flags & O_NONBLOCK) || (flags & MSG_DONTWAIT);
}

static int rs_is_cq_armed(struct usocket *us)
{
	return us->cq_armed;
}

static int rs_poll_all(struct usocket *us)
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
static int rs_can_send(struct usocket *us)
{
	return us->sqe_avail && (us->sbuf_bytes_avail >= RS_SNDLOWAT) &&
	       (us->sseq_no != us->sseq_comp) &&
	       (us->target_sgl[us->target_sge].length != 0);
}

static int rs_conn_can_send(struct usocket *us)
{
	return rs_can_send(us) || !(us->state & rs_connect_wr);
}

static int rs_conn_can_send_ctrl(struct usocket *us)
{
	return us->ctrl_avail || !(us->state & rs_connected);
}

static int rs_have_rdata(struct usocket *us)
{
	return (us->rmsg_head != us->rmsg_tail);
}

static int rs_conn_have_rdata(struct usocket *us)
{
	return rs_have_rdata(us) || !(us->state & rs_connect_rd);
}

static int rs_conn_all_sends_done(struct usocket *us)
{
	return ((us->sqe_avail + us->ctrl_avail) == us->sq_size) ||
	       !(us->state & rs_connected);
}

static ssize_t rs_peek(struct usocket *us, void *buf, size_t len)
{
	size_t left = len;
	uint32_t end_size, rsize;
	int rmsg_head, rbuf_offset;

	rmsg_head = us->rmsg_head;
	rbuf_offset = us->rbuf_offset;

	for (; left && (rmsg_head != us->rmsg_tail); left -= rsize) {
		if (left < us->rmsg[rmsg_head].data) {
			rsize = left;
		} else {
			rsize = us->rmsg[rmsg_head].data;
			if (++rmsg_head == us->rq_size + 1)
				rmsg_head = 0;
		}

		end_size = us->rbuf_size - rbuf_offset;
		if (rsize > end_size) {
			memcpy(buf, &us->rbuf[rbuf_offset], end_size);
			rbuf_offset = 0;
			buf += end_size;
			rsize -= end_size;
			left -= end_size;
		}
		memcpy(buf, &us->rbuf[rbuf_offset], rsize);
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
	struct usocket *us;
	size_t left = len;
	uint32_t end_size, rsize;
	int ret;

	us = idm_at(&idm, socket);
	if (us->state & rs_opening) {
		ret = rs_do_connect(us);
		if (ret) {
			if (errno == EINPROGRESS)
				errno = EAGAIN;
			return ret;
		}
	}
	fastlock_acquire(&us->rlock);
	do {
		if (!rs_have_rdata(us)) {
			ret = rs_get_comp(us, rs_nonblocking(us, flags),
					  rs_conn_have_rdata);
			if (ret)
				break;
		}

		ret = 0;
		if (flags & MSG_PEEK) {
			left = len - rs_peek(us, buf, left);
			break;
		}

		for (; left && rs_have_rdata(us); left -= rsize) {
			if (left < us->rmsg[us->rmsg_head].data) {
				rsize = left;
				us->rmsg[us->rmsg_head].data -= left;
			} else {
				us->rseq_no++;
				rsize = us->rmsg[us->rmsg_head].data;
				if (++us->rmsg_head == us->rq_size + 1)
					us->rmsg_head = 0;
			}

			end_size = us->rbuf_size - us->rbuf_offset;
			if (rsize > end_size) {
				memcpy(buf, &us->rbuf[us->rbuf_offset], end_size);
				us->rbuf_offset = 0;
				buf += end_size;
				rsize -= end_size;
				left -= end_size;
				us->rbuf_bytes_avail += end_size;
			}
			memcpy(buf, &us->rbuf[us->rbuf_offset], rsize);
			us->rbuf_offset += rsize;
			buf += rsize;
			us->rbuf_bytes_avail += rsize;
		}

	} while (left && (flags & MSG_WAITALL) && (us->state & rs_connect_rd));

	fastlock_release(&us->rlock);
	return ret ? ret : len - left;
}

ssize_t rrecvfrom(int socket, void *buf, size_t len, int flags,
		  struct sockaddr *src_addr, socklen_t *addrlen)
{
	int ret;

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

static int rs_send_iomaps(struct usocket *us, int flags)
{
	struct rs_iomap_mr *iomr;
	struct ibv_sge sge;
	struct rs_iomap iom;
	int ret;

	fastlock_acquire(&us->iomap_lock);
	while (!dlist_empty(&us->iomap_queue)) {
		if (!rs_can_send(us)) {
			ret = rs_get_comp(us, rs_nonblocking(us, flags),
					  rs_conn_can_send);
			if (ret)
				break;
			if (!(us->state & rs_connect_wr)) {
				ret = ERR(ECONNRESET);
				break;
			}
		}

		iomr = container_of(us->iomap_queue.next, struct rs_iomap_mr, entry);
		if (!(us->opts & RS_OPT_SWAP_SGL)) {
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

		if (us->sq_inline >= sizeof iom) {
			sge.addr = (uintptr_t) &iom;
			sge.length = sizeof iom;
			sge.lkey = 0;
			ret = rs_write_iomap(us, iomr, &sge, 1, IBV_SEND_INLINE);
		} else if (rs_sbuf_left(us) >= sizeof iom) {
			memcpy((void *) (uintptr_t) us->ssgl[0].addr, &iom, sizeof iom);
			us->ssgl[0].length = sizeof iom;
			ret = rs_write_iomap(us, iomr, us->ssgl, 1, 0);
			if (rs_sbuf_left(us) > sizeof iom)
				us->ssgl[0].addr += sizeof iom;
			else
				us->ssgl[0].addr = (uintptr_t) us->sbuf;
		} else {
			us->ssgl[0].length = rs_sbuf_left(us);
			memcpy((void *) (uintptr_t) us->ssgl[0].addr, &iom,
				us->ssgl[0].length);
			us->ssgl[1].length = sizeof iom - us->ssgl[0].length;
			memcpy(us->sbuf, ((void *) &iom) + us->ssgl[0].length,
			       us->ssgl[1].length);
			ret = rs_write_iomap(us, iomr, us->ssgl, 2, 0);
			us->ssgl[0].addr = (uintptr_t) us->sbuf + us->ssgl[1].length;
		}
		dlist_remove(&iomr->entry);
		dlist_insert_tail(&iomr->entry, &us->iomap_list);
		if (ret)
			break;
	}

	us->iomap_pending = !dlist_empty(&us->iomap_queue);
	fastlock_release(&us->iomap_lock);
	return ret;
}

/*
 * We overlap sending the data, by posting a small work request immediately,
 * then increasing the size of the send on each iteration.
 */
ssize_t rsend(int socket, const void *buf, size_t len, int flags)
{
	struct usocket *us;
	struct ibv_sge sge;
	size_t left = len;
	uint32_t xfer_size, olen = RS_OLAP_START_SIZE;
	int ret = 0;

	us = idm_at(&idm, socket);
	if (us->state & rs_opening) {
		ret = rs_do_connect(us);
		if (ret) {
			if (errno == EINPROGRESS)
				errno = EAGAIN;
			return ret;
		}
	}

	fastlock_acquire(&us->slock);
	if (us->iomap_pending) {
		ret = rs_send_iomaps(us, flags);
		if (ret)
			goto out;
	}
	for (; left; left -= xfer_size, buf += xfer_size) {
		if (!rs_can_send(us)) {
			ret = rs_get_comp(us, rs_nonblocking(us, flags),
					  rs_conn_can_send);
			if (ret)
				break;
			if (!(us->state & rs_connect_wr)) {
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

		if (xfer_size > us->sbuf_bytes_avail)
			xfer_size = us->sbuf_bytes_avail;
		if (xfer_size > us->target_sgl[us->target_sge].length)
			xfer_size = us->target_sgl[us->target_sge].length;

		if (xfer_size <= us->sq_inline) {
			sge.addr = (uintptr_t) buf;
			sge.length = xfer_size;
			sge.lkey = 0;
			ret = rs_write_data(us, &sge, 1, xfer_size, IBV_SEND_INLINE);
		} else if (xfer_size <= rs_sbuf_left(us)) {
			memcpy((void *) (uintptr_t) us->ssgl[0].addr, buf, xfer_size);
			us->ssgl[0].length = xfer_size;
			ret = rs_write_data(us, us->ssgl, 1, xfer_size, 0);
			if (xfer_size < rs_sbuf_left(us))
				us->ssgl[0].addr += xfer_size;
			else
				us->ssgl[0].addr = (uintptr_t) us->sbuf;
		} else {
			us->ssgl[0].length = rs_sbuf_left(us);
			memcpy((void *) (uintptr_t) us->ssgl[0].addr, buf,
				us->ssgl[0].length);
			us->ssgl[1].length = xfer_size - us->ssgl[0].length;
			memcpy(us->sbuf, buf + us->ssgl[0].length, us->ssgl[1].length);
			ret = rs_write_data(us, us->ssgl, 2, xfer_size, 0);
			us->ssgl[0].addr = (uintptr_t) us->sbuf + us->ssgl[1].length;
		}
		if (ret)
			break;
	}
out:
	fastlock_release(&us->slock);

	return (ret && left == len) ? ret : len - left;
}

ssize_t rsendto(int socket, const void *buf, size_t len, int flags,
		const struct sockaddr *dest_addr, socklen_t addrlen)
{
	if (dest_addr || addrlen)
		return ERR(EISCONN);

	return rsend(socket, buf, len, flags);
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
	struct usocket *us;
	const struct iovec *cur_iov;
	size_t left, len, offset = 0;
	uint32_t xfer_size, olen = RS_OLAP_START_SIZE;
	int i, ret = 0;

	us = idm_at(&idm, socket);
	if (us->state & rs_opening) {
		ret = rs_do_connect(us);
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

	fastlock_acquire(&us->slock);
	if (us->iomap_pending) {
		ret = rs_send_iomaps(us, flags);
		if (ret)
			goto out;
	}
	for (; left; left -= xfer_size) {
		if (!rs_can_send(us)) {
			ret = rs_get_comp(us, rs_nonblocking(us, flags),
					  rs_conn_can_send);
			if (ret)
				break;
			if (!(us->state & rs_connect_wr)) {
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

		if (xfer_size > us->sbuf_bytes_avail)
			xfer_size = us->sbuf_bytes_avail;
		if (xfer_size > us->target_sgl[us->target_sge].length)
			xfer_size = us->target_sgl[us->target_sge].length;

		if (xfer_size <= rs_sbuf_left(us)) {
			rs_copy_iov((void *) (uintptr_t) us->ssgl[0].addr,
				    &cur_iov, &offset, xfer_size);
			us->ssgl[0].length = xfer_size;
			ret = rs_write_data(us, us->ssgl, 1, xfer_size,
					    xfer_size <= us->sq_inline ? IBV_SEND_INLINE : 0);
			if (xfer_size < rs_sbuf_left(us))
				us->ssgl[0].addr += xfer_size;
			else
				us->ssgl[0].addr = (uintptr_t) us->sbuf;
		} else {
			us->ssgl[0].length = rs_sbuf_left(us);
			rs_copy_iov((void *) (uintptr_t) us->ssgl[0].addr, &cur_iov,
				    &offset, us->ssgl[0].length);
			us->ssgl[1].length = xfer_size - us->ssgl[0].length;
			rs_copy_iov(us->sbuf, &cur_iov, &offset, us->ssgl[1].length);
			ret = rs_write_data(us, us->ssgl, 2, xfer_size,
					    xfer_size <= us->sq_inline ? IBV_SEND_INLINE : 0);
			us->ssgl[0].addr = (uintptr_t) us->sbuf + us->ssgl[1].length;
		}
		if (ret)
			break;
	}
out:
	fastlock_release(&us->slock);

	return (ret && left == len) ? ret : len - left;
}

ssize_t rsendmsg(int socket, const struct msghdr *msg, int flags)
{
	if (msg->msg_control && msg->msg_controllen)
		return ERR(ENOTSUP);

	return rsendv(socket, msg->msg_iov, (int) msg->msg_iovlen, msg->msg_flags);
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

static int rs_poll_rs(struct usocket *us, int events,
		      int nonblock, int (*test)(struct usocket *us))
{
	struct pollfd fds;
	short revents;
	int ret;

check_cq:
	if ((us->state & rs_connected) || (us->state == rs_disconnected) ||
	    (us->state & rs_error)) {
		rs_process_cq(us, nonblock, test);

		revents = 0;
		if ((events & POLLIN) && rs_conn_have_rdata(us))
			revents |= POLLIN;
		if ((events & POLLOUT) && rs_can_send(us))
			revents |= POLLOUT;
		if (!(us->state & rs_connected)) {
			if (us->state == rs_disconnected)
				revents |= POLLHUP;
			else
				revents |= POLLERR;
		}

		return revents;
	}

	if (us->state == rs_listening) {
		fds.fd = us->cm_id->channel->fd;
		fds.events = events;
		fds.revents = 0;
		poll(&fds, 1, 0);
		return fds.revents;
	}

	if (us->state & rs_opening) {
		ret = rs_do_connect(us);
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

	if (us->state == rs_connect_error)
		return (us->err && events & POLLOUT) ? POLLOUT : 0;

	return 0;
}

static int rs_poll_check(struct pollfd *fds, nfds_t nfds)
{
	struct usocket *us;
	int i, cnt = 0;

	for (i = 0; i < nfds; i++) {
		us = idm_lookup(&idm, fds[i].fd);
		if (us)
			fds[i].revents = rs_poll_rs(us, fds[i].events, 1, rs_poll_all);
		else
			poll(&fds[i], 1, 0);

		if (fds[i].revents)
			cnt++;
	}
	return cnt;
}

static int rs_poll_arm(struct pollfd *rfds, struct pollfd *fds, nfds_t nfds)
{
	struct usocket *us;
	int i;

	for (i = 0; i < nfds; i++) {
		us = idm_lookup(&idm, fds[i].fd);
		if (us) {
			fds[i].revents = rs_poll_rs(us, fds[i].events, 0, rs_is_cq_armed);
			if (fds[i].revents)
				return 1;

			if (us->state >= rs_connected)
				rfds[i].fd = us->cm_id->recv_cq_channel->fd;
			else
				rfds[i].fd = us->cm_id->channel->fd;

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
	struct usocket *us;
	int i, cnt = 0;

	for (i = 0; i < nfds; i++) {
		if (!rfds[i].revents)
			continue;

		us = idm_lookup(&idm, fds[i].fd);
		if (us) {
			rs_get_cq_event(us);
			fds[i].revents = rs_poll_rs(us, fds[i].events, 1, rs_poll_all);
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
 * Note that we may receive events on an usocket that may not be reported
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
	struct usocket *us;
	int ctrl, ret = 0;

	us = idm_at(&idm, socket);
	if (how == SHUT_RD) {
		us->state &= ~rs_connect_rd;
		return 0;
	}

	if (us->fd_flags & O_NONBLOCK)
		rs_set_nonblocking(us, 0);

	if (us->state & rs_connected) {
		if (how == SHUT_RDWR) {
			ctrl = RS_CTRL_DISCONNECT;
			us->state &= ~(rs_connect_rd | rs_connect_wr);
		} else {
			us->state &= ~rs_connect_wr;
			ctrl = (us->state & rs_connect_rd) ?
				RS_CTRL_SHUTDOWN : RS_CTRL_DISCONNECT;
		}
		if (!us->ctrl_avail) {
			ret = rs_process_cq(us, 0, rs_conn_can_send_ctrl);
			if (ret)
				return ret;
		}

		if ((us->state & rs_connected) && us->ctrl_avail) {
			us->ctrl_avail--;
			ret = rs_post_write_msg(us, NULL, 0,
						rs_msg_set(RS_OP_CTRL, ctrl), 0, 0, 0);
		}
	}

	if (us->state & rs_connected)
		rs_process_cq(us, 0, rs_conn_all_sends_done);

	if ((us->fd_flags & O_NONBLOCK) && (us->state & rs_connected))
		rs_set_nonblocking(us, 1);

	return 0;
}

int rclose(int socket)
{
	struct usocket *us;

	us = idm_at(&idm, socket);
	if (us->state & rs_connected)
		rshutdown(socket, SHUT_RDWR);

	rs_free(us);
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
	struct usocket *us;

	us = idm_at(&idm, socket);
	rs_copy_addr(addr, rdma_get_peer_addr(us->cm_id), addrlen);
	return 0;
}

int rgetsockname(int socket, struct sockaddr *addr, socklen_t *addrlen)
{
	struct usocket *us;

	us = idm_at(&idm, socket);
	rs_copy_addr(addr, rdma_get_local_addr(us->cm_id), addrlen);
	return 0;
}

int rsetsockopt(int socket, int level, int optname,
		const void *optval, socklen_t optlen)
{
	struct usocket *us;
	int ret, opt_on = 0;
	uint64_t *opts = NULL;

	ret = ERR(ENOTSUP);
	us = idm_at(&idm, socket);
	switch (level) {
	case SOL_SOCKET:
		opts = &us->so_opts;
		switch (optname) {
		case SO_REUSEADDR:
			ret = rdma_set_option(us->cm_id, RDMA_OPTION_ID,
					      RDMA_OPTION_ID_REUSEADDR,
					      (void *) optval, optlen);
			if (ret && ((errno == ENOSYS) || ((us->state != rs_init) &&
			    us->cm_id->context &&
			    (us->cm_id->verbs->device->transport_type == IBV_TRANSPORT_IB))))
				ret = 0;
			opt_on = *(int *) optval;
			break;
		case SO_RCVBUF:
			if (!us->rbuf)
				us->rbuf_size = (*(uint32_t *) optval) << 1;
			ret = 0;
			break;
		case SO_SNDBUF:
			if (!us->sbuf)
				us->sbuf_size = (*(uint32_t *) optval) << 1;
			if (us->sbuf_size < RS_SNDLOWAT)
				us->sbuf_size = RS_SNDLOWAT << 1;
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
		opts = &us->tcp_opts;
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
		opts = &us->ipv6_opts;
		switch (optname) {
		case IPV6_V6ONLY:
			ret = rdma_set_option(us->cm_id, RDMA_OPTION_ID,
					      RDMA_OPTION_ID_AFONLY,
					      (void *) optval, optlen);
			opt_on = *(int *) optval;
			break;
		default:
			break;
		}
		break;
	case SOL_RDMA:
		if (us->state >= rs_opening) {
			ret = ERR(EINVAL);
			break;
		}

		switch (optname) {
		case RDMA_SQSIZE:
			us->sq_size = min((*(uint32_t *) optval), RS_QP_MAX_SIZE);
			break;
		case RDMA_RQSIZE:
			us->rq_size = min((*(uint32_t *) optval), RS_QP_MAX_SIZE);
			break;
		case RDMA_INLINE:
			us->sq_inline = min(*(uint32_t *) optval, RS_QP_MAX_SIZE);
			if (us->sq_inline < RS_MIN_INLINE)
				us->sq_inline = RS_MIN_INLINE;
			break;
		case RDMA_IOMAPSIZE:
			us->target_iomap_size = (uint16_t) rs_scale_to_value(
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
	struct usocket *us;
	int ret = 0;

	us = idm_at(&idm, socket);
	switch (level) {
	case SOL_SOCKET:
		switch (optname) {
		case SO_REUSEADDR:
		case SO_KEEPALIVE:
		case SO_OOBINLINE:
			*((int *) optval) = !!(us->so_opts & (1 << optname));
			*optlen = sizeof(int);
			break;
		case SO_RCVBUF:
			*((int *) optval) = us->rbuf_size;
			*optlen = sizeof(int);
			break;
		case SO_SNDBUF:
			*((int *) optval) = us->sbuf_size;
			*optlen = sizeof(int);
			break;
		case SO_LINGER:
			/* Value is inverted so default so_opt = 0 is on */
			((struct linger *) optval)->l_onoff =
					!(us->so_opts & (1 << optname));
			((struct linger *) optval)->l_linger = 0;
			*optlen = sizeof(struct linger);
			break;
		case SO_ERROR:
			*((int *) optval) = us->err;
			*optlen = sizeof(int);
			us->err = 0;
			break;
		default:
			ret = ENOTSUP;
			break;
		}
		break;
	case IPPROTO_TCP:
		switch (optname) {
		case TCP_NODELAY:
			*((int *) optval) = !!(us->tcp_opts & (1 << optname));
			*optlen = sizeof(int);
			break;
		case TCP_MAXSEG:
			*((int *) optval) = (us->cm_id && us->cm_id->route.num_paths) ?
					    1 << (7 + us->cm_id->route.path_rec->mtu) :
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
			*((int *) optval) = !!(us->ipv6_opts & (1 << optname));
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
			*((int *) optval) = us->sq_size;
			*optlen = sizeof(int);
			break;
		case RDMA_RQSIZE:
			*((int *) optval) = us->rq_size;
			*optlen = sizeof(int);
			break;
		case RDMA_INLINE:
			*((int *) optval) = us->sq_inline;
			*optlen = sizeof(int);
			break;
		case RDMA_IOMAPSIZE:
			*((int *) optval) = us->target_iomap_size;
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

int ufcntl(int socket, int cmd, ... /* arg */ )
{
	struct usocket *us;
	va_list args;
	long param;
	int ret = 0;

	us = idm_at(&idm, socket);
	va_start(args, cmd);
	switch (cmd) {
	case F_GETFL:
		ret = (int) us->fd_flags;
		break;
	case F_SETFL:
		param = va_arg(args, long);
		if (param & O_NONBLOCK)
			ret = rs_set_nonblocking(us, O_NONBLOCK);

		if (!ret)
			us->fd_flags |= param;
		break;
	default:
		ret = ERR(ENOTSUP);
		break;
	}
	va_end(args);
	return ret;
}
