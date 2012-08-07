/*
 * Copyright (c) 2011-2012 Intel Corporation.  All rights reserved.
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
#include <sys/stat.h>
#include <stdarg.h>
#include <dlfcn.h>
#include <netdb.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <unistd.h>
#include <semaphore.h>
#include <stdio.h>

#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>
#include <rdma/rsocket.h>
#include "cma.h"
#include "indexer.h"

struct socket_calls {
	int (*socket)(int domain, int type, int protocol);
	int (*bind)(int socket, const struct sockaddr *addr, socklen_t addrlen);
	int (*listen)(int socket, int backlog);
	int (*accept)(int socket, struct sockaddr *addr, socklen_t *addrlen);
	int (*connect)(int socket, const struct sockaddr *addr, socklen_t addrlen);
	ssize_t (*recv)(int socket, void *buf, size_t len, int flags);
	ssize_t (*recvfrom)(int socket, void *buf, size_t len, int flags,
			    struct sockaddr *src_addr, socklen_t *addrlen);
	ssize_t (*recvmsg)(int socket, struct msghdr *msg, int flags);
	ssize_t (*read)(int socket, void *buf, size_t count);
	ssize_t (*readv)(int socket, const struct iovec *iov, int iovcnt);
	ssize_t (*send)(int socket, const void *buf, size_t len, int flags);
	ssize_t (*sendto)(int socket, const void *buf, size_t len, int flags,
			  const struct sockaddr *dest_addr, socklen_t addrlen);
	ssize_t (*sendmsg)(int socket, const struct msghdr *msg, int flags);
	ssize_t (*write)(int socket, const void *buf, size_t count);
	ssize_t (*writev)(int socket, const struct iovec *iov, int iovcnt);
	int (*poll)(struct pollfd *fds, nfds_t nfds, int timeout);
	int (*shutdown)(int socket, int how);
	int (*close)(int socket);
	int (*getpeername)(int socket, struct sockaddr *addr, socklen_t *addrlen);
	int (*getsockname)(int socket, struct sockaddr *addr, socklen_t *addrlen);
	int (*setsockopt)(int socket, int level, int optname,
			  const void *optval, socklen_t optlen);
	int (*getsockopt)(int socket, int level, int optname,
			  void *optval, socklen_t *optlen);
	int (*fcntl)(int socket, int cmd, ... /* arg */);
	int (*dup2)(int oldfd, int newfd);
	int (*fstat)(int fd, struct stat *buf);
};

static struct socket_calls real;
static struct socket_calls rs;

static struct index_map idm;
static pthread_mutex_t mut = PTHREAD_MUTEX_INITIALIZER;

static int sq_size;
static int rq_size;
static int sq_inline;
static int fork_support;

static FILE fout;

enum fd_type {
	fd_normal,
	fd_rsocket,
	fd_fork
};

struct fd_info {
	enum fd_type type;
	int fd;
	int dupfd;
	atomic_t refcnt;
};

static int fd_open(void)
{
	struct fd_info *fdi;
	int ret, index;

	fdi = calloc(1, sizeof *fdi);
	if (!fdi)
		return ERR(ENOMEM);

	index = open("/dev/null", O_RDONLY);
	if (index < 0) {
		ret = index;
		goto err1;
	}

	fdi->dupfd = -1;
	atomic_init(&fdi->refcnt);
	atomic_set(&fdi->refcnt, 1);
	pthread_mutex_lock(&mut);
	ret = idm_set(&idm, index, fdi);
	pthread_mutex_unlock(&mut);
	if (ret < 0)
		goto err2;

	return index;

err2:
	real.close(index);
err1:
	free(fdi);
	return ret;
}

static void fd_store(int index, int fd, enum fd_type type)
{
	struct fd_info *fdi;

	fdi = idm_at(&idm, index);
	fdi->fd = fd;
	fdi->type = type;
}

static inline enum fd_type fd_get(int index, int *fd)
{
	struct fd_info *fdi;

	fdi = idm_lookup(&idm, index);
	if (fdi) {
		*fd = fdi->fd;
		return fdi->type;

	} else {
		*fd = index;
		return fd_normal;
	}
}

static inline int fd_getd(int index)
{
	struct fd_info *fdi;

	fdi = idm_lookup(&idm, index);
	return fdi ? fdi->fd : index;
}

static inline enum fd_type fd_gett(int index)
{
	struct fd_info *fdi;

	fdi = idm_lookup(&idm, index);
	return fdi ? fdi->type : fd_normal;
}

static enum fd_type fd_close(int index, int *fd)
{
	struct fd_info *fdi;
	enum fd_type type;

	fdi = idm_lookup(&idm, index);
	if (fdi) {
		idm_clear(&idm, index);
		*fd = fdi->fd;
		type = fdi->type;
		real.close(index);
		free(fdi);
	} else {
		*fd = index;
		type = fd_normal;
	}
	return type;
}

void getenv_options(void)
{
	char *var;

	var = getenv("RS_SQ_SIZE");
	if (var)
		sq_size = atoi(var);

	var = getenv("RS_RQ_SIZE");
	if (var)
		rq_size = atoi(var);

	var = getenv("RS_INLINE");
	if (var)
		sq_inline = atoi(var);

	var = getenv("RDMAV_FORK_SAFE");
	if (var)
		fork_support = atoi(var);
}

static void init_preload(void)
{
	static int init;

	/* Quick check without lock */
	if (init)
		return;

	pthread_mutex_lock(&mut);
	if (init)
		goto out;

	real.socket = dlsym(RTLD_NEXT, "socket");
	real.bind = dlsym(RTLD_NEXT, "bind");
	real.listen = dlsym(RTLD_NEXT, "listen");
	real.accept = dlsym(RTLD_NEXT, "accept");
	real.connect = dlsym(RTLD_NEXT, "connect");
	real.recv = dlsym(RTLD_NEXT, "recv");
	real.recvfrom = dlsym(RTLD_NEXT, "recvfrom");
	real.recvmsg = dlsym(RTLD_NEXT, "recvmsg");
	real.read = dlsym(RTLD_NEXT, "read");
	real.readv = dlsym(RTLD_NEXT, "readv");
	real.send = dlsym(RTLD_NEXT, "send");
	real.sendto = dlsym(RTLD_NEXT, "sendto");
	real.sendmsg = dlsym(RTLD_NEXT, "sendmsg");
	real.write = dlsym(RTLD_NEXT, "write");
	real.writev = dlsym(RTLD_NEXT, "writev");
	real.poll = dlsym(RTLD_NEXT, "poll");
	real.shutdown = dlsym(RTLD_NEXT, "shutdown");
	real.close = dlsym(RTLD_NEXT, "close");
	real.getpeername = dlsym(RTLD_NEXT, "getpeername");
	real.getsockname = dlsym(RTLD_NEXT, "getsockname");
	real.setsockopt = dlsym(RTLD_NEXT, "setsockopt");
	real.getsockopt = dlsym(RTLD_NEXT, "getsockopt");
	real.fcntl = dlsym(RTLD_NEXT, "fcntl");
	real.dup2 = dlsym(RTLD_NEXT, "dup2");
	real.fstat = dlsym(RTLD_NEXT, "fstat");

	rs.socket = dlsym(RTLD_DEFAULT, "rsocket");
	rs.bind = dlsym(RTLD_DEFAULT, "rbind");
	rs.listen = dlsym(RTLD_DEFAULT, "rlisten");
	rs.accept = dlsym(RTLD_DEFAULT, "raccept");
	rs.connect = dlsym(RTLD_DEFAULT, "rconnect");
	rs.recv = dlsym(RTLD_DEFAULT, "rrecv");
	rs.recvfrom = dlsym(RTLD_DEFAULT, "rrecvfrom");
	rs.recvmsg = dlsym(RTLD_DEFAULT, "rrecvmsg");
	rs.read = dlsym(RTLD_DEFAULT, "rread");
	rs.readv = dlsym(RTLD_DEFAULT, "rreadv");
	rs.send = dlsym(RTLD_DEFAULT, "rsend");
	rs.sendto = dlsym(RTLD_DEFAULT, "rsendto");
	rs.sendmsg = dlsym(RTLD_DEFAULT, "rsendmsg");
	rs.write = dlsym(RTLD_DEFAULT, "rwrite");
	rs.writev = dlsym(RTLD_DEFAULT, "rwritev");
	rs.poll = dlsym(RTLD_DEFAULT, "rpoll");
	rs.shutdown = dlsym(RTLD_DEFAULT, "rshutdown");
	rs.close = dlsym(RTLD_DEFAULT, "rclose");
	rs.getpeername = dlsym(RTLD_DEFAULT, "rgetpeername");
	rs.getsockname = dlsym(RTLD_DEFAULT, "rgetsockname");
	rs.setsockopt = dlsym(RTLD_DEFAULT, "rsetsockopt");
	rs.getsockopt = dlsym(RTLD_DEFAULT, "rgetsockopt");
	rs.fcntl = dlsym(RTLD_DEFAULT, "rfcntl");

	fout = fopen("rs-out.txt", "w+");
	getenv_options();
	init = 1;
out:
	pthread_mutex_unlock(&mut);
}

/*
 * We currently only handle copying a few common values.
 */
static int copysockopts(int dfd, int sfd, struct socket_calls *dapi,
			struct socket_calls *sapi)
{
	socklen_t len;
	int param, ret;

	ret = sapi->fcntl(sfd, F_GETFL);
	if (ret > 0)
		ret = dapi->fcntl(dfd, F_SETFL, ret);
	if (ret)
		return ret;

	len = sizeof param;
	ret = sapi->getsockopt(sfd, SOL_SOCKET, SO_REUSEADDR, &param, &len);
	if (param && !ret)
		ret = dapi->setsockopt(dfd, SOL_SOCKET, SO_REUSEADDR, &param, len);
	if (ret)
		return ret;

	len = sizeof param;
	ret = sapi->getsockopt(sfd, IPPROTO_TCP, TCP_NODELAY, &param, &len);
	if (param && !ret)
		ret = dapi->setsockopt(dfd, IPPROTO_TCP, TCP_NODELAY, &param, len);
	if (ret)
		return ret;

	return 0;
}

/*
 * Convert between an rsocket and a normal socket.
 */
static int transpose_socket(int socket, enum fd_type new_type)
{
	socklen_t len = 0;
	int sfd, dfd, param, ret;
	struct socket_calls *sapi, *dapi;

	sfd = fd_getd(socket);
	if (new_type == fd_rsocket) {
		dapi = &rs;
		sapi = &real;
	} else {
		dapi = &real;
		sapi = &rs;
	}

	ret = sapi->getsockname(sfd, NULL, &len);
	if (ret)
		return ret;

	param = (len == sizeof(struct sockaddr_in6)) ? PF_INET6 : PF_INET;
	dfd = dapi->socket(param, SOCK_STREAM, 0);
	if (dfd < 0)
		return dfd;

	ret = copysockopts(dfd, sfd, dapi, sapi);
	if (ret)
		goto err;

	fd_store(socket, dfd, new_type);
	return dfd;

err:
	dapi->close(dfd);
	return ret;
}

/*
 * Use defaults on failure.
 */
void set_rsocket_options(int rsocket)
{
	if (sq_size)
		rsetsockopt(rsocket, SOL_RDMA, RDMA_SQSIZE, &sq_size, sizeof sq_size);

	if (rq_size)
		rsetsockopt(rsocket, SOL_RDMA, RDMA_RQSIZE, &rq_size, sizeof rq_size);

	if (sq_inline)
		rsetsockopt(rsocket, SOL_RDMA, RDMA_INLINE, &sq_inline, sizeof sq_inline);
}

int socket(int domain, int type, int protocol)
{
	static __thread int recursive;
	int index, ret;

	if (recursive)
		goto real;

	init_preload();
	index = fd_open();
	if (index < 0)
		return index;

	recursive = 1;
	ret = rsocket(domain, type, protocol);
	recursive = 0;
	if (ret >= 0) {
		if (fork_support) {
			rclose(ret);
			ret = real.socket(domain, type, protocol);
			if (ret < 0)
				return ret;
			fd_store(index, ret, fd_fork);
		} else {
			fd_store(index, ret, fd_rsocket);
			set_rsocket_options(ret);
		}
		return index;
	}
	fd_close(index, &ret);
real:
	return real.socket(domain, type, protocol);
}

int bind(int socket, const struct sockaddr *addr, socklen_t addrlen)
{
	int fd;
	return (fd_get(socket, &fd) == fd_rsocket) ?
		rbind(fd, addr, addrlen) : real.bind(fd, addr, addrlen);
}

int listen(int socket, int backlog)
{
	int fd;
	return (fd_get(socket, &fd) == fd_rsocket) ?
		rlisten(fd, backlog) : real.listen(fd, backlog);
}

int accept(int socket, struct sockaddr *addr, socklen_t *addrlen)
{
	int fd, index, ret;
	enum fd_type type;

	type = fd_get(socket, &fd);
	if (type == fd_rsocket || type == fd_fork) {
		index = fd_open();
		if (index < 0)
			return index;

		ret = (type == fd_rsocket) ? raccept(fd, addr, addrlen) :
					     real.accept(fd, addr, addrlen);
		if (ret < 0) {
			fd_close(index, &fd);
			return ret;
		}

		fd_store(index, ret, type);
		return index;
	} else {
		return real.accept(fd, addr, addrlen);
	}
}

/*
 * We can't fork RDMA connections and pass them from the parent to the child
 * process.  Instead, we need to establish the RDMA connection after calling
 * fork.  To do this, we delay establishing the RDMA connection until we try
 * to send/receive on the server side.  On the client side, we don't expect
 * to fork, so we switch from a TCP connection to an rsocket when connecting.
 */
static int fork_active(int socket, const struct sockaddr *addr, socklen_t addrlen)
{
	int fd, ret;
	uint32_t msg;
	long flags;

	fd = fd_getd(socket);
	flags = real.fcntl(fd, F_GETFL);
	real.fcntl(fd, F_SETFL, 0);
	ret = real.connect(fd, addr, addrlen);
	if (ret)
		return ret;

	ret = real.recv(fd, &msg, sizeof msg, MSG_PEEK);
	if ((ret != sizeof msg) || msg) {
		fd_store(socket, fd, fd_normal);
		return 0;
	}

	real.fcntl(fd, F_SETFL, flags);
	ret = transpose_socket(socket, fd_rsocket);
	if (ret < 0)
		return ret;

	real.close(fd);
	return rconnect(ret, addr, addrlen);
}

static void fork_passive(int socket)
{
	struct sockaddr_in6 sin6;
	sem_t *sem;
	int lfd, sfd, dfd, ret, param;
	socklen_t len;
	uint32_t msg;

	fd_get(socket, &sfd);

	len = sizeof sin6;
	ret = real.getsockname(sfd, (struct sockaddr *) &sin6, &len);
	if (ret)
		goto out;
	sin6.sin6_flowinfo = sin6.sin6_scope_id = 0;
	memset(&sin6.sin6_addr, 0, sizeof sin6.sin6_addr);

	sem = sem_open("/rsocket_fork", O_CREAT | O_RDWR,
		       S_IRWXU | S_IRWXG, 1);
	if (sem == SEM_FAILED) {
		ret = -1;
		goto out;
	}

	lfd = rsocket(sin6.sin6_family, SOCK_STREAM, 0);
	if (lfd < 0) {
		ret  = lfd;
		goto sclose;
	}

	param = 1;
	rsetsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &param, sizeof param);

	sem_wait(sem);
	ret = rbind(lfd, (struct sockaddr *) &sin6, sizeof sin6);
	if (ret)
		goto lclose;

	ret = rlisten(lfd, 1);
	if (ret)
		goto lclose;

	msg = 0;
	len = real.write(sfd, &msg, sizeof msg);
	if (len != sizeof msg)
		goto lclose;

	dfd = raccept(lfd, NULL, NULL);
	if (dfd < 0) {
		ret  = dfd;
		goto lclose;
	}

	param = 1;
	rsetsockopt(dfd, IPPROTO_TCP, TCP_NODELAY, &param, sizeof param);
	set_rsocket_options(dfd);

	copysockopts(dfd, sfd, &rs, &real);
	real.shutdown(sfd, SHUT_RDWR);
	real.close(sfd);
	fd_store(socket, dfd, fd_rsocket);

lclose:
	rclose(lfd);
	sem_post(sem);
sclose:
	sem_close(sem);
out:
	if (ret)
		fd_store(socket, sfd, fd_normal);
}

static inline enum fd_type fd_fork_get(int index, int *fd)
{
	struct fd_info *fdi;

	fdi = idm_lookup(&idm, index);
	if (fdi) {
		if (fdi->type == fd_fork)
			fork_passive(index);
		*fd = fdi->fd;
		return fdi->type;

	} else {
		*fd = index;
		return fd_normal;
	}
}

int connect(int socket, const struct sockaddr *addr, socklen_t addrlen)
{
	int fd, ret;

	switch (fd_get(socket, &fd)) {
	case fd_fork:
		return fork_active(socket, addr, addrlen);
	case fd_rsocket:
		ret = rconnect(fd, addr, addrlen);
		if (!ret || errno == EINPROGRESS)
			return ret;

		ret = transpose_socket(socket, fd_normal);
		if (ret < 0)
			return ret;

		rclose(fd);
		fd = ret;
		break;
	default:
		break;
	}

	return real.connect(fd, addr, addrlen);
}

ssize_t recv(int socket, void *buf, size_t len, int flags)
{
	int fd;
	return (fd_fork_get(socket, &fd) == fd_rsocket) ?
		rrecv(fd, buf, len, flags) : real.recv(fd, buf, len, flags);
}

ssize_t recvfrom(int socket, void *buf, size_t len, int flags,
		 struct sockaddr *src_addr, socklen_t *addrlen)
{
	int fd;
	return (fd_fork_get(socket, &fd) == fd_rsocket) ?
		rrecvfrom(fd, buf, len, flags, src_addr, addrlen) :
		real.recvfrom(fd, buf, len, flags, src_addr, addrlen);
}

ssize_t recvmsg(int socket, struct msghdr *msg, int flags)
{
	int fd;
	return (fd_fork_get(socket, &fd) == fd_rsocket) ?
		rrecvmsg(fd, msg, flags) : real.recvmsg(fd, msg, flags);
}

ssize_t read(int socket, void *buf, size_t count)
{
	int fd;
	init_preload();
	return (fd_fork_get(socket, &fd) == fd_rsocket) ?
		rread(fd, buf, count) : real.read(fd, buf, count);
}

ssize_t readv(int socket, const struct iovec *iov, int iovcnt)
{
	int fd;
	init_preload();
	return (fd_fork_get(socket, &fd) == fd_rsocket) ?
		rreadv(fd, iov, iovcnt) : real.readv(fd, iov, iovcnt);
}

ssize_t send(int socket, const void *buf, size_t len, int flags)
{
	int fd;
	return (fd_fork_get(socket, &fd) == fd_rsocket) ?
		rsend(fd, buf, len, flags) : real.send(fd, buf, len, flags);
}

ssize_t sendto(int socket, const void *buf, size_t len, int flags,
		const struct sockaddr *dest_addr, socklen_t addrlen)
{
	int fd;
	return (fd_fork_get(socket, &fd) == fd_rsocket) ?
		rsendto(fd, buf, len, flags, dest_addr, addrlen) :
		real.sendto(fd, buf, len, flags, dest_addr, addrlen);
}

ssize_t sendmsg(int socket, const struct msghdr *msg, int flags)
{
	int fd;
	return (fd_fork_get(socket, &fd) == fd_rsocket) ?
		rsendmsg(fd, msg, flags) : real.sendmsg(fd, msg, flags);
}

ssize_t write(int socket, const void *buf, size_t count)
{
	int fd;
	init_preload();
	return (fd_fork_get(socket, &fd) == fd_rsocket) ?
		rwrite(fd, buf, count) : real.write(fd, buf, count);
}

ssize_t writev(int socket, const struct iovec *iov, int iovcnt)
{
	int fd;
	init_preload();
	return (fd_fork_get(socket, &fd) == fd_rsocket) ?
		rwritev(fd, iov, iovcnt) : real.writev(fd, iov, iovcnt);
}

static struct pollfd *fds_alloc(nfds_t nfds)
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

int poll(struct pollfd *fds, nfds_t nfds, int timeout)
{
	struct pollfd *rfds;
	int i, ret;

	init_preload();
	for (i = 0; i < nfds; i++) {
		if (fd_gett(fds[i].fd) == fd_rsocket)
			goto use_rpoll;
	}

	return real.poll(fds, nfds, timeout);

use_rpoll:
	rfds = fds_alloc(nfds);
	if (!rfds)
		return ERR(ENOMEM);

	for (i = 0; i < nfds; i++) {
		rfds[i].fd = fd_getd(fds[i].fd);
		rfds[i].events = fds[i].events;
		rfds[i].revents = 0;
	}

	ret = rpoll(rfds, nfds, timeout);

	for (i = 0; i < nfds; i++)
		fds[i].revents = rfds[i].revents;

	return ret;
}

static void select_to_rpoll(struct pollfd *fds, int *nfds,
			    fd_set *readfds, fd_set *writefds, fd_set *exceptfds)
{
	int fd, events, i = 0;

	for (fd = 0; fd < *nfds; fd++) {
		events = (readfds && FD_ISSET(fd, readfds)) ? POLLIN : 0;
		if (writefds && FD_ISSET(fd, writefds))
			events |= POLLOUT;

		if (events || (exceptfds && FD_ISSET(fd, exceptfds))) {
			fds[i].fd = fd_getd(fd);
			fds[i++].events = events;
		}
	}

	*nfds = i;
}

static int rpoll_to_select(struct pollfd *fds, int nfds,
			   fd_set *readfds, fd_set *writefds, fd_set *exceptfds)
{
	int fd, rfd, i, cnt = 0;

	for (i = 0, fd = 0; i < nfds; fd++) {
		rfd = fd_getd(fd);
		if (rfd != fds[i].fd)
			continue;

		if (readfds && (fds[i].revents & POLLIN)) {
			FD_SET(fd, readfds);
			cnt++;
		}

		if (writefds && (fds[i].revents & POLLOUT)) {
			FD_SET(fd, writefds);
			cnt++;
		}

		if (exceptfds && (fds[i].revents & ~(POLLIN | POLLOUT))) {
			FD_SET(fd, exceptfds);
			cnt++;
		}
		i++;
	}

	return cnt;
}

static int rs_convert_timeout(struct timeval *timeout)
{
	return !timeout ? -1 : timeout->tv_sec * 1000 + timeout->tv_usec / 1000;
}

int select(int nfds, fd_set *readfds, fd_set *writefds,
	   fd_set *exceptfds, struct timeval *timeout)
{
	struct pollfd *fds;
	int ret;

	fds = fds_alloc(nfds);
	if (!fds)
		return ERR(ENOMEM);

	select_to_rpoll(fds, &nfds, readfds, writefds, exceptfds);
	ret = rpoll(fds, nfds, rs_convert_timeout(timeout));

	if (readfds)
		FD_ZERO(readfds);
	if (writefds)
		FD_ZERO(writefds);
	if (exceptfds)
		FD_ZERO(exceptfds);

	if (ret > 0)
		ret = rpoll_to_select(fds, nfds, readfds, writefds, exceptfds);

	return ret;
}

int shutdown(int socket, int how)
{
	int fd;
	return (fd_get(socket, &fd) == fd_rsocket) ?
		rshutdown(fd, how) : real.shutdown(fd, how);
}

int close(int socket)
{
	struct fd_info *fdi;
	int ret;

	init_preload();
	fdi = idm_lookup(&idm, socket);
	if (!fdi)
		return real.close(socket);

	if (fdi->dupfd != -1) {
		ret = close(fdi->dupfd);
		if (ret)
			return ret;
	}

	if (atomic_dec(&fdi->refcnt))
		return 0;

	idm_clear(&idm, socket);
	real.close(socket);
	ret = (fdi->type == fd_rsocket) ? rclose(fdi->fd) : real.close(fdi->fd);
	free(fdi);
	return ret;
}

int getpeername(int socket, struct sockaddr *addr, socklen_t *addrlen)
{
	int fd;
	return (fd_get(socket, &fd) == fd_rsocket) ?
		rgetpeername(fd, addr, addrlen) :
		real.getpeername(fd, addr, addrlen);
}

int getsockname(int socket, struct sockaddr *addr, socklen_t *addrlen)
{
	int fd;
	init_preload();
	return (fd_get(socket, &fd) == fd_rsocket) ?
		rgetsockname(fd, addr, addrlen) :
		real.getsockname(fd, addr, addrlen);
}

int setsockopt(int socket, int level, int optname,
		const void *optval, socklen_t optlen)
{
	int fd;
	return (fd_get(socket, &fd) == fd_rsocket) ?
		rsetsockopt(fd, level, optname, optval, optlen) :
		real.setsockopt(fd, level, optname, optval, optlen);
}

int getsockopt(int socket, int level, int optname,
		void *optval, socklen_t *optlen)
{
	int fd;
	return (fd_get(socket, &fd) == fd_rsocket) ?
		rgetsockopt(fd, level, optname, optval, optlen) :
		real.getsockopt(fd, level, optname, optval, optlen);
}

int fcntl(int socket, int cmd, ... /* arg */)
{
	va_list args;
	long lparam;
	void *pparam;
	int fd, ret;

	init_preload();
	va_start(args, cmd);
	switch (cmd) {
	case F_GETFD:
	case F_GETFL:
	case F_GETOWN:
	case F_GETSIG:
	case F_GETLEASE:
		ret = (fd_get(socket, &fd) == fd_rsocket) ?
			rfcntl(fd, cmd) : real.fcntl(fd, cmd);
		break;
	case F_DUPFD:
	/*case F_DUPFD_CLOEXEC:*/
	case F_SETFD:
	case F_SETFL:
	case F_SETOWN:
	case F_SETSIG:
	case F_SETLEASE:
	case F_NOTIFY:
		lparam = va_arg(args, long);
		ret = (fd_get(socket, &fd) == fd_rsocket) ?
			rfcntl(fd, cmd, lparam) : real.fcntl(fd, cmd, lparam);
		break;
	default:
		pparam = va_arg(args, void *);
		ret = (fd_get(socket, &fd) == fd_rsocket) ?
			rfcntl(fd, cmd, pparam) : real.fcntl(fd, cmd, pparam);
		break;
	}
	va_end(args);
	return ret;
}

/*
 * dup2 is not thread safe
 */
int dup2(int oldfd, int newfd)
{
	struct fd_info *oldfdi, *newfdi;
	int ret;

	oldfdi = idm_lookup(&idm, oldfd);
	if (oldfdi && oldfdi->type == fd_fork)
		fork_passive(oldfd);

	newfdi = idm_lookup(&idm, newfd);
	if (newfdi) {
		 /* newfd cannot have been dup'ed directly */
		if (atomic_get(&newfdi->refcnt) > 1)
			return ERR(EBUSY);
		close(newfd);
	}

	ret = real.dup2(oldfd, newfd);
	if (!oldfdi || ret != newfd)
		return ret;

	newfdi = calloc(1, sizeof *newfdi);
	if (!newfdi) {
		close(newfd);
		return ERR(ENOMEM);
	}

	pthread_mutex_lock(&mut);
	idm_set(&idm, newfd, newfdi);
	pthread_mutex_unlock(&mut);

	newfdi->fd = oldfdi->fd;
	newfdi->type = oldfdi->type;
	if (oldfdi->dupfd != -1) {
		newfdi->dupfd = oldfdi->dupfd;
		oldfdi = idm_lookup(&idm, oldfdi->dupfd);
	} else {
		newfdi->dupfd = oldfd;
	}
	atomic_init(&newfdi->refcnt);
	atomic_set(&newfdi->refcnt, 1);
	atomic_inc(&oldfdi->refcnt);
	return newfd;
}

int fstat(int socket, struct stat *buf)
{
	int fd, ret;

	fprintf(fout, "fstat");
	if (fd_get(socket, &fd) == fd_rsocket) {
		ret = real.fstat(socket, buf);
		fprintf(fout, "fstat - rsocket %d", ret);
		if (!ret)
			buf->st_mode = (buf->st_mode & ~S_IFMT) | __S_IFSOCK;
	} else {
		ret = real.fstat(fd, buf);
		fprintf(fout, "fstat - normal %d", ret);
	}
	fflush(fout);
	return ret;
}
