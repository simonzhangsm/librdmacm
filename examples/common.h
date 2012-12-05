/*
 * Copyright (c) 2005-2012 Intel Corporation.  All rights reserved.
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
 * $Id$
 */

#include <stdlib.h>
#include <sys/types.h>
#include <byteswap.h>

#include <rdma/rdma_cma.h>

#if __BYTE_ORDER == __BIG_ENDIAN
static inline uint64_t cpu_to_be64(uint64_t x) { return x; }
static inline uint32_t cpu_to_be32(uint32_t x) { return x; }
#else
static inline uint64_t cpu_to_be64(uint64_t x) { return bswap_64(x); }
static inline uint32_t cpu_to_be32(uint32_t x) { return bswap_32(x); }
#endif

union socket_addr {
	struct sockaddr		sa;
	struct sockaddr_in	sin;
	struct sockaddr_in6	sin6;
};

int get_rdma_addr(char *src, char *dst, char *port,
		  struct rdma_addrinfo *hints, struct rdma_addrinfo **rai);

void size_str(char *str, size_t ssize, long long size);
void cnt_str(char *str, size_t ssize, long long cnt);
int size_to_count(int size);
void format_buf(void *buf, int size);
int verify_buf(void *buf, int size);
int do_poll(struct pollfd *fds, int timeout);
