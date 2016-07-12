/*
 * Copyright 2016 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef MONGOC_MPI_H
#define MONGOC_MPI_H

#if !defined (MONGOC_INSIDE) && !defined (MONGOC_COMPILATION)
# error "Only <mongoc.h> can be included directly."
#endif

#include <bson.h>

#ifdef _WIN32
# include <winsock2.h>
# include <ws2tcpip.h>
#else
# include <arpa/inet.h>
# include <poll.h>
# include <netdb.h>
# include <netinet/in.h>
# include <netinet/tcp.h>
# include <sys/types.h>
# include <sys/socket.h>
# include <sys/uio.h>
# include <sys/un.h>
#endif

#include "mongoc-iovec.h"
#include <mpi.h>


BSON_BEGIN_DECLS

typedef struct
{
   MPI_Comm         comm;
   int              events;
   int              revents;
} mongoc_mpi_poll_t;

ssize_t          mongoc_mpi_recv          (MPI_Comm              comm,
                                           void                  *buf,
                                           size_t                 buflen,
                                           int64_t                expire_at);

ssize_t          mongoc_mpi_sendv         (MPI_Comm              comm,
                                           mongoc_iovec_t        *iov,
                                           size_t                 iovcnt,
                                           int64_t                expire_at);

ssize_t			     mongoc_mpi_poll 		       (mongoc_mpi_poll_t    *mpids,     
						                               size_t                n_mpids,     
						                               int32_t               timeout_msec);

bool             mongoc_mpi_check_closed   (MPI_Comm             comm);

int              mongoc_mpi_connect        (int                    sock,
                                            MPI_Comm              *comm);

bool             mongoc_mpi_initialize     ();

void             mongoc_mpi_finalize       ();


BSON_END_DECLS



#endif /* MONGOC_MPI_H */
