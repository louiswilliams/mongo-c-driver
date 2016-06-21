
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


#include <errno.h>
#include <mpi.h>
#include <string.h>

#include "mongoc-counters-private.h"
#include "mongoc-errno-private.h"
#include "mongoc-host-list.h"
#include "mongoc-mpi.h"
#include "mongoc-trace.h"

#undef MONGOC_LOG_DOMAIN
#define MONGOC_LOG_DOMAIN "mpi"

ssize_t mongoc_mpi_recv (MPI_Comm     *comm,
                         void         *buf,
                         size_t        buflen,
                         int64_t       expire_at) {

	ssize_t ret = 0;

	BSON_ASSERT(comm);
	BSON_ASSERT(buf);
	BSON_ASSERT(buflen);
	BSON_ASSERT(expire_at); // Only the blocking version is implemented right now

    MPI_Status probeStatus;
    MPI_Probe(MPI_ANY_SOURCE,
              MPI_ANY_TAG,
              *comm,
              &probeStatus);

    int msgLen;
    MPI_Get_count(&probeStatus, MPI_CHAR, &msgLen);

    if (msgLen > buflen) {
    	// How do we throw errors?
    	RETURN(-1);
    }

    MPI_Status recvStatus;
    MPI_Recv(buf,
             msgLen,
             MPI_CHAR,
             MPI_ANY_SOURCE,
             MPI_ANY_TAG,
             *comm,
             &recvStatus);

	RETURN(msgLen);
}
