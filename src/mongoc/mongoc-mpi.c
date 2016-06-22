
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

ssize_t mongoc_mpi_sendv (MPI_Comm          *comm,
                          mongoc_iovec_t    *iov,
                          size_t            iovcnt,
                          int64_t           expire_at) {

    BSON_ASSERT (iovcnt > 0);    
    BSON_ASSERT(expire_at); // Only the blocking version is implemented right now

    /* in the case when it's only 1 we don't need to malloc or memcpy runs faster */
    if (iovcnt == 1){
        MPI_Send(iov[0].iov_base, iov[0].iov_len,MPI_CHAR,0,0,*comm);
        RETURN(iov[0].iov_len);
    }
    else {
        int i;
        int bytes = 0;
        for (i =0; i< iovcnt;i++){
            bytes+= iov[i].iov_len;
        }

        /* would ideally not malloc to send one big message but there is currently
         * no support for reforming segmented messages */

        char *msg = malloc(bytes*sizeof(char));
        char* msg_tail = msg;

        /* iovcnt is greater than 1 we will assemble the message */
        for (i = 0; i < iovcnt; i++) {
            memcpy (msg_tail, (char *) iov[i].iov_base, iov[i].iov_len);
            msg_tail+= iov[i].iov_len;
        }

        MPI_Send(msg, bytes, MPI_CHAR, 0, 0,*comm);

        free(msg);

        RETURN(bytes);
    }
}

static bool
_mongoc_mpi_wait (MPI_Comm          *comm,          /* IN */
                  int64_t           expire_at)      /* IN */
{
   int ret;
   int timeout;
   int64_t now;

   int probe_flag;
   MPI_Status probeStatus;

   ENTRY;

   now = bson_get_monotonic_time();

   for (;;) {
      ret = MPI_Iprobe(MPI_ANY_SOURCE,
              MPI_ANY_TAG,
              *comm,
              &probe_flag,
              &probeStatus);

      if (ret > 0 && probe_flag > 0) {
         /* Something happened, so return that */
         RETURN (true);
      } 
      /* error failure of the iprobe */
      else if (ret < 0){
        return (false);
      }
      /* flag false means nothing received inbound */
      else {
         /* probe itself failed */
          if (expire_at < now) {
             RETURN (false);
          } else {
             continue;
          }
         /* poll timed out */
         RETURN (false);
      }
   }
}

bool
mongoc_mpi_check_closed (MPI_Comm *comm) /* IN */
{
   ssize_t r;

   if (_mongoc_mpi_wait(comm, 1)) {
    return false;
   }
   else {
    return true;
   }
}
