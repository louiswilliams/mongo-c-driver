
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

ssize_t mongoc_mpi_recv (MPI_Comm      comm,
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
              comm,
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
             comm,
             &recvStatus);

    RETURN(msgLen);
}

ssize_t mongoc_mpi_sendv (MPI_Comm          comm,
                          mongoc_iovec_t    *iov,
                          size_t            iovcnt,
                          int64_t           expire_at) {

    BSON_ASSERT (iovcnt > 0);    
    BSON_ASSERT(expire_at); // Only the blocking version is implemented right now

    /* in the case when it's only 1 we don't need to malloc or memcpy runs faster */
    if (iovcnt == 1){
        int r = MPI_Send(iov[0].iov_base, iov[0].iov_len,MPI_CHAR,0,0,comm);
        assert(r == 0);
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

        MPI_Send(msg, bytes, MPI_CHAR, 0, MPI_ANY_TAG,comm);

        free(msg);

        RETURN(bytes);
    }
}

static bool
_mongoc_mpi_wait (MPI_Comm          comm,          /* IN */
                  int64_t           expire_at,
                  int*              errors)      /* IN */
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
              comm,
              &probe_flag,
              &probeStatus);

      if (ret > 0 && probe_flag) {
         /* there is a msg to recieve, */
         RETURN (true);
      } 
      /* error failure of the iprobe */
      else if (ret < 0){
        *errors |= POLLERR;
        return (false);
      }
      /* flag false means nothing received inbound */
      else {
         now = bson_get_monotonic_time();
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
mongoc_mpi_check_closed (MPI_Comm comm) /* IN */
{
   ssize_t r;
   int errors = 0;
   int ret;
   int probe_flag;
   int remote_size;
   int local_size;
   int is_intercom;
   MPI_Status probeStatus;

   ret = MPI_Comm_test_inter(comm, &is_intercom);
   if (ret != MPI_SUCCESS){
    return true;
   }

   if (is_intercom){
    ret = MPI_Comm_remote_size(comm, &remote_size);
    if (remote_size <= 0||ret != MPI_SUCCESS){
      return true;
    }
   }

   ret = MPI_Comm_size(comm,&local_size);
   if (local_size <= 0||ret != MPI_SUCCESS){
    return true;
   }

   ret = MPI_Iprobe(MPI_ANY_SOURCE,
              MPI_ANY_TAG,
              comm,
              &probe_flag,
              &probeStatus);

   if (ret != MPI_SUCCESS){
    return true;
   }
   return false;
}

ssize_t
mongoc_mpi_poll (mongoc_mpi_poll_t     *mpids,          /* IN */
                 size_t                n_mpids,         /* IN */
                 int32_t               timeout)         /* IN */
{
   int ret = 0;
   int i;
   int num_events;

   int probe_flag;
   MPI_Status probeStatus;

   ENTRY;

   BSON_ASSERT (mpids);

   int now = bson_get_monotonic_time();

   while (now <= timeout){
    num_events = 0;

    // check all mpi_ds nonblockingly for each event
    for (i =0; i < n_mpids; i++){
      
      // can it nonblock recieve
      // i am assuming recieving normal and priority is the same in mpi
      int errors = 0;

      if (mpids->events & (POLLIN|POLLPRI)) {
        ret = _mongoc_mpi_wait(mpids->comm,0,&errors);
        if (ret){
            mpids[i].revents |= (POLLIN & mpids->events);
            mpids[i].revents |= (POLLPRI & mpids->events);
        }
        else {
          mpids[i].revents |= errors;
          RETURN(-1);
        }
      }

      // a communicator send doesn't block so can always pollout
      if (mpids->events & POLLOUT){
        mpids[i].revents |= POLLOUT;
      }

      // check if all events are satisfied
      if (mpids->revents & mpids->events){
        num_events+= 1;
      }
    }

    // stop checking if all events are satisfied
    if (num_events == n_mpids){
      break;
    }
    // update the current time for the timeout
    else {
      now = bson_get_monotonic_time();
    }
   }

   // accumulate the number of elements in mpids arra that has
   // events occurred
   num_events = 0;
   for (i = 0;i< n_mpids;i++){
    if (mpids[i].revents > 0){
      num_events++;
    }
   }
   return num_events;
}
