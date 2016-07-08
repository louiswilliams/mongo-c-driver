
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

static BSON_INLINE int64_t
get_expiration (int32_t timeout_msec)
{
   if (timeout_msec < 0) {
      return -1;
   }
   else {
      return (bson_get_monotonic_time () + ((int64_t)timeout_msec * 1000L));
   }
}

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

        MPI_Send(msg, bytes, MPI_CHAR, 0, 0,comm);

        free(msg);

        RETURN(bytes);
    }
}

static bool
_mongoc_mpi_wait (MPI_Comm          comm,          /* IN */
                  int32_t           timeout,
                  int*              errors)      /* IN */
{
   int ret;
   int64_t now;
   int64_t expire_at;

   int probe_flag;
   MPI_Status probeStatus;

   ENTRY;

   now = bson_get_monotonic_time();
   expire_at = get_expiration(timeout);

   while ((now <= expire_at) || (expire_at < 0)) {
      ret = MPI_Iprobe(MPI_ANY_SOURCE,
              MPI_ANY_TAG,
              comm,
              &probe_flag,
              &probeStatus);

      // mpi_sucess is just 0
      if (ret >= 0 && probe_flag) {
         /* there is a msg to recieve, */
         RETURN (true);
      } 
      /* error failure of the iprobe */
      else if (ret < 0){
        *errors |= POLLERR;
        RETURN (false);
      }
      /* flag false means nothing received inbound */
      else {
         now = bson_get_monotonic_time();
      }
   }
   RETURN (false);
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

int mongoc_mpi_count_events(int events){
  int count = 0;
  for(int i = 0; i < sizeof(int); i++) {
    count += (events >> i) & 0x01; // Shift bit[i] to the first position, and mask off the remaining bits.
  }
  return count;
}

int
mongoc_mpi_num_events(mongoc_mpi_poll_t     *mpids,          /* IN */
                      size_t                n_mpids){
  int count = 0;
  for (int ids = 0; ids < n_mpids; ids++){
    // count number of sete bits of the int
    count += mongoc_mpi_count_events(mpids[ids].events);
  }
  return count;
}

ssize_t
mongoc_mpi_poll (mongoc_mpi_poll_t     *mpids,          /* IN */
                 size_t                n_mpids,         /* IN */
                 int32_t               timeout_msec)         /* IN */
{

   int ret = 0;
   int i;

   int num_events = 0;
   int num_event_comms = 0;

   int probe_flag;
   MPI_Status probeStatus;

   ENTRY;

   BSON_ASSERT (mpids);

   int64_t now = bson_get_monotonic_time();
   int64_t expire_at = get_expiration (timeout_msec);

   int total_events = mongoc_mpi_num_events(mpids,n_mpids);

   // expire_at = -1 causes it to loop forever
   while ((now <= expire_at) || (expire_at < 0)) {
    num_events = 0;
    num_event_comms = 0;

    // printf("now:%zd expires:%zd\n",now,expire_at);

    // check all mpi_ds nonblockingly for each event
    for (i =0; i < n_mpids; i++){
      
      // can it nonblock recieve
      // i am assuming recieving normal and priority (POLLIN and POLLPRI) is the same in mpi
      int errors = 0;

      if (mpids[i].events & (POLLIN|POLLPRI)) {
        ret = _mongoc_mpi_wait(mpids[i].comm,0,&errors);
        if (ret){
            mpids[i].revents |= (POLLIN & mpids[i].events);
            mpids[i].revents |= (POLLPRI & mpids[i].events);
        }
        else {
          // printf("ERROR: mongoc-mpi 248 we have an error in waiting\n");
          // mpids[i].revents |= errors;
          // RETURN(-1);
        }
      }

      // a communicator send doesn't block so can always pollout
      if (mpids[i].events & POLLOUT){
        mpids[i].revents |= POLLOUT;
      }

      // check if there has been an event on the mpids
      if (mpids[i].revents){
        num_event_comms += 1;
        num_events += mongoc_mpi_count_events(mpids[i].revents);
      }
    }
    // stop checking if all events are satisfied
    if (num_events == total_events){
      break;
    }
    // update the current time for the timeout
    else {
      now = bson_get_monotonic_time();
    }
   }
   return num_event_comms;
}