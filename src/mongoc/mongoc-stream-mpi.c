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

#include <stdio.h>
#include <math.h>

#include "mongoc-stream-private.h"
#include "mongoc-stream-mpi.h"
#include "mongoc-trace.h"



#undef MONGOC_LOG_DOMAIN
#define MONGOC_LOG_DOMAIN "stream"


struct _mongoc_stream_mpi_t
{
   mongoc_stream_t  vtable;
   MPI_Comm comm;
   char* buffer;
   size_t buff_len;
   int cur_ptr;
};


static BSON_INLINE int64_t
get_expiration (int32_t timeout_msec)
{
   if (timeout_msec < 0) {
      return -1;
   } else if (timeout_msec == 0) {
      return 0;
   } else {
      return (bson_get_monotonic_time () + ((int64_t)timeout_msec * 1000L));
   }
}


/* there is no way to close a communicator */
static int
_mongoc_stream_mpi_close (mongoc_stream_t *stream)
{
   RETURN (0);
}


static void
_mongoc_stream_mpi_destroy (mongoc_stream_t *stream)
{
   ENTRY;
   mongoc_stream_mpi_t *mpi_stream = (mongoc_stream_mpi_t *)stream;
   int ret;

   ENTRY;

   if (mpi_stream->buffer){
    free(mpi_stream->buffer);
    mpi_stream->buffer = NULL;
   }

   BSON_ASSERT (mpi_stream);

   bson_free (mpi_stream);
   mpi_stream = NULL;
   
   EXIT;
   return;
}



static void
_mongoc_stream_mpi_failed (mongoc_stream_t *stream)
{
   ENTRY;

   _mongoc_stream_mpi_destroy (stream);

   EXIT;
}

/** there is no setup of sockopt for the communicator */
static int
_mongoc_stream_mpi_setsockopt (mongoc_stream_t *stream,
                                  int              level,
                                  int              optname,
                                  void            *optval,
                                  socklen_t        optlen)
{
  RETURN(0);
}


/** atm we are not buffering and mpi is a message interface so not idea
  * of flushing a buffer */
static int
_mongoc_stream_mpi_flush (mongoc_stream_t *stream)
{
  RETURN(0);
}


/* Reads up to min_bytes of data. Blocks if no bytes have been read.
 * will continously read the messages until its full or there are no more 
 * messages left and it would have to block 

 * Base Case: Bytes have been read into iovec and there are no more messages left
 * in the network buffer.
 * Base Case 2: iovec buffer is full
 * Returns: Number of bytes read 
 */
static ssize_t
_mongoc_stream_mpi_probe_read (mongoc_stream_mpi_t* mpi_stream,
                               mongoc_iovec_t  *iov,
                               size_t           iovcnt,
                               size_t           min_bytes,
                               int64_t          expire_at,
                               size_t           bytes_read)
{
  int probe_flag;
  int ret;
  MPI_Status probeStatus;

    
  int nread = bytes_read;
    
  // offset of the base of the buffer for each iteration of the loop
  char* new_base = ((char *)iov[0].iov_base) + nread;
  // remaining bytes in the read buffer
  int bytes_to_read = iov[0].iov_len - nread;

  while (bytes_to_read > 0){
      if (nread > 0){
            ret = MPI_Iprobe(MPI_ANY_SOURCE,
                    MPI_ANY_TAG,
                    mpi_stream->comm,
                    &probe_flag,
                    &probeStatus);

        // since it has already recieved some number of bytes it will return what it currently
        // has read, probe flag states if there is something to read return value 0 if no error.

        // Base Case 2 - if no message available and has read bytes return
        if (ret < 0 ||(ret == 0 && !probe_flag)) {
            return nread;
        }
      }

      // otherwise it will probe (blocking recv) since it hasn't recieved any bytes yet
      // TODO to implement timeout you would while loop here until the timeout time
      else {
            ret = MPI_Probe(MPI_ANY_SOURCE,
                      MPI_ANY_TAG,
                      mpi_stream->comm,
                      &probeStatus);
      }

      // 3rd step if we are here there is a msg to read
      int mpiLen;
      MPI_Get_count(&probeStatus, MPI_CHAR, &mpiLen);
      
      // 4th step buffer to the global bufferstream the leftover of the msg
      // since the total number of bytes to read is done we return.

      if (mpiLen > bytes_to_read) {
        mpi_stream->buffer = malloc(mpiLen);
        mpi_stream->buff_len = mpiLen;
        mongoc_mpi_recv(mpi_stream->comm, mpi_stream->buffer, mpiLen, expire_at);

        memcpy((char*) iov[0].iov_base, mpi_stream->buffer,bytes_to_read);

        // add bytes of msg read to the nreads what is memcpy'd
        nread += bytes_to_read;
        mpi_stream->cur_ptr = nread;

        printf("3. copy is %s\n",iov[0].iov_base);

        return nread;
      }
      else {
        ret = mongoc_mpi_recv(mpi_stream->comm, new_base, iov[0].iov_len, expire_at);
        if (ret <= 0){

            // TODO error handling here not sure what to do yet
            printf("there is an error\n");
        }

        // update to the new state
        nread+=ret;
        new_base = ((char *)iov[0].iov_base) + nread;
        bytes_to_read = iov[0].iov_len - nread;
    }
  }
  return nread;
}


/* Returns in iov the msg of size up to min_bytes
 * Input Precondition:
 * iov - will only use the first element of the iov
 *
 * iovcnt - supports only an iovec of size 1
 *
 * min_bytes - no garunteeds of blocking until reading in min_bytes
 * reads up to min_bytes.
 *
 * timeout_msec - timeout is not supported as in other parts fo the code
 * it is not used passes in -1 indefinite timeout or 0 (immediate)
 * 
 * Returns in iov[0] a msg of size less than or equal to min_bytes.
 * Blocking until there exists a byte to read.
 */
static ssize_t
_mongoc_stream_mpi_readv (mongoc_stream_t *stream,
                             mongoc_iovec_t  *iov,
                             size_t           iovcnt,
                             size_t           min_bytes,
                             int32_t          timeout_msec)
{
  mongoc_stream_mpi_t* mpi_stream = (mongoc_stream_mpi_t*) stream;
  int64_t expire_at;
  ssize_t nread = 0;
  int ret;

  ENTRY;

  BSON_ASSERT(mpi_stream);
  BSON_ASSERT(mpi_stream->comm);
  BSON_ASSERT(iovcnt == 1); // We are not handling more than one iovec

  expire_at = get_expiration(timeout_msec);

  // 1st step read buffered remainder msg if it exists
  if (mpi_stream->buffer != NULL){
    int bytes_left;
    bytes_left = mpi_stream->buff_len - mpi_stream->cur_ptr;

    // move to the current position of the remainder of the message
    char* cur_buf = mpi_stream->buffer + mpi_stream->cur_ptr;
    
    // reads part of the buffer
    if (bytes_left > iov[0].iov_len){
        memcpy ((char*) iov[0].iov_base,cur_buf, iov[0].iov_len);

        nread += iov[0].iov_len;
        mpi_stream->cur_ptr += iov[0].iov_len;

        printf("1. copy is %s\n",iov[0].iov_base);
        return nread;
    }
    // reads the remainder
    else {
        memcpy((char*) iov[0].iov_base, cur_buf, bytes_left);
        
        nread += bytes_left;

        // free the buffer since we have read everything in it
        mpi_stream->cur_ptr = 0;
        mpi_stream->buff_len = 0;
        free(mpi_stream->buffer);
        mpi_stream->buffer = NULL;

        printf("2. copy is %s\n",iov[0].iov_base);

        // if this read satisfied the total size of the buffer we return
        if (bytes_left == iov[0].iov_len) {
            return nread;
        }
    }
  }
  return _mongoc_stream_mpi_probe_read(mpi_stream,iov,iovcnt,min_bytes,expire_at,nread);
}


static ssize_t
_mongoc_stream_mpi_writev (mongoc_stream_t *stream,
                              mongoc_iovec_t  *iov,
                              size_t           iovcnt,
                              int32_t          timeout_msec)
{
  mongoc_stream_mpi_t *mpi_stream = (mongoc_stream_mpi_t *)stream;

  BSON_ASSERT (iovcnt > 0);

  return mongoc_mpi_sendv(mpi_stream->comm,iov,iovcnt,timeout_msec);
}


static ssize_t
_mongoc_stream_mpi_poll (mongoc_stream_poll_t *streams,
                            size_t                nstreams,
                            int32_t               timeout_msec)

{
  // for each stream cast to a mpi stream and check their events for some timeout
  int i;
  ssize_t ret = -1;
  mongoc_mpi_poll_t *mpi_ds;
  mongoc_stream_mpi_t *mpi_stream;

  for (i=0; i < nstreams; i++){
    mpi_stream = (mongoc_stream_mpi_t *)streams[i].stream;

    if (!mpi_stream->comm){
      // cleanup
      bson_free(mpi_ds);
      RETURN (ret);
    }

    mpi_ds[i].comm = mpi_stream->comm;
    mpi_ds[i].events = streams[i].events;
    mpi_ds[i].revents = 0;
  }

  // check the buffer for pollin events to flag reads for th revents

  // call helper function that polls
  ret = mongoc_mpi_poll(mpi_ds,nstreams, timeout_msec);

  if (ret > 0){
    for (i = 0;i <nstreams;i++){
      streams[i].revents = mpi_ds[i].revents;
    }
  }

  bson_free(mpi_ds);
  RETURN(ret);
}

static bool
_mongoc_stream_mpi_check_closed (mongoc_stream_t *stream) /* IN */
{
  mongoc_stream_mpi_t *mpi_stream = (mongoc_stream_mpi_t *)stream;

  ENTRY;

  BSON_ASSERT (stream);

  if (mpi_stream->comm) {
    RETURN (mongoc_mpi_check_closed (mpi_stream->comm));
  }

  RETURN(true);
}


/*
 *--------------------------------------------------------------------------
 *
 * mongoc_stream_mpi_new --
 *
 *       Create a new mongoc_stream_t using the mongoc_mpi_t for
 *       read and write underneath.
 *
 * Returns:
 *       None.
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */

mongoc_stream_t *
mongoc_stream_mpi_new (MPI_Comm comm) /* IN */
{
   mongoc_stream_mpi_t *stream;

   stream = (mongoc_stream_mpi_t *)bson_malloc0 (sizeof *stream);
   stream->vtable.type = MONGOC_STREAM_MPI;
   stream->vtable.close = _mongoc_stream_mpi_close;
   stream->vtable.destroy = _mongoc_stream_mpi_destroy;
   stream->vtable.failed = _mongoc_stream_mpi_failed;
   stream->vtable.flush = _mongoc_stream_mpi_flush;
   stream->vtable.readv = _mongoc_stream_mpi_readv;
   stream->vtable.writev = _mongoc_stream_mpi_writev;
   stream->vtable.setsockopt = _mongoc_stream_mpi_setsockopt;
   stream->vtable.check_closed = _mongoc_stream_mpi_check_closed;
   stream->vtable.poll = _mongoc_stream_mpi_poll;
   stream->comm = comm;
   stream-> buffer = NULL;
   stream->buff_len = 0;
   stream->cur_ptr = 0;

   return (mongoc_stream_t *)stream;
}
