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

#include "mongoc-stream-private.h"
#include "mongoc-stream-mpi.h"
#include "mongoc-trace.h"


#undef MONGOC_LOG_DOMAIN
#define MONGOC_LOG_DOMAIN "stream"


struct _mongoc_stream_mpi_t
{
   mongoc_stream_t  vtable;
   MPI_Comm* comm;
   char* buffer;
   int buff_len;
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

   BSON_ASSERT(mpi_stream->buffer != NULL);
   if (mpi_stream->buffer){
    free(mpi_stream->buffer);
    mpi_stream->buffer = NULL;
   }

   BSON_ASSERT (mpi_stream);

   if (mpi_stream->comm) {
      MPI_Comm_free(mpi_stream->comm);
      mpi_stream->comm = NULL;
   }
   bson_free (mpi_stream);
   
   EXIT;
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


static ssize_t
_mongoc_stream_mpi_readv (mongoc_stream_t *stream,
                             mongoc_iovec_t  *iov,
                             size_t           iovcnt,
                             size_t           min_bytes,
                             int32_t          timeout_msec)
{
  mongoc_stream_mpi_t* mpi_stream = (mongoc_stream_mpi_t*) stream;
  int64_t expire_at;
  ssize_t ret = 0;
  ssize_t nread;

  ENTRY;

  BSON_ASSERT(mpi_stream);
  BSON_ASSERT(mpi_stream->comm);
  BSON_ASSERT(iovcnt == 1); // We can't handle more than one iovec at the moment

  expire_at = get_expiration(timeout_msec);
  // check the buffer length if it is nonempty read the bytes from buffer first
  // before doing a recv on the communicator
  if (mpi_stream->buffer != NULL && (mpi_stream->buff_len - mpi_stream->cur_ptr) >= min_bytes){

    // iov_len is the amount of bytes it wants to read
    memcpy (mpi_stream->buffer, (char*) iov[0].iov_base, iov[0].iov_len);
    mpi_stream->cur_ptr += iov[0].iov_len;
    if (mpi_stream->cur_ptr >= mpi_stream->buff_len){
      mpi_stream->cur_ptr = 0;
      mpi_stream->buff_len = 0;
      free(mpi_stream->buffer);
      mpi_stream->buffer = NULL;
    }
  }
  else {
    // read the iov_len (this is the amount of bytes it wants to read)
    // store the rest of the message in a buffer
    MPI_Status probeStatus;
    MPI_Probe(MPI_ANY_SOURCE,
              MPI_ANY_TAG,
              *(mpi_stream->comm),
              &probeStatus);

    int mpiLen;
    MPI_Get_count(&probeStatus, MPI_CHAR, &mpiLen);

    if (mpiLen > iov[0].iov_len) {
      mpi_stream->buffer = malloc(mpiLen);
      mpi_stream->buff_len = mpiLen;
      nread = mongoc_mpi_recv(mpi_stream->comm, mpi_stream->buffer, mpiLen, expire_at);
      mpi_stream->cur_ptr = nread;
      memcpy(mpi_stream->buffer,iov[0].iov_base, iov[0].iov_len);
    }
    else {
      mpi_stream->buffer = malloc(mpiLen);
      nread = mongoc_mpi_recv(mpi_stream->comm, (char *)iov[0].iov_base, iov[0].iov_len, expire_at);
    }
  }

  RETURN(0);
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
  RETURN(0);
}

static bool
_mongoc_stream_mpi_check_closed (mongoc_stream_t *stream) /* IN */
{
  RETURN(false);
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
mongoc_stream_mpi_new (MPI_Comm* comm) /* IN */
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
