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
};


static BSON_INLINE int64_t
get_expiration (int32_t timeout_msec)
{
  RETURN(0);
}


static int
_mongoc_stream_mpi_close (mongoc_stream_t *stream)
{
  RETURN(0);
}


static void
_mongoc_stream_mpi_destroy (mongoc_stream_t *stream)
{}


static void
_mongoc_stream_mpi_failed (mongoc_stream_t *stream)
{}


static int
_mongoc_stream_mpi_setsockopt (mongoc_stream_t *stream,
                                  int              level,
                                  int              optname,
                                  void            *optval,
                                  socklen_t        optlen)
{
  RETURN(0);
}


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
  RETURN(0);
}


static ssize_t
_mongoc_stream_mpi_writev (mongoc_stream_t *stream,
                              mongoc_iovec_t  *iov,
                              size_t           iovcnt,
                              int32_t          timeout_msec)
{
  RETURN(0);
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
  RETURN(0);
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
mongoc_stream_mpi_new () /* IN */
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

   return (mongoc_stream_t *)stream;
}
