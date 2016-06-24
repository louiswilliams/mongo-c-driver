#include <mpi.h>
#include <fcntl.h>
#include <mongoc.h>

#include "mongoc-stream-mpi.h"
#include "mongoc-socket-private.h"
#include "mongoc-thread-private.h"
#include "mongoc-errno-private.h"

#include "test-libmongoc.h"

#define TIMEOUT 10000
#define WAIT 1000

typedef struct
{
   unsigned short server_port;
   mongoc_cond_t  cond;
   mongoc_mutex_t cond_mutex;
   bool           closed_comm;
   int            amount;
} mpi_test_data_t;

static void*
test_mpi_server (void *data_) 
{
   mpi_test_data_t *data = (mpi_test_data_t *)data_;
   struct sockaddr_in server_addr = { 0 };
   mongoc_socket_t *listen_sock;
   mongoc_socket_t *conn_sock;
   mongoc_iovec_t iov;
   socklen_t sock_len;
   ssize_t r;
   char buf[5];
   mongoc_stream_t *stream;

   iov.iov_base = buf;
   iov.iov_len = sizeof (buf);

   listen_sock = mongoc_socket_new (AF_INET, SOCK_STREAM, 0);
   assert (listen_sock);

   server_addr.sin_family = AF_INET;
   server_addr.sin_addr.s_addr = htonl (INADDR_LOOPBACK);
   server_addr.sin_port = htons (0);

   r = mongoc_socket_bind (listen_sock,
                           (struct sockaddr *)&server_addr,
                           sizeof server_addr);
   assert(r == 0);

   r = mongoc_socket_listen (listen_sock, 10);
   assert(r == 0);

   mongoc_mutex_lock(&(data->cond_mutex));
   data->server_port = ntohs(server_addr.sin_port);
   mongoc_cond_signal(&(data->cond));
   mongoc_mutex_unlock(&(data->cond_mutex));

   conn_sock = mongoc_socket_accept (listen_sock, -1);
   assert (conn_sock);

   MPI_Comm* intercom = malloc(sizeof(MPI_Comm));
   MPI_Comm_join(conn_sock->sd,intercom);

   mongoc_socket_destroy(conn_sock);

   stream = mongoc_stream_mpi_new (intercom);

   r = mongoc_stream_readv (stream, &iov, 1, 5, TIMEOUT);
   assert (r == 5);
   assert (strcmp (buf, "ping") == 0);

   strcpy (buf, "pong");

   r = mongoc_stream_writev (stream, &iov, 1, TIMEOUT);
   assert (r == 5);

   mongoc_stream_destroy (stream);

   mongoc_mutex_lock(&data->cond_mutex);
   data->closed_comm = true;
   mongoc_cond_signal(&data->cond);
   mongoc_mutex_unlock(&data->cond_mutex);

   mongoc_socket_destroy (listen_sock);

   return NULL;
}

static void*
test_mpi_client (void *data_)
{
   mpi_test_data_t *data = (mpi_test_data_t *)data_;
   mongoc_socket_t *conn_sock;
   char buf[5];
   ssize_t r;
   bool closed;
   struct sockaddr_in server_addr = { 0 };
   mongoc_iovec_t iov;
   mongoc_stream_t *stream;

   iov.iov_base = buf;
   iov.iov_len = sizeof (buf);

   conn_sock = mongoc_socket_new (AF_INET, SOCK_STREAM, 0);
   assert (conn_sock);

   mongoc_mutex_lock(&data->cond_mutex);
   while (! data->server_port) {
      mongoc_cond_wait(&data->cond, &data->cond_mutex);
   }
   mongoc_mutex_unlock(&data->cond_mutex);

   server_addr.sin_family = AF_INET;
   server_addr.sin_port = htons(data->server_port);
   server_addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

   r = mongoc_socket_connect (conn_sock, (struct sockaddr *)&server_addr, sizeof(server_addr), -1);
   assert (r == 0);

      // comm join is blocking will connect when other side calls comm join
   MPI_Comm* intercom = malloc(sizeof(MPI_Comm));
   MPI_Comm_join(conn_sock->sd,intercom);

   mongoc_socket_destroy(conn_sock);

   stream = mongoc_stream_mpi_new (intercom);

   strcpy (buf, "ping");

   closed = mongoc_stream_check_closed(stream);
   assert (closed == false);

   r = mongoc_stream_writev(stream,&iov, 1 ,TIMEOUT);
   assert (r == 5);

   closed = mongoc_stream_check_closed (stream);
   assert (closed == false);

   r = mongoc_stream_readv (stream, &iov, 1, 5, TIMEOUT);
   assert (r == 5);
   assert (strcmp (buf, "pong") == 0);

   mongoc_mutex_lock(&data->cond_mutex);
   while (! data->closed_comm) {
      mongoc_cond_wait(&data->cond, &data->cond_mutex);
   }
   mongoc_mutex_unlock(&data->cond_mutex);

   closed = mongoc_stream_check_closed (stream);
   assert (closed == true);

   mongoc_stream_destroy (stream);

   return NULL;
}

static void
test_mongoc_mpi_check_closed (void)
{
   mpi_test_data_t data = { 0 };
   mongoc_thread_t threads[2];
   int i, r;

   mongoc_mutex_init (&data.cond_mutex);
   mongoc_cond_init (&data.cond);

   r = mongoc_thread_create (threads, &test_mpi_server, &data);
   assert (r == 0);

   r = mongoc_thread_create (threads + 1, &test_mpi_client, &data);
   assert (r == 0);

   for (i = 0; i < 2; i++) {
      r = mongoc_thread_join (threads[i]);
      assert (r == 0);
   }

   mongoc_mutex_destroy (&data.cond_mutex);
   mongoc_cond_destroy (&data.cond);
}



int
main (int   argc,
      char *argv[])
{
	test_mongoc_mpi_check_closed();
	return 0;
}