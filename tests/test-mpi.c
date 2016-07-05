#include <mpi.h>
#include <fcntl.h>
#include <mongoc.h>

#include "mongoc-stream-mpi.c"
#include "mongoc-stream-mpi.h"

#include "mongoc-socket-private.h"
#include "mongoc-thread-private.h"
#include "mongoc-errno-private.h"

#include "test-libmongoc.h"

#define TIMEOUT 10000
#define WAIT 1000



/* Test 2 - Single Message - Multiple Reads
 *
 * The server will read from the stream 1->(n = number of chars)
 * number of bytes to extract the message.
 * This will test mimicking a stream reads by segmenting a single message
 */

static void*
sendv_test2_client(mongoc_stream_t * stream){
   
   mongoc_stream_mpi_t* mpi_stream = (mongoc_stream_mpi_t*) stream;
   MPI_Barrier(mpi_stream->comm);

   printf("test 2 client begin\n");

   char buf[9] = {0};
   ssize_t r;

   mongoc_iovec_t iov;
   iov.iov_base = buf;
   iov.iov_len = sizeof (buf);


   strcpy(buf,"pingpong");
   
   for (int i = 1; i<=9 ;i++){
      r = mongoc_stream_writev(stream,&iov,1,TIMEOUT);
      MPI_Barrier(mpi_stream->comm);
   }
   return NULL;
}

static void*
sendv_test2_server(mongoc_stream_t * stream){
   
   mongoc_stream_mpi_t* mpi_stream = (mongoc_stream_mpi_t*) stream;
   MPI_Barrier(mpi_stream->comm);

   printf("test 2 server begin\n");

   char cmpbuf[9] = "pingpong";
   ssize_t r;

   for (int i=1;i <= 9;i++){
      ssize_t len_read = 0;
      char buf[9] = {0};
      while (len_read < 9){
         mongoc_iovec_t iov;
         iov.iov_base = buf + len_read;
         iov.iov_len = fmin(i,sizeof(buf) - len_read);
         len_read += mongoc_stream_readv (stream, &iov, 1,iov.iov_len,TIMEOUT);
      }

      printf("%zd. this iter is %s\n",i,buf);

      assert(r == 9);
      assert (memcmp (buf,cmpbuf,9) == 0);
      MPI_Barrier(mpi_stream->comm);
   }

   return NULL;
}

/* Test 3 - Multiple Message - Single Read
 * Sending segmented messsages. All messages will be sent before the single recv
 * A single stream read reading through multiple segmented messages */
static void*
sendv_test3_client(mongoc_stream_t * stream){
   
   mongoc_stream_mpi_t* mpi_stream = (mongoc_stream_mpi_t*) stream;
   MPI_Barrier(mpi_stream->comm);

   printf("\n test 3 begin client\n");
   
   char buf[9] = {0};
   ssize_t r;

   strcpy(buf,"pingpong");
   
   for (int i = 1; i<=9 ;i++){
      ssize_t len_write = 0;
      while (len_write < 9){
         mongoc_iovec_t iov;
         iov.iov_base = buf + len_write;
         iov.iov_len = fmin(i,sizeof(buf) - len_write);
         len_write += mongoc_stream_writev(stream,&iov, 1, TIMEOUT);
      }

      MPI_Barrier(mpi_stream->comm);
   }
   return NULL;
}

static void*
sendv_test3_server(mongoc_stream_t * stream){

   mongoc_stream_mpi_t* mpi_stream = (mongoc_stream_mpi_t*) stream;
   MPI_Barrier(mpi_stream->comm);

   printf("\n test 3 begin server\n");

   char cmpbuf[9] = "pingpong";
   ssize_t r;


   for (int i=1;i <= 9;i++){
      MPI_Barrier(mpi_stream->comm);
      char buf[9] = {0};
      mongoc_iovec_t iov;
      iov.iov_base = buf;
      iov.iov_len = sizeof(buf);

      r = mongoc_stream_readv (stream, &iov, 1, iov.iov_len, TIMEOUT);
      printf("%zd. this iter is %s with bytes read of %zd \n",i,buf,r);

      assert(r == 9);
      assert (memcmp (buf,cmpbuf,9) == 0);
   }

   return NULL;
}


static void*
sendv_test_server () 
{
   mongoc_iovec_t iov;
   ssize_t r;
   char buf[5];
   mongoc_stream_t *stream;

   iov.iov_base = buf;
   iov.iov_len = sizeof (buf);

   int listen_sock;
   listen_sock = socket(AF_INET,SOCK_STREAM,0);
   assert(listen_sock);

   struct sockaddr_in server_addr = { 0 };
   server_addr.sin_family = AF_INET;
   server_addr.sin_addr.s_addr = htonl (INADDR_ANY);
   server_addr.sin_port = htons (27020);

   r = bind(listen_sock,(struct sockaddr *)&server_addr,sizeof server_addr);
   assert(r == 0);

   r = listen(listen_sock, 5);
   assert(r == 0);

   int conn_sock;
   socklen_t clilen;
   struct sockaddr_in cli_addr;

   conn_sock = accept(listen_sock, (struct sockaddr *) &cli_addr,&clilen);
   assert (conn_sock);

   // comm join is blocking will connect when other side calls comm join
   MPI_Comm intercom;
   r = MPI_Comm_join(conn_sock,&intercom);
   assert (r == 0);

   stream = mongoc_stream_mpi_new (intercom);

   // r = mongoc_stream_readv (stream, &iov, 1, 5, TIMEOUT);
   // assert (r == 5);
   // assert (strcmp (buf, "ping") == 0);

   // strcpy (buf, "pong");

   // r = mongoc_stream_writev (stream, &iov, 1, TIMEOUT);
   // assert (r == 5);

   sendv_test2_server(stream);

   sendv_test3_server(stream);

   mongoc_stream_destroy (stream);
   return NULL;
}
/* Testing stream readv timeout after n seconds */




static void*
sendv_test_client ()
{
   char buf[5];
   ssize_t r;
   bool closed;
   mongoc_iovec_t iov;
   mongoc_stream_t *stream;

   iov.iov_base = buf;
   iov.iov_len = sizeof (buf);

   int conn_sock;
   struct sockaddr_in server_addr = { 0 };
   conn_sock = socket(AF_INET, SOCK_STREAM, 0);
   assert (conn_sock);

   struct hostent *server;
   server = gethostbyname("Kenneths-MacBook-Pro.local");
   server_addr.sin_family = AF_INET;
   server_addr.sin_port = htons(27020);

   bcopy((char *)server->h_addr, (char *)&server_addr.sin_addr.s_addr,server->h_length);

   r = connect (conn_sock, (struct sockaddr *)&server_addr, sizeof(server_addr));
   assert (r == 0);

   // comm join is blocking will connect when other side calls comm join
   MPI_Comm intercom;
   r = MPI_Comm_join(conn_sock,&intercom);
   assert (r==0);

   // comm join is blocking will connect when other side calls comm join
   stream = mongoc_stream_mpi_new (intercom);

   strcpy (buf, "ping");

   close(conn_sock);

   // closed = mongoc_stream_check_closed(stream);
   // assert (closed == false);

   // r = mongoc_stream_writev(stream,&iov, 1 ,TIMEOUT);
   // assert (r == 5);

   // closed = mongoc_stream_check_closed (stream);
   // assert (closed == false);

   // r = mongoc_stream_readv (stream, &iov, 1, 5, 2);
   // assert (r == 5);
   // assert (strcmp (buf, "pong") == 0);

   sendv_test2_client(stream);

   sendv_test3_client(stream);

   mongoc_stream_destroy (stream);

   return NULL;
}

static void
test_mongoc_mpi_test (void)
{
   int provided;
   MPI_Init_thread(NULL, NULL, MPI_THREAD_MULTIPLE, &provided);

   int world_rank;
   MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);

   if (world_rank == 0){
      sendv_test_server();
   }
   else if (world_rank == 1){
      sendv_test_client();
   }

   /* Shutdown */
   MPI_Finalize();
}



int
main (int   argc,
      char *argv[])
{
	test_mongoc_mpi_test();
	return 0;
}