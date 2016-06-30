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

static void*
test_mpi_server () 
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
   MPI_Comm* intercom = (MPI_Comm*)malloc(sizeof(MPI_Comm));
   r = MPI_Comm_join(conn_sock,intercom);
   assert (r == 0);

   stream = mongoc_stream_mpi_new (*intercom);

   r = mongoc_stream_readv (stream, &iov, 1, 5, TIMEOUT);
   assert (r == 5);
   assert (strcmp (buf, "ping") == 0);

   strcpy (buf, "pong");

   printf("\n server sending out %s\n",buf);

   r = mongoc_stream_writev (stream, &iov, 1, TIMEOUT);
   assert (r == 5);

   printf("\n server is past writing\n");

   mongoc_stream_destroy (stream);
   return NULL;
}

static void*
test_mpi_client ()
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
   MPI_Comm* intercom = (MPI_Comm*)malloc(sizeof(MPI_Comm));
   r = MPI_Comm_join(conn_sock,intercom);
   assert (r==0);

   // comm join is blocking will connect when other side calls comm join
   stream = mongoc_stream_mpi_new (*intercom);

   strcpy (buf, "ping");

   close(conn_sock);

   closed = mongoc_stream_check_closed(stream);
   assert (closed == false);

   r = mongoc_stream_writev(stream,&iov, 1 ,TIMEOUT);
   assert (r == 5);

   closed = mongoc_stream_check_closed (stream);
   assert (closed == false);

   printf("\n client is recieving\n");

   r = mongoc_stream_readv (stream, &iov, 1, 5, 2);
   assert (r == 5);
   assert (strcmp (buf, "pong") == 0);

   // not sure if this works properly
   printf("\n client check closed\n");
   closed = mongoc_stream_check_closed (stream);
   assert (closed == true);

   mongoc_stream_destroy (stream);

   return NULL;
}

static void
test_mongoc_mpi_check_closed (void)
{
   int provided;
   MPI_Init_thread(NULL, NULL, MPI_THREAD_MULTIPLE, &provided);

   int world_rank;
   MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);

   if (world_rank == 0){
      test_mpi_server();
   }
   else if (world_rank == 1){
      test_mpi_client();
   }

   /* Shutdown */
   MPI_Finalize();
}



int
main (int   argc,
      char *argv[])
{
	test_mongoc_mpi_check_closed();
	return 0;
}