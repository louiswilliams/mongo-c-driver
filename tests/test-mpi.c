#include <stdlib.h>
#include <mpi.h>
#include <fcntl.h>
#include <mongoc.h>
#include <time.h>
#include <poll.h>

#include "mongoc-stream-mpi.c"
#include "mongoc-stream-mpi.h"

#include "mongoc-socket-private.h"
#include "mongoc-thread-private.h"
#include "mongoc-errno-private.h"

#include "test-libmongoc.h"

#define TIMEOUT 100
#define WAIT 1000
#define MAX_MESSAGE_SIZE 4096

struct thread_args {
   int conn_sock;
   int count;
   int (*pollin_table)[4];
};

/* Test 1 - Single Message Send and Recieve
 *
 * The client will send ping and the server will respond with pong
 */

static void*
sendv_test1_client(mongoc_stream_t* stream){

    mongoc_stream_mpi_t* mpi_stream = (mongoc_stream_mpi_t*) stream;
    MPI_Barrier(mpi_stream->comm);

    char buf[5];
    ssize_t r;
    bool closed;
    mongoc_iovec_t iov;

    iov.iov_base = buf;
    iov.iov_len = sizeof (buf);

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

    r = mongoc_stream_readv (stream, &iov, 1, 5, TIMEOUT);
    assert(r == -1);

    MPI_Barrier(mpi_stream->comm);

    return NULL;
};


static void*
sendv_test1_server(mongoc_stream_t* stream){
   
    mongoc_stream_mpi_t* mpi_stream = (mongoc_stream_mpi_t*) stream;
    MPI_Barrier(mpi_stream->comm);

    mongoc_iovec_t iov;
    ssize_t r;
    char buf[5];

    iov.iov_base = buf;
    iov.iov_len = sizeof (buf);

    r = mongoc_stream_readv (stream, &iov, 1, 5, TIMEOUT);
    assert (r == 5);
    assert (strcmp (buf, "ping") == 0);

    strcpy (buf, "pong");

    r = mongoc_stream_writev (stream, &iov, 1, TIMEOUT);
    assert (r == 5);

    MPI_Barrier(mpi_stream->comm);

    return NULL;
}


/* Test 2 - Single Message - Multiple Reads
*
* The server will read from the stream 1->(n = number of chars)
* number of bytes to extract the message.
* This will test mimicking a stream reads by segmenting a single message
*/

static void*
sendv_test2_client(mongoc_stream_t* stream){

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
sendv_test2_server(mongoc_stream_t* stream){

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
 *
 * Sending segmented messsages. All messages will be sent before the single recv
 * A single stream read reading through multiple segmented messages 
 */

static void*
sendv_test3_client(mongoc_stream_t* stream){

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
sendv_test3_server(mongoc_stream_t* stream){

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

/* Test 4 - Multiple Random Size Segmented Message - Multiple Random Size Reads
 *
 * Segment the pingpong message randomly and send it out as multiple different sized
 * message and the read will be of random size to reform the message.
 * To mimick a stream interface.
 */

static void*
sendv_test4_server(mongoc_stream_t* stream){
    mongoc_stream_mpi_t* mpi_stream = (mongoc_stream_mpi_t*) stream;
    MPI_Barrier(mpi_stream->comm);

    time_t t;
    srand((unsigned) time(&t)+100);

    printf("\n test 4 begin server \n");

    char cmpbuf[9] = "pingpong";

    for (int i=1;i<=9;i++){
        int len_read = 0;
        char buf[9] = {0};
        while (len_read < 9){
            mongoc_iovec_t iov;
            iov.iov_base = &(buf[len_read]);

            int rand_num = (rand() % 9) + 1;
            iov.iov_len = fmin(sizeof(buf) - len_read,rand_num);
            len_read += mongoc_stream_readv (stream, &iov, 1,iov.iov_len,TIMEOUT);

            printf("%zd. SERVER: string is currently %s after recieving %zd bytes.\n",i,buf,len_read);
        }

        printf("%zd. this iter is %s\n",i,buf);
        assert(len_read == 9);
        assert (memcmp (buf,cmpbuf,9) == 0);
    }

    MPI_Barrier(mpi_stream->comm);
    return NULL;
}


static void*
sendv_test4_client(mongoc_stream_t* stream){
    mongoc_stream_mpi_t* mpi_stream = (mongoc_stream_mpi_t*) stream;
    MPI_Barrier(mpi_stream->comm);

    time_t t;
    srand((unsigned) time(&t));

    printf("\n test 4 begin client\n");

    char buf[9] = {0};
    ssize_t r;

    strcpy(buf,"pingpong");

    for (int i = 1; i<=9 ;i++){
        ssize_t len_write = 0;

        while (len_write < 9){
            mongoc_iovec_t iov;
            iov.iov_base = &(buf[len_write]);

            int prev_len = len_write;

            int rand_num = (rand() % 9) + 1;
            iov.iov_len = fmin(rand_num,sizeof(buf) - len_write);
            len_write += mongoc_stream_writev(stream,&iov, 1, TIMEOUT);

            printf("\n%zd. CLIENT: sent string is ",i);
            for (int j=prev_len;j<len_write;j++){
                printf("%c",buf[j]);
            }
            printf(" after sending %zd bytes.\n",len_write);
        }
    }

    MPI_Barrier(mpi_stream->comm);
    return NULL;
}


/* Test 5 - Poll Test 1 Expiration
 *
 * Tests if the poll expires if no events occur.
 *
 */

static void*
poll_test1_client(mongoc_stream_t* stream){

    mongoc_stream_mpi_t* mpi_stream = (mongoc_stream_mpi_t*) stream;
    MPI_Barrier(mpi_stream->comm);
    ssize_t r;

    mongoc_stream_poll_t poller;
    poller.stream = stream;
    poller.events = POLLIN;
    poller.revents = 0;

    // will test if it timeout for 2 seconds
    r = mongoc_stream_poll(&poller,1,500);

    assert(r == 0);

    // non blocking instantenous
    r = mongoc_stream_poll(&poller,1,0);

    assert(r == 0);

    MPI_Barrier(mpi_stream->comm);

return NULL;
}


static void*
poll_test1_server(mongoc_stream_t* stream){
    mongoc_stream_mpi_t* mpi_stream = (mongoc_stream_mpi_t*) stream;
    MPI_Barrier(mpi_stream->comm);

    MPI_Barrier(mpi_stream->comm);
return NULL;
}


/* Test 6 - Poll Test 2 Single Connection pollin
 *
 * Tests if the pollin works for a single stream after there is something
 * sent to it.
 *
 */

static void*
poll_test2_client(mongoc_stream_t* stream){
    mongoc_stream_mpi_t* mpi_stream = (mongoc_stream_mpi_t*) stream;
    MPI_Barrier(mpi_stream->comm);

    ssize_t r;

    mongoc_stream_poll_t poller;
    poller.stream = stream;
    poller.events = POLLIN;
    poller.revents = 0;

    // will test if poll returns correct revents
    r = mongoc_stream_poll(&poller,1,-1);

    assert(r == 1);

    assert(poller.revents == POLLIN);

    char buf[9];
    char cmpbuf[9] = "pingpong";

    mongoc_iovec_t iov;
    iov.iov_base = buf;
    iov.iov_len = sizeof (buf);

    r = mongoc_stream_readv (stream, &iov, 1, iov.iov_len, TIMEOUT);

    assert(r == 9);
    assert (memcmp (buf,cmpbuf,9) == 0);

    MPI_Barrier(mpi_stream->comm);

    return NULL;
}


static void*
poll_test2_server(mongoc_stream_t* stream){
    mongoc_stream_mpi_t* mpi_stream = (mongoc_stream_mpi_t*) stream;
    MPI_Barrier(mpi_stream->comm);

    char buf[9] = {0};
    ssize_t r;

    mongoc_iovec_t iov;
    iov.iov_base = buf;
    iov.iov_len = sizeof (buf);


    strcpy(buf,"pingpong");
    r = mongoc_stream_writev(stream,&iov,1,TIMEOUT);

    assert(r == 9);

    MPI_Barrier(mpi_stream->comm);
    return NULL;
}

/* Test 7 - 4 Stream Permutation Test: Single Client Poller Connection to 4 Servers
 *
 * Create every permutation of pollin/NOP event for 4 connections and poll these four
 * connections with there associated events on the client side. The server 4 connection
 * will either SEND/NOP for each round based on the current iterations event on the server
 * side. Since there are 16 permutations for POLLIN/NOP for 4 connections. And for each 
 * permutation on the client side we send over all 16 configuration of events from the server
 * to validate the proper number of events that are triggered in each round. There are a total of 
 * 256 (16*16) round that are tested.
 *
 */


/* Generate all permutations of a 0 and 1s for 4 ints. */
static int
generate_pollin_table_rec(int pollin_table[][4],int* number,int n,int length,int total){
    int final_total = total;
    if(n > 0) {
        number[4-n] = 0;
        final_total = generate_pollin_table_rec(pollin_table,number,n - 1, length,final_total);
        
        number[4-n] = POLLIN;
        final_total = generate_pollin_table_rec(pollin_table,number,n - 1, length,final_total);
        return final_total;
    }
    else {
        for (int i = 0;i<length;i++){
            pollin_table[final_total][i] = number[i];
        }
        final_total += 1;
        return final_total;
    }
}


// TODO fix this to use bitwise operations 
static int
num_poll_events(int client_event[4], int server_event[4]) {
    int count = 0;
    for (int i = 0; i < 4 ;i++){
        if ((client_event[i] != 0) && (client_event[i] != 0) && (client_event[i] == server_event[i])) {
            count++;
        }
    }
    return count;
};


static int
create_mongoc_stream_poll_t_list(mongoc_stream_t* stream_list[4],int client_event[4],mongoc_stream_poll_t poll_list[4]){
    for (int i = 0;i< 4;i++){
        poll_list[i].stream = stream_list[i];
        poll_list[i].events = client_event[i];
        poll_list[i].revents = 0;
    }
    return 0;
}


static int
retrieve_stream_polls(mongoc_stream_poll_t poll_list[4], int server_event[4]){

    ssize_t r;
    mongoc_iovec_t iov;

    char cmpbuf[9] = "pingpong";

    int count = 0;
    for (int i =0;i<4;i++){
        char buf[9] = {0};
        iov.iov_base = buf;
        iov.iov_len = sizeof (buf);

        if (poll_list[i].revents == POLLIN){
            r = mongoc_stream_readv (poll_list[i].stream, &iov, 1, 9, TIMEOUT);

            assert((server_event[i] & POLLIN) == POLLIN);
            assert((poll_list[i].events & POLLIN) == POLLIN);
            assert(r == 9);
            assert (memcmp (buf,cmpbuf,9) == 0);
            count++;
        }

        // this is to just read away the extraneous msg that came in when we weren't
        // polling for it so we take out the message for the next round
        else if (server_event[i] == POLLIN){
            assert(poll_list[i].revents != POLLIN);
            assert((poll_list[i].events & POLLIN) != POLLIN);
            assert((server_event[i] & POLLIN) == POLLIN);

            mongoc_stream_mpi_t* mpi_stream = (mongoc_stream_mpi_t *)poll_list[i].stream;

            r = mongoc_stream_readv (poll_list[i].stream, &iov, 1, 9, TIMEOUT);
            assert(r == 9);
            assert (memcmp (buf,cmpbuf,9) == 0);
        }

        // we have to also read from the communicators that did not have pollin but had stuff
        // sent to them by the server

        // this is to unblock the server so that it can send for the next row to poll
        iov.iov_base = cmpbuf;
        iov.iov_len = sizeof(cmpbuf);

        r = mongoc_stream_writev (poll_list[i].stream, &iov, 1, TIMEOUT);
    }
    return count;
}


static void
print_row(int client_event[4]){
    for (int i = 0;i<4;i++){
        printf("%zd",client_event[i]);
    }
    printf("\n");
}

/* global declaration here */

static void*
poll_test3_client(){  
    // no threading from client just hit the server with multiple intercoms then poll
    MPI_Comm comm_list[4];
    mongoc_stream_t* stream_list[4];

    char message[MAX_MESSAGE_SIZE] = {0};
    ssize_t r;
    MPI_Status status;

    mongoc_stream_poll_t poll_list[4];
    int pollin_table[16][4];
    int permute_buf[4];

    for (int i = 0;i < 4;i++){
        int conn_sock;
        struct sockaddr_in server_addr = { 0 };
        conn_sock = socket(AF_INET, SOCK_STREAM, 0);
        assert (conn_sock);

        server_addr.sin_family = AF_INET;
        server_addr.sin_port = htons(27020);
        server_addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

        r = connect (conn_sock, (struct sockaddr *)&server_addr, sizeof(server_addr));
        assert (r == 0);

        printf("client %zd : connected socket\n",i);

        // comm join is blocking will connect when other side calls comm join
        r = MPI_Comm_join(conn_sock,&comm_list[i]);

        assert (r==0);

        close(conn_sock);

        stream_list[i] = mongoc_stream_mpi_new (comm_list[i]);
    }

    r = generate_pollin_table_rec(pollin_table,permute_buf,4,4,0);
    assert(r == 16);

    for (int clientRow = 0;clientRow < 16; clientRow++){

        // poll based on the table one row at a time
        for (int serverRow = 0; serverRow < 16; serverRow++){
            // find number of matching events between the rows
            int num_events = num_poll_events(pollin_table[clientRow],pollin_table[serverRow]);

            printf("client row: ");
            print_row(pollin_table[clientRow]);
            printf("server row: ");
            print_row(pollin_table[serverRow]);

            // make stream_poll struct based on client row
            create_mongoc_stream_poll_t_list(stream_list,pollin_table[clientRow],poll_list);

            r = mongoc_stream_poll(poll_list,4,TIMEOUT);

            printf("num events: %zd r: %zd \n",num_events,r);

            assert(r == num_events);

            // retrieve all events that are found
            r = retrieve_stream_polls(poll_list,pollin_table[serverRow]);
        }
    }

    for (int i = 0;i< 4;i++){
        mongoc_stream_destroy(stream_list[i]);
    }

    printf("CLIENT: done\n");

    return NULL;
}


static void*
poll_test3_send_server(void * argp){
    ssize_t r;
    mongoc_stream_t *stream;

    struct thread_args *args = (struct thread_args *)argp;

    int conn_sock = args->conn_sock;
    
    int count = args->count;
    
    int (*pollin_table)[4] = args->pollin_table;

    free(args);

    // comm join is blocking will connect when other side calls comm join
    MPI_Comm intercom;
    r = MPI_Comm_join(conn_sock,&intercom);
    assert (r == 0);

    close(conn_sock);

    stream = mongoc_stream_mpi_new(intercom);

    printf("Server %zd: Joined \n",count);

    char buf[9] = "pingpong";

    mongoc_iovec_t iov;

    // 16 iterations so that each combination of the server and client having
    // a particular polling configuration will be tested
    for (int iter = 0;iter< 16;iter++){
        for (int serverRow = 0;serverRow<16;serverRow++){
            iov.iov_base = buf;
            iov.iov_len = sizeof (buf);

            // if pollin is not in that cell table we don't send
            // we iterate down the column since each server thread is a particular column
            // with each row being the next test.
            if (pollin_table[serverRow][count] == POLLIN){  
                r = mongoc_stream_writev(stream,&iov,1,TIMEOUT);
                assert(r == 9);

            }

            char recv_buf[9] = {0};
            iov.iov_base = recv_buf;
            iov.iov_len = sizeof (recv_buf);
            
            // wait on a recv to block and to garunteed sends aren't queued on the network buffer
            r = mongoc_stream_readv (stream, &iov, 1,iov.iov_len,TIMEOUT);
            assert(r == 9);
            assert (memcmp (buf,recv_buf,9) == 0);
        }
    }

    printf("SERVER: Done\n");

    MPI_Comm_free(&intercom);
    return NULL;
}


static void*
poll_test3_server(int listen_sock){
    ssize_t r;
    int conn_sock;
    socklen_t clilen;
    struct sockaddr_in cli_addr;

    // generate all permutations of sends/NOPs of length n the server
    // will run through to then get tested by the clients poll
    int permute_buf[4];

    int pollin_table[16][4];

    r = generate_pollin_table_rec(pollin_table,permute_buf,4,4,0);
    assert(r == 16);

    pthread_t threads[4];

    // each server thread job is to send/not send each polling round to the client
    for (int i = 0; i< 4;i++){
        conn_sock = accept(listen_sock, (struct sockaddr *) &cli_addr,&clilen);
        assert (conn_sock);

        struct thread_args *args = (struct thread_args *) malloc(sizeof(struct thread_args));

        if (args != NULL)
        {
            args->conn_sock = conn_sock;
            args->count = i;
            args->pollin_table = pollin_table;

            r =  pthread_create(&(threads[i]), NULL, &(poll_test3_send_server),args);
            assert (r == 0);
        }
    }

    for (int i =0;i< 4;i++){
        pthread_join(threads[i], NULL);
    }

    return NULL;
}


/* Main MPI Server that setups the socket listener that will spawn the MPI 
 * connection stream then initiate the tests on the created MPI Stream.
 */

static void*
mpi_test_server () 
{
    ssize_t r;
    int listen_sock;
    mongoc_stream_t *stream;

    listen_sock = socket(AF_INET,SOCK_STREAM,0);
    assert(listen_sock);

    struct sockaddr_in server_addr = { 0 };
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = htonl (INADDR_LOOPBACK);
    server_addr.sin_port = htons(27020);

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

    sendv_test1_server(stream);

    sendv_test2_server(stream);

    sendv_test3_server(stream);

    sendv_test4_server(stream);

    poll_test1_server(stream);

    poll_test2_server(stream);

    poll_test3_server(listen_sock);

    close(listen_sock);
    close(conn_sock);
    mongoc_stream_destroy (stream);

    return NULL;
}


/* Main MPI Client that connects to the socket server then creates
 * a MPI connectionf from this TCP socket then calls client tests w
 * ith the created MPI Stream.
 */

static void*
mpi_test_client ()
{
    ssize_t r;
    int conn_sock;
    mongoc_stream_t *stream;

    struct sockaddr_in server_addr = { 0 };
    conn_sock = socket(AF_INET, SOCK_STREAM, 0);
    assert (conn_sock);

    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(27020);
    server_addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

    r = connect (conn_sock, (struct sockaddr *)&server_addr, sizeof(server_addr));
    assert (r == 0);

    // comm join is blocking will connect when other side calls comm join
    MPI_Comm intercom;
    r = MPI_Comm_join(conn_sock,&intercom);
    assert (r==0);

    // comm join is blocking will connect when other side calls comm join
    stream = mongoc_stream_mpi_new (intercom);

    sendv_test1_client(stream);

    sendv_test2_client(stream);

    sendv_test3_client(stream);

    sendv_test4_client(stream);

    poll_test1_client(stream);

    poll_test2_client(stream);

    poll_test3_client();

    close(conn_sock);
    mongoc_stream_destroy (stream);

    return NULL;
}

/* MPI Test are two processes that run the same file, the exec call
 * mpirun -np 2 ./<executable-name> is used to run 2 processes of
 * this file creating a server and a client to test the stream functionality.
 */
static void
test_mongoc_mpi_test (void)
{
    int provided;
    MPI_Init_thread(NULL, NULL, MPI_THREAD_MULTIPLE, &provided);

    int world_rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);

    if (world_rank == 0){
        mpi_test_server();
    }
    else if (world_rank == 1){
        mpi_test_client();
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