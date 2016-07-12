#include <bson.h>
#include <bcon.h>
#include <mongoc.h>

int
main (int   argc,
      char *argv[])
{
   mongoc_client_t      *client;
   mongoc_database_t    *database;
   mongoc_collection_t  *collection;
   bson_t               *command,
                         reply,
                        *insert;
   bson_error_t          error;
   char                 *str;
   bool                  retval;

   /*
    * Required to initialize libmongoc's internals
    */
   mongoc_init ();

   /*
    * Create a new client instance port 27020 is the mpi port
    */
   client = mongoc_client_new ("mongodb://localhost:27020");

   printf("1. we are connecting\n");

   /*
    * Get a handle on the database "db_name" and collection "coll_name"
    */
   database = mongoc_client_get_database (client, "db_name");

   printf("2. database \n");
   collection = mongoc_client_get_collection (client, "db_name", "coll_name");
   printf("3. collection \n");
   /*
    * Do work. This example pings the database, prints the result as JSON and
    * performs an insert
    */
   command = BCON_NEW ("ping", BCON_INT32 (1));

   retval = mongoc_client_command_simple (client, "admin", command, NULL, &reply, &error);

    printf("4. command simple \n");

   if (!retval) {
      fprintf (stderr, "%s\n", error.message);
      return EXIT_FAILURE;
   }

   str = bson_as_json (&reply, NULL);
   printf ("%s\n", str);

   insert = BCON_NEW ("hello", BCON_UTF8 ("world"));

   if (!mongoc_collection_insert (collection, MONGOC_INSERT_NONE, insert, NULL, &error)) {
      fprintf (stderr, "%s\n", error.message);
   }

  printf("5. insert \n");

   bson_destroy (insert);
   bson_destroy (&reply);
   bson_destroy (command);
   bson_free (str);

   /*
    * Release our handles and clean up libmongoc
    */
   mongoc_collection_destroy (collection);
   mongoc_database_destroy (database);
   mongoc_client_destroy (client);
   mongoc_cleanup ();

   return 0;
}