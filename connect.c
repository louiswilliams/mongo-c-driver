#include <bson.h>
#include <bcon.h>
#include <mongoc.h>
#include <stdio.h>

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
    */// adding in mpi to the uri see if it works
   client = mongoc_client_new ("mongodb://localhost:27020/?mpi=true");

   /*
    * Get a handle on the database "db_name" and collection "coll_name"
    */
   database = mongoc_client_get_database (client, "db_name");

   collection = mongoc_client_get_collection (client, "db_name", "coll_name");
   /*
    * Do work. This example pings the database, prints the result as JSON and
    * performs an insert
    */
   command = BCON_NEW ("ping", BCON_INT32 (1));

   retval = mongoc_client_command_simple (client, "admin", command, NULL, &reply, &error);

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


   bson_t *document;
   bson_t  child;

   document = bson_new ();

   /*
    * Append {"hello" : "world"} to the document.
    * Passing -1 for the length argument tells libbson to calculate the string length.
    */
   bson_append_utf8 (document, "hello", -1, "world", -1);

   /*
    * For convenience, this macro is equivalent.
    */
   BSON_APPEND_UTF8 (document, "hello", "world");

   /*
    * Begin a subdocument.
    */
   BSON_APPEND_DOCUMENT_BEGIN (document, "subdoc", &child);
   BSON_APPEND_UTF8 (&child, "subkey", "value");
   bson_append_document_end (document, &child);

   // /*
   //  * Print the document as a JSON string.
   //  */
   // str = bson_as_json (document, NULL);
   // printf ("%s\n", str);

   if (!mongoc_collection_insert (collection, MONGOC_INSERT_NONE, document, NULL, &error)) {
      fprintf (stderr, "%s\n", error.message);
   }

   bson_t *query;
   mongoc_cursor_t *cursor;
   const bson_t *doc;

   query = bson_new ();
   cursor = mongoc_collection_find (collection, MONGOC_QUERY_NONE, 0, 0, 0, query, NULL, NULL);

   while (mongoc_cursor_next (cursor, &doc)) {
     str = bson_as_json (doc, NULL);
     printf ("%s\n", str);
     bson_free (str);
   }

   bson_destroy(query);
   mongoc_cursor_destroy(cursor);

   /*
    * Clean up allocated bson documents.
    */
   bson_destroy (document);

   bson_destroy (insert);
   bson_destroy (&reply);
   bson_destroy (command);

   /*
    * Release our handles and clean up libmongoc
    */
   mongoc_collection_destroy (collection);
   mongoc_database_destroy (database);
   mongoc_client_destroy (client);
   mongoc_cleanup ();

   return 0;
}