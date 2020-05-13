package storage

/* Package messages handles all the logic that deals with the Local messages of a broker.
It acts like a wrapper on top of BadgerDB offering 2 major views: Metadata and Messages.

Internally the data is kept in the same database instance (metadata, topics, partitions and messages),
but the complexity to handle them is abstracted.


*/
