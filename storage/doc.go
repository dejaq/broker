/*Package storage handles all the logic that deals with the Local metadata of a broker.
It acts like a wrapper on top of BadgerDB offering 2 major views: Metadata and Messages.

Internally the data is kept in the same database instance (metadata, topics, partitions and metadata),
but the complexity to handle them is abstracted.
*/
package storage
