/*-------------------------------------------------------------------------
 * CS448:
 * rdb.c
 *	  This code manages relations stored in RocksDB.
 *
 *
 * Portions Copyright (c) 1996-2013, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/smgr/rdb.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <unistd.h>
#include <fcntl.h>
#include <sys/file.h>

#include "catalog/catalog.h"
#include "common/relpath.h"
#include "storage/relfilenode.h"
#include "storage/smgr.h"
#include "utils/memutils.h"
#include "pg_trace.h"
#include "rocksdb/c.h"


/* CS648: Importing necessary headers to unpack and pack a buffer with rows. */
#include "storage/bufpage.h"
#include "access/htup.h"
#include "access/htup_details.h"



/* CS448: helper functions */

/* Location to store RocksDB files. */

#define ROCKSDB_DATA_DIR "/tmp/"

typedef char RocksDBPath[101];


static char*
replaceChar (char *relp)
{
	/* Replace / with _ . */
	int i = 0;
	while (relp[i] != '\0')
	{
		if (relp[i] == '/')
		{
			relp[i] = '_';
		}
		i++;
	}
	return relp;
}

/*
 * Gets a relation name from SMgrRelation and returns it in RocksDBPath
 */

static void
getRocksDbPath (RocksDBPath outBuffer, SMgrRelation reln, ForkNumber forknum)
{
	char *relp = relpath(reln->smgr_rnode, forknum);
	relp = replaceChar(relp);
	snprintf(outBuffer, sizeof(RocksDBPath), ROCKSDB_DATA_DIR"%s", relp);
}

/* end CS448: helper functions */

/* CS448:
 *	RDBinit() -- called during backend startup to initialize.
 */
void
RDBinit (void)
{
	/* nothing to be done */
}

/* CS448:
 *	RDBclose() -- Close the specified relation, if it isn't closed already.
 *
 *	We open/close and flush to disk per write, so nothing to do here.
 */
void
RDBclose (SMgrRelation reln, ForkNumber forknum)
{
	/* nothing to be done */
}

/* CS448:
 *	This should be left as is. You are not required to implement this.
 *	We are not going to support pre-fetching from disk.
 *	See mdprefetch() in md.c for further details on what this was supposed to
 *	do.
 */
void
RDBprefetch (SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum)
{
}

/*
 * CS448:
 *	This should be left as is. You are not required to implement this.
 *	We are not going to support deletion so do nothing here.
 *	See mdunlink() in md.c for details on what this was supposed to do.
 */
void
RDBunlink (RelFileNodeBackend rnode, ForkNumber forkNum, bool isRedo)
{
	/* do nothing */
}

/*
 * CS448:
 *	This should be left as is. You are not required to implement this.
 *	We are not going to support deletion so do nothing here.
 *	See mdtruncate() in md.c for details on what this was supposed to do.
 */
void
RDBtruncate (SMgrRelation reln, ForkNumber forknum, BlockNumber nblocks)
{
	/* nothing to be done */
}

/*
 * CS448:
 *	RDBimmedsync() -- Force the specified relation to stable storage.
 *
 *	We flush to disk per write, so nothing to do here.
 */
void
RDBimmedsync (SMgrRelation reln, ForkNumber forknum)
{
	/* nothing to be done */
}


/* CS448:
 *	RDBextend() -- For our purpose this has the same semantics as RDBwrite().
 *
 *	See the comment above mdextend() in md.c for details.
 */
void
RDBextend (SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
			char *buffer,
			bool skipFsync)
{
	RDBwrite(reln, forknum, blocknum, buffer, skipFsync);
}


/*
 *  ============================================================================================================
 *  DO NOT CHANGE FUNCTIONS ABOVE THIS LINE
 *  ============================================================================================================
 */

/* CS448:
 *	RDBexists() -- is the relation/fork in RocksDB ?
 *	
 
 * 	IMPORTANT: this function used to detect rdb tables.
 */
bool
RDBexists (SMgrRelation reln, ForkNumber forkNum)
{

/*
 * 	YOUR CODE HERE
 *	Hint: To check if relation exists we simply try to open the database in RocksDB, 
 *	and if it opens without error, then the relation exists.
 *	Steps to do it:
 *	1) Create rocksdb options using rocksdb_options_create function
 *	2) Do not create the database while opening it. To do this check out the function: rocksdb_options_set_create_if_missing
 *	3) Get a database name from the relation name, using function: getRocksDbPath(path, reln, forkNum)
 *	   Here, reln, and fornNum will be used to create a relation name which will be stored in path.
 *	4) Then open the rocksdb database using rocksdb_open
 *	5) Check for error, if error, return false. If no error then close the database and return true.
*/


	//Opens a database if it exists.
	rocksdb_t *db;
  	rocksdb_options_t *options = rocksdb_options_create();
  	rocksdb_options_set_create_if_missing(options, 0);
	RocksDBPath path;
	getRocksDbPath(path, reln, forkNum);
	char *err = NULL;
 	db = rocksdb_open(options, path, &err);
  	

	//Return false if it doesn't exist. True otherwise.
	if(err)
		return false;
	else
		{
			rocksdb_options_destroy(options);
			rocksdb_close(db);
			return true;
		}
}

/* CS448:
 *	RDBcreate() -- Creates a relation in RocksDB.
 */
void
RDBcreate (SMgrRelation reln, ForkNumber forkNum, bool isRedo)
{
	elog(DEBUG1,"RDBcreate");

	/*
	 * YOUR CODE HERE
	 * Hint: open a rocksdb connection from the reln
	 * create the rocksdb relation if not there using one of the rocksdb open options
	 * close the connection and free rocksdb resources  before exit
	 */

	//Opens a database if it exists. Creates it if it doesn't exist.
	rocksdb_t *db;
  	rocksdb_options_t *options = rocksdb_options_create();
	RocksDBPath path;
	char *err = NULL;


	rocksdb_options_set_create_if_missing(options, 1);
	getRocksDbPath(path, reln, forkNum);
 	db = rocksdb_open(options, path, &err);

 	/*CS648: The number of blocks in a table is stored as meta-data. Here it is initialized to be 0 during table creation.*/
 	rocksdb_writeoptions_t *writeoptions = rocksdb_writeoptions_create();
 	BlockNumber num_blocks = 0;
 	rocksdb_put(db, writeoptions, "NUM_BLOCKS", strlen("NUM_BLOCKS"), &num_blocks,sizeof(BlockNumber),&err);

	//Close connection and clean-up.
 	rocksdb_writeoptions_destroy(writeoptions);
	rocksdb_options_destroy(options);
	rocksdb_close(db);

	
}

/* CS448:
 *	RDBread() --  Read the specified block from a relation.
 */
void
RDBread (SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
			char *buffer)
{
	elog(DEBUG1,"Entering RDBread with blocknum %d",blocknum);
	/*
	 * YOUR CODE HERE
	 * Hint: open the rocksdb connection from reln before reading the block
	 * close the connection and free rocksdb resources  before exit
	 */

	//Opens a database if it exists.
	rocksdb_t *db;
  	rocksdb_options_t *options = rocksdb_options_create();
	RocksDBPath path;
	char *err = NULL;
	size_t len;
	int *lines,*spl;
	char key[101];

	rocksdb_options_set_create_if_missing(options, 0);
	getRocksDbPath(path, reln, forknum);
 	db = rocksdb_open(options, path, &err);

 	rocksdb_readoptions_t *readoptions = rocksdb_readoptions_create();


 	/*CS648: Checking the number of tuples in this block which is stored as meta-data.*/
	sprintf(key,"%dB",blocknum);
	lines = (int*)rocksdb_get(db, readoptions, key, strlen(key), &len, &err);
	elog(DEBUG1,"RDBread:There appears to be %d tuples",*lines);

	/*CS648: Checking the "special size" of this block which is stored as meta-data.*/
	sprintf(key,"%dBS",blocknum);
	spl = (int*)rocksdb_get(db, readoptions, key, strlen(key), &len, &err);

	/*CS648:Initializing the buffer and filling it with tuples.*/
	PageInit((Page)buffer,BLCKSZ,*spl);
	for(OffsetNumber lineoff = FirstOffsetNumber; lineoff <= *lines; lineoff++)
	{
		sprintf(key,"%dB%d",blocknum,lineoff);
		elog(DEBUG1,"RDBread: Reading block %s",key);
		HeapTupleHeader loctup = (HeapTupleHeader)rocksdb_get(db, readoptions, key, strlen(key), &len, &err);
		PageAddItem((Page)buffer,(Item)loctup,len,PageGetMaxOffsetNumber((Page)buffer)+1,true,true);
	}
	

	//close connection and clean-up
	rocksdb_readoptions_destroy(readoptions);
	rocksdb_options_destroy(options);
	rocksdb_close(db);


}

/* CS448:
 *	RDBwrite() --  Write the supplied block at the appropriate location.
 *
 *	Note that we ignore skipFsync since it's used for checkpointing which we
 *	are not going to care about. We synchronously flush to disk anyways.
 */
void
RDBwrite (SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
			char *buffer,
			bool skipFsync)
{

		/*
		 * YOUR CODE HERE
		 * Hint: open the rocksdb connection from reln before writing the block
		 * close the connection and free rocksdb resources  before exit
		 */

	elog(DEBUG1,"Entering RDBwrite with blocknum %d",blocknum);
	

	//Opens a database if it exists. Writes the value associated with blocknum found in the buffer input parameter.
	rocksdb_t *db;
  	rocksdb_options_t *options = rocksdb_options_create();
	RocksDBPath path;
	char *err = NULL;
 	Page dp = (Page) buffer;
	int lines,spl;
	ItemId lpp;
	OffsetNumber lineoff;
	size_t len;
	char key[101];
	BlockNumber *num_blocks;

	getRocksDbPath(path, reln, forknum);
	rocksdb_options_set_create_if_missing(options, 0);
	db = rocksdb_open(options, path, &err);

	rocksdb_writeoptions_t *writeoptions = rocksdb_writeoptions_create();
	rocksdb_readoptions_t *readoptions = rocksdb_readoptions_create();
	
	sprintf(key,"%dB",blocknum);
	lines = (int)PageGetMaxOffsetNumber(dp);

	/*CS648:Unpacking rows and writing them out*/
	if(PageGetPageSize(dp)==BLCKSZ)
	{
		elog(DEBUG1,"RDBwrite:%d tuples!",lines);
		/*CS648:Updating the number of rows in this block*/
		rocksdb_put(db, writeoptions, key, strlen(key), &lines,sizeof(int),&err);
		/*CS648:Updating the "special" size of this block*/
		spl = PageGetSpecialSize(dp);
		sprintf(key,"%dBS",blocknum);
		rocksdb_put(db, writeoptions, key, strlen(key), &spl,sizeof(int),&err);

		for (lineoff = FirstOffsetNumber, lpp = PageGetItemId(dp, lineoff);
				 lineoff <= lines;
				 lineoff++, lpp++)
			{
				if (ItemIdIsNormal(lpp))
				{
					HeapTupleHeader loctup =  PageGetItem(dp, lpp);
					sprintf(key,"%dB%d",blocknum,loctup->t_ctid.ip_posid);
					elog(DEBUG1,"RDBwrite:writing %s",key);
					rocksdb_put(db, writeoptions, key, strlen(key), loctup, ItemIdGetLength(lpp),&err);

				}
			}
			
	}
	else
	{
		/*CS648: We reach here if the buffer written out is a bunch of 0s. Instead of writing 0s out, we increment the number of blocks
		in the relation by 1 */
		num_blocks = (BlockNumber*)rocksdb_get(db, readoptions, "NUM_BLOCKS", strlen("NUM_BLOCKS"), &len, &err);
		elog(DEBUG1,"RDBwrite:NUM_BLOCKS currently is %d",*num_blocks);
		*num_blocks = *num_blocks+1;
		elog(DEBUG1,"RDBwrite:Updating NUM_BLOCKS to %d",*num_blocks);
		rocksdb_put(db, writeoptions, "NUM_BLOCKS", strlen("NUM_BLOCKS"), num_blocks,sizeof(BlockNumber),&err);

		/*CS648: The number of tuples in a block is stored as meta-data. We initialize it with 0 here*/
		elog(DEBUG1,"RDBwrite:Updating %s to %d",key,lines);
		rocksdb_put(db, writeoptions, key, strlen(key), &lines,sizeof(int),&err);
	}
	//Close connection and cleanup
  	rocksdb_writeoptions_destroy(writeoptions);
	rocksdb_options_destroy(options);
	rocksdb_close(db);
}

/*
 * CS448:
 *	RDBnblocks() -- Calculate the number of blocks in the
 *					supplied relation.
 */
BlockNumber
RDBnblocks (SMgrRelation reln, ForkNumber forknum)
{

		/*
		 * YOUR CODE HERE
		 * Hint: open the rocksdb connection from reln before reading the block
		 * close the connection and free rocksdb resources  before exit
		 * You will need an iterator over rocksdb relation to count the number of blocks.
		 * Look for: rocksdb_create_iterator
		 *
		 *
		*/

	elog(DEBUG1,"Entering RDBnblocks");
	//Opens a database if it exists.
	rocksdb_t *db;
  	rocksdb_options_t *options = rocksdb_options_create();
  	rocksdb_readoptions_t* readoptions = rocksdb_readoptions_create();
	RocksDBPath path;
	char *err = NULL;
	size_t len;
	BlockNumber *to_return;

	rocksdb_options_set_create_if_missing(options, 0);
	getRocksDbPath(path, reln, forknum);
 	db = rocksdb_open(options, path, &err);

 	/*CS648: Number of blocks is stored as meta-data.*/
	to_return = (BlockNumber*)rocksdb_get(db, readoptions, "NUM_BLOCKS", strlen("NUM_BLOCKS"), &len, &err);


	rocksdb_options_destroy(options);
	rocksdb_close(db);
	rocksdb_readoptions_destroy(readoptions);

	elog(DEBUG1,"RDBnblocks:%d",*to_return);
	
	return *to_return;
}


