#!/usr/bin/env python

"""
Implementation of simple deduplicating object store
"""

import hashlib
import threading
import os
import json
import struct

DATADIR = 'data'
METADIR = 'links'
STATEFILE = 'state.json'
HEADER_SIZE = 8

"""
Design
======

The datastore main directory consists of two directories links and data.
The data directory stores objects by its content addressed name using sha1 digest.
Link files are objectid => content links
File format for data file is [HEADER|DATA]. HEADER is an 8 byte field that stores
the reference count. Eg. If 5 objects are stored with same content, it will create
one data file with [5|DATA] as its content. Reference count is incremented on duplicate
put and decremented on deletes. When rfcount becomes zero, data file is deleted.

All data file reads or writes happen outside of locks. Hence concurrent threads are free
to execute without blocking (Since data files are never updated and once a fd is obtained,
it can read even when files is deleted). Core data structures are protected by a global lock.

Deleted object ids are reused for next inserted objects
Concurrent threads can share a global ObjectStore class object and invoke put(), get() and delete() methods.
"""

def update_rc(datafile, offset):
    """
    Update reference count field of data file
    """
    f = open(datafile, 'rb+')
    rc = struct.unpack("q", f.read(HEADER_SIZE))[0]
    f.seek(0)
    rc += offset

    if rc > 0:
        f.write(struct.pack("q", rc))

    f.close()

    return rc

def add_link(hashcode, dst):
    """
    Update link file content sha1
    """
    f = open(dst,'w')
    f.write(hashcode)
    f.close()

def read_link(src):
    """
    Read content sha1 from link
    """
    f = open(src)
    hashcode = f.read()
    f.close()
    return hashcode


class ObjectStore:
    """
    Implementation of deduplicating object store
    """
    def __init__(self, path):
        self.lock = threading.Lock()
        self.datadir = os.path.join(path, DATADIR)
        self.metadir = os.path.join(path, METADIR)
        self.statefile = os.path.join(path, STATEFILE)
        # Create main db directories if not present
        try:
            os.makedirs(self.datadir)
            os.makedirs(self.metadir)
        except:
            pass

        self._load_states()

    def _store_states(self):
        """
        Store current object id and list of free object ids
        """
        data = {"object_id":self.objectid, "free_objectids": self.free_objectids}
        f = open(self.statefile, 'w')
        json.dump(data, f)
        f.close()

    def _load_states(self):
        """
        Read back persisted state from disk
        """
        data = {"object_id":0, "free_objectids": []}
        if os.path.exists(self.statefile):
            f = open(self.statefile)
            data = json.load(f)
            f.close()
        self.objectid = data["object_id"]
        self.free_objectids = data["free_objectids"]

    def close(self):
        """
        Persist required metadata
        """
        self._store_states()

    def put(self, data):
        """
        Put object API
        """
        hashcode = hashlib.sha1(data).hexdigest()
        datapath = os.path.join(self.datadir, hashcode)

        self.lock.acquire()
        # Get next object id
        if self.free_objectids:
            oid = self.free_objectids.pop()
        else:
            self.objectid += 1
            oid = self.objectid

        objpath = os.path.join(self.metadir, "%d" %oid)
        if not os.path.exists(datapath):
            # Let us create the file and reacquire lock
            self.lock.release()
            tmpfile = "%s.%d" %(datapath, oid)
            f = open(tmpfile, 'wb')
            # Put header
            f.seek(HEADER_SIZE)
            # Write actual data
            f.write(data)
            f.close()
            self.lock.acquire()
            # Somebody has created object with same content
            # let us remove what i have created
            if os.path.exists(datapath):
                os.remove(tmpfile)
            else:
                # Move tmpfile to content data file
                os.rename(tmpfile, datapath)

        # Add object id pointer to content address
        add_link(hashcode, objpath)

        # Increment refcount
        update_rc(datapath, 1)
        self.lock.release()

        return oid

    def get(self, oid):
        """
        Get object API
        """
        objpath = os.path.join(self.metadir, "%d" %oid)
        fhandle = None
        self.lock.acquire()
        if os.path.exists(objpath):
            hashcode = read_link(objpath)
            datapath = os.path.join(self.datadir, hashcode)
            fhandle = open(datapath, 'rb')
        self.lock.release()

        if fhandle:
            # Ignore header
            fhandle.read(HEADER_SIZE)
            # Read object data
            data = fhandle.read()
            fhandle.close()
            return data

        return None

    def delete(self, oid):
        """
        Delete object API
        """
        objpath = os.path.join(self.metadir, "%d" %oid)
        self.lock.acquire()
        if os.path.exists(objpath):
            hashcode = read_link(objpath)
            datapath = os.path.join(self.datadir, hashcode)
            # Decrement refcount
            rc = update_rc(datapath, -1)
            # No body needs this data
            if rc == 0:
                os.remove(datapath)

            # Remove the link
            os.remove(objpath)

            # Add object id to free pool
            self.free_objectids.insert(0, oid)

        self.lock.release()

if __name__ == '__main__':

    store = ObjectStore('teststore')

    for i in xrange(100):
        store.put("hello world!")

    store.put("another data 1")
    store.put("another data 2")

    print store.get(1)
    print store.get(2)
    print store.get(101)
    print store.get(102)

    store.delete(100)
    print store.get(100)
    print store.put("hello world! 3")
    print store.get(100)

    store.close()

