#!/usr/bin/env python

import os
import objstore

if __name__ == '__main__':

    if not os.path.isdir('teststore'):
        os.makedirs('teststore')

    store = objstore.ObjectStore('teststore')

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


