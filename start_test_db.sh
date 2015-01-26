#!/bin/sh

if [ -z $(which postgres) ] ; then
    echo "Postgres not found, you may need to launch like so: PATH=\$PATH:/usr/lib/postgresql/9.3/bin/ $0"
    exit 1
fi

postgres -D datadir/ -p 10432 -k `pwd`/datadir/
