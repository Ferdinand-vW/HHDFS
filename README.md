# HHDFS

Haskell Hadoop Distributed File System

HHDFS is an re-implementation of HDFS in Haskell.
As this is a University related project, our main focus will be on the performance and scalability
of HHDFS. So do not expect HHDFS to be very robust. Take a look at our features to see what we have
implemented.


# Features

- Data chunking
- Replication
- Persistency
- Remote client access

# Installation

Cabal install
Cabal build

# Usage

First start up a NameNode supplying the local host and a port:
./hhdfs namenode 127.0.0.1 44444

Now you can start a number of DataNodes. This time also supply 
the host and port of the NameNode:
./hhdfs datanode 127.0.0.1 44446 127.0.0.1 44444

It is important that in this case you do not use port 44445 as each Node uses
2 ports: The given port and the same port + 1

Finally you can start up a Client only suppling the host and port of the NameNode:
./hhdfs client 127.0.0.1 44444

It is possible for the Client to not be on the same Network as the NameNode and DataNodes,
but you have to make sure yourself that any messages from the Client to the NameNode or
DataNode are correctly forwarded (e.g. port forwarding).

# Authors

Giovanni Garufi
Wilco Kusee
Ferdinand van Walree