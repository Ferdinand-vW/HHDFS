# HHDFS

Haskell Hadoop Distributed File System

HHDFS is an re-implementation of HDFS in Haskell.
As this is a University related project, our main focus will be on the performance and scalability
of HHDFS. So do not expect HHDFS to be very robust. Take a look at our features to see what we have
implemented.


# Features

- Data chunking
- Replication pipelining
- Client-side streaming io
- Remote client access

# Architecture

The architecture of HHDFS is mostly a copy of HDFS. We therefore recommend you to read the documentation
of hdfs: https://hadoop.apache.org/docs/r1.2.1/hdfs_design.html

The only change to the architecture we made is adding a 'proxy' to each node. The proxy sits between the 
client and the namenode/datanode. It is used to allow clients to remotely connect to the network. The proxy 
of the namenode simply passed any received messages from the client to the namenode and it also sends any 
responses from the namenode back to the client. The proxy of the datanode also does reading from and writing
to file.

# Installation

Cabal install
Cabal build

# Usage

You can manually start up the network or use our testing scripts, which are
located in the test folder.

To manually start the network:

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

Using our scripts to startup a network:

These scripts have proven to be working on our own systems: Linux Mint 17.3 and OSX (??)

Linux:
Before being able to start the network you need to ensure that you have a working gnome-terminal, which
should be the default terminal. Then you have to add a profile to the terminal named "KEEPOPEN". The only thing
you have to change is that after executing a script the terminal will remain open. Next you can simply do the following:
1. ./build.sh
2. ./clean.sh
3. ./run.linux.sh
4. ./runTest.sh

./build.sh will build the project and copy the executable to the folders located in this folder. ./clean.sh will remove any
persitent data from the network (and client). ./run.linux.sh will start a namenode, two datanodes and a client. It is possible
to start a total of 8 datanodes, but you have to uncomment the lines in the scripts. ./runTest.sh will run the test client. It 
will immediately start a test. Simply press enter to run the next. There is a total of three tests.

OSX:


Usage of the client:
There are five commands possible:
show --list all files on the network
write local remote --write to the network
read remote --read from the network
quit --Closes client application
help --Shows the above comands

# Authors

Giovanni Garufi
Wilco Kusee
Ferdinand van Walree