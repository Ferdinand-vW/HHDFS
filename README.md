# HHDFS

Haskell Hadoop Distributed File System

HHDFS is an re-implementation of HDFS in Haskell.
As this is a University related project, our main focus will be on the performance and scalability of HHDFS.
So do not expect HHDFS to be very robust. Take a look at our features to see what we have implemented.


# Features

- Data chunking
- Replication pipelining
- Client-side streaming io
- Remote client access

# Architecture

The architecture of HHDFS is mostly a copy of HDFS. We therefore recommend you to read the [documentation](https://hadoop.apache.org/docs/r1.2.1/hdfs_design.html)

The only change to the architecture we made is adding a 'proxy' to each node. The proxy sits between the
client and the Namenode/Datanode. It is used to allow clients to remotely connect to the network.

The proxy of the Namenode simply passed any received messages from the client to the Namenode and it also sends any responses from the Namenode back to the client.

The proxy of the Datanode also does reading from and writing to the local filesystem.

# Installation

To install on you system simply clone this repository to a folder. Then from that folder type

    cabal sandbox init
    cabal install --only-dependencies
    cabal build

# Usage

You can manually start up the network or use our testing scripts, which are
located in the test folder.

## To manually start the network:

First start up a NameNode supplying the local host and a port:

    ./hhdfs namenode 127.0.0.1 44444

Now you can start a number of Datanodes. This time also supply
the host and port of the Namenode:

    ./hhdfs datanode 127.0.0.1 44446 127.0.0.1 44444

In this case we are starting up a Datanode on port 44446 and also telling him that the Namenode is running on port 44444.

It is important that in this case you do not use port 44445 for the Datanode as each Node uses
2 ports: The given port and the same port + 1

Finally you can start up a Client only suppling the host and port of the NameNode:

    ./hhdfs client 127.0.0.1 44444

It is possible for the Client to not be on the same Network as the Namenode and Datanodes,
but you have to make sure yourself that any messages from the Client to the Namenode or
Datanode are correctly forwarded (e.g. port forwarding).

## Using our scripts to startup a network:


#### Linux:
Before being able to start the network you need to ensure that you have a working gnome-terminal, which
should be the default terminal. Then you have to add a profile to the terminal named **"KEEPOPEN"**. The only thing
you have to change is that after executing a script the terminal will remain open. Next you can simply do the following:

#### OSX:
The startup script just assume you are using iTerm as you terminal application. The run.osx.sh can be easily adapted to whatever terminal application you are currently using.

----
These scripts have proven to be working on our own systems: Linux Mint 17.3 and OSX 10.10.4

1. ./build.sh
2. ./clean.sh
3. ./run.linux.sh
4. ./run.osx.sh
5. ./runTest.sh

**./build.sh** will build the project and copy the executable to the folders located in this folder.

**./clean.sh** will remove any persistent data from the network (and client).

**./runTest.sh** will run the test client. It will immediately start a test. Simply press enter to run the next. There is a total of three tests.

#### Linux:
**./run.linux.sh** will start a Namenode, a number of Datanodes and a client. This script
assumes you are using the gnome-terminal

#### OSX:
**./run.osx.sh** starts up a Namenode, a client and a number of Datanodes. This is a simple
osascript file that launches up everything in a new terminal window.

## Usage of the client:
There are five commands possible:

* **show** - Lists all files on the network
* **write** *local* *remote* - Writes to the network
* **read** *remote* - Reads from the network
* **quit** - Closes client application
* **help** - Shows the above comands

# Authors

Giovanni Garufi
Wilco Kusee
Ferdinand van Walree
