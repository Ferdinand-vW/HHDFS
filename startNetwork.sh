# Starting up the name node
./dist/build/hhdfs/hhdfs namenode 127.0.0.1 44444 &

# Starting up two data nodes
./dist/build/hhdfs/hhdfs datanode 127.0.0.1 44445 127.0.0.1:44444:0 &
./dist/build/hhdfs/hhdfs datanode 127.0.0.1 44446 127.0.0.1:44444:0 &

# Start up the client
./dist/build/hhdfs/hhdfs client 127.0.0.1 44447 127.0.0.1:44444:0
