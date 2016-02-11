../build.sh

cp ../dist/build/hhdfs/hhdfs ./Client
chmod +x ./Client/hhdfs
chmod +x ./Client/*.sh

cp ../dist/build/hhdfs/hhdfs ./TestClient
chmod +x ./TestClient/hhdfs
chmod +x ./TestClient/*.sh

cp ../dist/build/hhdfs/hhdfs ./DataNode#1
cp ../dist/build/hhdfs/hhdfs ./DataNode#2
cp ../dist/build/hhdfs/hhdfs ./DataNode#3
cp ../dist/build/hhdfs/hhdfs ./DataNode#4
cp ../dist/build/hhdfs/hhdfs ./DataNode#5
cp ../dist/build/hhdfs/hhdfs ./DataNode#6
cp ../dist/build/hhdfs/hhdfs ./DataNode#7
cp ../dist/build/hhdfs/hhdfs ./DataNode#8

chmod +x ./DataNode#*/hhdfs
chmod +x ./DataNode#*/*.sh

cp ../dist/build/hhdfs/hhdfs ./NameNode
chmod +x ./NameNode/hhdfs
chmod +x ./NameNode/*.sh
