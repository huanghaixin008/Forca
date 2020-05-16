dd if=/dev/zero of=data/nvm.sim bs=1G count=8
make clean
make server
./server
