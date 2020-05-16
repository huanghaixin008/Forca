# dd if=/dev/zero of=data/nvm.sim bs=1M count=128
make clean
make client
./client
