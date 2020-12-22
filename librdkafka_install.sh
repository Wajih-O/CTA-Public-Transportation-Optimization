sudo apt-get install -y libssl-dev zlib1g-dev gcc g++ make
git clone https://github.com/edenhill/librdkafka && cd librdkafka && ./configure --prefix=/usr && make && sudo make install