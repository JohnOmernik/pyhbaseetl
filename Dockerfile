FROM ubuntu:latest

WORKDIR /app

RUN mkdir -p /app/code && mkdir -p /app/data && mkdir -p /opt/mapr

#RUN apt-get update && apt-get install -y python3 python3-dev python3-setuptools python3-pip zlib1g-dev libevent-pthreads-2.0-5 libssl-dev libsasl2-dev liblz4-dev libsnappy1v5 libsnappy-dev liblzo2-2 liblzo2-dev clang-3.9 lldb-3.9 && apt-get clean && apt-get autoremove -y
RUN apt-get update && apt-get install -y git build-essential python python-dev wget openjdk-8-jdk python-setuptools python-pip zlib1g-dev libssl-dev libsasl2-dev liblz4-dev libsnappy1v5 libsnappy-dev liblzo2-2 liblzo2-dev  && apt-get clean && apt-get autoremove -y
RUN pip install python-snappy python-lzo brotli
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/
RUN wget https://github.com/edenhill/librdkafka/archive/v0.9.4.tar.gz && tar zxf v0.9.4.tar.gz && cd librdkafka-0.9.4 && ./configure && make && make install && ldconfig && cd .. && rm -rf librdkafka-0.9.4 && rm v0.9.4.tar.gz && pip install confluent-kafka confluent-kafka[avro] kazoo

ADD lib /opt/mapr/lib
ADD include /opt/mapr/include


ENV LD_LIBRARY_PATH=/usr/lib/jvm/java-8-openjdk-amd64/jre/lib/amd64/server:/opt/mapr/lib
ENV PYCHBASE_LIBJVM_DIR=/usr/lib/jvm/java-8-openjdk-amd64/jre/lib/amd64/server
ENV LD_PRELOAD=/usr/lib/jvm/java-8-openjdk-amd64/jre/lib/amd64/server/libjvm.so

RUN git clone https://github.com/mkmoisen/pychbase.git && cd pychbase && sed -i "s/library_dirs=library_dirs,/library_dirs=library_dirs,extra_compile_args=['-fpermissive'],/g" setup.py && python setup.py install && cd .. && rm -rf pychbase

ADD pybase.py /app/code/

CMD ["/bin/bash"]
