FROM ubuntu:18.04

RUN apt-get update
RUN apt-get install -y gnupg openjdk-8-jdk scala python3 iputils-ping nano

# https://www.scala-sbt.org/1.x/docs/Installing-sbt-on-Linux.html
RUN echo "deb https://dl.bintray.com/sbt/debian /" | tee -a /etc/apt/sources.list.d/sbt.list
RUN apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 2EE0EA64E40A89B84B2DF73499E82A75642AC823

RUN apt-get update
RUN apt-get install -y sbt

COPY . /usr/lib/dbeam

CMD ["/bin/bash"]