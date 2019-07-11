# A docker image for post run processing

FROM ubuntu:18.04

RUN apt-get -y update && apt-get install -y  python3-pip && rm -rf /var/lib/apt/lists/*
# Install AWS CLI, boto3 and watchover
RUN pip3 install \
  pandas==0.22.0 \
  awscli==1.16.187 \
  boto3==1.9.177 \
  watchtower==0.6.0 \
  urllib3==1.21.1

# SETUP NEW ENTRYPOINT
RUN mkdir -p /home/ubuntu/
WORKDIR /home/ubuntu
COPY ./* .
RUN chmod 755 initialize-worker.sh

ENTRYPOINT ["/bin/bash"]
# ENTRYPOINT ["/home/ubuntu/initialize-worker.sh"]
# CMD [""]