# A docker instance for accessing AWS resources
# This wraps the cellprofiler docker registry
#

FROM cellprofiler/cellprofiler:3.1.8

# Install S3FS. 
# Note: mounting fs needs privileged mode when running the container, use "-privileged" flag
RUN apt-get update && yes | apt-get install curl s3fs

# Install AWS CLI, boto3 and watchover
RUN pip install \
  pandas==0.22.0 \
  awscli==1.16.187 \
  boto3==1.9.177 \
  watchtower==0.6.0 \
  urllib3==1.21.1

# SETUP NEW ENTRYPOINT
RUN mkdir -p /home/ubuntu/
WORKDIR /home/ubuntu
COPY cp-worker.py .
COPY initialize-worker.sh .
RUN chmod 755 initialize-worker.sh

ENTRYPOINT ["/bin/bash"]
# ENTRYPOINT ["/home/ubuntu/initialize-worker.sh"]
# CMD [""]