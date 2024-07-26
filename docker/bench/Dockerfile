FROM ubuntu:22.04
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y \
    ca-certificates \
    curl
ADD ./target/release/greptime /greptime/bin/
ENV PATH /greptime/bin/:$PATH
ENTRYPOINT ["greptime"]

