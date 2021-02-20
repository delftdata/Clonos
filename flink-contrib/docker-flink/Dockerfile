################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

#FROM java:8-jre-alpine
FROM openjdk:8-jre


# Install dependencies
RUN set -ex; \
  apt-get update; \
  apt-get -y install libsnappy1v5 gettext-base; \
  rm -rf /var/lib/apt/lists/*

# Grab gosu for easy step-down from root
ENV GOSU_VERSION 1.11
RUN set -ex; \
  wget -nv -O /usr/local/bin/gosu "https://github.com/tianon/gosu/releases/download/$GOSU_VERSION/gosu-$(dpkg --print-architecture)"; \
  wget -nv -O /usr/local/bin/gosu.asc "https://github.com/tianon/gosu/releases/download/$GOSU_VERSION/gosu-$(dpkg --print-architecture).asc"; \
  export GNUPGHOME="$(mktemp -d)"; \
  for server in ha.pool.sks-keyservers.net $(shuf -e \
                          hkp://p80.pool.sks-keyservers.net:80 \
                          keyserver.ubuntu.com \
                          hkp://keyserver.ubuntu.com:80 \
                          pgp.mit.edu) ; do \
      gpg --batch --keyserver "$server" --recv-keys B42F6819007F00F88E364FD4036A9C25BF357DD4 && break || : ; \
  done && \
  gpg --batch --verify /usr/local/bin/gosu.asc /usr/local/bin/gosu; \
  gpgconf --kill all; \
  rm -rf "$GNUPGHOME" /usr/local/bin/gosu.asc; \
  chmod +x /usr/local/bin/gosu; \
  gosu nobody true

# Flink environment variables
ENV FLINK_INSTALL_PATH=/opt
ENV FLINK_HOME $FLINK_INSTALL_PATH/flink
ENV PATH $PATH:$FLINK_HOME/bin
RUN groupadd --system --gid=9999 flink && \
    useradd --system --home-dir $FLINK_HOME --uid=9999 --gid=flink flink


#============================

# flink-dist can point to a directory or a tarball on the local system
ARG flink_dist=NOT_SET
ADD $flink_dist $FLINK_INSTALL_PATH



# Install build dependencies and flink
RUN set -x && \
  ln -s $FLINK_INSTALL_PATH/flink-* $FLINK_HOME && \
	mkdir -p $FLINK_HOME/rocks/chk && \
	mkdir -p $FLINK_HOME/rocks/tmp 

COPY beam-jars/* /opt/flink/lib/

RUN set -x && \
  chown -hR flink:flink $FLINK_HOME && \
  chown -hR flink:flink /opt/flink-* 

WORKDIR $FLINK_HOME
COPY docker-entrypoint.sh /



USER flink
EXPOSE 8081 6123
ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["--help"]
