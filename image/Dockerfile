FROM registry.jiagouyun.com/basis/kodo-basis:kodo-basis-2026-03-25

ARG TARGETARCH
MAINTAINER "tanbiao@jiagouyun.com"

COPY ./image/cmd/$TARGETARCH/ /graphdb/
COPY ./image/config /graphdb/config

WORKDIR /graphdb

# download iploc and geolite2 data
# NOTE: the following commads have been moved to Dockerfile.basis to accelerate image building
#RUN mkdir -p plugins/ipdb/iploc && \
#    mkdir -p plugins/ipdb/geolite2 && \
#    bash -c "tar -C plugins/ipdb/iploc -zxf <(curl -sL 'https://zhuyun-static-files-production.oss-cn-hangzhou.aliyuncs.com/datakit/ipdb/iploc.tar.gz')" && \
#    bash -c "rm -rf plugins/ipdb/geolite2/GeoLite2-City/ && \
#        tar -C plugins/ipdb/geolite2 --transform 's/_[0-9]*//' -zxf <(\
#        curl -sL https://zhuyun-static-files-production.oss-cn-hangzhou.aliyuncs.com/datakit/ipdb/version.json | \
#        jq -r '.\"geolite2-city\".file' | \
#        xargs -I {} curl -sL 'https://zhuyun-static-files-production.oss-cn-hangzhou.aliyuncs.com/datakit/ipdb/{}')"

# 添加普通用户并将/kodo 文件夹授予权限
RUN groupadd -r -g 1000 guance && useradd -r -g guance -u 1000 -s /sbin/nologin guance && chown -R guance:guance /graphdb
CMD /graphdb/graphdb serve

