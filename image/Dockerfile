ARG BASE_IMAGE=registry.jiagouyun.com/basis/kodo-basis:kodo-basis-2026-03-25
FROM ${BASE_IMAGE}

ARG TARGETARCH=amd64
MAINTAINER "tanbiao@jiagouyun.com"

COPY ./image/cmd/${TARGETARCH}/graphdb /graphdb/graphdb
COPY ./docs/openapi.yaml /graphdb/docs/openapi.yaml

WORKDIR /graphdb

RUN groupadd -r -g 1000 guance && \
    useradd -r -g guance -u 1000 -s /sbin/nologin guance && \
    chmod +x /graphdb/graphdb && \
    chown -R guance:guance /graphdb

USER guance
ENTRYPOINT ["/graphdb/graphdb"]
CMD ["serve"]
