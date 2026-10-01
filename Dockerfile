FROM golang:1.26.7-bookworm AS build
ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_DATE=unknown
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN buildinfo_package=github.com/SamuelSupe/graphdb/v2/internal/buildinfo && \
    CGO_ENABLED=0 go build -mod=readonly -trimpath -buildvcs=false \
    -ldflags="-s -w -X ${buildinfo_package}.Version=${VERSION} -X ${buildinfo_package}.Commit=${COMMIT} -X ${buildinfo_package}.Date=${BUILD_DATE}" \
    -o /out/graphdb ./cmd/graphdb

FROM alpine:3.20
RUN adduser -D -H graphdb
RUN mkdir -p /usr/local/share/graphdb /var/lib/graphdb && chown graphdb:graphdb /var/lib/graphdb
ENV GRAPHDB_DATA_DIR=/var/lib/graphdb
USER graphdb
COPY --from=build /out/graphdb /usr/local/bin/graphdb
COPY docs/openapi.yaml /usr/local/share/graphdb/openapi.yaml
EXPOSE 8080 8081
ENTRYPOINT ["graphdb"]
CMD ["serve"]
