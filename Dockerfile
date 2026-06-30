FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
COPY vendor ./vendor
COPY cmd ./cmd
COPY internal ./internal
RUN go build -mod=vendor -o /out/graphdb ./cmd/graphdb

FROM alpine:3.20
RUN adduser -D -H graphdb
RUN mkdir -p /usr/local/share/graphdb
USER graphdb
COPY --from=build /out/graphdb /usr/local/bin/graphdb
COPY docs/openapi.yaml /usr/local/share/graphdb/openapi.yaml
EXPOSE 8080
ENTRYPOINT ["graphdb"]
CMD ["serve"]
