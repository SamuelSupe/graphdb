FROM golang:1.25-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/graphdb ./cmd/graphdb

FROM alpine:3.20
RUN adduser -D -H graphdb
RUN mkdir -p /usr/local/share/graphdb
USER graphdb
COPY --from=build /out/graphdb /usr/local/bin/graphdb
COPY docs/openapi.yaml /usr/local/share/graphdb/openapi.yaml
EXPOSE 8080
ENTRYPOINT ["graphdb"]
CMD ["serve"]
