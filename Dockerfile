# Builds the Helix image ghcr.io/helix-nextgen-cellular-core/smf:v1.0.0
# the same way it was built and verified:
#   docker build -t smf:local .
FROM golang:1.16.0-stretch AS builder
WORKDIR /go/src/smf
COPY . .
RUN go mod vendor \
 && make all && (cd upfadapter && CGO_ENABLED=0 go build) \
 && mkdir -p /out/free5gc/smf /out/free5gc/bin && cp bin/* /out/free5gc/smf/ && cp upfadapter/upf-adapter /out/free5gc/bin/

FROM alpine:3.16
LABEL org.opencontainers.image.source="https://github.com/Helix-NextGen-Cellular-Core/smf" \
      org.opencontainers.image.licenses="Apache-2.0"
RUN apk update && apk add -U vim strace net-tools curl netcat-openbsd bind-tools bash tcpdump
COPY --from=builder /out/ /
WORKDIR /free5gc/smf
