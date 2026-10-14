ARG BASE_IMAGE

FROM ${BASE_IMAGE}

ARG TARGETARCH

COPY --chmod=755 ${TARGETARCH}/temporal-server /usr/local/bin/temporal-server
