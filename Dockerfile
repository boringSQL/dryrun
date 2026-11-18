# syntax=docker/dockerfile:1
# just wraps the static binary from CI, ctx = dist/ (dist/<os>/<arch>/dryrun)
FROM gcr.io/distroless/static-debian13:nonroot@sha256:aafe993665bf6200db38a02c11a9351e782a84b6a45e7f33b6695638c88e3931

ARG TARGETOS TARGETARCH

# artifacts lose +x
COPY --chmod=0755 ${TARGETOS}/${TARGETARCH}/dryrun /usr/local/bin/dryrun

# /home/nonroot is 0700, breaks w/ --user
ENV DOCKER_CONFIG=/docker
WORKDIR /work
ENTRYPOINT ["/usr/local/bin/dryrun"]
