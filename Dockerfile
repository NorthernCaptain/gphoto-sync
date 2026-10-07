FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w -X main.version=${VERSION}" -o /out/gphoto-sync ./cmd/gphoto-sync

FROM rclone/rclone:1.72 AS rclone

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=rclone /usr/local/bin/rclone /usr/local/bin/rclone
COPY --from=build /out/gphoto-sync /usr/local/bin/gphoto-sync
ENV GPS_CONFIG=/config/config.yaml
ENTRYPOINT ["/usr/local/bin/gphoto-sync"]
CMD ["sync"]
