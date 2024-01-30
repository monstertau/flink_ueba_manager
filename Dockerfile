### BUILD
# Base image `docker pull golang:1.13.14-alpine3.11`
FROM golang:1.19-alpine as build
# Folder in Container, /sample same level as /home
WORKDIR /building_stage

# Copy project code to Container
COPY . .

# Go build in Container
RUN go build -mod=vendor -o /building_stage/main ./main.go


FROM alpine

# Create workdir in target Container
WORKDIR /flink_ueba_manager

# Copy binary from `build` to target Container
COPY --from=build /building_stage/main /flink_ueba_manager/main
COPY --from=build /building_stage/config.yml /flink_ueba_manager/config.yml

# Run command
CMD /flink_ueba_manager/main