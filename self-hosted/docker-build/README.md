# Building docker images

The contents of this directory are used to build the docker images for the
self-hosted backend and dashboard. If you're looking for ways to run self-hosted
Convex, see the [these instructions](../README.md). You may build the images
locally from here, but we recommend using the images we provide on GHCR.

Build the backend from scratch by running:

```sh
docker build -t convex-backend -f self-hosted/docker-build/Dockerfile.backend .
```

The backend build uses Cargo's `release` profile by default. Select another
built-in or custom workspace profile with `CARGO_BUILD_PROFILE`:

```sh
docker build \
  -t convex-backend \
  -f self-hosted/docker-build/Dockerfile.backend \
  --build-arg CARGO_BUILD_PROFILE=slim-release \
  .
```

For a release profiling build, request Cargo debuginfo and disable both Cargo's
strip setting and the Dockerfile's final strip pass:

```sh
docker build \
  -t convex-backend \
  -f self-hosted/docker-build/Dockerfile.backend \
  --build-arg CARGO_PROFILE_RELEASE_DEBUG=1 \
  --build-arg CARGO_PROFILE_RELEASE_STRIP=none \
  .
```

Build the dashboard from scratch by running:

```sh
docker build -t convex-dashboard -f self-hosted/docker-build/Dockerfile.dashboard .
```
