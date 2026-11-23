{ pkgs, lib, config, inputs, ... }:
{
  packages = [
    pkgs.nixd
    pkgs.ko
    pkgs.gcc
  ];

  tasks."app:build-image" = {
    exec = ''
      set -euo pipefail
      KO_DOCKER_REPO=local/local-pvc-cleaner ko build \
        --push=false \
        --bare \
        --platform=linux/amd64,linux/arm64 \
        .
    '';
  };

  tasks."app:release" = {
    exec = ''
      set -euo pipefail
      TAG="''${RELEASE_TAG:-$(git describe --tags --exact-match 2>/dev/null || true)}"
      if [ -z "$TAG" ]; then
        echo "Error: RELEASE_TAG is not set and HEAD is not on an exact git tag." >&2
        exit 1
      fi
      for repo in \
        quay.io/drangon/local-pvc-cleaner \
        ghcr.io/clusterfucked/local-pvc-cleaner; do
        KO_DOCKER_REPO="$repo" ko build \
          --bare \
          --platform=linux/amd64,linux/arm64 \
          --tags="$TAG,latest" \
          .
      done
    '';
  };

  enterTest = ''
    go vet ./...
    go test -race -count=1 ./...
  '';

  languages.nix.enable = true;

  languages.go.enable = true;
  languages.go.package = pkgs.go_1_27;
}
