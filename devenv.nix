{ pkgs, lib, config, inputs, ... }:
{
  packages = [
    pkgs.gitFull
    pkgs.goreleaser
    pkgs.hut
    pkgs.k9s
    pkgs.kubectl
    pkgs.kubelogin-oidc
  ];

  tasks."app:release" = {
    exec = "goreleaser release --clean";
  };

  languages.go.enable = true;
}
