{ pkgs, lib, config, inputs, ... }:
{
  packages = [
    pkgs.gitFull
    pkgs.nixd

    pkgs.goreleaser
    pkgs.hut

    pkgs.k9s
    pkgs.kubectl
    pkgs.kubelogin-oidc
  ];

  tasks."app:release" = {
    exec = "goreleaser release --clean";
  };

  languages.nix.enable = true;

  languages.go.enable = true;
}
