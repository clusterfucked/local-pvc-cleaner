{ pkgs, lib, config, inputs, ... }:
{
  packages = [
    pkgs.nixd

    pkgs.goreleaser
  ];

  tasks."app:release" = {
    exec = "goreleaser release --clean";
  };

  languages.nix.enable = true;

  languages.go.enable = true;
}
