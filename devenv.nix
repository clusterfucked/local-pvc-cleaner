{ pkgs, lib, config, inputs, ... }:
{
  packages = [
    pkgs.gitFull
    pkgs.goreleaser
    pkgs.hut
  ];

  tasks."app:release" = {
    exec = "goreleaser release --clean";
  };

  languages.go.enable = true;
}
