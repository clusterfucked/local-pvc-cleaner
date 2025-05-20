{ pkgs, lib, config, inputs, ... }:
let
  pkgs-unstable = import inputs.nixpkgs-unstable { system = pkgs.stdenv.system; };
in
{
  packages = [
    pkgs.gitFull
    pkgs-unstable.goreleaser
    pkgs-unstable.hut
  ];

  tasks."app:release" = {
    exec = "goreleaser release --clean";
  };

  languages.go.enable = true;
  languages.go.package = pkgs-unstable.go;
}
