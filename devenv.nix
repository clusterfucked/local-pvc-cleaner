{ pkgs, lib, config, inputs, ... }:

{
  packages = [ 
    pkgs.gitFull
    pkgs.goreleaser
    pkgs.hut
  ];
  
  languages.go.enable = true;
}
