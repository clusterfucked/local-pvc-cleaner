{ pkgs, lib, config, inputs, ... }:

{
  packages = [ 
    pkgs.gitFull
    pkgs.goreleaser
  ];
  
  languages.go.enable = true;
}
