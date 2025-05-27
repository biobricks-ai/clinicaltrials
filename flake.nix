{
  description = "ClinicalTrials.gov BioBrick";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-25.05";
    flake-utils.url = "github:numtide/flake-utils";
    dev-shell = {
      url = "github:biobricks-ai/dev-shell";
      inputs.flake-utils.follows = "flake-utils";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { self, nixpkgs, flake-utils, dev-shell }:
    flake-utils.lib.eachDefaultSystem (system:
      with import nixpkgs { inherit system; }; {
        devShells.default = dev-shell.devShells.${system}.default.overrideAttrs
          (oldAttrs:
            let
            in {
              buildInputs = [
                (with pkgs; [ wget unzip parallel-full coreutils gettext ])
                # Use duckdb from dev-shell
                dev-shell.packages.${system}.duckdb
              ];
              env = oldAttrs.env // {
                LC_ALL = "C";
              };
          });
      });
}
