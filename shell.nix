let
  localLib = import ./nix/lib.nix;
in
{ system ? builtins.currentSystem
, config ? {}
, compiler ? "ghc863"
, doBenchmark ? false
, pkgs ? (import (localLib.fetchNixPkgs) { inherit system config; })
}:
with pkgs;
let

  haskellPackages = if compiler == "default"
                       then haskellPackages
                       else haskell.packages.${compiler};

  raft = import ./stack-shell.nix { ghc = haskellPackages.ghc; };
  # variant = if doBenchmark then haskell.lib.doBenchmark else lib.id;

in
  raft
