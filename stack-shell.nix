{ghc}:
let
  localLib = import ./nix/lib.nix;
  pkgs = (import (localLib.fetchNixPkgs) { });
in
with pkgs;
let

  libfiu = stdenv.mkDerivation {
    name = "libfiu-0.98";
    src = fetchurl {
      url = https://blitiri.com.ar/p/libfiu/files/0.98/libfiu-0.98.tar.gz;
      sha256 = "1chrfmgixb1afl7l6zsbamix61iafs66n42cx2pbi7sw11l59sk0";
    };
    makeFlags = [ "PREFIX=$(out)" ];
    buildInputs = [ python ];
  };
in
  haskell.lib.buildStackProject {
    inherit ghc;
    name = "raft";
    buildInputs = [ postgresql zlib cabal-install stack libfiu ];
  }
