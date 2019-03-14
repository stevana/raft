let
  # Allow overriding pinned nixpkgs for debugging purposes via adjoint_pkgs
  fetchNixPkgs = let try = builtins.tryEval <adjoint_pkgs>;
    in if try.success
    then builtins.trace "using host <adjoint_pkgs>" try.value
    else import ./fetchNixpkgs.nix (builtins.fromJSON (builtins.readFile ./nixpkgs-src.json));

  maybeEnv = env: default:
    let
      result = builtins.getEnv env;
    in if result != ""
       then result
       else default;

  # Removes files within a Haskell source tree which won't change the
  # result of building the package.
  # This is so that cached build products can be used whenever possible.
  # It also applies the lib.cleanSource filter from nixpkgs which
  # removes VCS directories, emacs backup files, etc.
  cleanSourceTree = src:
    if (builtins.typeOf src) == "path"
      then lib.cleanSourceWith {
        filter = with pkgs.stdenv;
          name: type: let baseName = baseNameOf (toString name); in ! (
            # Filter out cabal build products.
            baseName == "dist" || baseName == "dist-newstyle" ||
            baseName == "cabal.project.local" ||
            # Filter out stack build products.
            lib.hasPrefix ".stack-work" baseName ||
            # Filter out files which are commonly edited but don't
            # affect the cabal build.
            lib.hasSuffix ".nix" baseName
          );
        src = lib.cleanSource src;
      } else src;

  pkgs = import fetchNixPkgs {};
  lib = pkgs.lib;
in lib // (rec {
  inherit fetchNixPkgs cleanSourceTree;
  isBenchmark = args: !((args.isExecutable or false) || (args.isLibrary or true));

  # Insert this into builder scripts where programs require a UTF-8
  # locale to work.
  utf8LocaleSetting = ''
    export LC_ALL=en_GB.UTF-8
    export LANG=en_GB.UTF-8
  '';
})
