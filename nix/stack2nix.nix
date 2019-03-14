{ mkDerivation, async, base, Cabal, cabal2nix, containers
, directory, distribution-nixpkgs, fetchgit, filepath, hackage-db
, hspec, language-nix, lens, optparse-applicative, path, pretty
, process, regex-pcre, SafeSemaphore, stack, stdenv, temporary
, text, time
}:
mkDerivation {
  pname = "stack2nix";
  version = "0.2.2";
  src = fetchgit {
    url = "https://github.com/input-output-hk/stack2nix.git";
    sha256 = "1yd56cpr64bx728sgi80yml10zp939xb78mx62j0ajxvjglnb3p7";
    rev = "19cf66276d044bae537898cc92e8b17770e6b601";
  };
  isLibrary = true;
  isExecutable = true;
  libraryHaskellDepends = [
    async base Cabal cabal2nix containers directory
    distribution-nixpkgs filepath hackage-db language-nix lens
    optparse-applicative path pretty process regex-pcre SafeSemaphore
    stack temporary text time
  ];
  executableHaskellDepends = [
    base Cabal optparse-applicative time
  ];
  testHaskellDepends = [ base hspec ];
  description = "Convert stack.yaml files into Nix build instructions.";
  license = stdenv.lib.licenses.mit;
}
