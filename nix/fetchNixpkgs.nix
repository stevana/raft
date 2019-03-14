{ rev                             # The Git revision of nixpkgs to fetch
, sha256                          # The SHA256 of the downloaded data
, system ? builtins.currentSystem # This is overridable if necessary
}:

builtins.fetchTarball {
  url = "https://github.com/NixOS/nixpkgs/archive/${rev}.tar.gz";
  inherit sha256;
}
