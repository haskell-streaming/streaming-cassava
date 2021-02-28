{ nixpkgs ? import <nixpkgs> {}, compiler ? "default", doBenchmark ? false }:

let

  inherit (nixpkgs) pkgs;

  f = { mkDerivation, base, bytestring, cassava, hspec, lib, mtl
      , QuickCheck, quickcheck-instances, streaming, streaming-bytestring
      , text, transformers, vector
      }:
      mkDerivation {
        pname = "streaming-cassava";
        version = "0.1.0.1";
        src = ./.;
        libraryHaskellDepends = [
          base bytestring cassava mtl streaming streaming-bytestring
          transformers
        ];
        testHaskellDepends = [
          base hspec mtl QuickCheck quickcheck-instances streaming text
          vector
        ];
        description = "Cassava support for the streaming ecosystem";
        license = lib.licenses.mit;
      };

  haskellPackages = if compiler == "default"
                       then pkgs.haskellPackages
                       else pkgs.haskell.packages.${compiler};

  variant = if doBenchmark then pkgs.haskell.lib.doBenchmark else pkgs.lib.id;

  drv = variant (haskellPackages.callPackage f {});

in

  if pkgs.lib.inNixShell then drv.env else drv
