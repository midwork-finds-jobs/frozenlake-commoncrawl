{
  pkgs,
  lib,
  config,
  inputs,
  ...
}:

{
  packages = [
    # AWS CLI v2 supports `aws login`
    pkgs.awscli2
    # Infrastructure in infra/
    pkgs.opentofu
    # Cross compiling the Rust lambda
    pkgs.cargo-lambda
    pkgs.zig
    # llvm-strip for libduckdb.so
    pkgs.llvm
  ];

  # https://devenv.sh/languages/
  languages = {
    # For the frozen ducklake importer
    python = {
      enable = true;
      # Use a faster package manager
      uv.enable = true;
      venv = {
        enable = true;
        requirements = "duckdb>=2.0.0";
      };
    };
    # For the query lambda in lambda/
    rust = {
      enable = true;
      channel = "stable";
      targets = [ "aarch64-unknown-linux-gnu" ];
    };
  };

  git-hooks.hooks = {
    # Nix files
    nixfmt.enable = true;

    # Stop leaking secrets
    ripsecrets.enable = true;

    # Python code
    ruff.enable = true;

    # Rust code
    rustfmt = {
      enable = true;
      settings.manifest-path = "./lambda/Cargo.toml";
    };
  };
}
