{
  pkgs,
  lib,
  config,
  inputs,
  ...
}:

{
  # https://devenv.sh/languages/
  languages = {
    # For developing the mkdocs-based documentation
    python = {
      enable = true;
      # Use a faster package manager
      uv.enable = true;
      venv = {
        enable = true;
        requirements = ./requirements.txt;
      };
    };
  };

  git-hooks.hooks = {
    # Nix files
    nixfmt-rfc-style.enable = true;

    # Leaking secrets
    trufflehog.enable = true;
    ripsecrets.enable = true;

    # Python code
    ruff.enable = true;
  };
}
