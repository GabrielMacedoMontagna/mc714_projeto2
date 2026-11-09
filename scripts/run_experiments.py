#!/usr/bin/env python3
"""Script reproduzível para executar a bateria experimental completa do Trabalho 2."""

from __future__ import annotations

import sys

from mc714_p2.cli import main

if __name__ == "__main__":
    sys.exit(main(["experiments", *sys.argv[1:]]))
