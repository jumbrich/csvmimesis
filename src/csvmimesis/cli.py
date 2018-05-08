#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import division, print_function, absolute_import

import argparse
import sys
import logging

from csvmimesis import __version__

from csvmimesis.mimesis_data_providers import print_mimesis,print_unique

__author__ = "jumbrich"
__copyright__ = "jumbrich"
__license__ = "mit"

_logger = logging.getLogger(__name__)

import sys
import click


@click.group()
def main():
    pass


@main.command()
@click.option("-p","--provider", default=None)
def list_all(provider):
    click.echo("Available providers and methods")
    print_mimesis(provider=provider)

@main.command()
@click.option("-p","--provider", default=None)
@click.option("-m","--method", default=None)
@click.option("-l","--local", default=None)
@click.option("--max", default=1000, type=int)
def unique(provider, method, local, max):
    click.echo("Number of unique values for each providers and methods (max_unique={} )".format(max))
    print_unique(provider=provider, method=method, local=local, max=max)

if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover
