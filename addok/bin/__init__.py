#!/usr/bin/env python

import argparse
import os


def main():

    config_parser = argparse.ArgumentParser(add_help=False)
    config_parser.add_argument('--config', help='Local config')
    args, unknown = config_parser.parse_known_args()
    if args.config:
        os.environ['ADDOK_CONFIG_MODULE'] = args.config

    main_parser = argparse.ArgumentParser(description='Addok command line.')
    main_parser.add_argument('--config', help='Local config')
    subparsers = main_parser.add_subparsers(title='Available commands',
                                            metavar='')

    from addok import config
    config.load(config)
    config.pm.hook.addok_register_command(subparsers=subparsers)
    args = main_parser.parse_args()
    if getattr(args, 'func', None):
        args.func(args)
    else:
        main_parser.print_help()

    # elif args['ngrams']:
    #     create_edge_ngrams()

if __name__ == '__main__':
    main()
