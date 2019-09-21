#!/usr/bin/env python3

import os
import argparse
import subprocess

from tqdm import tqdm
from halo import Halo
import humanfriendly
from colorama import init, Fore, Back, Style
init()

parser = argparse.ArgumentParser(description='Marauder')
parser.add_argument('source', metavar='Seeding/Movies', type=str,
                    help='Source folder')
parser.add_argument('-o', dest='destination', metavar='local-sorted/Movies', required=True, type=str,
                    help='Destination folder')
parser.add_argument('-c', dest='command', metavar='sleep 5', required=True, type=str,
                help='Shell command to be run after each copy')
parser.add_argument('-X', dest='delete_source', action='store_true',
                help='When enabled, will delete source files')
args = parser.parse_args()

## Calculate file size
spinner = Halo(text='Calculating file size: ...', spinner='dots')
spinner.start()

DISK_USAGE = 0
FILE_COUNT = 0
for root, dirs, files in os.walk(os.path.abspath(args.source)):
    for name in files:
        FILE_COUNT += 1
        DISK_USAGE += os.stat(os.path.join(root, name)).st_size
        #print(f'{os.path.join(root, name)} => {os.path.relpath(os.path.join(root, name), start=os.path.abspath(args.source))}')
        #print(f'\t{os.path.dirname(os.path.join(root, name))}')
        #with open(os.path.join(root, name), 'rb', buffering=30) as f:
            #f.peek(8)
spinner.text = f'Calculating file size: {DISK_USAGE} bytes over {FILE_COUNT} files.'
spinner.succeed()

## Re-create folder structure in destination
spinner = Halo(text='Recreating folder structure in destination', spinner='dots')
spinner.start()

for root, dirs, files in os.walk(os.path.abspath(args.source)):
    #print(os.path.normpath(os.path.join(os.path.abspath(args.destination), os.path.relpath(root, start=os.path.abspath(args.source)))))
    os.makedirs(os.path.normpath(os.path.join(os.path.abspath(args.destination), os.path.relpath(root, start=os.path.abspath(args.source)))), exist_ok=True)
spinner.succeed()

## Re-create folder structure in destination
spinner = Halo(text='Queuing files', spinner='dots')
spinner.start()

QUEUED_FILES = []

for root, dirs, files in os.walk(os.path.abspath(args.source)):
    for name in files:
        # create an array of tuples: (file_size,  src_path, dest_path)
        QUEUED_FILES += [(os.stat(os.path.join(root, name)).st_size, os.path.join(root, name), os.path.join(os.path.abspath(args.destination), os.path.relpath(os.path.join(root, name), start=os.path.abspath(args.source))))]
spinner.succeed()
QUEUED_FILES.sort()

print(f'Copying {FILE_COUNT} files')

i = 1
with tqdm(total=DISK_USAGE, unit="bytes", unit_scale=True) as t:
    for item in QUEUED_FILES:
        # unwrap tuple
        FILE_SIZE, SRC, DESTINATION = item
        t.write(f'[{i} / {FILE_COUNT}] {Fore.YELLOW}{os.path.relpath(SRC, start=os.path.abspath(args.source))} @ {humanfriendly.format_size(FILE_SIZE, binary=True)}{Fore.RESET}')
        with open(SRC, 'rb', buffering=64) as infile:
            with open(DESTINATION, 'wb', buffering=64) as outfile:
                # copy file
                LAST = 0
                t.write(f'\t{Fore.YELLOW}Started copy... {Fore.RESET}')
                while infile.tell() < FILE_SIZE:
                    outfile.write(infile.read())
                    t.update(infile.tell() - LAST)
                    LAST = infile.tell()
                t.write(f'\t{Fore.YELLOW}Finished copy {Fore.RESET}{Fore.GREEN}✔{Fore.RESET}')
                # run command (upload)
                t.write(f'\t{Fore.YELLOW}Waiting for command to exit: {args.command} {Fore.RESET}')
                subprocess.run(args.command.split(' '))
                t.write(f'\t{Fore.YELLOW}Exited! {Fore.GREEN}✔{Fore.RESET}')
        # delete source file
        if args.delete_source:
            t.write(f'\t{Fore.YELLOW}Deleting source and desination... {Fore.RESET}', end='')
            os.remove(SRC)
            os.remove(DESTINATION)
            t.write(f'\t{Fore.YELLOW}Deleting source and desination... {Fore.RESET}{Fore.GREEN}✔{Fore.RESET}')
        else:
            t.write(f'\t{Fore.YELLOW}Deleting desination... {Fore.RESET}', end='')
            os.remove(DESTINATION)
            t.write(f'\t{Fore.YELLOW}Deleting desination... {Fore.RESET}{Fore.GREEN}✔{Fore.RESET}')
        i += 1
            
        