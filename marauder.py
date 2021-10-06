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
parser.add_argument('-c', dest='command', metavar='\"sleep 5\"', required=True, type=str,
                help='Shell command to be run after each copy')
parser.add_argument('-X', dest='delete_source', action='store_true',
                help='When enabled, will delete source files')
parser.add_argument('-K', dest='keep_destination', action='store_true',
                help='When enabled, will keep destination files. This is ignored when CHUNK_SIZE is supplied.')
parser.add_argument('--chunk-size', dest='chunk_size', metavar='16G', required=False, type=str, default=None,
                help='When supplied, files will be copied up to the chunk size before running the command.')
args = parser.parse_args()

## Calculate file size
spinner = Halo(text='Calculating file size: ...', spinner='dots')
spinner.start()

DISK_USAGE = 0
FILE_COUNT = 0
CHUNK_SIZE = humanfriendly.parse_size(args.chunk_size) if args.chunk_size != None else None
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

QUEUED_FILES: list[tuple[int, str, str, str]] = []

for root, dirs, files in os.walk(os.path.abspath(args.source)):
    for name in files:
        # create an array of tuples: (file_size,  src_path, dest_path, relative_thing)
        QUEUED_FILES += [
            (os.stat(os.path.join(root, name)).st_size,
            os.path.join(root, name),
            os.path.join(os.path.abspath(args.destination),
            os.path.relpath(os.path.join(root, name), start=os.path.abspath(args.source))))
        ]
spinner.succeed()
# smallest files appear first
QUEUED_FILES.sort()

if CHUNK_SIZE != None and len(QUEUED_FILES) > 0 and QUEUED_FILES[0][0] > CHUNK_SIZE:
    print(f'Smallest file is greater than CHUNK_SIZE ({humanfriendly.format_size(QUEUED_FILES[0][0], binary=True)} > {humanfriendly.format_size(CHUNK_SIZE, binary=True)})')
    print(f'Please enter a larger CHUNK_SIZE')
    exit(1)

print(f'Copying {FILE_COUNT} files')

CURRENT_CHUNK_TRANSFERRED = 0
with tqdm(total=DISK_USAGE, unit='bytes', unit_scale=True, colour='yellow') as t:
    for i in range(0, len(QUEUED_FILES)):
        # unwrap tuple
        FILE_SIZE, SRC, DESTINATION = QUEUED_FILES[i]
        
        t.write(f'[{i+1} / {FILE_COUNT}] {Fore.YELLOW}{os.path.relpath(SRC, start=os.path.abspath(args.source))} @ {humanfriendly.format_size(FILE_SIZE, binary=True)}{Fore.RESET}')
        
        if CHUNK_SIZE != None:
            # if the current file won't fit, run the command and then delete the files so far
            if CURRENT_CHUNK_TRANSFERRED + FILE_SIZE > CHUNK_SIZE:
                t.write(f'\t{Fore.YELLOW}Current file won\'t fit within CHUNK_SIZE, waiting for command to exit: {args.command} {Fore.RESET}')
                subprocess.run(args.command.split(' '))
                t.write(f'\t{Fore.YELLOW}Exited! {Fore.GREEN}✔{Fore.RESET}')
                
                t.write(f'\t{Fore.YELLOW}Deleting previous desination files up to this point... {Fore.RESET}', end='')
                j = 0
                for previous in QUEUED_FILES[:i]:
                    P_FILE_SIZE, P_SRC, P_DESTINATION = previous
                    if os.path.exists(P_DESTINATION):
                        j += 1
                        os.remove(P_DESTINATION)
                t.write(f'\t{Fore.YELLOW}Deleting previous desination files up to this point... {Fore.RESET}{Fore.GREEN}✔ {j} files removed{Fore.RESET}')
                # reset the transferred count
                CURRENT_CHUNK_TRANSFERRED = 0
                t.write(f'\t{Fore.YELLOW}Current chunk reset{Fore.RESET}')
        
        with open(SRC, 'rb', buffering=64) as infile:
            with open(DESTINATION, 'wb', buffering=4096) as outfile:
                # copy file
                LAST = 0
                t.write(f'\t{Fore.YELLOW}Started copy... {Fore.RESET}')
                while infile.tell() < FILE_SIZE:
                    outfile.write(infile.read(4096))
                    t.update(infile.tell() - LAST)
                    LAST = infile.tell()
                t.write(f'\t{Fore.YELLOW}Finished copy {Fore.RESET}{Fore.GREEN}✔{Fore.RESET}')
        
        if CHUNK_SIZE != None:
            CURRENT_CHUNK_TRANSFERRED += FILE_SIZE
            t.write(f'\t{Fore.YELLOW}Current chunk: {humanfriendly.format_size(CURRENT_CHUNK_TRANSFERRED, binary=True)} transferred ({humanfriendly.format_size(CHUNK_SIZE, binary=True)} limit){Fore.RESET}')

        if CHUNK_SIZE == None:
            # run command (upload)
            t.write(f'\t{Fore.YELLOW}Waiting for command to exit: {args.command} {Fore.RESET}')
            subprocess.run(args.command.split(' '))
            t.write(f'\t{Fore.YELLOW}Exited! {Fore.GREEN}✔{Fore.RESET}')
        # delete source file
        if args.delete_source:
            t.write(f'\t{Fore.YELLOW}Deleting source... {Fore.RESET}', end='')
            os.remove(SRC)
            t.write(f'\t{Fore.YELLOW}Deleting source... {Fore.RESET}{Fore.GREEN}✔{Fore.RESET}')

        if CHUNK_SIZE == None:
            if args.keep_destination:
                t.write(f'\t{Fore.YELLOW}Keeping desination! {Fore.RESET}{Fore.GREEN}✔{Fore.RESET}')
            else:
                t.write(f'\t{Fore.YELLOW}Deleting desination... {Fore.RESET}', end='')
                os.remove(DESTINATION)
                t.write(f'\t{Fore.YELLOW}Deleting desination... {Fore.RESET}{Fore.GREEN}✔{Fore.RESET}')
        else:
            t.write(f'\t{Fore.YELLOW}Chunk size provided, keeping desination! {Fore.RESET}{Fore.GREEN}✔{Fore.RESET}')
        
            
        