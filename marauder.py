#!/usr/bin/env python3

import os
import argparse

from tqdm import tqdm
from halo import Halo

parser = argparse.ArgumentParser(description='Marauder')
parser.add_argument('source', metavar='Seeding/Movies', type=str,
                    help='Source folder')
parser.add_argument('-o', dest='destination', metavar='local-sorted/Movies', required=True, type=str,
                    help='Destination folder')
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

TRAVERSED = 0
with tqdm(total=DISK_USAGE, unit="bytes", unit_scale=True) as t:
    for root, dirs, files in os.walk(os.path.abspath(args.source)):
        for name in files:
            t.write(f'{os.path.join(root, name)} => {os.path.join(os.path.abspath(args.destination), os.path.relpath(os.path.join(root, name), start=os.path.abspath(args.source)))}')
            with open(os.path.join(root, name), 'rb', buffering=64) as infile:
                with open(os.path.join(os.path.abspath(args.destination), os.path.relpath(os.path.join(root, name), start=os.path.abspath(args.source))), 'wb', buffering=64) as outfile:
                    FILE_SIZE = os.stat(os.path.join(root, name)).st_size
                    LAST = 0
                    # copy file
                    while infile.tell() < FILE_SIZE:
                        outfile.write(infile.read())
                        t.update(infile.tell() - LAST)
                        LAST = infile.tell()
                    # run command (upload)
                    # delete source file