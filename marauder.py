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

TRAVERSED = 0
with tqdm(total=DISK_USAGE, unit="bytes") as t:
    for root, dirs, files in os.walk(os.path.abspath(args.source)):
        for name in files:
            print(f'{os.path.join(root, name)} => {os.path.join(os.path.abspath(args.destination), os.path.relpath(os.path.join(root, name), start=os.path.abspath(args.source)))}')
            with open(os.path.join(root, name), 'rb', buffering=64) as infile:
                with open(os.path.join(os.path.abspath(args.destination), os.path.relpath(os.path.join(root, name), start=os.path.abspath(args.source))), 'wb', buffering=64) as outfile:
                    while len(infile.peek(64)) > 0:
                        n = len(infile.peek(64))
                        TRAVERSED += infile.tell()
                        outfile.write(infile.read(64))
                        t.update(n)