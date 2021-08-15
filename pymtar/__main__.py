
# Global libraries
import argparse
import datetime
import json
import os
import subprocess
import sys
import tempfile

# This library
import pymtar


def main():
	p = argparse.ArgumentParser(add_help=False)
	p.add_argument('-h', '--help', action='store_true', default=False, help='Show usage information')
	p.add_argument('-f', '--file', default='/dev/nst0', help='Device file path')
	p.add_argument('-j', '--json', default=False, action='store_true', help="Print responses, where appropriate, in JSON instead")
	p.add_argument('-d', '--db', nargs='?', required=True, help="Database file to use, will be created if not found")
	p.add_argument('action', nargs=argparse.REMAINDER, help='Action/command to execute')


	action_help = """
  Actions help:
    find tape.barcode   Find tapes by barcode
    find tape.sn        Find tapes by serial number
    list tapes          List all tapes
    list tars           List all tars
                            tape          Tape rowid, serial number, or barcode to limit search by
    list files          List all files
                            tape          Tape rowid, serial number, or barcode to limit search by
                            tar           Tar rowid to limit search by
                            tarnum        Tar num to limit search by
    new tape            Create a new tape record
                            manufacturer  Manufacturer of the cartridge
                            model         Model number of catridge
                            gen           Generation (eg, LTO8RW, LTO8WORM)
                            sn            Serial number printed on cartridge
                            barcode       Standard LTO barcode value (optional)
                            ptime         Purchase time (YYYY-MM-DD)
    new tar             Create a new tar file
                            tape          Tape rowid, serial number, or barcode
                            num           File number on the cartridge
                            stime         Start time of write (YYYY-MM-DD HH:MM:SS)
                            etime         End time of write (YYYY-MM-DD HH:MM:SS)
                            access_cnt    Access count (optional, default is zero)
                            options       Options passed to tar (eg, '-z')
                            uname         Value of `uname -a` (optional, uname invoked if not provided)
    new file            Create a new file within a tar file
                            tape          Tape rowid, serial number, or barcode
                            tar           Tar rowid or tar.num on this cartridge
                            fullpath      Absolute path of file on host system
                            relpath       Relative path supplied to tar as stored in the tar
                            fname         File name (no directory path included)
                            sz            File size in bytes
                            sha256        SHA256 hash of the file
    queue               Add a bunch of files to a tar file to queue up for writing
                            tape          Tape identifier
                            tar           Tar file to add files to
                            *             List of files to add
    write               Write a tar file to the tape drive
                            tape          Tape identifier
                            tar           Tar file to write
"""

	args = p.parse_args()
	if args.help:
		p.print_help()
		print(action_help)
		sys.exit(2)

	try:
		return pymtar.actions.action(args)
	except pymtar.PrintHelpException as e:
		p.print_help()
		print(action_help)

		print("Error: %s" % str(e))
		sys.exit(2)

if __name__ == '__main__':
	main()
