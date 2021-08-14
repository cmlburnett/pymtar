
# Global libraries
import argparse
import datetime
import json
import os
import sys

# This library
import pymtar

class PrintHelpException(Exception): pass

def dateYYYYMMDD(v):
	if v == 'now':
		return datetime.datetime.utcnow().date()
	else:
		return datetime.datetime.strptime(v, "%Y-%m-%d").date()

def dateYYYYMMDDHHMMSS(v):
	if v == 'now':
		return datetime.datetime.utcnow()
	else:
		return datetime.datetime.strptime(v, "%Y-%m-%d %H:%M:%S")

class actions:
	"""
	Actions performed by the command line.
	This utilizes the pymtar.db class to interact with the database.
	"""

	@classmethod
	def _db_open(kls, args):
		db = pymtar.db(os.path.join(os.getcwd(), args.db))
		db.open()

		return db

	# -------------------------------------------------------------------------
	# -------------------------------------------------------------------------
	@classmethod
	def action(kls, args):
		acts = {}
		acts['find'] = kls.action_find
		acts['list'] = kls.action_list
		acts['new'] = kls.action_new

		if args.action[0] in acts:
			acts[ args.action[0] ](args)
		else:
			raise PrintHelpException("Action '%s' not recognized" % args.action[0])

	# -------------------------------------------------------------------------
	# -------------------------------------------------------------------------
	@classmethod
	def action_find(kls, args):
		if args.action[1] == 'tape.barcode':
			kls.action_find_tape_barcode(args, args.action[2])

		elif args.action[1] == 'tape.sn':
			kls.action_find_tape_sn(args, args.action[2])

		else:
			raise PrintHelpException("Unrecognized find command: %s" % args.action[1])

	@classmethod
	def action_find_tape_barcode(kls, args, bcode):
		db = kls._db_open(args)
		print(db.find_tape_by_barcode(bcode))

	@classmethod
	def action_find_tape_sn(kls, args, sn):
		db = kls._db_open(args)
		print(db.find_tape_by_sn(sn))


	# -------------------------------------------------------------------------
	# -------------------------------------------------------------------------
	@classmethod
	def action_list(kls, args):
		if args.action[1] == 'tapes':
			kls.action_list_tapes(args)

		elif args.action[1] == 'tars':
			kls.action_list_tars(args, args.action[2:])

		elif args.action[1] == 'files':
			kls.action_list_tarfiles(args, args.action[2:])

		else:
			raise PrintHelpException("Unrecognized list command: %s" % args.action[1])

	@classmethod
	def action_list_tapes(kls, args):
		if len(args.action) != 2:
			raise PrintHelpException("No parameters are accepted for list tapes")

		db = kls._db_open(args)
		rows = db.find_tapes()
		for row in rows:
			print(row)

	@classmethod
	def action_list_tars(kls, args, vals):
		# Split ['foo=bar', 'baz=bat'] into [['foo','bar'], ['baz','bat']]
		vals = dict([_.split('=',1) for _ in vals])

		db = kls._db_open(args)

		# No filtering
		if not len(vals):
			rows = db.find_tars()
		else:
			if 'tape' in vals:
				rows = db.find_tars_by_tape_multi(vals['tape'])

				if rows is None:
					raise PrintHelpException("Tape with rowid, serial number, or barcode '%s' not found" % vals['tape'])

			else:
				raise PrintHelpException("Unsupported filter for tar listing: %s" % str(vals))

		for row in rows:
			print(row)

	@classmethod
	def action_list_tarfiles(kls, args, vals):
		# Split ['foo=bar', 'baz=bat'] into [['foo','bar'], ['baz','bat']]
		vals = dict([_.split('=',1) for _ in vals])

		db = kls._db_open(args)

		raise NotImplementedError

	# -------------------------------------------------------------------------
	# -------------------------------------------------------------------------
	@classmethod
	def action_new(kls, args):
		if args.action[1] == 'tape':
			kls.action_new_tape(args, args.action[2:])

		elif args.action[1] == 'tar':
			kls.action_new_tar(args, args.action[2:])

		elif args.action[1] == 'file':
			kls.action_new_tarfile(args, args.action[2:])

		else:
			raise PrintHelpException("Unrecognized new command: %s" % args.action[1])

	@classmethod
	def action_new_tape(kls, args, vals):
		# Split ['foo=bar', 'baz=bat'] into [['foo','bar'], ['baz','bat']]
		vals = dict([_.split('=',1) for _ in vals])

		# Parse paramaters
		p = DataArgsParser('new tape')
		p.add('manufacturer', str, required=True)
		p.add('model', str, required=True)
		p.add('gen', str, required=True)
		p.add('sn', str, required=True)
		p.add('barcode', str, required=False)
		p.add('ptime', dateYYYYMMDD, required=True)

		vals = p.check(vals)

		# Check that there's not tape already
		db = kls._db_open(args)
		try:
			return db.new_tape(**vals)
		except ItemExists as e:
			raise PrintHelpException(str(e))
		except ItemNotFound as e:
			raise PrintHelpException(str(e))


	@classmethod
	def action_new_tar(kls, args, vals):
		# Split ['foo=bar', 'baz=bat'] into [['foo','bar'], ['baz','bat']]
		vals = dict([_.split('=',1) for _ in vals])

		# Parse paramaters
		p = DataArgsParser('new tar')
		p.add('tape', str, required=True)
		p.add('num', int, required=True)
		p.add('stime', dateYYYYMMDDHHMMSS, required=True)
		p.add('etime', dateYYYYMMDDHHMMSS, required=True)
		p.add('access_cnt', int, required=False, default=0)
		p.add('blk_offset', int, required=True)
		p.add('options', str, required=False)
		p.add('uname', str, required=False, default=None)

		vals = p.check(vals)

		# TODO: invoke `uname -a` if vals['uname'] is None

		db = kls._db_open(args)
		try:
			return db.new_tar(**vals)
		except ItemExists as e:
			raise PrintHelpException(str(e))
		except ItemNotFound as e:
			raise PrintHelpException(str(e))


	@classmethod
	def action_new_tarfile(kls, args, vals):
		# Split ['foo=bar', 'baz=bat'] into [['foo','bar'], ['baz','bat']]
		vals = dict([_.split('=',1) for _ in vals])

		# Parse paramaters
		p = DataArgsParser('new tar file')
		p.add('tape', str, required=True)
		p.add('tar', int, required=True)
		p.add('fullpath', str, required=True)
		p.add('relpath', str, required=True)
		p.add('fname', str, required=True)
		p.add('sz', int, required=True)
		p.add('sha256', str, required=True)

		vals = p.check(vals)

		db = kls._db_open(args)

		try:
			return db.new_tarfile(**vals)
		except ItemExists as e:
			raise PrintHelpException(str(e))
		except ItemNotFound as e:
			raise PrintHelpException(str(e))



class DataArgsParser:
	"""
	Accepts a key=value items from the command line, validates the type, and makes a dictionary.
		vals = ["model=monkey", "serial=10"]
		vals = dict([_.split('=',1) for _ in vals])
		p = DataArgsParser(name) # Name is something useful when throwing exceptions
		p.add('model', str, required=True, default="ACME")
		p.add('serial', int, required=True)
		vals = p.check(vals)
	"""
	def __init__(self, name):
		self.name = name
		self.parms = []
		self.keys = []

	def add(self, key, typ, *, required=False, default=None):
		self.parms.append( {'key': key, 'type': typ, 'required': required, 'default': default} )
		self.keys.append(key)

	def check(self, vals, set_absent_as_none=False):
		# Check for required parameters
		for z in self.parms:
			if z['required']:
				if z['key'] in vals: continue

				if 'default' in z and z['default'] is not None:
					vals[z['key']] = z['default']

				raise PrintHelpException("Data parameter '%s' required for %s, but not provided" % (z['key'], self.name))

		# Check that there are no extras
		for k,v in vals.items():
			if k not in self.keys:
				raise PrintHelpException("Data parameter '%s' not recognized as valid for %s" % (k, self.name))

		# Validate types
		for z in self.parms:
			k = z['key']

			# Must not be required if it reaches this point, so skip it
			if k not in vals:
				# If want the absent ones set as None, do that now
				if set_absent_as_none:
					vals[k] = None
				continue

			try:
				vals[k] = z['type']( vals[k] )
			except:
				raise PrintHelpException("Data parameter '%s' supposed to be type %s but failed to convert for %s" % (k, z['type'], self.name))

		return vals


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
                            blk_offset    Block offset of the file on the cartridge
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
"""

	args = p.parse_args()
	if args.help:
		p.print_help()
		print(action_help)
		sys.exit(2)

	try:
		return actions.action(args)
	except PrintHelpException as e:
		p.print_help()
		print(action_help)

		print("Error: %s" % str(e))
		sys.exit(2)

if __name__ == '__main__':
	main()
