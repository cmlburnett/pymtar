
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
	return datetime.datetime.strptime(v, "%Y-%m-%d").date()

def dateYYYYMMDDHHMMSS(v):
	return datetime.datetime.strptime(v, "%Y-%m-%d %H:%M:%S")

class actions:
	@classmethod
	def _db_open(kls, args):
		db = pymtar.db(os.path.join(os.getcwd(), args.db))
		db.open()

		return db

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
		print('find tape barcode', bcode)

		db = kls._db_open(args)
		res = db.tape.select('*', 'barcode=?', [bcode])
		rows = res.fetchall()
		print(rows)

	@classmethod
	def action_find_tape_sn(kls, args, sn):
		print('find tape sn', sn)

		db = kls._db_open(args)
		res = db.tape.select('*', 'sn=?', [sn])
		rows = res.fetchall()
		print(rows)


	# -------------------------------------------------------------------------
	# -------------------------------------------------------------------------
	@classmethod
	def action_list(kls, args):
		if args.action[1] == 'tapes':
			kls.action_list_tapes(args)

		else:
			raise PrintHelpException("Unrecognized list command: %s" % args.action[1])

	@classmethod
	def action_list_tapes(kls, args):
		db = kls._db_open(args)
		res = db.tape.select('*')
		for row in res:
			row = dict(row)
			print(row)

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
		print(vals)

		# Check that there's not tape already
		db = kls._db_open(args)
		res = db.tape.select('rowid', 'sn=?', [vals['sn']])
		rows = res.fetchall()
		if len(rows):
			raise PrintHelpException("Tape with serial number '%s' already exists: rowid=%d" % (vals['sn'], rows[0]['rowid']))

		if 'barcode' in vals and vals['barcode'] is not None:
			res = db.tape.select('rowid', 'barcode=?', [vals['barcode']])
			rows = res.fetchall()
			if len(rows):
				raise PrintHelpException("Tape with barcode '%s' already exists: rowid=%d" % (vals['barcode'], rows[0]['rowid']))

		db.begin()
		ret = db.tape.insert(**vals)
		db.commit()
		return ret

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
		p.add('access_cnt', int, required=True)
		p.add('blk_offset', int, required=True)
		p.add('options', str, required=False)
		p.add('uname', str, required=True)

		vals = p.check(vals)
		print(vals)

		# Check that there's not tar already
		# insert into database
		raise NotImplementedError

	@classmethod
	def action_new_tarfile(kls, args, vals):
		# Split ['foo=bar', 'baz=bat'] into [['foo','bar'], ['baz','bat']]
		vals = dict([_.split('=',1) for _ in vals])

		# Parse paramaters
		p = DataArgsParser('new tar file')
		p.add('tape', str, required=True)
		p.add('tar', int, required=True)
		p.add('fullpath', str, required=True)
		p.add('fname', str, required=True)
		p.add('sz', int, required=True)
		p.add('sha256', str, required=True)

		vals = p.check(vals)
		print(vals)

		# Check that there's not tar file already
		# insert into database
		raise NotImplementedError

class DataArgsParser:
	def __init__(self, name):
		self.name = name
		self.parms = []
		self.keys = []

	def add(self, key, typ, *, required=False):
		self.parms.append( {'key': key, 'type': typ, 'required': required} )
		self.keys.append(key)

	def check(self, vals, set_absent_as_none=False):
		# Check for required parameters
		for z in self.parms:
			if z['required']:
				if z['key'] not in vals:
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

	acts = {}
	acts['find'] = actions.action_find
	acts['list'] = actions.action_list
	acts['new'] = actions.action_new

	action_help = """
  Actions help:
    find tape.barcode   Find tapes by barcode
    find tape.sn        Find tapes by serial number
    list tapes          List all tapes
    new tape            Create a new tape record
                            manufacturer  Manufacturer of the cartridge
                            model         Model number of catridge
                            gen           Generation (eg, LTO8RW, LTO8WORM)
                            sn            Serial number printed on cartridge
                            barcode       Standard LTO barcode value (optional)
                            ptime         Purchase time
    new tar             Create a new tar file
    new file            Create a new file within a tar file
"""

	args = p.parse_args()
	if args.help:
		p.print_help()
		print(action_help)
		sys.exit(2)

	try:
		if args.action[0] in acts:
			acts[ args.action[0] ](args)
	except PrintHelpException as e:
		p.print_help()
		print(action_help)

		print("Error: %s" % str(e))
		sys.exit(2)
	return

	mt = pymtar.mt(sys.argv[1])

	r = mt.status()
	print(r)
	print(r)

if __name__ == '__main__':
	main()
