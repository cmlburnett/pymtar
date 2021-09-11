"""
pymtar -- python magnetic tape tar interface

Stores file data in a sqlite database for rapid localization of files on a tape archive.
"""

# Global libraries
import datetime
import os
import subprocess
import sys
import tempfile
import time

# Installed libraries
import sqlite3

# My installed libraries
from sqlitehelper import SH, DBTable, DBCol, DBColROWID

# Used for notifications
try:
	import pushover
except:
	pushover = None

# Path of configuration file for Pushover
PUSHOVER_CFG_FILE = "~/.pushoverrc"
PUSHOVER_CFG_FILE = os.path.expanduser(PUSHOVER_CFG_FILE)


from .util import PrintHelpException, ItemExists, ItemNotFound, DataArgsParser, getuname
from .util import dateYYYYMMDD, dateYYYYMMDDHHMMSS, rangeint, hashfile


def send_notification(args, flags, msg):
	"""
	Fault-tolerant (if not installed or throws an exception) push notification to update the user.
	"""
	# Not configured, cannot notify
	if pushover is None: return

	# User doesn't want notifications
	if args.notify == 'none': return

	# Flags specified by the code 
	if args.notify not in flags: return

	try:
		pushover.Client().send_message(msg, title="pymtar")
	except Exception as e:
		print("Failed to send message: %s" % e)
	finally:
		print("notify: %s" % msg)

def send_notification_queue_start(args, num_files, vals):
	send_notification(args, ('all','limited'), "starting queue of %d files to tape=%s and num=%d" % (num_files, vals['tape'], vals['tar']))

def send_notification_queue_step(args, x, num_files, id_tape, num):
	send_notification(args, ('all',), "queue %d files of %d done to tape=%s and num=%d" % (x,num_files,id_tape,num))

def send_notification_queue_done(args, id_tape, num):
	send_notification(args, ('all','limited'), "queue completed to tape=%s and num=%d" % (id_tape,num))

def send_notification_write_start(args, vals):
	if vals['tar'][0] == vals['tar'][1]:
		send_notification(args, ('all','limited'), "starting write to tape=%s and num=%d" % (vals['tape'], vals['tar'][0]))
	else:
		send_notification(args, ('all','limited'), "starting write to tape=%s and num=%d-%d" % (vals['tape'], vals['tar'][0], vals['tar'][1]))

def send_notification_write_done(args, vals):
	if vals['tar'][0] == vals['tar'][1]:
		send_notification(args, ('limited','all'), "write completed to tape=%s and num=%d" % (vals['tape'],vals['tar'][0]))
	else:
		send_notification(args, ('limited','all'), "write completed to tape=%s and num=%d-%d" % (vals['tape'],vals['tar'][0], vals['tar'][1]))

def send_notification_tar_done(args, id_tape, num):
	send_notification(args, ('all',), "write completed to tape=%s and num=%d" % (id_tape,num))



class db(SH):
	"""DB schema"""

	__schema__ = [
		DBTable('tape',
			DBColROWID(),
			DBCol('manufacturer', 'text'), # Tape manufacturer
			DBCol('model', 'text'), # Tape model number
			DBCol('gen', 'text'), # Generation (eg, "LTO8RW")
			DBCol('sn', 'text'), # Serial number on cartridge
			DBCol('barcode', 'text'), # Standard LTO barcode, null if not used
			DBCol('ptime', 'date') # Purchase date and time
		),
		# A tape "file" equivalent to a tar file
		DBTable('tar',
			DBColROWID(),
			DBCol('id_tape', 'integer'), # Tape this tar belongs to
			DBCol('num', 'integer'), # File number on the tape
			DBCol('stime', 'datetime'), # Star write time
			DBCol('etime', 'datetime'), # End write time
			DBCol('access_cnt', 'integer'), # Read counter
			DBCol('blk_offset', 'integer'), # Block offset on tape
			DBCol('options', 'text'), # Options supplied to tar (eg, z, j)
			DBCol('uname', 'text'), # uname -a value at time of write
		),
		# One row per file stored in a tar file
		DBTable('tarfile',
			DBColROWID(),
			DBCol('id_tape', 'integer'), # Tape this file belongs to
			DBCol('id_tar', 'integer'), # tar file this file belongs to
			DBCol('fullpath', 'text'), # Full, absolute path
			DBCol('relpath', 'text'), # Relative path as supplied to tar
			DBCol('fname', 'text'), # File name
			DBCol('sz', 'integer'), # Size of file in bytes
			DBCol('sha256', 'text') # sha256 hash
		)
	]

	def open(self, rowfactory=None):
		ex = os.path.exists(self.Filename)

		super().open()

		if not ex:
			self.MakeDatabaseSchema()

	def reopen(self):
		super().reopen()

	@staticmethod
	def _now():
		return datetime.datetime.utcnow()

	# -------------------------------------------------------------------------
	# -------------------------------------------------------------------------
	# Tapes

	def find_tapes(self):
		res = self.tape.select('*')
		return [dict(_) for _ in res]

	def find_tape_by_id(self, rowid):
		res = self.tape.select('*', 'rowid=?', [rowid])
		return [dict(_) for _ in res]

	def find_tape_by_sn(self, sn):
		res = self.tape.select('*', 'sn=?', [sn])
		return [dict(_) for _ in res]

	def find_tape_by_barcode(self, bcode):
		res = self.tape.select('*', 'barcode=?', [bcode])
		return [dict(_) for _ in res]

	def find_tape_by_multi(self, val):
		res = self.tape.select('*', 'rowid=? or sn=? or barcode=?', [val,val,val])
		return [dict(_) for _ in res]

	def new_tape(self, manufacturer, model, gen, sn, barcode, ptime):
		rows = self.find_tape_by_sn(sn)
		if len(rows):
			raise ItemExists("Tape with serial number '%s' already exists, cannot add it again" % sn)

		if barcode is not None:
			rows = self.find_tape_by_barcode(barcode)
			if len(rows):
				raise ItemExists("Tape with barcode '%s' already exists, cannot add it again" % barcode)

		# TODO: validate gen
		# TODO: validate ptime

		self.begin()
		ret = self.tape.insert(manufacturer=manufacturer, model=model, gen=gen, sn=sn, barcode=barcode, ptime=ptime)
		self.commit()
		return ret

	# -------------------------------------------------------------------------
	# -------------------------------------------------------------------------
	# Tars

	def find_tars(self):
		res = self.tar.select('*')
		return [dict(_) for _ in res]

	def find_tars_by_tape_multi(self, val):
		rows = self.find_tape_by_multi(val)
		if not len(rows):
			return None

		res = self.tar.select('*', 'id_tape=?', [rows[0]['rowid']])
		return [dict(_) for _ in res]

	def find_tars_by_tape_num(self, tape, num):
		rows = self.find_tape_by_multi(tape)
		if not len(rows):
			raise ItemNotFound("Unable to find tape with rowid, serial number, or barcode '%s', cannot create tar" % tape)

		id_tape = rows[0]['rowid']

		res = self.tar.select('*', 'id_tape=? and num=?', [id_tape, num])
		rows = [dict(_) for _ in res]
		if not len(rows):
			raise ItemNotFound("Unable to find tar with num %d for tape '%s' (rowid=%d)" % (num, tape, id_tape))

		return rows[0]

	def new_tar(self, tape, num, stime, etime, access_cnt, options, uname):
		rows = self.find_tape_by_multi(tape)
		if not len(rows):
			raise ItemNotFound("Unable to find tape with rowid, serial number, or barcode '%s', cannot create tar" % tape)

		id_tape = rows[0]['rowid']

		res = self.tar.select('rowid', 'id_tape=? and num=?', [id_tape, num])
		rows = res.fetchall()
		if len(rows):
			raise ItemExists("Tar file num %d with tape '%s' (rowid=%d) already exists, cannot add it again" % (num, tape, id_tape))

		self.begin()
		ret = self.tar.insert(id_tape=id_tape, num=num, stime=stime, etime=etime, access_cnt=access_cnt, options=options, uname=uname)
		self.commit()
		return ret

	# -------------------------------------------------------------------------
	# -------------------------------------------------------------------------
	# Tar files

	def find_tarfiles(self):
		res = self.tar.select('*')
		return [dict(_) for _ in res]

	def find_tarfiles_by_tape(self, val):
		rows = self.find_tape_by_multi(val)
		if not len(rows):
			raise ItemNotFound("Unable to find tape with rowid, serial number, or barcode '%s', cannot create tar" % tape)

		id_tape = rows[0]['rowid']

		res = self.tarfile.select('*', 'id_tape=?', [id_tape])
		return [dict(_) for _ in res]

	def find_tarfiles_by_tar(self, tape, tar):
		rows = self.find_tape_by_multi(tape)
		if not len(rows):
			raise ItemNotFound("Unable to find tape with rowid, serial number, or barcode '%s', cannot create tar" % tape)

		id_tape = rows[0]['rowid']

		res = self.tar.select('rowid', 'id_tape=? and num=?', [id_tape, int(tar)])
		rows = res.fetchall()
		if not len(rows):
			raise ItemNotFound("Unable to find tar with num %d for tape '%s' (rowid=%d), cannot add tar file" % (tar, tape, id_tape))

		id_tar = rows[0]['rowid']

		res = self.tarfile.select('*', 'id_tape=? and id_tar=?', [id_tape, id_tar])
		return [dict(_) for  _ in res]

	def new_tarfile(self, tape, tar, fullpath, relpath, fname, sz, sha256):
		rows = self.find_tape_by_multi(tape)
		if not len(rows):
			raise ItemNotFound("Unable to find tape with rowid, serial number, or barcode '%s', cannot create tar" % tape)

		id_tape = rows[0]['rowid']

		res = self.tar.select('rowid', 'id_tape=? and num=?', [id_tape, int(tar)])
		rows = res.fetchall()
		if not len(rows):
			raise ItemNotFound("Unable to find tar with num %d for tape '%s' (rowid=%d), cannot add tar file" % (tar, tape, id_tape))

		id_tar = rows[0]['rowid']

		self.begin()
		ret = self.tarfile.insert(id_tape=id_tape, id_tar=id_tar, fullpath=fullpath, relpath=relpath, fname=fname, sz=sz, sha256=sha256)
		self.commit()
		return ret


class mt:
	"""
	Wrapper class to the command line tool mt(1) that provides tape control function.
	"""

	def __init__(self, dev):
		"""
		mt(1) wrapper for command-line manipulation of a tape drive.
		@dev is the device file used to manipulate the drive.
		"""

		dev = os.path.abspath(dev)

		parts = os.path.split(dev)
		if parts[0] != '/dev':
			raise Exception("Tape drive not a device: %s" % dev)
		if not parts[1].startswith('n'):
			raise Exception("Recommend aginst using anything but a non-rewinding tape drive: %s" % dev)

		self._dev = dev

		# Execute a command
		self.status()

	@staticmethod
	def _run(*args, timeout=5):
		r = subprocess.run(args, timeout=timeout, check=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)

		return r.stdout.decode('ascii')

	def status(self):
		"""
		Get status information on the tape.
		Specifically return a tuple of (file numer, block position, partition).
		Partition isn't used but returned anyway.

		If no tape, then values are -1.
		"""

		ret = self._run('mt', '-f', self._dev, 'status')
		lines = ret.split('\n')
		parts = lines[1].strip('.').split(',')
		parts = [_.strip() for _ in parts]

		fnum = int(parts[0].split('=')[-1])
		blk = int(parts[1].split('=')[-1])
		part = int(parts[2].split('=')[-1])

		return (fnum, blk, part)

	def rewind(self):
		"""Rewind tape to the beginning"""
		self._run('mt', '-f', self._dev, 'rewind', timeout=None)

	def offline(self):
		"""aka eject"""
		self._run('mt', '-f', self._dev, 'offline', timeout=None)

	def bsf(self, cnt=1):
		"""Move back one file, or @cnt if provided"""
		if type(cnt) is not int:
			raise Exception("bsf: cnt parameter must be an integer, got '%s' type %s" % (cnt,type(cnt)))

		self._run('mt', '-f', self._dev, 'bsf', str(cnt), timeout=None)

	def fsf(self, cnt=1):
		"""Move forward one file, or @cnt if provided"""
		if type(cnt) is not int:
			raise Exception("fsf: cnt parameter must be an integer, got '%s' type %s" % (cnt,type(cnt)))

		self._run('mt', '-f', self._dev, 'fsf', str(cnt), timeout=None)

	def asf(self, cnt):
		"""Rewind the tape and advance to @cnt files"""
		if type(cnt) is not int:
			raise Exception("fsf: cnt parameter must be an integer, got '%s' type %s" % (cnt,type(cnt)))

		self._run('mt', '-f', self._dev, 'asf', str(cnt), timeout=None)



class actions:
	"""
	Actions performed by the command line.
	This utilizes the pymtar.db class to interact with the database.
	"""

	@classmethod
	def _db_open(kls, args):
		d = db(os.path.join(os.getcwd(), args.db))
		d.open()

		return d

	# -------------------------------------------------------------------------
	# -------------------------------------------------------------------------
	@classmethod
	def action(kls, args):
		acts = {}
		acts['find'] = kls.action_find
		acts['list'] = kls.action_list
		acts['new'] = kls.action_new
		acts['queue'] = kls.action_queue
		acts['write'] = kls.action_write

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
		d = kls._db_open(args)
		print(d.find_tape_by_barcode(bcode))

	@classmethod
	def action_find_tape_sn(kls, args, sn):
		d = kls._db_open(args)
		print(d.find_tape_by_sn(sn))

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

		d = kls._db_open(args)
		rows = d.find_tapes()
		for row in rows:
			print(row)

	@classmethod
	def action_list_tars(kls, args, vals):
		# Split ['foo=bar', 'baz=bat'] into [['foo','bar'], ['baz','bat']]
		vals = dict([_.split('=',1) for _ in vals])

		d = kls._db_open(args)

		# No filtering
		if not len(vals):
			rows = d.find_tars()
		else:
			if 'tape' in vals:
				rows = d.find_tars_by_tape_multi(vals['tape'])

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

		d = kls._db_open(args)

		# No filtering
		if not len(vals):
			rows = d.find_tarfiles()
		else:
			if 'tape' in vals:
				rows = d.find_tarfiles_by_tape(vals['tape'])

			elif 'tar' in vals:
				raise NotImplementedError

			elif 'tarnum' in vals:
				raise NotImplementedError

			else:
				raise PrintHelpException("Unsupported filter for tar listing: %s" % str(vals))

		for row in rows:
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

		vals = p.check(vals, set_absent_as_none=True)

		# Check that there's not tape already
		d = kls._db_open(args)
		try:
			return d.new_tape(**vals)
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
		p.add('options', str, required=False)
		p.add('uname', str, required=False, default=None)

		vals = p.check(vals, set_absent_as_none=True)

		# Get system info if not provided
		if vals['uname'] is None:
			vals['uname'] = getuname()

		d = kls._db_open(args)
		try:
			return d.new_tar(**vals)
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

		d = kls._db_open(args)

		try:
			return d.new_tarfile(**vals)
		except ItemExists as e:
			raise PrintHelpException(str(e))
		except ItemNotFound as e:
			raise PrintHelpException(str(e))


	# -------------------------------------------------------------------------
	# -------------------------------------------------------------------------
	@classmethod
	def action_queue(kls, args):
		vals = args.action[1:4]
		# Split ['foo=bar', 'baz=bat'] into [['foo','bar'], ['baz','bat']]
		vals = dict([_.split('=',1) for _ in vals])

		# Parse paramaters
		p = DataArgsParser('new tar file')
		p.add('tape', str, required=True)
		p.add('tar', int, required=True)
		p.add('basedir', str, required=True)
		vals = p.check(vals, set_absent_as_none=True)

		d = kls._db_open(args)

		files = args.action[4:]

		if len(files) == 1 and files[0] == '-':
			files = sys.stdin.readlines()
			files = [_.strip() for _ in files]

		# Notify on 10% or 100 files, whicever is larger
		num_files = len(files)
		if num_files > 1000:
			num_10percent = num_files // 10
		else:
			num_10percent = 100

		# Send start notification
		send_notification_queue_start(args, num_files, vals)

		# Iterate other all of the files
		for x,fl in enumerate(files):
			# Make it an absolute path
			fl = os.path.abspath(fl)

			# Ensure it's under the base directory
			if not fl.startswith(vals['basedir']):
				raise PrintHelpException("File '%s' is not under the specified base directory '%s'" % (fl, vals['basedir']))

			# Make a relative path
			z = os.path.relpath(fl, vals['basedir'])
			if z.startswith('..'):
				raise Exception("Should not reach this point as base dir was already checked: %s" % ([fl, vals['basedir'], z]))

			# See if file is already queued
			res = d.tarfile.select('rowid', 'fullpath=?', [fl])
			rows = [dict(_) for _ in res]
			if len(rows):
				print("Skipping: %s" % fl)
			else:
				print("Adding:   %s" % fl)

				h = hashfile(fl)
				sz = os.path.getsize(fl)
				fname = os.path.basename(fl)

				d.new_tarfile(tape=vals['tape'], tar=vals['tar'], fullpath=fl, relpath=z, fname=fname, sz=sz, sha256=h)

				# Send notification
				if (x+1) % num_10percent == 0:
					send_notification_queue_step(args, x+1,num_files,vals['tape'],vals['tar'])

		# TODO: ensure all files in the same tar have the same base directory

		# Send completion notification
		send_notification_queue_done(args, vals['tape'], vals['tar'])

	@classmethod
	def action_write(kls, args):
		# 1) Find tape, find tar, find files
		# 2) Move tape to correct location
		# 3) Write file list to a temp file
		# 4) Run tar against the file list to write to tape


		vals = args.action[1:]
		# Split ['foo=bar', 'baz=bat'] into [['foo','bar'], ['baz','bat']]
		vals = dict([_.split('=',1) for _ in vals])

		# Parse paramaters
		p = DataArgsParser('new tar file')
		p.add('tape', str, required=True)
		p.add('tar', rangeint, required=True)
		vals = p.check(vals)

		d = kls._db_open(args)

		# 1)
		# Get tape
		tape = d.find_tape_by_multi(vals['tape'])
		if not len(tape):
			print("Tape not found")
			return
		tape = tape[0]

		id_tape = tape['rowid']
		print("Tape: SN=%s, barcode=%s" % (tape['sn'], tape['barcode']))

		# Send start notification
		send_notification_write_start(args, vals)

		# Iterate over tar numbers
		for num in range(vals['tar'][0], vals['tar'][1]+1):
			print("-"*80)
			print("Tar: num=%d" % (num))

			# 2-4)
			kls._action_write_num(args, vals, id_tape, num, d)

		# Send done notification
		send_notification_write_done(args, vals)

	@classmethod
	def _action_write_num(kls, args, vals, id_tape, num, d):
		# Beep
		print('\a')
		# Check that user wants to continue onward
		print("Pausing for 30 seconds, ctrl-c to stop writing")
		# Wait for input
		time.sleep(30)

		# Get tar file info
		tar = d.find_tars_by_tape_num(id_tape, num)
		if not len(tar):
			print("Tar not found")
			return

		# Get files for this tar file that have been queued
		files = d.find_tarfiles_by_tar(vals['tape'], num)

		print("Found %d files to write" % len(files))
		if not len(files):
			print("\tNo files found to write, aborint")
			return

		# Sort by path
		files = sorted(files, key=lambda _: _['fullpath'])
		# Check that files are present
		for fl in files:
			if not os.path.exists(fl['fullpath']):
				raise Exception("Could not find file '%s' for tape=%s and tar=%d, aborting" % (fl['fullpath'], id_tape, num))

		# Get the base directory to change working directory to
		basedir = files[0]['fullpath'][:-(len(files[0]['relpath']))]

		# 2)
		# Get tape drive controller
		m = mt(args.file)

		# Move the tape as appropriate
		ret = m.status()
		if ret[0] == -1:
			raise Exception("no tape present, cannot write")

		# Go all the way back to start of the tape
		elif num == 0:
			m.rewind()

		# Not the first file, so look for it
		else:
			# 3 cases of being at the start, middle, or end of the desired file number
			if num == ret[0] and ret[1] == 0:
				# Already there
				pass

			elif num == ret[0] and ret[1] > 0:
				# In the middle of the desired file number, so have to back up and then forward

				# Back up to (ret[0]-1, -1)
				m.bsf()
				# Forward to (ret[0], 0)
				m.fsf()

			elif num == ret[0] and ret[1] == -1:
				# At the end of the desired file number, so have to back up and then forward

				# Back up to (ret[0]-1, -1)
				m.bsf()
				# Forward to (ret[0], 0)
				m.fsf()

			# Need to advance a number of files:
			elif ret[0] < num:
				m.fsf(num - ret[0])

			# Need to backup a number of files
			elif num < ret[0]:
				# Have to back up one more than desired (to end of previous file)
				m.bsf(ret[0] - num + 1)
				# then advance one to start of desired file
				m.fsf(1)

		ret2 = m.status()
		if num != ret2[0] and ret2[1] != 0:
			raise Exception("Failed to seek tape: desired file number %d, was at %s and now at %s" % (num, ret, ret2))

		# set start time
		n = db._now()
		print("Start: %s" % n)
		d.begin()
		d.tar.update({'rowid': tar['rowid']}, {'stime': n})
		d.commit()

		try:
			# 3)
			# Write relative file list to a file and tell tar to read from it
			with tempfile.NamedTemporaryFile() as f:
				# Write files in sorted order into the temp file
				for fl in files:
					f.write( (fl['relpath'] + '\n').encode('utf-8') )
					print("Preparing: %s" % fl['relpath'])
				f.seek(0)
				dat = f.read()

				cur_cwd = os.getcwd()
				try:
					print("cwd: %s" % basedir)
					os.chdir(basedir)

					# 4)
					# Verbose to watch progress on the screen
					subargs = ['tar', 'vcf', args.file, '--verbatim-files-from', '-T', f.name]
					# print the args for debugging
					print(subargs)
					subprocess.run(subargs)
				finally:
					# Move CWD to original location
					os.chdir(cur_cwd)

			# And temp file auto-cleaned up

		finally:
			# set end time
			n = db._now()
			print("End: %s" % n)
			d.begin()
			d.tar.update({'rowid': tar['rowid']}, {'etime': n})
			d.commit()

		# Send notification of finishing a file
		send_notification_tar_done(args, id_tape, num)

