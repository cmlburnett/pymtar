"""
pymtar -- python magnetic tape tar interface

Stores file data in a sqlite database for rapid localization of files on a tape archive.
"""

# Global libraries
import datetime
import hashlib
import os
import subprocess

# Installed libraries
import sqlite3

# My installed libraries
from sqlitehelper import SH, DBTable, DBCol, DBColROWID

class ItemExists(Exception): pass
class ItemNotFound(Exception): pass

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

	def new_tar(self, tape, num, stime, etime, access_cnt, blk_offset, options, uname):
		rows = self.find_tape_by_multi(tape)
		if not len(rows):
			raise ItemNotFound("Unable to find tape with rowid, serial number, or barcode '%s', cannot create tar" % tape)

		id_tape = rows[0]['rowid']

		rows = self.tar.select('rowid', 'id_tape=? and num=?', [id_tape, num])
		if len(rows):
			raise ItemExists("Tar file num %d with tape '%s' (rowid=%d) already exists, cannot add it again" % (num, tape, id_tape))

		self.begin()
		ret = self.tar.insert(id_tape=id_tape, num=num, stime=stime, etime=etime, access_cnt=access_cnt, blk_offset=blk_offset, options=options, uname=uname)
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

