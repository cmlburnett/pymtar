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
			DBCol('ptime', 'datetime') # Purchase date and time
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
			DBCol('fullpath', 'text'), # Full path
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



class mt:
	def __init__(self, dev):
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
		ret = self._run('mt', '-f', self._dev, 'status')
		lines = ret.split('\n')
		parts = lines[1].strip('.').split(',')
		parts = [_.strip() for _ in parts]

		fnum = int(parts[0].split('=')[-1])
		blk = int(parts[1].split('=')[-1])
		part = int(parts[2].split('=')[-1])

		return (fnum, blk, part)

	def rewind(self):
		self._run('mt', '-f', self._dev, 'rewind')

	def offline(self):
		self._run('mt', '-f', self._dev, 'offline')

	def bsf(self, cnt):
		if cnt is not int:
			raise Exception("bsf: cnt parameter must be an integer, got '%s'" % cnt)

		self._run('mt', '-f', self._dev, 'bsf', cnt)

	def fsf(self, cnt):
		if cnt is not int:
			raise Exception("fsf: cnt parameter must be an integer, got '%s'" % cnt)

		self._run('mt', '-f', self._dev, 'bsf', cnt)

