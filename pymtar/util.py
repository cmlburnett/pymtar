
import datetime
import subprocess

class ItemExists(Exception): pass
class ItemNotFound(Exception): pass
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

def rangeint(v):
	try:
		ret = int(v)
		return (ret,ret)
	except:
		pass

	if '-' not in v:
		raise ValueError("Unrecognized integer range '%s'" % v)

	parts = v.split('-')
	if len(parts) != 2:
		raise ValueError("Unrecognized integer range '%s'" % v)

	return (int(parts[0]), int(parts[1]))

def hashfile(f):
	"""
	Hash file with path @f.
	Either call sha256sum command line tool, or implement it directly in python.
	As I suspect sha256sum is faster I will invoke it through subprocess.
	"""

	args = ['sha256sum', f]
	ret = subprocess.run(args, stdout=subprocess.PIPE)
	return ret.stdout.decode('utf-8').split(' ')[0]

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

def getuname():
	"""Call `uname -a` to get system information"""
	ret = subprocess.run(['uname', '-a'], stdout=subprocess.PIPE)
	return ret.stdout.decode('ascii').strip()

