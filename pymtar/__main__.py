
# Global libraries
import argparse
import json
import sys

# This library
import pymtar

class PrintHelpException(Exception): pass

class actions:
	@classmethod
	def action_find(kls, args):
		print('find', args)

		if args.action[1] == 'tape.barcode':
			kls.action_find_tape_barcode(args, args.action[2])

		elif args.action[1] == 'tape.sn':
			kls.action_find_tape_sn(args, args.action[2])

		else:
			raise PrintHelpException("Unrecognized find command: %s" % args.action[1])

	@classmethod
	def action_find_tape_barcode(kls, args, bcode):
		print('find tape barcode', bcode)

	@classmethod
	def action_find_tape_sn(kls, args, sn):
		print('find tape sn', sn)

	@classmethod
	def action_new(kls, args):
		print('new', args)



def main():
	p = argparse.ArgumentParser(add_help=False)
	p.add_argument('-h', '--help', action='store_true', default=False, help='Show usage information')
	p.add_argument('-f', '--file', default='/dev/nst0', help='Device file path')
	p.add_argument('-j', '--json', default=False, action='store_true', help="Print responses, where appropriate, in JSON instead")
	p.add_argument('action', nargs=argparse.REMAINDER, help='Action/command to execute')

	acts = {}
	acts['find'] = actions.action_find
	acts['new'] = actions.action_new

	action_help = """
  Actions help:
    find tape.barcode   Find tapes by barcode
    find tape.sn        Find tapes by serial number
    new file            Create a new file within a tar file
    new tape            Create a new tape record
    new tar             Create a new tar file
"""

	args = p.parse_args()
	if args.help:
		p.print_help()
		print(action_help)
		sys.exit(2)
	print(args)

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
