import argparse
from process_handlers.views_process_handler import ViewsProcessHandler


def main():

	parser = argparse.ArgumentParser(
			description="Let's Parse some Steam data!"
	)
	parser.add_argument('-t','--type', help="views")
	parser.add_argument('-l','--local', action='store_const', const=1, help="turns on Local mode")
	args = parser.parse_args()

	if args.type == 'views':
		vph = ViewsProcessHandler(args.local)
		vph.run()


if __name__ == '__main__':
	main()