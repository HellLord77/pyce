__author__ = "HellLord"
__copyright__ = ""
__credits__ = ["HellLord"]

__license__ = ""
__version__ = "0.0.3"
__maintainer__ = "HellLord"
__email__ = "ratul.debnath.year@gmail.com"
__status__ = "Development"

import sys


def main():
    import main

    try:
        main.main()
    except SystemExit:
        raise
    except KeyboardInterrupt:
        print("Ctrl+C received, exiting...")
    except Exception as exception:
        print(exception, file=sys.stderr)
        print("Unexpected error, exiting...")


if __name__ == "__main__":
    main()
