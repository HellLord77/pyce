__version__ = "0.0.2"

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
