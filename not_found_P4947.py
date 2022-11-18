# pyright: strict

from not_found_tmdb import main

if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO)
    main(type="movie")
