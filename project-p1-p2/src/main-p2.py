"""Batch Transformation and Data Quality Pipeline"""


def main_p2():
    """Pipeline orchestration script that:

    - Reads dataset from collisions_raw table
    - Applies `clean` transformation
    - Writes dataset to collisions_clean table
    - Reads dataset from collisions_clean table
    - Applies `curated` transformation
    - Writes dataset to collisions_curated table
    """
    pass


if __name__ == "__main__":
    main_p2()
