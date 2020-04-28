def get_full_paths_in_dir(directory, config_file, name=None):
    # deprecated, get rid of this!
    # function wrapped around previous code
    """
    Calls csv-validation function and return full paths as two arguments,
     a list and a string, for compartments and image separately.
    :directory: path to directory containing .csv files, e.g. "path/plate_a/set_1/"
    :config_file: parsed configuration data (output from cytominer_database.utils.read_config(config_path))

    """
    # compartments: list of strings (full paths to compartment csv files in directory)
    # image:  string (full path to image.csv in directory)
    # We return two separate arguments.
    # NOTE:
    # - Changes s.t. it does not return None values
    # - Would prefer to replace the function by get_dict_of_paths(), which returns a dictionary.
    # cytominer_database.utils.validate_csv_set(config_file, directory) is a rabbit hole!
    print("In  get_full_paths_in_dir(): ")
    try:
        compartments, image = cytominer_database.utils.validate_csv_set(
            config_file, directory
        )
    except IOError as e:
        print("IOError, no return value")
        click.echo(e)
        return
    # get rid of None values in compartments:
    compartments_without_None_values = [comp for comp in compartments if comp]
    # check image for being a None value:
    """
    if image:
        return compartments_without_None_values, image
    else:
        return compartments_without_None_values # will give unpacking error
    """
    #--------------- temporary -----------------
    print("#*********************************** In get_full_paths_in_dir(): ***********************************#")
    print("compartments_without_None_values : ")
    print(compartments_without_None_values)
    print(" image : ")
    print(image)
    print("#***************************************************************************************************#")
    #-------------------------------------------
    return compartments_without_None_values, image
