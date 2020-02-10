def test_on_data_a(directories, target, basename, skip_image_prefix=True):
    print("------------------ test_on_data_a: --------------------------")
    destination = os.path.join(target, basename + ".parquet")
    print("destination =", destination)
    # 1. get Parquet files
    loaded_parquet = pq.ParquetFile(destination)

    # 2. read entire Parquet file as pyarrow table (not necessary)
    # loaded_pyarrow = loaded_parquet.read()
    print("loaded_parquet.num_row_groups = ")
    print(loaded_parquet.num_row_groups)
    print("loaded_parquet.metadata = ")
    print(loaded_parquet.metadata)
    # get individual row groups
    loaded_pyarrow_tables = []  # get individual tables from row_groups
    loaded_pandas = []
    for i in range(loaded_parquet.num_row_groups):
        loaded_pyarrow_tables.append(loaded_parquet.read_row_group(i))
        loaded_pandas.append(loaded_parquet.read_row_group(i).to_pandas())
        print(loaded_pyarrow_tables[i].shape)

    # 3. get original table and type convert
    compartm_paths = []
    local_modif_dframes = []
    local_tables = []

    for i in range(loaded_parquet.num_row_groups):
        # get path (&deal with annoying case inconsistency)
        # temporary: here we assume that the order of the entries in directories
        # corresponds to the order with which the tables were written to the writer.
        # Note that we can't do this is we have missing .csv's for some compartments.
        directory = directories[i]
        path = get_csv_path(directory, basename=basename)
        compartm_paths.append(path)
        # get identifier
        if basename.lower() == "image":
            identifier = checksum(path)
            df_local = get_df(compartm_paths[i], identifier, skip_image_prefix)
        else:
            identifier = checksum(get_csv_path(directory, basename="image"))
            df_local = get_df(compartm_paths[i], identifier)
        # get modified pandas DF
        print("df_local.shape = ", df_local.shape)
        df_local_converted_i = convert_cols_int2float(df_local)
        local_modif_dframes.append(df_local_converted_i)
        table_local = pyarrow.Table.from_pandas(df_local_converted_i)
        local_tables.append(table_local)

    # 4. compare tables --> same: yay.
    for i in range(loaded_parquet.num_row_groups):
        print("In test_on_data_a: num_row_group = ", str(i))
        # compare pyarrow tables
        if loaded_pyarrow_tables[i] == local_tables[i]:
            assert loaded_pyarrow_tables[i] == local_tables[i]
            print("--------------------- Passed assert: -----------------------------")
            print("assert(loaded_pyarrow_tables[i] == local_tables[i])")
        else:
            print("--------------------- Failed assert: -----------------------------")
            print(
                "In test_on_data_a: FAIL on 'assert(loaded_pyarrow_tables[i] == local_tables[i])'"
            )
            print("directories[i] =", directories[i])

        # compare pandas df
        if loaded_pandas[i].all().all() == local_modif_dframes[i].all().all():
            assert loaded_pandas[i].all().all() == local_modif_dframes[i].all().all()
            print("--------------------- Passed assert: -----------------------------")
            print(
                "assert(loaded_pandas[i].all().all() == local_modif_dframes[i].all().all())"
            )
        else:
            print("--------------------- Failed assert: -----------------------------")
            print(
                "In test_on_data_a: FAIL on 'loaded_pandas[i].all().all() == local_modif_dframes[i].all().all()'"
            )
            print("directories[i] =", directories[i])

        print("loaded_pyarrow_tables[i].shape", loaded_pyarrow_tables[i].shape)
        print("local_tables[i].shape", local_tables[i].shape)

    return loaded_pyarrow_tables, loaded_pandas, local_tables, local_modif_dframes


def get_csv_path(
    directory, basename
):  # will return full path regardless of capitalization
    path_lowercase = os.path.join(directory, basename.lower() + ".csv")
    path_capit = os.path.join(directory, basename.capitalize() + ".csv")
    print("In get_csv_path: ", path_capit)
    print("In get_csv_path: ", type(path_capit))
    if os.path.exists(path_lowercase):
        path = path_lowercase
    elif os.path.exists(path_capit):
        path = path_capit
    return path


# Test code designed for Parquet files generated from Tables with missing columns
# (works for both cases)


def load_parquet_files(target):
    written_parquet = {}
    written_pyarrow_tables = {}
    written_pandas = {}
    for name in ["Image", "Cytoplasm", "Cells", "Nuclei"]:
        # slice into individual tables using the num_row_groups
        path = os.path.join(target, name + ".parquet")
        parquet_file = pq.ParquetFile(path)
        written_pyarrow_tables[name] = []  # get individual tables from row_groups
        written_pandas[name] = []

        for i in range(parquet_file.num_row_groups):
            row_group = parquet_file.read_row_group(i)
            written_pyarrow_tables[name].append(row_group)
            written_pandas[name].append(row_group.to_pandas())
            print(written_pyarrow_tables[name][i].shape)
    return written_pyarrow_tables, written_pandas


def load_reference_csv_files(
    source, target, config_file, directories=None, skip_image_prefix=True
):
    # init dictionary that stores reference dataframes
    ref_dataframes = {}
    # get dirs if not prespecified
    if not directories:
        directories = sorted(list(cytominer_database.utils.find_directories(source)))
    # -------------------------- get reference df --------------------------------
    ref_fraction = float(config_file["schema"]["ref_fraction"])
    # ref_dir       = get_reference_dir(directories, ref_fraction, config_file) #deprecated
    full_paths = get_dict_of_pathlist(source=None, directories=directories)
    ref_paths = get_reference_paths(ref_fraction, full_paths)

    refIdentifier = 999 & 0xFFFFFFFF
    for name, path in ref_paths.items():
        ref_df = get_df(path, refIdentifier)
        # convert type
        type_conversion = config_file["schema"]["type_conversion"]
        if type_conversion == "int2float":
            ref_df = convert_cols_int2float(
                ref_df
            )  # converts all columns int -> float (except for "TableNumber")
        elif type_conversion == "all2string":
            ref_df = convert_cols_2string(ref_df)
        # store
        ref_dataframes[name] = ref_df
    return ref_dataframes


def load_csv_files(
    source,
    target,
    config_file,
    ref_dataframes,
    directories=None,
    skip_image_prefix=True,
):
    dataframes = {}
    aligned_dataframes = {}
    # get dirs if not prespecified
    if not directories:
        directories = sorted(list(cytominer_database.utils.find_directories(source)))
    for i, directory in enumerate(directories):
        dataframes[i] = {}
        aligned_dataframes[i] = {}
        compartments, image = get_full_paths_in_dir(
            directory, config_file
        )  # returns a list of strings and a string
        identifier = checksum(image)
        for path in compartments + [image]:
            # get dataframes
            name, _ = os.path.splitext(os.path.basename(path))
            name = name.capitalize()
            if name == "Image":
                dataframe = get_df(path, identifier, skip_image_prefix)
            else:
                dataframe = get_df(path, identifier)
            # type conversion
            dataframe = type_convert_dataframe(dataframe, config_file)
            ref_dataframe = ref_dataframes[name]
            aligned_dataframe, _ = dataframe.align(ref_dataframe, axis=1, join="right")
            # store
            dataframes[i][name] = dataframe
            aligned_dataframes[i][name] = aligned_dataframe
    return dataframes, aligned_dataframes


"""Driver Code : Generate Parquet Files."""


# Generate parquet files (regular driver code)

# ------------------- Regular files ---------------------------------
# directories = [dir_1, dir_2]
# ------------------- Files incl. missing columns --------------------
directories = [dir_1, dir_2, dir_modif]
print("directories = ")
print(directories)
# ---------------------------------------------------------------------

# call main function
seed_modified(source, target, config_file, directories=directories)

"""#Driver Code : Test Parquet Files."""

# test equivalence (of written Parquet files and .csv files)
# Regular driver code
# Result: Passes for all basenames and case with no missing columns!

basename = "Cytoplasm"  # Image, Cells, Nuclei, Cytoplasm
destination = os.path.join(target, basename + ".parquet")

# call testing function "test_on_data_a() "
(
    loaded_pyarrow_tables,
    loaded_pandas,
    local_tables,
    local_modif_dframes,
) = test_on_data_a(
    directories, target, basename
)  # works!

group = 2
print("loaded_pandas[group].head(25)")
print(loaded_pandas[group].head(25))
print("print(local_modif_dframes[group].head(25))")
print(local_modif_dframes[group].head(25))

# local code
loaded_parquet = pq.ParquetFile(destination)
print("loaded_parquet.num_row_groups")
print(loaded_parquet.num_row_groups)  # 2
print("loaded_parquet.metadata")
print(loaded_parquet.metadata)
pa_local = loaded_parquet.read_row_group(group)
pd_local = pa_local.to_pandas()
pd_local.head(25)

# Driver code that specifically tests for equivalence of Parquet files
# .... generated from tables that have missing columns.
# Result: Passes for all basenames and both cases!

# 1. Get dataframes from parquet files and from csv files
written_pyarrow_tables, written_pandas = load_parquet_files(target)
ref_dataframes = load_reference_csv_files(source, target, config_file)
dataframes, aligned_dataframes = load_csv_files(
    source, target, config_file, ref_dataframes, directories
)  # includes type conversion and alignment (padding with None columns)


# 2. Deal with missing columns
# Here: Use TableNumber to match rowgroup (from partitioned Parquet file)
# with dir_number (from .csv files in different directories.

number_of_directories = len(directories)

for rowgroup in range(len(written_pandas[basename])):
    from_parquet = written_pandas[basename][rowgroup]
    parquet_TableNumber = from_parquet["TableNumber"][0]
    print("Loading rowgroup=", str(rowgroup), " of Parquet Files")
    print("--->  TableNumber =", str(parquet_TableNumber))

    print("Checking for matching csv Table numbers:")
    for dir_number in range(number_of_directories):
        csv_TableNumber = aligned_dataframes[dir_number][basename]["TableNumber"][0]
        print("csv_TableNumber")
        print(csv_TableNumber)
        if parquet_TableNumber == csv_TableNumber:  # same table
            print("Found identical TableNumber = ", str(parquet_TableNumber))
            # recall: aligned_dataframes have been processed like the pandas tables before writing them to Parquet. Hence we don't need to check for schema disagreements in type or column number

            print(
                ">>>>>>>>> Dataframe from loaded row group in .parquet == processed dataframe loaded from .csv : <<<<<<<<<<< "
            )
            print(
                "--------------------> ",
                aligned_dataframes[dir_number][basename].equals(from_parquet),
            )
            print("Continue with next row_group.")
            break


# Note: For this to work, the table numbers of different tables need to differ, but be reproducible.

# 2. Deal with missing columns
# Also for the case where .csv files might be missing entirely.
# Idea: get list of valid directories, load each .csv file and get TableNumber.
# Then look for the rowgroup with that TableNumber. Then assert equality of all entries (csv-file<->Parquet row group)
# Do this for every kind of table (Image, Cells, Nuclei, Cytoplasm) separately.

number_of_directories = len(directories)

for dir_number in range(number_of_directories):
    csv_TableNumber = aligned_dataframes[dir_number][basename]["TableNumber"][
        0
    ]  # to do : first check if table exists and skip otherwise
    print("csv_TableNumber")
    print(csv_TableNumber)

    for rowgroup in range(len(written_pandas[basename])):
        from_parquet = written_pandas[basename][rowgroup]
        parquet_TableNumber = from_parquet["TableNumber"][0]
        print("Loading rowgroup=", str(rowgroup), " of Parquet Files")
        print("--->  TableNumber =", str(parquet_TableNumber))
        if parquet_TableNumber == csv_TableNumber:  # same table
            print("Found identical TableNumber = ", str(parquet_TableNumber))
            print(
                ">>>>>>>>> Dataframe from loaded row group in .parquet == processed dataframe loaded from .csv : <<<<<<<<<<< "
            )
            print(
                "--------------------> ",
                aligned_dataframes[dir_number][basename].equals(from_parquet),
            )
            print("Continue with next row_group.")
            break

rowgroup = 2

print("written_pandas[basename][rowgroup].shape")
print(written_pandas[basename][rowgroup].shape)
written_pandas[basename][rowgroup].head(25)

print("aligned_dataframes[dir_number][basename].shape")

print(aligned_dataframes[dir_number][basename].shape)
aligned_dataframes[dir_number][basename].head(25)
# [solved] columns are mixed up relative to ref table ---> check settings of align. (same shape, different order) [done]
