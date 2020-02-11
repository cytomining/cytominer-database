def write_csv_to_parquet(
    input, output, identifier, writers_dict, skip_table_prefix=False
):

    # [Solved] Problem: align() aligns the ref frame with the dataframe,
    # but the TableNumber column is sometimes moved from being the first column
    # to the middle. This was solved by choosing "join = 'right' ", which assures
    # that the new tables columns are aligned to the reference table's columns
    # (whereas the reference table needs to remain unaffacted by the alignment)



def get_df_from_temp_dir(input, identifier, skip_table_prefix=False):
    # Note: Do not use pandas_df = pd.read_csv(tmp_source, index_col=0) --> uses TableNumber as a row label, not a column


def get_df(input, identifier, skip_table_prefix=False):
    """
  # Returns the pandas data frame of the modified csv (prefixed column names).
  # This function is an alternative to get_df_from_temp_dir().
  # - It does not use a temporary location backports.tempfile.TemporaryDirectory()
  # - it imports csv  -> Pandas dataframe directly
  # - it modifies the table just like get_df_from_temp_dir(), but in Pandas DF format. [tested!]
  # - it does not use the helper function __format__(name, header), which converts all types to "object"
  """


  def open_writers(source, target, config_file, skip_image_prefix=True):
      # returns values as single string in a dict
      # old :ref_dir   = get_dict_of_pathlist(source=None, directories=[reference_folder]) #returns values as list in a dict
      # Argument needs to be passed as dict, however, the output needs to be a single string (the ref path)
       """
       #  Note: We do not need to call "get_full_paths_in_dir" again,
       since the paths have been verified before they were added to the dictionary,
           using(cytominer_database.utils.validate_csv_set(config_file, directory)).

       Note: the actal values of e.g. the identifier can be chosen w.l.o.g., as long as they're of the right type.
       We will not write yet, we only want to open a writer with the correct reference schema.
       """
       # fullpath = os.path.join(ref_dir[name], name + ".csv") #deprecated, since get_reference_paths returns full paths
       print("------------------- Full path: -------------------")
       print(path)
       # Get full path to reference table. This table needs to have all column names.
       # A not very nice way to ensure the prefixes are skipped only for image-tables
       # ...(or not at all, if " skip_image_prefix" was passed as False)

        # ---------------------- temporary -------------------------------------
        refPyTable_before_conversion = pyarrow.Table.from_pandas(ref_df)
        ref_schema_before_conversion = refPyTable_before_conversion.schema[0]
        print("------ In open_writers(): ref_schema_before_conversion --------")
        # print(ref_schema_before_conversion)
        # ----------------------------------------------------------------------
        # Modify the dataframe types. (---> pyarrow schema)
        # Several options.
        # Safest: Simply convert all values to string.
        # Works best for our example data : simply convert int->float
        """
        flavor = {'spark'} ---> allows us to read the Parquet file with Apache Spark
        compression= ‘GZIP’ ---> allows us to read the Parquet file with FastParquet.
        However: Gives "AttributeError: 'set' object has no attribute 'iteritems"
        """

        # metadatacollector = {'metadata_collector': []}
        # used as an argument to store metadata from writer
        # writers_dict[name]["writer"] = pq.ParquetWriter(destination, ref_schema,
        # flavor = {'spark'}, compression= {'GZIP'})

        def get_dict_of_pathlist(directories=None, source=None):
                # def get_dict_of_pathlist(source, directories=None):
                # get a dictionary containing all paths of all table kinds from all subsubdirectories of "source"
                # ... or from the list of directories passed.
                # returns "full_paths" dict with
                #   key == table kind (capitalized)
                #   value == list of all full paths to that table kind

                # Note : This function can also be called with a single folder reference, i.e. source=None, directories = [directory]
                # This is used e.g. if a single folder is provided that contains all reference tables


    get_reference_paths(ref_fraction, full_paths):




    def convert_cols_int2float(pandas_df):


    seed(source, target, config_file, skip_image_prefix=True, directories=None):
