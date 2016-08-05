"""

"""

def test_seed(dataset):
	assert 1 == 1

    # dataset = datasets[selected_dataset]

    # if dataset["munge"]:
    #     subprocess.call(["./munge.sh", dataset["data_dir"]])

    # config_file = os.path.join(dataset["data_dir"], "config.ini")

    # config = configparser.ConfigParser()

    # config.read(config_file)

    # perturbation.ingest.seed(config=config, input=dataset["data_dir"], session=session, stage="images", sqlfile="views.sql")

    # for directory in glob.glob(os.path.join(dataset["data_dir"], "*/")):
    #     perturbation.database.seed(config=config, input=directory, session=session, stage="objects", sqlfile="views.sql")
