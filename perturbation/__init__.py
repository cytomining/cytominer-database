import perturbation.metadata


def __main__(filename):
    example = perturbation.metadata.Metadata(filename)

    print(example.__shape__('Cells', 1, 1))

if __name__ == '__main__':
    __main__(filename='../object.csv')
