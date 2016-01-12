import perturbation.base
import perturbation.metadata
import perturbation.model.angle
import perturbation.model.coordinate
import perturbation.model.correlation
import perturbation.model.image
import perturbation.model.intensity
import perturbation.model.location
import perturbation.model.match
import perturbation.model.moment
import perturbation.model.neighborhood
import perturbation.model.pattern
import perturbation.model.radial_distribution
import perturbation.model.ring
import perturbation.model.scale
import perturbation.model.shape
import perturbation.model.stain
import perturbation.model.texture
import sqlalchemy
import sqlalchemy.orm


def __main__(filename):
    engine = sqlalchemy.create_engine('sqlite://', echo=False)

    perturbation.base.Base.metadata.create_all(engine)

    Session = sqlalchemy.orm.sessionmaker(bind=engine)

    session = Session()

    example = perturbation.metadata.Metadata(filename)

    image_identifiers = example.records[('Image', 'ImageNumber')].unique()

    for image_identifier in image_identifiers:
        image = perturbation.model.image.Image().find_or_create_by(
            session,

            id=image_identifier
        )

        match_identifiers = example.records[
            example.records[
                ('Image', 'ImageNumber')
            ] == image_identifier
        ][
            ('Object', 'ObjectNumber')
        ].unique()

        for pattern_description in example.__patterns__():
            pattern = perturbation.model.pattern.Pattern().find_or_create_by(
                session,

                description=pattern_description
            )

            for match_identifier in match_identifiers:
                match = perturbation.model.match.Match().find_or_create_by(
                    session,

                    id=match_identifier
                )

if __name__ == '__main__':
    __main__(filename='../test/data/object.csv')
