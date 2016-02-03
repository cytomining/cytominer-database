from perturbation.model import *

wells = session.query(Well).options(sqlalchemy.orm.joinedload(Well.images)).filter(Well.description.in_(['A01', 'A02', 'A03', 'A04']))



images = sum([well.images for well in wells], [])

matches = sum([image.matches for image in images], [])

intensities = sum([match.intensities for match in matches], [])

ids = [intensity.id for intensity in intensities]


Intensity.first_quartile
Intensity.integrated
Intensity.mass_displacement
Intensity.maximum
Intensity.mean
Intensity.median
Intensity.median_absolute_deviation
Intensity.minimum
Intensity.standard_deviation
Intensity.third_quartile

name = Intensity.integrated





a = session.query(
    sqlalchemy.func.avg(name).label('integrated_μ')
).filter(name.in_(ids)).first()

b = session.query(
    sqlalchemy.func.standard_deviation(name).label('integrated_σ')
).filter(name.in_(ids)).first()

session.query(
    sqlalchemy.func.standard_score(name, a[0], b[0]).label('integrated_standard_score')
).all()

