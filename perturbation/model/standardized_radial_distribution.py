"""

"""

import perturbation.base
import perturbation.model.radial_distribution
import perturbation.view
import sqlalchemy


class StandardizedRadialDistribution(perturbation.base.Base):
    """

    """

    __table__ = perturbation.view.create_view(
        metadata=perturbation.base.Base.metadata,
        name="standardized_radial_distributions",
        selectable=sqlalchemy.select(
            [
                perturbation.model.radial_distribution.RadialDistribution.id.label('id'),
                sqlalchemy.func.standard_deviation(
                    perturbation.model.radial_distribution.RadialDistribution.frac_at_d
                ).label('frac_at_d'),
                sqlalchemy.func.standard_deviation(
                    perturbation.model.radial_distribution.RadialDistribution.mean_frac
                ).label('mean_frac'),
                sqlalchemy.func.standard_deviation(
                    perturbation.model.radial_distribution.RadialDistribution.radial_cv
                ).label('radial_cv')
            ]
        ).select_from(
            perturbation.model.radial_distribution.RadialDistribution
        ).group_by(
            perturbation.model.radial_distribution.RadialDistribution.id
        )
    )
