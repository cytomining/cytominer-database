DROP VIEW IF EXISTS 'view_correlations';
CREATE VIEW 'view_correlations' AS
SELECT
	plates.description                      AS plate_description,
	wells.description                       AS well_description,
	images.description                      AS image_description,
	objects.description                     AS object_description,
	patterns.description                    AS pattern_description,
	channels1.description                   AS channel1_description,
	channels2.description                   AS channel2_description,
  correlations.coefficient                AS correlations_coefficient
FROM plates
INNER JOIN wells                 ON wells.plate_id              = plates.id
INNER JOIN images                ON images.well_id              = wells.id
INNER JOIN objects               ON objects.image_id            = images.id
INNER JOIN matches               ON matches.object_id           = objects.id
INNER JOIN patterns              ON matches.pattern_id          = patterns.id
INNER JOIN channels as channels1 ON correlations.dependent_id   = channels1.id
INNER JOIN channels as channels2 ON correlations.independent_id = channels2.id
INNER JOIN correlations          ON correlations.match_id       = matches.id
;

DROP VIEW IF EXISTS 'view_edges';
CREATE VIEW 'view_edges' AS
 SELECT
	plates.description                      AS plate_description,
	wells.description                       AS well_description,
	images.description                      AS image_description,
	objects.description                     AS object_description,
	patterns.description                    AS pattern_description,
	channels.description                    AS channel_description,
	edges.integrated                        AS edge_integrated,
	edges.maximum                           AS edge_maximum,
	edges.mean                              AS edge_mean,
	edges.minimum                           AS edge_minimum,
	edges.standard_deviation                AS edge_standard_deviation
FROM plates
INNER JOIN wells         ON wells.plate_id           = plates.id
INNER JOIN images        ON images.well_id           = wells.id
INNER JOIN objects       ON objects.image_id         = images.id
INNER JOIN matches       ON matches.object_id        = objects.id
INNER JOIN patterns      ON matches.pattern_id       = patterns.id
INNER JOIN channels      ON edges.channel_id         = channels.id
INNER JOIN edges         ON edges.match_id           = matches.id
;

DROP VIEW IF EXISTS 'view_intensities';
CREATE VIEW 'view_intensities' AS
SELECT
	plates.description                      AS plate_description,
	wells.description                       AS well_description,
	images.description                      AS image_description,
	objects.description                     AS object_description,
	patterns.description                    AS pattern_description,
	channels.description                    AS channel_description,
  intensities.first_quartile              AS intensity_first_quartile,
  intensities.integrated                  AS intensity_integrated,
  intensities.maximum                     AS intensity_maximum,
  intensities.mean                        AS intensity_mean,
  intensities.median                      AS intensity_median,
  intensities.median_absolute_deviation   AS intensity_median_absolute_deviation,
  intensities.minimum                     AS intensity_minimum,
  intensities.standard_deviation          AS intensity_standard_deviation,
  intensities.third_quartile              AS intensity_third_quartile
FROM plates
INNER JOIN wells         ON wells.plate_id           = plates.id
INNER JOIN images        ON images.well_id           = wells.id
INNER JOIN objects       ON objects.image_id         = images.id
INNER JOIN matches       ON matches.object_id        = objects.id
INNER JOIN patterns      ON matches.pattern_id       = patterns.id
INNER JOIN channels      ON intensities.channel_id   = channels.id
INNER JOIN intensities   ON intensities.match_id     = matches.id
;

DROP VIEW IF EXISTS 'view_locations';
CREATE VIEW 'view_locations' AS
SELECT
	plates.description                      AS plate_description,
	wells.description                       AS well_description,
	images.description                      AS image_description,
	objects.description                     AS object_description,
	patterns.description                    AS pattern_description,
	channels.description                    AS channel_description,
  center_mass_intensity.abscissa          AS locations_center_mass_intensity_x,
  center_mass_intensity.ordinate          AS locations_center_mass_intensity_y,
  max_intensity.abscissa                  AS locations_max_intensity_x,
  max_intensity.ordinate                  AS locations_max_intensity_y
FROM plates
INNER JOIN wells                                ON wells.plate_id                     = plates.id
INNER JOIN images                               ON images.well_id                     = wells.id
INNER JOIN objects                              ON objects.image_id                   = images.id
INNER JOIN matches                              ON matches.object_id                  = objects.id
INNER JOIN patterns                             ON matches.pattern_id                 = patterns.id
INNER JOIN channels                             ON locations.channel_id               = channels.id
INNER JOIN locations                            ON locations.match_id                 = matches.id
INNER JOIN coordinates as center_mass_intensity ON locations.center_mass_intensity_id = center_mass_intensity.id
INNER JOIN coordinates as max_intensity         ON locations.max_intensity_id         = max_intensity.id
;

DROP VIEW IF EXISTS 'view_moments';
CREATE VIEW 'view_moments' AS
SELECT
	plates.description          AS plate_description,
	wells.description           AS well_description,
	images.description          AS image_description,
	objects.description         AS object_description,
	patterns.description        AS pattern_description,
	moments.a                   AS moments_a,
	moments.b                   AS moments_b,
	moments.score               AS moments_score
FROM plates
INNER JOIN wells                 ON wells.plate_id      = plates.id
INNER JOIN images                ON images.well_id      = wells.id
INNER JOIN objects               ON objects.image_id    = images.id
INNER JOIN matches               ON matches.object_id   = objects.id
INNER JOIN patterns              ON matches.pattern_id  = patterns.id
INNER JOIN shapes                ON matches.shape_id    = shapes.id
INNER JOIN moments               ON moments.shape_id    = shapes.id
;

DROP VIEW IF EXISTS 'view_neighborhoods';
CREATE VIEW 'view_neighborhoods' AS
SELECT
	plates.description                                   AS plate_description,
	wells.description                                    AS well_description,
	images.description                                   AS image_description,
	objects.description                                  AS object_description,
	patterns.description                                 AS pattern_description,
  neighborhoods.angle_between_neighbors_5              AS neighborhoods_angle_between_neighbors_5,
  neighborhoods.angle_between_neighbors_adjacent       AS neighborhoods_angle_between_neighbors_adjacent,
  neighborhoods.first_closest_distance_5               AS neighborhoods_first_closest_distance_5,
  neighborhoods.first_closest_distance_adjacent        AS neighborhoods_first_closest_distance_adjacent,
  neighborhoods.first_closest_object_number_adjacent   AS neighborhoods_first_closest_object_number_adjacent,
  neighborhoods.number_of_neighbors_5                  AS neighborhoods_number_of_neighbors_5,
  neighborhoods.number_of_neighbors_adjacent           AS neighborhoods_number_of_neighbors_adjacent,
  neighborhoods.percent_touching_5                     AS neighborhoods_percent_touching_5,
  neighborhoods.percent_touching_adjacent              AS neighborhoods_percent_touching_adjacent,
  neighborhoods.second_closest_distance_5              AS neighborhoods_second_closest_distance_5,
  neighborhoods.second_closest_distance_adjacent       AS neighborhoods_second_closest_distance_adjacent,
  neighborhoods.second_closest_object_number_adjacent  AS neighborhoods_second_closest_object_number_adjacent
FROM plates
INNER JOIN wells          ON wells.plate_id            = plates.id
INNER JOIN images         ON images.well_id            = wells.id
INNER JOIN objects        ON objects.image_id          = images.id
INNER JOIN matches        ON matches.object_id         = objects.id
INNER JOIN patterns       ON matches.pattern_id        = patterns.id
INNER JOIN neighborhoods  ON matches.neighborhood_id   = neighborhoods.id
;

DROP VIEW IF EXISTS 'view_radial_distributions';
CREATE VIEW 'view_radial_distributions' AS
SELECT
	plates.description                      AS plate_description,
	wells.description                       AS well_description,
	images.description                      AS image_description,
	objects.description                     AS object_description,
	patterns.description                    AS pattern_description,
	channels.description                    AS channel_description,
  radial_distributions.bins               AS radial_distributions_bins,
  radial_distributions.frac_at_d          AS radial_distributions_frac_at_d,
  radial_distributions.mean_frac          AS radial_distributions_mean_frac,
  radial_distributions.radial_cv          AS radial_distributions_radial_cv
FROM plates
INNER JOIN wells                 ON wells.plate_id                   = plates.id
INNER JOIN images                ON images.well_id                   = wells.id
INNER JOIN objects               ON objects.image_id                 = images.id
INNER JOIN matches               ON matches.object_id                = objects.id
INNER JOIN patterns              ON matches.pattern_id               = patterns.id
INNER JOIN channels              ON radial_distributions.channel_id  = channels.id
INNER JOIN radial_distributions  ON radial_distributions.match_id    = matches.id
;

DROP VIEW IF EXISTS 'view_shapes';
CREATE VIEW 'view_shapes' AS
SELECT
	plates.description          AS plate_description,
	wells.description           AS well_description,
	images.description          AS image_description,
	objects.description         AS object_description,
	patterns.description        AS pattern_description,
  center.abscissa             AS center_x,
  center.ordinate             AS center_y,
	shapes.area                 AS shapes_area,
  shapes.compactness          AS shapes_compactness,   
  shapes.eccentricity         AS shapes_eccentricity,      
  shapes.euler_number         AS shapes_euler_number,     
  shapes.extent               AS shapes_extent,     
  shapes.form_factor          AS shapes_form_factor,
  shapes.major_axis_length    AS shapes_major_axis_length, 
  shapes.max_feret_diameter   AS shapes_max_feret_diameter,
  shapes.maximum_radius       AS shapes_maximum_radius,
  shapes.mean_radius          AS shapes_mean_radius,   
  shapes.median_radius        AS shapes_median_radius,     
  shapes.min_feret_diameter   AS shapes_min_feret_diameter,
  shapes.minor_axis_length    AS shapes_minor_axis_length,
  shapes.orientation          AS shapes_orientation,
  shapes.perimeter            AS shapes_perimeter,      
  shapes.solidity             AS shapes_solidity        
FROM plates
INNER JOIN wells                 ON wells.plate_id      = plates.id
INNER JOIN images                ON images.well_id      = wells.id
INNER JOIN objects               ON objects.image_id    = images.id
INNER JOIN matches               ON matches.object_id   = objects.id
INNER JOIN patterns              ON matches.pattern_id  = patterns.id
INNER JOIN shapes                ON matches.shape_id    = shapes.id
INNER JOIN coordinates as center ON shapes.center_id    = center.id
;

DROP VIEW IF EXISTS 'view_textures';
CREATE VIEW 'view_textures' AS
SELECT
	plates.description                      AS plate_description,
	wells.description                       AS well_description,
	images.description                      AS image_description,
	objects.description                     AS object_description,
	patterns.description                    AS pattern_description,
	channels.description                    AS channel_description,
  textures.scale                          AS textures_scale,
  textures.angular_second_moment          AS textures_angular_second_moment,           
  textures.contrast                       AS textures_contrast,
  textures.correlation                    AS textures_correlation,           
  textures.difference_entropy             AS textures_difference_entropy,
  textures.difference_variance            AS textures_difference_variance,           
  textures.entropy                        AS textures_entropy,
  textures.gabor                          AS textures_gabor,        
  textures.info_meas_1                    AS textures_info_meas_1,
  textures.info_meas_2                    AS textures_info_meas_2,           
  textures.inverse_difference_moment      AS textures_inverse_difference_moment,
  textures.sum_average                    AS textures_sum_average,
  textures.sum_entropy                    AS textures_sum_entropy,          
  textures.sum_variance                   AS textures_sum_variance,           
  textures.variance                       AS textures_variance  
FROM plates                                         
INNER JOIN wells     ON wells.plate_id       = plates.id
INNER JOIN images    ON images.well_id       = wells.id
INNER JOIN objects   ON objects.image_id     = images.id
INNER JOIN matches   ON matches.object_id    = objects.id
INNER JOIN patterns  ON matches.pattern_id   = patterns.id
INNER JOIN channels  ON textures.channel_id  = channels.id
INNER JOIN textures  ON textures.match_id    = matches.id

    