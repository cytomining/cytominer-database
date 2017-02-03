DROP VIEW IF EXISTS "view_correlations";
CREATE VIEW "view_correlations" AS
SELECT
  plates.description          AS g_plate,
  wells.description           AS g_well,
  images.description          AS g_image,
  objects.description         AS g_object,
  patterns.description        AS g_pattern,
  channels1.description       AS g_channel1,
  channels2.description       AS g_channel2,
  quality.count_cell_clump    AS q_cell_clump,
  quality.count_debris        AS q_debris,
  quality.count_low_intensity AS q_low_intensity,
  correlations.coefficient    AS m_correlations_coefficient
FROM plates
INNER JOIN wells                 ON wells.plate_id              = plates.id
INNER JOIN images                ON images.well_id              = wells.id
INNER JOIN quality               ON quality.image_id            = images.id
INNER JOIN objects               ON objects.image_id            = images.id
INNER JOIN matches               ON matches.object_id           = objects.id
INNER JOIN patterns              ON matches.pattern_id          = patterns.id
INNER JOIN correlations          ON correlations.match_id       = matches.id
INNER JOIN channels as channels1 ON correlations.dependent_id   = channels1.id
INNER JOIN channels as channels2 ON correlations.independent_id = channels2.id
;

DROP VIEW IF EXISTS "view_edges";
CREATE VIEW "view_edges" AS
 SELECT
  plates.description          AS g_plate,
  wells.description           AS g_well,
  images.description          AS g_image,
  objects.description         AS g_object,
  patterns.description        AS g_pattern,
  channels.description        AS g_channel,
  quality.count_cell_clump    AS q_cell_clump,
  quality.count_debris        AS q_debris,
  quality.count_low_intensity AS q_low_intensity,
	edges.integrated            AS m_edge_integrated,
	edges.maximum               AS m_edge_maximum,
	edges.mean                  AS m_edge_mean,
	edges.minimum               AS m_edge_minimum,
	edges.standard_deviation    AS m_edge_standard_deviation
FROM plates
INNER JOIN wells    ON wells.plate_id     = plates.id
INNER JOIN images   ON images.well_id     = wells.id
INNER JOIN quality  ON quality.image_id   = images.id
INNER JOIN objects  ON objects.image_id   = images.id
INNER JOIN matches  ON matches.object_id  = objects.id
INNER JOIN patterns ON matches.pattern_id = patterns.id
INNER JOIN edges    ON edges.match_id     = matches.id
INNER JOIN channels ON edges.channel_id   = channels.id
;

DROP VIEW IF EXISTS "view_intensities";
CREATE VIEW "view_intensities" AS
SELECT
	plates.description                    AS g_plate,
	wells.description                     AS g_well,
	images.description                    AS g_image,
	objects.description                   AS g_object,
	patterns.description                  AS g_pattern,
	channels.description                  AS g_channel,
	quality.count_cell_clump              AS q_cell_clump,
	quality.count_debris                  AS q_debris,
	quality.count_low_intensity           AS q_low_intensity,
  intensities.first_quartile            AS m_intensities_first_quartile,
  intensities.integrated                AS m_intensities_integrated,
  intensities.maximum                   AS m_intensities_maximum,
  intensities.mean                      AS m_intensities_mean,
  intensities.median                    AS m_intensities_median,
  intensities.median_absolute_deviation AS m_intensities_median_absolute_deviation,
  intensities.minimum                   AS m_intensities_minimum,
  intensities.standard_deviation        AS m_intensities_standard_deviation,
  intensities.third_quartile            AS m_intensities_third_quartile
FROM plates
INNER JOIN wells        ON wells.plate_id         = plates.id
INNER JOIN images       ON images.well_id         = wells.id
INNER JOIN quality      ON quality.image_id       = images.id
INNER JOIN objects      ON objects.image_id       = images.id
INNER JOIN matches      ON matches.object_id      = objects.id
INNER JOIN patterns     ON matches.pattern_id     = patterns.id
INNER JOIN intensities  ON intensities.match_id   = matches.id
INNER JOIN channels     ON intensities.channel_id = channels.id
;

DROP VIEW IF EXISTS "view_locations";
CREATE VIEW "view_locations" AS
SELECT
  plates.description               AS g_plate,
  wells.description                AS g_well,
  images.description               AS g_image,
  objects.description              AS g_object,
  patterns.description             AS g_pattern,
  channels.description             AS g_channel,
  quality.count_cell_clump         AS q_cell_clump,
  quality.count_debris             AS q_debris,
  quality.count_low_intensity      AS q_low_intensity,
  center_mass_intensity.abscissa   AS m_locations_center_mass_intensity_x,
  center_mass_intensity.ordinate   AS m_locations_center_mass_intensity_y,
  max_intensity.abscissa           AS m_locations_max_intensity_x,
  max_intensity.ordinate           AS m_locations_max_intensity_y
FROM plates
INNER JOIN wells                                ON wells.plate_id                     = plates.id
INNER JOIN images                               ON images.well_id                     = wells.id
INNER JOIN quality                              ON quality.image_id                   = images.id
INNER JOIN objects                              ON objects.image_id                   = images.id
INNER JOIN matches                              ON matches.object_id                  = objects.id
INNER JOIN patterns                             ON matches.pattern_id                 = patterns.id
INNER JOIN locations                            ON locations.match_id                 = matches.id
INNER JOIN channels                             ON locations.channel_id               = channels.id
INNER JOIN coordinates as center_mass_intensity ON locations.center_mass_intensity_id = center_mass_intensity.id
INNER JOIN coordinates as max_intensity         ON locations.max_intensity_id         = max_intensity.id
;

DROP VIEW IF EXISTS "view_moments";
CREATE VIEW "view_moments" AS
SELECT
  plates.description          AS g_plate,
  wells.description           AS g_well,
  images.description          AS g_image,
  objects.description         AS g_object,
  patterns.description        AS g_pattern,
  quality.count_cell_clump    AS q_cell_clump,
  quality.count_debris        AS q_debris,
  quality.count_low_intensity AS q_low_intensity,
	moments.a                   AS p_moments_a,
	moments.b                   AS p_moments_b,
	moments.score               AS m_moments_score
FROM plates
INNER JOIN wells    ON wells.plate_id     = plates.id
INNER JOIN images   ON images.well_id     = wells.id
INNER JOIN quality  ON quality.image_id   = images.id
INNER JOIN objects  ON objects.image_id   = images.id
INNER JOIN matches  ON matches.object_id  = objects.id
INNER JOIN patterns ON matches.pattern_id = patterns.id
INNER JOIN shapes   ON matches.shape_id   = shapes.id
INNER JOIN moments  ON moments.shape_id   = shapes.id
;

DROP VIEW IF EXISTS "view_neighborhoods";
CREATE VIEW "view_neighborhoods" AS
SELECT
  plates.description                         AS g_plate,
  wells.description                          AS g_well,
  images.description                         AS g_image,
  objects.description                        AS g_object,
  quality.count_cell_clump                   AS q_cell_clump,
  quality.count_debris                       AS q_debris,
  quality.count_low_intensity                AS q_low_intensity,
  neighborhoods.angle_between_neighbors      AS m_neighborhoods_angle_between_neighbors,
  neighborhoods.first_closest_distance       AS m_neighborhoods_first_closest_distance,
  neighborhoods.first_closest_object_number  AS m_neighborhoods_first_closest_object_number,
  neighborhoods.number_of_neighbors          AS m_neighborhoods_number_of_neighbors,
  neighborhoods.percent_touching             AS m_neighborhoods_percent_touching,
  neighborhoods.scale                        AS p_neighborhoods_scale,
  neighborhoods.second_closest_distance      AS m_neighborhoods_second_closest_distance,
  neighborhoods.second_closest_object_number AS m_neighborhoods_second_closest_object_number
FROM plates
INNER JOIN wells         ON wells.plate_id          = plates.id
INNER JOIN images        ON images.well_id          = wells.id
INNER JOIN quality       ON quality.image_id        = images.id
INNER JOIN objects       ON objects.image_id        = images.id
INNER JOIN neighborhoods ON neighborhoods.object_id = objects.id
;

DROP VIEW IF EXISTS "view_radial_distributions";
CREATE VIEW "view_radial_distributions" AS
SELECT
  plates.description               AS g_plate,
  wells.description                AS g_well,
  images.description               AS g_image,
  objects.description              AS g_object,
  patterns.description             AS g_pattern,
  channels.description             AS g_channel,
  quality.count_cell_clump         AS q_cell_clump,
  quality.count_debris             AS q_debris,
  quality.count_low_intensity      AS q_low_intensity,
  radial_distributions.bins        AS p_radial_distributions_bins,
  radial_distributions.frac_at_d   AS m_radial_distributions_frac_at_d,
  radial_distributions.mean_frac   AS m_radial_distributions_mean_frac,
  radial_distributions.radial_cv   AS m_radial_distributions_radial_cv
FROM plates
INNER JOIN wells                ON wells.plate_id                  = plates.id
INNER JOIN images               ON images.well_id                  = wells.id
INNER JOIN quality              ON quality.image_id                = images.id
INNER JOIN objects              ON objects.image_id                = images.id
INNER JOIN matches              ON matches.object_id               = objects.id
INNER JOIN patterns             ON matches.pattern_id              = patterns.id
INNER JOIN radial_distributions ON radial_distributions.match_id   = matches.id
INNER JOIN channels             ON radial_distributions.channel_id = channels.id
;

DROP VIEW IF EXISTS "view_shapes";
CREATE VIEW "view_shapes" AS
SELECT
  plates.description          AS g_plate,
  wells.description           AS g_well,
  images.description          AS g_image,
  objects.description         AS g_object,
  patterns.description        AS g_pattern,
  quality.count_cell_clump    AS q_cell_clump,
  quality.count_debris        AS q_debris,
  quality.count_low_intensity AS q_low_intensity,
  center.abscissa             AS m_shapes_center_x,
  center.ordinate             AS m_shapes_center_y,
	shapes.area                 AS m_shapes_area,
  shapes.compactness          AS m_shapes_compactness,   
  shapes.eccentricity         AS m_shapes_eccentricity,      
  shapes.euler_number         AS m_shapes_euler_number,     
  shapes.extent               AS m_shapes_extent,     
  shapes.form_factor          AS m_shapes_form_factor,
  shapes.major_axis_length    AS m_shapes_major_axis_length, 
  shapes.max_feret_diameter   AS m_shapes_max_feret_diameter,
  shapes.maximum_radius       AS m_shapes_maximum_radius,
  shapes.mean_radius          AS m_shapes_mean_radius,   
  shapes.median_radius        AS m_shapes_median_radius,     
  shapes.min_feret_diameter   AS m_shapes_min_feret_diameter,
  shapes.minor_axis_length    AS m_shapes_minor_axis_length,
  shapes.orientation          AS m_shapes_orientation,
  shapes.perimeter            AS m_shapes_perimeter,      
  shapes.solidity             AS m_shapes_solidity        
FROM plates
INNER JOIN wells                 ON wells.plate_id     = plates.id
INNER JOIN images                ON images.well_id     = wells.id
INNER JOIN quality               ON quality.image_id   = images.id
INNER JOIN objects               ON objects.image_id   = images.id
INNER JOIN matches               ON matches.object_id  = objects.id
INNER JOIN patterns              ON matches.pattern_id = patterns.id
INNER JOIN shapes                ON matches.shape_id   = shapes.id
INNER JOIN coordinates as center ON shapes.center_id   = center.id
;

DROP VIEW IF EXISTS "view_textures";
CREATE VIEW "view_textures" AS
SELECT
  plates.description                 AS g_plate,
  wells.description                  AS g_well,
  images.description                 AS g_image,
  objects.description                AS g_object,
  patterns.description               AS g_pattern,
  channels.description               AS g_channel,
  quality.count_cell_clump           AS q_cell_clump,
  quality.count_debris               AS q_debris,
  quality.count_low_intensity        AS q_low_intensity,
  textures.scale                     AS p_textures_scale,
  textures.angular_second_moment     AS m_textures_angular_second_moment,           
  textures.contrast                  AS m_textures_contrast,
  textures.correlation               AS m_textures_correlation,           
  textures.difference_entropy        AS m_textures_difference_entropy,
  textures.difference_variance       AS m_textures_difference_variance,           
  textures.entropy                   AS m_textures_entropy,
  textures.gabor                     AS m_textures_gabor,        
  textures.info_meas_1               AS m_textures_info_meas_1,
  textures.info_meas_2               AS m_textures_info_meas_2,           
  textures.inverse_difference_moment AS m_textures_inverse_difference_moment,
  textures.sum_average               AS m_textures_sum_average,
  textures.sum_entropy               AS m_textures_sum_entropy,          
  textures.sum_variance              AS m_textures_sum_variance,           
  textures.variance                  AS m_textures_variance  
FROM plates                                         
INNER JOIN wells    ON wells.plate_id      = plates.id
INNER JOIN images   ON images.well_id      = wells.id
INNER JOIN quality  ON quality.image_id    = images.id
INNER JOIN objects  ON objects.image_id    = images.id
INNER JOIN matches  ON matches.object_id   = objects.id
INNER JOIN patterns ON matches.pattern_id  = patterns.id
INNER JOIN textures ON textures.match_id   = matches.id
INNER JOIN channels ON textures.channel_id = channels.id
;