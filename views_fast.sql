DROP TABLE IF EXISTS 'match_lookup';
CREATE TABLE match_lookup AS
SELECT
  plates.description          AS plate_description, 
  wells.description           AS well_description,  
  images.description          AS image_description,
  objects.description         AS object_description, 
  matches.id                  AS match_id, 
  objects.id                  AS object_id, 
  patterns.description        AS pattern_description,
  quality.count_cell_clump    AS count_cell_clump,
  quality.count_debris        AS count_debris,
  quality.count_low_intensity AS count_low_intensity
FROM plates
INNER JOIN wells    ON wells.plate_id     = plates.id
INNER JOIN images   ON images.well_id     = wells.id
INNER JOIN quality  ON quality.image_id   = images.id
INNER JOIN objects  ON objects.image_id   = images.id
INNER JOIN matches  ON matches.object_id  = objects.id
INNER JOIN patterns ON matches.pattern_id = patterns.id
;
CREATE INDEX ix_match_lookup_plate_description ON match_lookup(plate_description);
CREATE INDEX ix_match_lookup_well_description ON match_lookup(well_description);
CREATE INDEX ix_match_lookup_image_description ON match_lookup(image_description);
CREATE INDEX ix_match_lookup_object_description ON match_lookup(object_description);
CREATE INDEX ix_match_lookup_match_id ON match_lookup(match_id);
CREATE INDEX ix_match_lookup_object_id ON match_lookup(object_id);
CREATE INDEX ix_match_lookup_pattern_description ON match_lookup(pattern_description);

DROP VIEW IF EXISTS 'view_fast_intensities';
CREATE VIEW 'view_fast_intensities' AS
SELECT
  match_lookup.plate_description        AS g_plate,
  match_lookup.well_description         AS g_well,
  match_lookup.image_description        AS g_image,
  match_lookup.object_description       AS g_object,
  match_lookup.pattern_description      AS g_pattern,
  match_lookup.count_cell_clump         AS q_cell_clump,
  match_lookup.count_debris             AS q_debris,
  match_lookup.count_low_intensity      AS q_low_intensity,
  channels.description                  AS g_channel,
  intensities.first_quartile            AS m_intensities_first_quartile,
  intensities.integrated                AS m_intensities_integrated,
  intensities.maximum                   AS m_intensities_maximum,
  intensities.mean                      AS m_intensities_mean,
  intensities.median                    AS m_intensities_median,
  intensities.median_absolute_deviation AS m_intensities_median_absolute_deviation,
  intensities.minimum                   AS m_intensities_minimum,
  intensities.standard_deviation        AS m_intensities_standard_deviation,
  intensities.third_quartile            AS m_intensities_third_quartile
FROM match_lookup
INNER JOIN channels     ON intensities.channel_id = channels.id
INNER JOIN intensities  ON intensities.match_id   = match_lookup.match_id
;