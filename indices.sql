CREATE INDEX table_image_idx ON Image(TableNumber, ImageNumber);

CREATE INDEX table_image_object_cells_idx ON Cells(TableNumber, ImageNumber, ObjectNumber);
CREATE INDEX table_image_object_cytoplasm_idx ON Cytoplasm(TableNumber, ImageNumber, ObjectNumber);
CREATE INDEX table_image_object_nuclei_idx ON Nuclei(TableNumber, ImageNumber, ObjectNumber);

CREATE INDEX plate_well_image_idx ON Image(Image_Metadata_Plate, Image_Metadata_Well);

