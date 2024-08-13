#!/bin/bash

# List of table names to extract
tables=('tblPlots'
      'tblLines'
      'tblLPIDetail'
      'tblLPIHeader'
      'tblGapDetail'
      'tblGapHeader'
      'tblQualHeader'
      'tblQualDetail'
      'tblSoilStabHeader'
      'tblSoilStabDetail'
      'tblSoilPitHorizons'
      'tblSoilPits'
      'tblSpecRichHeader'
      'tblSpecRichDetail'
      'tblPlantProdHeader'
      'tblPlantProdDetail'
      'tblPlotNotes'
      'tblPlantDenHeader'
      'tblPlantDenDetail'
      'tblSpecies'
      'tblSpeciesGeneric'
      'tblSites'
      'tblBSNE_Box'
      'tblBSNE_BoxCollection'
      'tblBSNE_Stack'
      'tblBSNE_TrapCollection'
      'tblCompactDetail'
      'tblCompactHeader'
      'tblDKDetail'
      'tblDKHeader'
      'tblDryWtCompYield'
      'tblDryWtDetail'
      'tblDryWtHeader'
      'tblESDDominantPerennialHeights'
      'tblESDRockFragments'
      'tblESDWaypoints'
      'tblInfiltrationDetail'
      'tblInfiltrationHeader'
      'tblLICDetail'
      'tblLICHeader'
      'tblLICSpecies'
      'tblNestedFreqDetail'
      'tblNestedFreqHeader'
      'tblNestedFreqSpeciesDetail'
      'tblNestedFreqSpeciesSummary'
      'tblOcularCovDetail'
      'tblOcularCovHeader'
      'tblPlantDenQuads'
      'tblPlantDenSpecies'
      'tblPlantLenDetail'
      'tblPlantLenHeader'
      'tblPlotHistory'
      'tblPTFrameDetail'
      'tblPTFrameHeader'
      'tblQualDetail'
      'tblQualHeader'
      'tblSpeciesGrowthHabits'
      'tblSpeciesRichAbundance'
      'tblTreeDenDetail'
      'tblTreeDenHeader')

# Directory to store the extracted CSV files
output_dir="/extracted"
mkdir -p "$output_dir"

# Loop over each .mdb file in the /dimas directory
for mdb_file in /dimas/*.mdb; do
    # Extract the base filename without the path and extension, and replace spaces with underscores
    base_filename=$(basename "$mdb_file" .mdb)
    base_filename="${base_filename// /}"
    base_filename="${base_filename//_/\-}"

    # Loop over each table in the list
    for table in "${tables[@]}"; do
        cleaned_table_name=$(echo "$table" | tr -d '_') # Remove spaces in tablenames
        # Construct the output CSV filename
        csv_filename="${output_dir}/${base_filename}_${cleaned_table_name}.csv"

        # Use mdb-export to extract the table into the CSV file
        mdb-export "$mdb_file" "$table" > "$csv_filename"

        # Check if the extraction was successful
        if [ $? -eq 0 ]; then
            echo "Extracted $table from $mdb_file to $csv_filename"
        else
            echo "Failed to extract $table from $mdb_file"
        fi
    done
done
