#!/usr/bin/env ruby

# This script converts Claire's Excel files into a set of CSV files that serve
# as the test data for the python_etl implementation.
#
# To run the script, navigate to the hand_converted folder and run
# ruby csvify.rb
#
# This script assumes you're running on a POSIX compatible OS and have the
# xlsx2csv command available, which can be installed using Python's pip
require 'pathname'

def csvify(excel_file, dest, names)
  csv_dir = Pathname.new('../python_etl/test_data')
  csv_dir += dest
  csv_dir.mkpath unless csv_dir.exist?
  names.each.with_index do |sheet_name, index|
    csv_name = sheet_name + '.csv'
    puts "Making #{csv_name}"
    system(%{xlsx2csv --ignoreempty --sheet #{index + 1} #{excel_file} #{csv_dir + csv_name}})
  end
end

input_names = %w(
  beneficiary_summary
  prescription_drug_events
  outpatient_claims
  inpatient_claims
  carrier_claims
)

csvify('Input_Test_Files.xlsx', 'input', input_names)

output_names = %w(
  location
  person
  observation_period
  care_site
  visit_occurrence
  provider
  condition_occurrence
  death
  drug_exposure
  device_exposure
  procedure_occurrence
  measurement
  observation
  note
  specimen
  fact_relationship
  procedure_cost
  visit_cost
  drug_cost
  payer_plan_period
  device_cost
)

csvify('Output_Test_Files.xlsx', 'output', output_names)
