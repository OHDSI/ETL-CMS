#!/usr/bin/env ruby

# This script converts Claire's Excel files into a set of CSV files that serve
# as the test data for the python_etl implementation.
#
# To run the script, navigate to the hand_converted folder and run
# ruby csvify.rb
#
# This script assumes you're running on a POSIX compatible OS and have the
# xlsx2csv 0.7.2+ command available, which can be installed using Python's pip
require 'pathname'

def csvify(excel_file, dest, names)
  csv_dir = Pathname.new('../python_etl/test_data')
  csv_dir += dest
  csv_dir.mkpath unless csv_dir.exist?
  puts "Converting #{excel_file}"
  system(%{xlsx2csv --dateformat '%Y-%m-%d' --ignoreempty --all #{excel_file} #{csv_dir}})
end
csvify('Input_Test_Files.xlsx', 'input')

csvify('Output_Test_Files.xlsx', 'output')
