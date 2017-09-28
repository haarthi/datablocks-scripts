#!/usr/bin/env ruby

require 'json'
require 'csv'
require 'json_converter'

require 'logger'
require 'date'
require 'rubygems'

require_relative 'aws'
require_relative 'gcs'

# Get Previous date.
current = DateTime.now.prev_day.to_date

if (ARGV.empty?)
  to_date = current
  from_date = current
else
  ((Date.parse(ARGV[0]).is_a?(Date))) ? from_date = Date.parse(ARGV[0]) : from_date = current
  (ARGV.length == 2 && (Date.parse(ARGV[1]).is_a?(Date))) ? to_date = Date.parse(ARGV[1]) : to_date = from_date
end

@bigquery = Google::Cloud::Bigquery.new(
  # project: "haarthi-156616",
  project: "bigquery-public-data",
  keyfile: "../publickey.json"
)
@dataset = @bigquery.dataset "noaa_gsod"


from_date.to_s.upto(to_date.to_s) do |date|
  date = Date.parse(date).to_date
  year = date.year.to_s

  filename = "gsod" + (date.strftime ("%Y-%m-%d"))

  sql = "
    SELECT * 
    FROM `bigquery-public-data.noaa_gsod.gsod2017` 
    where year = @year and mo = @month and da = @day
  "

  query_job = @dataset.query_job sql, params: { year: date.year.to_s, month: date.strftime("%m").to_s, day: date.strftime("%d").to_s }


  puts "Running Query for: " + date.year.to_s + date.strftime("%m").to_s + date.strftime("%d").to_s

  query_job.wait_until_done!


  begin
    json_converter= JsonConverter.new
    csv = json_converter.generate_csv query_job.query_results.to_json
    File.open("#{filename}", 'w') { |fo| fo.puts csv }
  rescue => e
      puts "error:" + e.response 
      # @logger.error(e.response)
      return
  end 

  AWS.new.upload_to_s3("gsod/gsod/", filename)
  GCS.new.upload_to_gcs("gsod/#{year}/#{filename}", filename)

  File.delete("#{filename}")

end
