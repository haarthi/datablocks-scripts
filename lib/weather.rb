#!/usr/bin/env ruby

require 'json'
require 'csv'
require 'json_converter'

require 'logger'
require 'date'

require "google/cloud/bigquery"
require "google/cloud/storage"
require 'rubygems'
require 'aws-sdk'




##### AWS S3 BUCKET UPLOAD #######
def upload_to_s3(file_name)
  puts "upload_to_s3 #{file_name}" 
  bucket_name = 'looker-datablocks'

  puts "before Aws::S3::Resource.new"
  s3 = Aws::S3::Resource.new(region: 'us-east-1')

  puts "before s3_path"
  s3_path = "gsod/gsod/"

  puts "before s3.bucket"
  bucket = s3.bucket(bucket_name)
  
  puts "before begin"

  begin
    puts "Uploading file #{file_name} to S3 bucket #{bucket}."
    s3.bucket(bucket_name).object(s3_path + file_name).upload_file(file_name)

    rescue Aws::S3::Errors::ServiceError
      puts "Weather ETL for datablocks failed to load into S3"
      # rescues all errors returned by Amazon Simple Storage Service
      sns = Aws::SNS::Resource.new(region: 'us-east-1')
      topic = sns.topic('arn:aws:sns:us-east-1:734261250617:datablock-etl-notifications')

      topic.publish({
               subject: 'Weather ETL for datablocks failed to load into S3',
               message: 'The script and the logs are on partneretl running on a cronjob.'
})
  end
end


##### Bigquery GCS Upload #######

def upload_to_gcs(file_name)
  puts "upload_to_gcs #{file_name}"
  @storage = Google::Cloud::Storage.new(
    project: "bigquery-public-data",
    keyfile: "../publickey.json"
  )
  bucket = @storage.bucket "looker-datablocks"
  gcs_path = "gsod/2017/#{file_name}"

  begin
    bucket.create_file file_name, gcs_path
    puts "Uploading file #{file_name} to GCS bucket #{bucket} #{gcs_path}."

  rescue => e
    puts "Error Creating File in Bucket"
    puts e.message
    sns = Aws::SNS::Resource.new(region: 'us-east-1')
    topic = sns.topic('arn:aws:sns:us-east-1:734261250617:datablock-etl-notifications')

    puts "Weather ETL for datablocks failed to load into GS"    

    topic.publish({
               subject: 'Weather ETL for datablocks failed to load into GS',
               message: 'The script and the logs are on partneretl running on a cronjob.'
})

  end
end

puts "starting"

# Get Previous date.
current = DateTime.now.prev_day

filename = "gsod" + (current.strftime ("%Y-%m-%d"))

puts "new bigquery"
@bigquery = Google::Cloud::Bigquery.new(
  # project: "haarthi-156616",
  project: "bigquery-public-data",
  keyfile: "../publickey.json"
)
@dataset = @bigquery.dataset "noaa_gsod"

sql = "
  SELECT * 
  FROM `bigquery-public-data.noaa_gsod.gsod2017` 
  where year = @year and mo = @month and da = @day
"
puts "query_job"
query_job = @dataset.query_job sql, params: { year: current.year.to_s, month: current.strftime("%m").to_s, day: current.strftime("%d").to_s }

puts "query_job.wait_until_done"
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

upload_to_s3(filename)
upload_to_gcs(filename)

puts "file.delete #{filename}"
File.delete("#{filename}")
