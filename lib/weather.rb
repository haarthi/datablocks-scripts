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
  bucket_name = 'looker-datablocks'

  s3 = Aws::S3::Resource.new(region: 'us-east-1')

  s3_path = "gsod/gsod/"

  bucket = s3.bucket(bucket_name)
  
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


# Get Previous date.
current = DateTime.now.prev_day.to_date


if ARGV.empty?
  from_date = current
else 
  (!ARGV[0].empty? && (Date.parse(ARGV[0]).is_a?(Date))) ? from_date = Date.parse(ARGV[0]) : from_date = current
end


@bigquery = Google::Cloud::Bigquery.new(
  # project: "haarthi-156616",
  project: "bigquery-public-data",
  keyfile: "../publickey.json"
)
@dataset = @bigquery.dataset "noaa_gsod"


from_date.to_s.upto(current.strftime("%Y-%m-%d")) do |date|
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

  upload_to_s3(filename)
  upload_to_gcs(filename)

  File.delete("#{filename}")

end
