#!/usr/bin/env ruby
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
  end
end


upload_to_s3("blah")
