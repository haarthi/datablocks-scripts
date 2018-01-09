require 'aws-sdk'

class AWS

  ##### AWS S3 BUCKET UPLOAD #######
  def upload_to_s3(s3_path, file_name)
    bucket_name = 'looker-datablocks'

    s3 = Aws::S3::Resource.new(region: 'us-east-1')

    bucket = s3.bucket(bucket_name)
    
    begin
      puts "Uploading file #{file_name} to S3 bucket #{s3_path} + #{bucket}."
      s3.bucket(bucket_name).object(s3_path).upload_file(file_name)

      rescue Aws::S3::Errors::ServiceError
        puts "#{file_name} for datablocks failed to load into S3"
        # rescues all errors returned by Amazon Simple Storage Service
        sns = Aws::SNS::Resource.new(region: 'us-east-1')
        topic = sns.topic('arn:aws:sns:us-east-1:734261250617:datablock-etl-notifications')

        topic.publish({
                 subject: '#{s3_path} for datablocks failed to load into S3',
                 message: 'The script and the logs are on partneretl running on a cronjob.'
        })
    end
  end

end 