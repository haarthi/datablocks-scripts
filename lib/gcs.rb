require "google/cloud/bigquery"
require "google/cloud/storage"


class GCS
  # @gcs
  # constructor
  def initialize
    @storage = Google::Cloud::Storage.new(
      project: "looker-datablocks",
      keyfile: "../publickey.json"
      )
    @bucket = @storage.bucket "looker-datablocks"
  end

  def upload_to_gcs(gcs_path, file_name)
    puts "upload_to_gcs #{file_name}"

    begin
      @bucket.create_file file_name, gcs_path
      puts "Uploading file #{file_name} to GCS bucket #{@bucket} #{gcs_path}."

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
end