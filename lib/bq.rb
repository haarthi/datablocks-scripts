require "google/cloud/bigquery"
require "google/cloud/storage"

class BigQuery
  # constructor
  def initialize
    @bigquery = Google::Cloud::Bigquery.new(
      project: "looker-datablocks",
      keyfile: "../publickey.json"
      )
  end

  def upload_to_bq(dataset, table, file_name)
    # puts "upload_to_gcs #{file_name}"

    # dataset = @bigquery.dataset "exchange_rates"
    # puts dataset
    # table = dataset.table "forex_real"
    # puts table


    # begin
      
    #   file = File.open "#{file_name}"
    #   load_job = @table.load file

    #   puts "Uploading file #{file_name} to GCS bucket #{@bucket} #{gcs_path}."

    # rescue => e
    #   puts "Error Creating File in Bucket"
    #   puts e.message
    #   # sns = Aws::SNS::Resource.new(region: 'us-east-1')
    #   # topic = sns.topic('arn:aws:sns:us-east-1:734261250617:datablock-etl-notifications')

    #   # puts "Weather ETL for datablocks failed to load into GS"    

    #   # topic.publish({
    #   #            subject: 'Weather ETL for datablocks failed to load into GS',
    #   #            message: 'The script and the logs are on partneretl running on a cronjob.'
    #   # })
    # end
  end
end