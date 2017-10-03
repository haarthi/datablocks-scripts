#!/usr/bin/env ruby
require "google/cloud/bigquery"

class BigQuery
  # constructor
  def initialize
    @bigquery = Google::Cloud::Bigquery.new(
      project: "looker-datablocks",
      keyfile: "../lookerpublickey.json"
      )
  end

  def upload_to_bq(dataset, table, data)
    # puts "upload_to_gcs #{dataset}"

    begin

      dataset = @bigquery.dataset dataset
      table = dataset.table table

      table.insert data

      puts "Inserting Data in BQ for #{table}"


    rescue => e
      puts "Error Inserting Data in BQ"
      puts e.message
      sns = Aws::SNS::Resource.new(region: 'us-east-1')
      topic = sns.topic('arn:aws:sns:us-east-1:734261250617:datablock-etl-notifications')

      puts "Data for #{table} failed to load into BQ"    

      topic.publish({
                 subject: 'Data for #{table} failed to load into BQ',
                 message: 'The script and the logs are on partneretl running on a cronjob.'
      })
    end
  end
end

