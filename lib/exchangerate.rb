#!/usr/bin/env ruby

require 'json'
require 'csv'
require 'json_converter'

require 'logger'
require 'date'

require 'rubygems'
require 'rest-client'

require_relative 'aws'
require_relative 'gcs'
require_relative 'bq'


@config = YAML.load_file('../config/application.yml')

current = DateTime.now.prev_day.to_date

if (ARGV.empty?)
	to_date = current
	from_date = current
else
	((Date.parse(ARGV[0]).is_a?(Date))) ? from_date = Date.parse(ARGV[0]) : from_date = current
	(ARGV.length == 2 && (Date.parse(ARGV[1]).is_a?(Date))) ? to_date = Date.parse(ARGV[1]) : to_date = from_date
end

puts "From " + from_date.to_s + " to " + to_date.to_s

from_date.to_s.upto(to_date.to_s) do |date|
	puts "Exchage Rate for: " + date
	filename = "exchangerate-" + (date.to_s)

	@url = @config['EXCHANGEURL'].to_s + date.to_s + "?access_key=" + @config['EXCHANGEURL_PASSWORD'].to_s
	puts @url

	begin
		response = RestClient.get(@url)
	rescue => e
		e.response
		return
	end

	row = JSON.parse(response.body)

	field = {
      "exchange_date" => row["date"],
      "base_currency" => row["base"],
	}

	rates = row["rates"]

	rates.each do |key, value|
		field["#{key}"] = "#{value}"
	end

	puts field.to_json

	begin
		json_converter= JsonConverter.new
		csv = json_converter.generate_csv field.to_json
		File.open("#{filename}", 'w') { |fo| fo.puts csv }
	rescue => e
		puts "error:" + e.response 
		return
	end 

  	# AWS.new.upload_to_s3("exchangerate/forex_real/#{filename}", filename)
	GCS.new.upload_to_gcs("exchangerate/forex_real/#{filename}", filename)
	
	BigQuery.new.upload_to_bq("exchangerate", "forex_real", field)


	File.delete("#{filename}")

end



