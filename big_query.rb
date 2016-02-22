require 'elasticsearch/api'
require 'elasticsearch'
# require 'logger'

def format_log log
  %/#{log["@timestamp"]},#{log["cmap_requestForwardedFor"]},#{log["cmap_requestUri"]},#{log["message"]}/
end

def write_to_file(client, index, result)
  File.open("./logs/#{index}.txt", 'a') do |file|

    while result = client.scroll(scroll: '4m', scroll_id: result['_scroll_id']) and not result['hits']['hits'].empty? do
      p  "writing to ./logs/#{index}.txt"
      lines = 0
      result['hits']['hits'].each do |log|
        file.write log
        file.write "\n"
        lines = lines + 1
      end
      p  "written #{lines} to file"
    end
  end
end

def log_transaction(index, client, start_time, end_time)
  params = {
      index: index,
      scroll: '4m',
      search_type: 'scan',
      body: {
          size: 10000,
          query: {
              bool: {
                  must: [
                      {
                          match: { cmap_environment: 'production'}
                      },
                      {
                          match: {cmap_region: 'au'}
                      },
                      {
                          range: {
                              '@timestamp': {
                                  from: start_time,
                                  to: end_time
                              }
                          }
                      }

                  ]
              }
          }
          }
      }


  result = client.search params
  scroll_size = result['hits']['total']
  p  "total number of rows returned : #{scroll_size}"
  write_to_file(client, index, result)
  return scroll_size
end

def setup_query(epoch_end_time, indexes)
  indexes.each do |index|
    client = Elasticsearch::Client.new host: HOST_NAME,
                                       retry_on_failure: true, reload_on_failure: true,
                                       request_timeout: 5*60
    p "checking logstash index : #{index} with epoch start time: #{@start_time} and epoch end time #{@end_time}"
    hits = 1
    while @start_time < epoch_end_time and hits > 0

      hits = log_transaction(index, client, @start_time, @end_time)
      if hits > 0
        @start_time = @end_time + 1
        @end_time = @start_time + @epoch_increment
        p  "updated start time #{@start_time}, and updated end time #{@end_time}"
      end
    end
  end
end

def logstash_query
  indexes = %w(logstash-2015.12.15 logstash-2015.12.16 logstash-2015.12.17
  logstash-2015.12.18 logstash-2015.12.19 logstash-2015.12.20
  logstash-2015.12.21)

  # epoch_start_time = 1449493200000
  epoch_start_time = 1450218600000
  epoch_end_time = 1450742400000
  @start_time = epoch_start_time
  # @epoch_increment = 3599999
  @epoch_increment = 1799999
  @end_time = @start_time + @epoch_increment

  setup_query(epoch_end_time, indexes)
end

# $LOG = Logger.new('logstash_query.log')
logstash_query