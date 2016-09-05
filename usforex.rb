#!/usr/bin/env ruby

$LOAD_PATH.unshift File.expand_path($0)

# This program processes the data copied from this page
# http://www.usforex.com/forex-tools/historical-rate-tools/monthly-average-rates
# comparing GBP against USD, and placed into a file called usforex.csv.
#
# The index mapping is described in usforex.json. Use the following commands to manage the mapping
#
# EXISTS: curl -u <user>:<pass> -XHEAD -i 'https://aws-us-east-1-portal9.dblayer.com:11062/usforex?pretty'
#    ADD: curl -u <user>:<pass> -XPUT 'https://aws-us-east-1-portal9.dblayer.com:11062/usforex?pretty' -d @usforex.json
# DELETE: curl -u <user>:<pass> -XDELETE 'https://aws-us-east-1-portal9.dblayer.com:11062/usforex?pretty'
#
# This program generates an output file called GBP-USD.json from the input file GBP-USD with the 
# command 'usforex.rb --from GBP --to USD'.
# The data can be added to the index with the following command
#
# curl -u <user>:<pass> -XPOST 'https://aws-us-east-1-portal9.dblayer.com:11062/usforex/currency_rate/_bulk' --data-binary @GBP-USD.json
#
# The 'from' and 'to' logic is based on multiplication. As an example, if we say that the exchange
# rate between GBP and USD is 1.5 then that means that to convert 'from' GBP 'to' USD then we must
# multiply each pound by 1.5 to obtain the equivalent number of dollars.

load 'mylogger.rb'

require 'optparse'
require 'ostruct'
require 'date'
require 'pp'

class USFOptions
  def self.parse(args)
    opt = OpenStruct.new
    opt.all = false
    opt.action = 'create'
    opt.from = nil
    opt.to = nil
    opt.logfile = nil
    
    opt.rebuild_all = false
    opt.reindex = false
    opt.update_new = false
    opt.index = nil
    opt.alias = nil
    
    opt_parser = OptionParser.new do |o|
      o.on("-a", "--all") do |arg|
        opt.all = true
      end
      o.on("--action ACTION") do |arg|
        opt.action = arg
      end
      #o.on("-f", "--from FROM") do |arg|
      #  opt.from = arg
      #end
      #o.on("-t", "--to TO") do |arg|
      #  opt.to = arg
      #end
      o.on("-l", "--logfile LOGFILE") do |arg|
        opt.logfile = arg
      end
    end
    opt_parser.parse!
    opt
  end
end

class USForex
  
  attr_accessor :action
  
  include MyLogger
  
  @@currency = %w{ USD GBP EUR DKK SEK }
  
  def initialize(log_file=nil)
    @logfile = log_file
    @verbose = false
    @debug = false
    @files = []
    @action = 'create'
  end
  
  def get_forex_files
    Dir.new('.').each do |f|
      csv = f[/^([A-Z]{3}-[A-Z]{3}.csv)$/,1]
      @files.push(csv) if csv
    end
    self._log("Found files #{@files}", __LINE__, __method__, __FILE__)
    @files
  end
  
  def gen_bulk_file(bulk_name, cutoff_date)
    bulk = File.open(bulk_name, "w")
    self._log("Creating file for bulk update of elasticsearch", __LINE__, __method__, __FILE__)

    @files.each do |f|
      self._log("Processing file #{f}", __LINE__, __method__, __FILE__)
      line_count = 0
      from = f[/^([A-Z]{3})-/,1]
      to = f[/^[A-Z]{3}-([A-Z]{3}).csv$/,1]
      puts "from=#{from}, to=#{to}"
      File.open(f).each_line do |line|
        date = line[/^([0-9\-\/]+)/,1]
        exch = line[/,([0-9\.]+)/,1]
        if !date.nil?
          d = date.split('/')
          dd = Date.new(d[2].to_i, d[1].to_i, d[0].to_i)
          if dd.to_s >= cutoff_date
            line_count += 1
            id = dd.strftime("%Y%m")
            bulk.print "{ \"#{@action}\" : { \"_id\" : \"#{id}_#{from}-#{to}\" } }\n"
            bulk.print "{ \"month\" : \"#{dd}\", \"rate\" : \"#{exch}\", \"from\" : \"#{from}\", \"to\" : \"#{to}\" }\n"
            #puts line
          end
        end
      end
      self._log("  Processed #{line_count} lines from input", __LINE__, __method__, __FILE__)
    end
    
    bulk.print("\n")
    bulk.close
  end
  
end

if __FILE__ == $0
  
  opt = USFOptions.parse(ARGV)
  logf = "#{File.dirname($0)}/logs"
  if !opt.logile.nil?
    logf = "#{logf}/#{opt.logfile}"
  else
    logf = "#{logf}/#{File.basename($0, '.rb')}.log"
  end
  logfile = File.open("#{logf}", 'a')
  
  usf = USForex.new(logfile)

  if opt.rebuild_all
    # Read all CSV files
    # action = 'index' all docs
    # on opt.index (which must be specified)
  elsif opt.reindex
    # Create new index and set mapping
    # Re-index all docs from old index (can use alias for old index) to new (index must be specified)
    # Swap the alias (which must be specified) over to new index
  elsif opt.update_new
    # Read all CSV files
    # action = 'create'
    # Look for 'missing' docs 
    # on opt.index (which must be specified)
  end
  
  usf.get_forex_files
  usf.action = opt.action if !opt.action.nil?
  usf.gen_bulk_file('usforex.bulk', '2016-01-01')
  
end
