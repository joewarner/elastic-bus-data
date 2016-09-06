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

require_relative './MyLogger.rb'
require_relative './MyElastic.rb'

require 'optparse'
require 'ostruct'
require 'date'
require 'pp'

class USForex
  attr_accessor   :action
  attr_reader     :elastic
  
  include MyLogger
  
  @@opt = OpenStruct.new
  @@currency = %w{ USD GBP EUR DKK SEK }
  
  def initialize(log_file=nil)
    logdir = "#{File.dirname($0)}/logs"
    if log_file.nil?
      @@opt.logfile = "#{logdir}/#{File.basename($0, '.rb')}.log"
    else
      @@opt.logfile = "#{logdir}/#{log_file}"
    end
    @logfile = File.open("#{@@opt.logfile}", 'a')
    self._log("Logging file opened #{@@opt.logfile}", __LINE__, __method__, __FILE__)
    @verbose = true if @@opt.verbose
    @debug = false
    
    @elastic = MyElastic.new(@logfile)
    @elastic.set_verbose(@@verbose, @@debug)
    
    @files = []
  end
  
  def self.parse(args)
    @@opt = OpenStruct.new
    #@@opt.all = false
    @@opt.action = 'index'
    @@opt.mapping_file = 'usforex.json'
    #@@opt.from = nil
    #@@opt.to = nil
    @@opt.logfile = nil
    
    @@opt.rebuild_all = false
    @@opt.reindex = false
    @@opt.update_new = false
    @@opt.index = nil
    @@opt.alias = nil
    
    @@verbose = false
    @@debug = false
    
    opt_parser = OptionParser.new do |o|
      o.on("--rebuild-all") do |no_arg|
        @@opt.rebuild_all = true
      end
      o.on("--reindex") do |no_arg|
        @@opt.reindex = true
      end
      o.on("--update-new") do |no_arg|
        @@opt.update_new = true
      end
      
      #o.on("-a", "--all") do |no_arg|
      #  @@opt.all = true
      #end
      o.on("--action ACTION") do |arg|
        @@opt.action = arg
      end
      o.on("-i", "--index INDEX") do |arg|
        @@opt.index = arg
      end
      o.on("-a", "--alias ALIAS") do |arg|
        @@opt.alias = arg
      end
      o.on("--mapping-file MAPFILE") do |arg|
        @@opt.mapping_file = arg
      end
      o.on("-l", "--logfile LOGFILE") do |arg|
        @@opt.logfile = arg
      end
      o.on("-v", "--verbose") do |no_arg|
        @@opt.verbose = true
      end
      o.on("-d", "--debug") do |no_arg|
        @@opt.debug = true
      end
    end
    opt_parser.parse!
    @@opt
  end
  
  def self.parse_and_validate(args)
    self.parse(args)
    valid = true
    if @@opt.rebuild_all 
      valid = false if @@opt.reindex or @@opt.update_new
      valid = false if !@@opt.alias.nil?
      valid = false if @@opt.index.nil?
    elsif @@opt.reindex
      valid = false if @@opt.rebuild_all or @@opt.update_new
      valid = false if @@opt.alias.nil?
      valid = false if @@opt.index.nil?
      @@opt.action = 'create'
    elsif @@opt.update_new
      valid = false if @@opt.rebuild_all or @@opt.reindex
      valid = false if !@@opt.alias.nil?
      valid = false if @@opt.index.nil?
    end
    return @@opt, valid
  end
  
  def get_forex_files
    Dir.new('.').each do |f|
      csv = f[/^([A-Z]{3}-[A-Z]{3}.csv)$/,1]
      if csv
        @files.push(csv)
        #puts "Found Forex file #{csv}" if @@opt.verbose
      end
    end
    self._log("Found files #{@files}", __LINE__, __method__, __FILE__)
    @files
  end
  
  # Method is deprecated now - get rid asap
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
  
  def gen_bulk(file)
    self._log("Creating instructions for bulk processing: #{file}", __LINE__, __method__, __FILE__)
    
    from  = file[/^([A-Z]{3})-[A-Z]{3}.csv$/,1]
    to    = file[/^[A-Z]{3}-([A-Z]{3}).csv$/,1]

    bulk = []
    File.open(file).each_line do |line|
      #puts line
      date = line[/^([0-9\-\/]+)/,1]
      exch = line[/,([0-9\.]+)/,1]
      
      d = date.split('/')
      dd = Date.new(d[2].to_i, d[1].to_i, d[0].to_i)
      data = {}
      data['month'] = dd.to_s
      data['rate']  = exch
      data['from']  = from
      data['to']    = to
      
      id = "#{dd.strftime('%Y%m')}_#{from}_#{to}"
      doc = {}
      doc['_index'] = @@opt.index
      doc['_type']  = 'currency_rate'
      doc['_id']    = id
      doc['data']   = data
      
      action = {}
      action[@@opt.action] = doc
      #pp action
      bulk.push(action)
    end
    self._log("  Processed #{bulk.size} lines from input", __LINE__, __method__, __FILE__)
    bulk
  end
  
end

if __FILE__ == $0
  opt, valid = USForex.parse_and_validate(ARGV)
  if !valid
    puts "Invalid program options"
    puts
    exit
  end
  
  usf = USForex.new

  if opt.rebuild_all
    # Read all CSV files
    files = usf.get_forex_files
    bulk = []
    # Build one massive bulk file
    files.each do |file|
      puts "Processing Forex file: #{file}"
      bulk.push(usf.gen_bulk(file))
    end
    bulk.flatten!
    bulk
    usf.elastic.bulk_action(bulk)
  elsif opt.reindex
    # Create new index and set mapping
    # Re-index all docs from old index (can use alias for old index) to new (index must be specified)
    # Swap the alias (which must be specified) over to new index
  elsif opt.update_new
    # Read all CSV files
    files = usf.get_forex_files

    # action = 'create'
    # Look for 'missing' docs 
    # on opt.index (which must be specified)
  end
  
=begin
  usf.action = opt.action if !opt.action.nil?
  usf.gen_bulk_file('usforex.bulk', '2016-01-01')
=end  
end
