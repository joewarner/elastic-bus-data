#!/usr/bin/env ruby

$LOAD_PATH.unshift File.expand_path($0)

# This program processes the data copied from this page
# http://www.usforex.com/forex-tools/historical-rate-tools/monthly-average-rates.
#
# The index mapping is described in usforex.json. Use the following commands to manage the mapping
#
=begin
# EXISTS: curl -u <user>:<pass> -XHEAD -i 'https://aws-us-east-1-portal9.dblayer.com:11062/usforex?pretty'
#    ADD: curl -u <user>:<pass> -XPUT 'https://aws-us-east-1-portal9.dblayer.com:11062/usforex?pretty' -d @usforex.json
# DELETE: curl -u <user>:<pass> -XDELETE 'https://aws-us-east-1-portal9.dblayer.com:11062/usforex?pretty'
#
# This program generates an output file called GBP-USD.json from the input file GBP-USD with the 
# command 'usforex.rb --from GBP --to USD'.
# The data can be added to the index with the following command
=end
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
  
  # Code that include the MyLogger module can all do logging in the same way
  include MyLogger
  
  ##
  # Class Variables
  ##
  # @@opt class variable is here to support command line option processing
  @@opt = OpenStruct.new
  # @@currency contains the three-letter codes for the currencies that we currently support
  @@currency = %w{ USD GBP EUR DKK SEK }
  
  ##
  # Class Methods
  ##
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
      # The principal operating modes for this program
      o.on("--rebuild-all") do |no_arg|
        @@opt.rebuild_all = true
      end
      o.on("--reindex") do |no_arg|
        @@opt.reindex = true
      end
      o.on("--update-new") do |no_arg|
        @@opt.update_new = true
      end
      
      # Subsidiary options for this program
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
    
    # The working assumption is that command line optins are valid
    # Now we will check if that is the case
    valid = true
    if @@opt.rebuild_all
      # If we are rebuilding then we must have an index
      # We don't rebuild aliases
      valid = false if @@opt.reindex or @@opt.update_new
      valid = false if !@@opt.alias.nil?
      valid = false if @@opt.index.nil?
    elsif @@opt.reindex
      # If we are reindexing then we must have an index specified which we will first create and 
      # then into which we will reindex the documents
      # The index that we are reindexing from should be specified by its alias since
      # once reindexing is complete we will change to alias so that it points to the new index.
      valid = false if @@opt.rebuild_all or @@opt.update_new
      valid = false if @@opt.alias.nil?
      valid = false if @@opt.index.nil?
      # Using 'create' when reindexing means that we will get an error if the document already exists
      @@opt.action = 'create'
    elsif @@opt.update_new
      # If we are updating with new values then we must have an index
      # We don't add docs to aliases
      valid = false if @@opt.rebuild_all or @@opt.reindex
      valid = false if !@@opt.alias.nil?
      valid = false if @@opt.index.nil?
      @@opt.action = 'create'
    end
    return @@opt, valid
  end
  
  def self.set_option(option, value)
    cmd = "@@opt.#{option} = #{value}" if value.is_a?(Fixnum)
    cmd = "@@opt.#{option} = '#{value}'" if value.is_a?(String)
    puts "Setting option, #{cmd}"
    class_eval(cmd)
  end
  
  ##
  # Instance Methods
  ##
  # You can override where the logging output gets written by specifying an alternative
  # file/path for this method
  def initialize(log_file=nil)
    logdir = "#{File.dirname($0)}/logs"
    if log_file.nil?
      @@opt.logfile = "#{logdir}/#{File.basename($0, '.rb')}.log"
    else
      @@opt.logfile = "#{logdir}/#{log_file}"
    end
    @logfile = File.open("#{@@opt.logfile}", 'a')
    self._log("Logging file opened #{@@opt.logfile}", __LINE__, __method__, __FILE__)
    
    # This class supports both verbose and debug output options
    @verbose = false
    @debug = false
    
    self._log("Get instance of elasticsearch client", __LINE__, __method__, __FILE__)
    @elastic = MyElastic.new(@logfile)
    @elastic.set_verbose(@@verbose, @@debug)
    @elastic.get_elasticsearch_config
    @elastic.init_elasticsearch_client
    
    # I think this is no longer used
    @files = []
  end
  
  def set_verbose(verbose, debug)
    @verbose = verbose
    @debug = debug
    @elastic.set_verbose(verbose, debug) if !@elastic.nil?
  end
  
  # This could be modified to take a dir='.' parameter.
  # Would need to change the RE though to deal with the dirname
  def get_forex_files
    Dir.new('.').each do |f|
      csv = f[/^([A-Z]{3}-[A-Z]{3}.csv)$/,1]
      if csv
        @files.push(csv)
        puts "Found Forex file #{csv}" if @@opt.debug
      end
    end
    self._log("Found files #{@files}", __LINE__, __method__, __FILE__)
    @files
  end
  
  def gen_bulk_entry(type, from, to, line)
    self._log("Generate bulk entry for forex line = #{line}", __LINE__, __method__, __FILE__)
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
    doc['_type']  = type
    doc['_id']    = id
    doc['data']   = data
    
    action = {}
    action[@@opt.action] = doc
    self._log("Bulk entry line is #{action}", __LINE__, __method__, __FILE__)
    action
  end
    
  def gen_bulk(file)
    self._log("Creating instructions for bulk processing: #{file}", __LINE__, __method__, __FILE__)
    
    from  = file[/^([A-Z]{3})-[A-Z]{3}.csv$/,1]
    to    = file[/^[A-Z]{3}-([A-Z]{3}).csv$/,1]
    # Maybe want to check that 'from' and 'to' are in @@currency

    bulk = []
    File.open(file).each_line do |line|
=begin
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
=end
      action = self.gen_bulk_entry('currency_rate', from, to, line)
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
  aliases = usf.elastic.get_aliases
  pp aliases if opt.debug
  found_as_index = false
  found_as_alias = false
  found_as_index = true if aliases.keys.include?(opt.index)

  if opt.rebuild_all

    # Want to check and see if the specified index exists
    # It could be either the index or an alias to it
    # If it doesn't exist then we are going to need to create it
    aliases.each_pair do |name,value|
      #puts "name=#{name}, value=#{value}"
      found_as_alias = true if value['aliases'].include?(opt.index)
    end
    if !(found_as_index or found_as_alias)
      # We need to create the index from the mapping file
      puts "Creating index #{opt.index}"
      rc = usf.elastic.create_index(opt.index, opt.mapping_file)
      # NEED TO CHECK IF THIS WORKED AND EXIT IF NOT
    
      # Build one massive bulk file
      bulk = []
      usf.get_forex_files.each do |file|
        puts "Processing Forex file: #{file}"
        bulk.push(usf.gen_bulk(file))
      end
      bulk.flatten!
      rc = usf.elastic.bulk_action(bulk)
      puts "Processed #{bulk.size} actions"
      puts "Errors => #{rc['errors']}" 
    end
    
  elsif opt.reindex
    # Create new index and set mapping
    # Re-index all docs from old index (can use alias for old index) to new (index must be specified)
    # Swap the alias (which must be specified) over to new index
  elsif opt.update_new
    
    # I suppose we should check for the index to be on the safe side, 
    # but we aren't going to do anything with it
    aliases.each_pair do |name,value|
      #puts "name=#{name}, value=#{value}"
      found_as_alias = true if value['aliases'].include?(opt.index)
    end

    if found_as_index and !found_as_alias
      # Look for 'missing' docs 

      # This probably means get all of the docs for the last N months
      # Then we can go through them and see which ones are missing
      now = Date.today
      #now = Date.new(2016, 10, 1) # This to frig the outcome for testing
      #puts now
      
      # So, get all of the docs in ES for last month
      dd = now.prev_month
      mth = Date.new(dd.year, dd.month, 1).strftime('%Y-%m-%d')
      qq = { "filter" => { "match" => { "month" => "#{mth}" }}}
      #pp qq
      res = usf.elastic.search(opt.index, 'currency_rate', qq)
      #pp res
      
      # Now what we need is a list of files we have to access
      # We can get that by removing from the list of all files those rates that we find
      files = usf.get_forex_files

      # Lets look at those docs and if we have have a hit, then remove them from 'files'
      if res['hits']['total'].to_i > 0
        # Found some
        rates = res['hits']['hits']
        rates.each do |rate|
          #pp rate
          f = "#{rate['_source']['from']}-#{rate['_source']['to']}.csv"
          #pp f
          files.delete(f)
        end
      end
      #pp files
      
      # Not finished with this yet - but essentially we assume we have to look in all files
      # There is probably a faster way to seek to the end of the file, but this works
      bulk = []
      files.each do |file|
        from  = file[/^([A-Z]{3})-[A-Z]{3}.csv$/,1]
        to    = file[/^[A-Z]{3}-([A-Z]{3}).csv$/,1]
        #puts "from=#{from}, to=#{to}"
        File.open(file).each_line do |line|
          date = line[/^([0-9\-\/]+)/,1]
          d = date.split('/')
          if d[2].to_i.eql?(dd.year) and d[1].to_i.eql?(dd.month)
            # This is if we found the entry we were looking for
            bulk.push(usf.gen_bulk_entry('currency_rate', from, to, line))
          else
            # And this is when we don't find it
            # Do nothing!
          end
        end
      end
      #pp bulk
      
      if bulk.size > 0
        rc = usf.elastic.bulk_action(bulk)
        puts "Processed #{bulk.size} actions"
        puts "Errors => #{rc['errors']}" 
      else
        puts "All forex docs are up-to-date"
      end
    end

  end
  
=begin
  usf.action = opt.action if !opt.action.nil?
  usf.gen_bulk_file('usforex.bulk', '2016-01-01')
=end  
end
puts
