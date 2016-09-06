#!/usr/bin/env ruby

$LOAD_PATH.unshift File.expand_path($0)

require_relative './esoptions.rb'
require_relative './MyLogger.rb'

require 'json'
require 'pp'
require 'yaml'

class MyCache
  attr_accessor :path, :logfile
  
  include MyLogger
  
  def initialize(log_file=nil)
    @logfile = log_file
    @verbose = false
    @debug = false
    @path = nil
  end

  def set_verbose(verbose, debug=false)
    @verbose = verbose
    @debug = debug
  end

  def read_cache_(fname)
    data = File.open(fname, 'r') do |json|
      JSON.load(json)
    end
  end

  def write_cache_(fname, data)
    File.open(fname, 'w') do |json|
      json.puts(JSON.pretty_generate(data))
    end
  end

  def read_cache(type, unwind=false, postfix=nil)
    fname = "#{@path}/#{type}"
    fname = "#{fname}-#{postfix}" if postfix
    msg = "Reading #{type} records from cache file #{fname}.json"
    puts msg if @verbose
    self._log(msg, __LINE__, __method__, __FILE__)
    arr = self.read_cache_("#{fname}.json")
    data = arr
    if unwind
      # The problem with writing out data to a JSON file and then reading it back 
      # is that you lose the detail of the type of the array elements.
      # When you load the data back from the file each element is a Hash with the
      # original type name as its single index.
      data = []
      arr.each do |element|
        data.push(element[type])
      end
    end
    data
  end

  def write_cache(type, data, postfix=nil)
    fname = "#{@path}/#{type}"
    fname = "#{fname}-#{postfix}" if postfix
    puts ".. and writing cache file #{fname}.json" if @verbose
    self._log("Writing to file #{fname}", __FILE__, __method__, __FILE__)
    self.write_cache_("#{fname}.json", data)
    # If we have just cached the data then make sure that we read it back from the file
    self.read_cache(type, true)
  end

  def read_time_cache(type, postfix=nil)
    self._log("Reading from time cache", __LINE__, __method__, __FILE__)
    data = self.read_cache(type, true, postfix)
    self._log("Reading #{data.size} records from time cache", __LINE__, __method__, __FILE__)
    data
  end

  def write_time_cache(type, data, postfix=nil)
    fname = "#{@path}/#{type}"
    fname = "#{fname}-#{postfix}" if postfix
    msg = "Writing cache file #{fname}.json"
    puts msg if @verbose
    self._log(msg,  __LINE__, __method__, __FILE__)
    self.write_cache_("#{fname}.json", data)
    msg =  ".. and reading cache file #{fname}.json"
    puts msg if @verbose
    self._log(msg,  __LINE__, __method__, __FILE__)
    self.read_time_cache(type, postfix)
  end

end
  


if __FILE__ == $0
  puts
end


