#!/usr/bin/env ruby

$LOAD_PATH.unshift File.expand_path($0)

require_relative './MyLogger.rb'

require 'json'
require 'pp'
require 'yaml'

class MyElastic
  attr_accessor :logfile

  include MyLogger
  
  def initialize(log_file=nil)
    @logfile = log_file
    @verbose = false
    @debug = false

    @config_file = ".esutils"
    @config = {}
    @client = {}
  end
  
  def set_verbose(verbose, debug=false)
    @verbose = verbose
    @debug = debug
  end
  
  def bulk_action(data)
    self._log("Process bulk data", __LINE__, __method__, __FILE__)
  end
  
end


if __FILE__ == $0
  puts
end
  
  
