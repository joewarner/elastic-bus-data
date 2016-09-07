#!/usr/bin/env ruby

$LOAD_PATH.unshift File.expand_path($0)

load 'mylogger.rb'

require 'elasticsearch'
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
  
  def get_elasticsearch_config(conf_file=nil)
    @config_file = conf_file.nil? ? @config_file : conf_file
    self._log("Reading elasticsearch config from file '#{@config_file}' ", __LINE__, __method__, __FILE__)
    @config = YAML.load_file(@config_file)
  end
  
  def init_elasticsearch_client(conf_file=nil)
    self.get_elasticsearch_config(conf_file) if @config.empty?
    self._log("Initialising elasticsearch client", __LINE__, __method__, __FILE__)
    @client = Elasticsearch::Client.new(host: @config['host'], 
                                        user: @config['user'], 
                                        password: @config['pass'])
  end
  
  def load_mapping(fname)
    mapping = File.open(fname, 'r') do |json|
      JSON.load(json)
    end
  end

  def create_index(name, fname)
    self._log("Load index mapping from file #{fname}", __LINE__, __method__, __FILE__)
    mapping = self.load_mapping(fname)
    #pp mapping
    self._log("Creating index from mapping #{mapping}")
    rc = @client.indices.create(index: name, body: mapping)
  end
  
  def get_aliases
    self._log("Getting aliases", __LINE__, __method__, __FILE__)
    indexes = @client.indices.get_aliases(name: '_all')
    #pp indexes
    #aliases = []
    #indexes.keys.each do |idx|
    #  indexes[idx]['aliases'].keys.each do |als|
    #    self._log("  found #{als} as alias for #{idx}", __LINE__, __method__, __FILE__)
    #    aliases.push(als)
    #  end
    #end
    #pp aliases
    indexes
  end
  
  def bulk_action(data)
    self._log("Process bulk data", __LINE__, __method__, __FILE__)
    rc = @client.bulk(body: data)
    self._log("Bulk action completed, errors => #{rc['errors']}")
    pp data if @debug
    rc
  end
  
  def search(index, type, query)
    self._log("Search #{index}/#{type }using query #{query}", __LINE__, __method__, __FILE__)
    rc = @client.search(index: index, type: type, body: query)
    rc
  end
  
end


if __FILE__ == $0
  puts
end
  
  
