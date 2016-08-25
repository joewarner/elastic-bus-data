#!/usr/bin/env ruby

require 'optparse'
require 'ostruct'

# Am hoping that the required command line options will be pretty standard
# So attempting to abstract them here 
#
# --verbose & -- debug should be pretty obvious
# A program probably then wants to determine whether it is doing extraction 
# --export || (--rebuild_index || --index)
# Then we can look to see if a --type was specified.
# Finally some types require --year && --month_num to be specified
#
# --harvest and --es-config can be used to override config parameters as appropriate

class ESOptions
  def self.parse(args)
    opt = OpenStruct.new
    opt.debug = false
    opt.no_action = false
    opt.verbose = false
    opt.full = false
    opt.type = nil
    opt.year = nil
    opt.month = nil

    opt.harvest = '.harvest'
    opt.export = false

    opt.es_config = '.esutils'
    opt.rebuild_index = false
    opt.index = nil
    
    opt.cache_update = false
    opt.cache_update_all = false
    opt.cache_reload = false
    opt.update_es = false
    opt.update_es_all = false
    
    opt_parser = OptionParser.new do |o|

      # Generic options
      o.on("-d", "--debug") do |arg|
        opt.debug = true
      end
      o.on("-n", "--no-action") do |arg|
        opt.no_action = true
      end
      o.on("-v", "--verbose") do |arg|
        opt.verbose = true
      end
      o.on("-f", "--full") do |arg|
        opt.full = true
      end
      o.on("-t", "--type TYPE") do |arg|
        opt.type = arg
      end
      o.on("-y", "--year YEAR") do |arg|
        opt.year = arg.to_i
      end
      o.on("-m", "--month-num MONTH") do |arg|
        opt.month = arg.to_i
      end

      # Harvest options
      o.on("-h", "--harvest CONF") do |arg|
        opt.harvest = arg
      end
      o.on("-e", "--export") do |arg|
        opt.export = true
      end

      # Elasticsearch options
      o.on("--es-config CONF") do |arg|
        opt.es_config = arg
      end
      o.on("--rebuild-index") do |arg|
        opt.rebuild_index = true
      end
      o.on("-i", "--index INDEX") do |arg|
        opt.index = arg
      end

      o.on("--cache-update") do |arg|
        opt.export = true
        opt.cache_update = true
      end
      o.on("--cache-update-all") do |arg|
        opt.export = true
        opt.cache_update_all = true
      end
      o.on("--cache-reload") do |arg|
        opt.export = true
        opt.cache_reload = true
      end
      o.on("--update-es") do |arg|
        opt.update_es = true
      end
      o.on("--update-es-all") do |arg|
        opt.update_es_all = true
      end

    end
    opt_parser.parse!
    opt.verbose = true if opt.debug
    opt
  end
  
  def self.show_options(opt)
    puts
    puts "      --verbose is '#{opt.verbose}'"
    puts "        --debug is '#{opt.debug}'"
    puts "    --no-action is '#{opt.no_action}'"
    puts
    if opt.export
      puts "      --harvest is '#{opt.harvest}'"
      puts "       --export is '#{opt.export}'"
      puts "           --type is '#{opt.type}'" if opt.type
      puts "             --year is '#{opt.year}'" if opt.type and opt.year
      puts "              --month is '#{opt.month}'" if opt.type and opt.month
      puts 
    end
    if opt.rebuild_index #or opt.index
      puts "    --es-config is '#{opt.es_config}'"
      puts "--rebuild-index is '#{opt.rebuild_index}'"
      puts "        --index is '#{opt.index}'"
      puts "           --type is '#{opt.type}'" if opt.type
      puts "             --year is '#{opt.year}'" if opt.type and opt.year
      puts "              --month is '#{opt.month}'" if opt.type and opt.month
      puts
    end
    if opt.export and (opt.rebuild ) #or opt.index)
      puts "Your program should likely be built so that it rejects this combination of options"
      puts 
    end
  end
end

if __FILE__ == $0
  opt = ESOptions.parse(ARGV)
  ESOptions.show_options(opt)
end
