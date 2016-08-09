#!/usr/bin/env ruby

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
# This program generates and output file called GBP-USD.json from the input file GBP-USD with the 
# command 'usforex.rb --from GBP --to USD'.
# The data can be added to the index with the following command
#
# curl -u <user>:<pass> -XPOST 'https://aws-us-east-1-portal9.dblayer.com:11062/usforex/currency_rate/_bulk' --data-binary @GBP-USD.json
#
# The 'from' and 'to' logic is based on multiplication. As an example, if we say that the exchange
# rate between GBP and USD is 1.5 then that means that to convert 'from' GBP 'to' USD then we must
# multiply each pound by 1.5 to obtain the equivalent number of dollars.

require 'optparse'
require 'ostruct'
require 'date'

class USFOptions
  def self.parse(args)
    opt = OpenStruct.new
    opt.all = false
    opt.action = 'create'
    opt.from = nil
    opt.to = nil
    opt_parser = OptionParser.new do |o|
      o.on("-a", "--all") do |arg|
        opt.all = true
      end
      o.on("--action ACTION") do |arg|
        opt.action = arg
      end
      o.on("-f", "--from FROM") do |arg|
        opt.from = arg
      end
      o.on("-t", "--to TO") do |arg|
        opt.to = arg
      end
    end
    opt_parser.parse!
    opt
  end
end

opt = USFOptions.parse(ARGV)
from = opt.from
action = opt.action
to = opt.to
out = File.open("#{from}-#{to}.bulk", "w")

File.open("#{from}-#{to}.csv").each_line do |line|
  date = line[/^([0-9\-\/]+)/,1]
  exch = line[/,([0-9\.]+)/,1]
  d = date.split('/')
  dd = Date.new(d[2].to_i, d[1].to_i, d[0].to_i)
  if opt.all or dd.to_s >= "2016-01-01"
    id = dd.strftime("%Y%m")
    out.print "{ \"#{action}\" : { \"_id\" : \"#{id}_#{from}-#{to}\" } }\n"
    out.print "{ \"month\" : \"#{dd}\", \"rate\" : \"#{exch}\", \"from\" : \"#{from}\", \"to\" : \"#{to}\" }\n"
  end
end

out.print "\n"
out.close
