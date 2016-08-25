#!/usr/bin/env ruby

$LOAD_PATH.unshift File.expand_path($0)

load 'esoptions.rb'
load 'harvest.rb'


#curl -u <user>:<pass> 'https://aws-us-east-1-portal9.dblayer.com:11062/_cluster/health?pretty'

# To create an index
#curl -u <user>:<pass> -XPOST 'https://aws-us-east-1-portal9.dblayer.com:11062/<index>' -d @<index>/<index>.json

# To get an index
#curl -u <user>:<pass> -XGET 'https://aws-us-east-1-portal9.dblayer.com:11062/<index>?pretty'

# To delete an index
#curl -u <user>:<pass> -XDELETE 'https://aws-us-east-1-portal9.dblayer.com:11062/<index>'


def setup_months(start, finish)
  months = []
  #puts start, finish
  Array(start.year..finish.year).each do |year|
    months_in_year = Array(1..12)
    months_in_year = Array(start.month..12) if year.eql?(start.year)
    months_in_year = Array(1..finish.month) if year.eql?(finish.year)
    months_in_year.each do |month|
      month = {:year => year, :month => month}
      #puts month
      months.push(month)
    end
  end
  months  
end

def run_cmd(cmd, echo=true, output=false)
  puts "#{cmd}" unless echo == false
  out = %x{ #{cmd} }
  puts "#{out}" unless output == false
end

# There are the following scenarios that we want to be able to support
#
# - Complete refresh of the [Harvest] cache (--cache-reload)
# - Update all of the [Harvest] cache (--cache-update-all), last N months
# - Update the specified type in the cache (--cache_update -t <type> [--year --month-num])
#
# - Completely reconstruct the elasticsearch index (--rebuild-index)
# - Update the elasticsearch index with recent changes (--update-es-all)
# - Update the specified type in the elasticsearch index with recent changes (--update-es -t <type> [--year --month-num])
#

def cache_update(opt)
  harvest = get_harvest_handle(opt)
  esu = get_esu_handle(opt)
  esu.set_harvest(harvest[0], harvest[1])
  puts "cache_update('#{opt.type}')"
  if esu.stype?(opt.type)
    puts "  pull_#{opt.type}s('#{opt.index}')"
    # Year and Month not 'required' for stypes, so put dummy values in there
    esu.instance_eval("pull_#{opt.type}s('#{opt.index}', 0, 0)") if !opt.no_action
  elsif esu.ctype?(opt.type)
    syear = 2014
    smonth = 10
    now = Date.today
    if opt.year and opt.month
      year = opt.year ? opt.year.to_i : syear
      month = opt.month ? opt.month.to_i : smonth
      puts "  pull_#{opt.type}s('#{opt.index}', #{year}, #{month})"
      esu.instance_eval("pull_#{opt.type}s('#{opt.index}', #{year}, #{month})") if !opt.no_action
    else
      start = Date.new(syear, smonth, 1)
      today = Date.today
      months = setup_months(start, today)
      months.each do |month|
        puts "  pull_#{opt.type}s('#{opt.index}', #{month[:year]}, #{month[:month]})"
        esu.instance_eval("pull_#{opt.type}s('#{opt.index}', #{month[:year]}, #{month[:month]})") if !opt.no_action
      end
    end
  end
end

def cache_update_all(opt, window)
  stypes = ESUtils.class_eval("@@stypes")
  ctypes = ESUtils.class_eval("@@ctypes")
  # Haven't seen a good way to implement 'update_all' as yet
  # Loop over stypes and just do a cache_update on all of them
  stypes.each do |type|
    opt.send("type=", type)
    puts "cache_update_all('#{opt.type}')"
    cache_update(opt)
    sleep(1) if !opt.no_action
  end
  # Need Date.today and count back window months
  months = setup_months(Date.today.prev_month(window), Date.today)
  # Then for each of those months do a cache_update for the specified month on all ctypes
  months[-window..-1].each do |month|
    opt.send("year=", month[:year])
    opt.send("month=", month[:month])
    ctypes.each do |type|
      opt.send("type=", type)
      puts "cache_update_all('#{opt.type}', #{month[:year]}, #{month[:month]})"
      cache_update(opt) 
      sleep(1) if !opt.no_action
    end
  end
end

def cache_reload(opt)
  stypes = ESUtils.class_eval("@@stypes")
  ctypes = ESUtils.class_eval("@@ctypes")
  types = stypes.concat(ctypes)
  
  # The brutal approach
  types.each do |type|
    opt.send("type=", type)
    puts "cache_reload('#{opt.type}')"
    cache_update(opt)
    sleep(1) if !opt.no_action
  end
end

def update_es(opt)
  stypes = ESUtils.class_eval("@@stypes")
  ctypes = ESUtils.class_eval("@@ctypes")
  esu = get_esu_handle(opt)
  init_elasticsearch(opt, esu)
  year = opt.year ? opt.year.to_i : 2015
  month = opt.month ? opt.month.to_i : 1
  # Last 3 months
  puts "update_es('#{opt.type}')"
  puts "index_outdated_#{opt.type}s('#{opt.index}', #{year}, #{month})"
  esu.instance_eval("index_outdated_#{opt.type}s('#{opt.index}', year, month)")
=begin
  if stypes.include?(opt.type)
    puts "update_es('#{opt.type}')"
    esu.instance_eval("index_outdated_#{opt.type}s('#{opt.index}')")
  elsif ctypes.include?(opt.type)
    # Want an array of all years and months
    start = Date.new(2014, 10, 1)
    today = Date.today
    months = setup_months(start, today)
    months = setup_months(start, today)[-3..-1] if opt.full == false
    months.each do |month|
      puts "update_es('#{opt.type}', '#{month[:year]}', '#{month[:month]}')"
      esu.instance_eval("index_outdated_#{opt.type}s('#{opt.index}', '#{opt.year}', '#{opt.month}')")
      sleep(60) if month != months.last
    end
  end
=end
end

def update_es_all(opt)
  stypes = ESUtils.class_eval("@@stypes")
  ctypes = ESUtils.class_eval("@@ctypes")
  # Everything
  [].concat(stypes).concat(ctypes).each do |type|
    opt.send("type=", type)
    update_es(opt)
  end
end

def rebuild_index(opt)
  esu = get_esu_handle(opt)
  init_elasticsearch(opt, esu)
  esu.create_es_index(opt.index, true)
end
  
opt = ESOptions.parse(ARGV)
if opt.cache_update
  # For simple types we do a complete reload
  # For complex types we do a reload of just one month
  cache_update(opt)
elsif opt.cache_update_all
  # In reality we only reload the last N months
  cache_update_all(opt, 4)
elsif opt.cache_reload
  # Everything
  cache_reload(opt)
  
elsif opt.rebuild_index
  # Rebuid in the index on its own is sismple
  rebuild_index(opt)
  update_es_all(opt) if opt.full == true
elsif opt.update_es
  update_es(opt)
elsif opt.update_es_all
  update_es_all(opt)
end
