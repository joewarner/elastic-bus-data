#!/usr/bin/env ruby

$LOAD_PATH.unshift File.expand_path($0)

load 'esoptions.rb'
load 'mylogger.rb'

require 'elasticsearch'
require 'harvested'
require 'json'
require 'pp'
require 'yaml'
require 'csv'

class ESUtils
  
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
  
  class MyHarvest
    attr_reader :client, :cache, :logfile
    
    include MyLogger
    
    @@stypes = %w{ client expense_category project task user }
    @@ctypes = %w{ expense invoice time }
    
    def initialize(log_file=nil)
      @logfile = log_file
      @verbose = false
      @debug = false

      @config_file = ".harvest"
      @config = {}
      @client = {}
      @cache = MyCache.new(log_file)
      
      @@stypes.each do |typ|
        eval("@#{typ}_id = {}")
      end
      
    end
    
    def method_missing(*args)
      m = args.shift
      self._log("Method called = #{m}", __LINE__, __method__, __FILE__)
    
      if type = m[/^build_([a-z_]+)_hash$/,1] and self.stype?(type)

        puts "method_missing: #{m}, #{type}" if @debug
        self._log("  Extracted type = #{type}", __LINE__, __method__, __FILE__)
        index = args.shift
        msg = "Make call to build_type_hash('#{type}')"
        puts msg if @verbose
        self._log("  #{msg}", __LINE__, __method__, __FILE__)
        self.build_type_hash(type)

      elsif type = m[/^pull_([a-z_]+)s$/,1] and @hinst.stype?(type)
      
        puts "method_missing: #{m}, #{type}" if @debug
        self.log("  Extracted type = #{type}", __method__, __LINE__)
        index = args.shift
        msg = "Make call to pull_type('#{index}', '#{type}')"
        puts msg if @verbose
        self._log("  #{msg}", __LINE__, __method__, __FILE__)
        @hinst.pull_type(type) if @hinst.stype?(type)
      
=begin
      elsif type = m[/^pull_([a-z_]+)s$/,1] and @hinst.ctype?(type)
      
        puts "method_missing: #{m}, #{type}" if @debug
        self.log("  Extracted type = #{type}", __method__, __LINE__)
        index = args.shift
        year = args.shift.to_i
        month = args.shift.to_i
        puts "Make call to pull_type_for_month('#{index}', '#{type}', #{year}, #{month})" if @verbose
        self.log("  Handing off to pull_type_for_month('#{index}', '#{type}', #{year}, #{month})", __method__, __LINE__)
        self.pull_type_for_month(index, type, year, month)
      
      elsif type = m[/^index_outdated_([a-z_]+)s$/,1] and @hinst.stype?(type)
      
        puts "method_missing: #{m}, #{type}" if @debug
        self.log("  Extracted type = #{type}", __method__, __LINE__)
        index = args.shift
        puts "Make call to index_outdated('#{index}', '#{type}')" if @verbose
        self.log("  Handing off to index_outdated('#{index}', '#{type}')", __method__, __LINE__)
        self.index_outdated(index, type)

      elsif type = m[/^index_outdated_([a-z_]+)s$/,1] and @hinst.ctype?(type)
      
        puts "method_missing: #{m}, #{type}" if @debug
        self.log("  Extracted type = #{type}", __method__, __LINE__)
        index = args.shift
        year = args.shift.to_i
        month = args.shift.to_i
        puts "Make call to index_outdated('#{index}', '#{type}', '#{year}', '#{month}')" if @verbose
        if type == 'invoice'
          self.log("  Handing off to index_invoices('#{index}', '#{type}')", __method__, __LINE__)
          self.index_invoices(index, type, year, month)
        else
          self.log("  Handing off to index_outdated_for_month('#{index}', '#{type}', #{year}, #{month})", __method__, __LINE__)
          self.index_outdated_for_month(index, type, year, month)
=end
      end
    end

    def set_verbose(verbose, debug=false)
      @verbose = verbose
      @debug = debug
      @cache.set_verbose(verbose, debug)
    end

    def get_harvest_config(conf_file=nil)
      @config_file = conf_file.nil? ? @config_file : conf_file
      self._log("Reading harvest config from file '#{@config_file}' ", __LINE__, __method__, __FILE__)
      @config = YAML.load_file(@config_file)
    end
    
    def init_harvest_client(conf_file=nil)
      get_harvest_config(conf_file) if @config.empty?
      self._log("Initialising harvest client", __LINE__, __method__, __FILE__)
      @client = Harvest.client(subdomain: @config['subdomain'], 
                                username: @config['username'], 
                                password: @config['password'])
    end
    
    def set_cache_path(path)
      @cache.path = path
    end
    
    def stype?(type)
      @@stypes.include?(type)
    end
  
    def ctype?(type)
      @@ctypes.include?(type)
    end
  
    def initialize_reverse_indexes
      self._log("Entering", __LINE__, __method__, __FILE__)
      @@stypes.each do |typ|
        if eval("@#{typ}_id.empty?")
          self._log("  ... for #{typ}", __LINE__, __method__, __FILE__)
          eval("@#{typ}_id = self.build_#{typ}_hash if @#{typ}_id.empty?")
        end
      end
      self._log("Completed", __LINE__, __method__, __FILE__)
      self
    end
  
    def build_type_hash(type, year=nil, month=nil)
      # The @<type>_id maps are helpful for de-normalising docs before indexing them
      msg = "Return Id Hash for #{type}"
      puts msg if @verbose
      self._log(msg, __LINE__, __method__, __FILE__)
      if !year.nil? and !month.nil?
        postfix = sprintf("%d-%02d", year, month)
        arr = self.cache.read_time_cache(type, postfix)
      else
        arr = self.cache.read_cache(type, true)
      end
      typ_id = arr

      idmap = {}
      arr.each do |element|
        idmap[element['id']] = element
      end
      #pp idmap
      idmap = self.augment_clients(type, idmap) if type.eql?('client')
      self._log("Completed",  __LINE__, __method__, __FILE__)
      idmap
    end

    def augment_clients(type, idmap)
      # A bit of a hack for now
      self._log("Adding location data", __LINE__, __method__, __FILE__)
      #loc = self.read_cache_("#{index}/#{type}-location.json")
      loc = self.cache.read_cache("#{type}-location", false)
      loc.each do |locn|
        #pp locn
        id = locn['client']['id']
        #pp id
        #pp idmap[id]
        idmap[id]['location'] = locn['client']['location']
        #pp idmap[id]
      end
      idmap
    end

    def get_days_for_month(year, month)
      # Need to build an array with the right day numbers for the specified month
      mon = Date.new(year, month, 1)
      sday = mon.yday
      eday = mon.next_month.prev_day.yday
      puts "Array(#{sday}..#{eday})" if @debug
      self._log("Range: (#{mon}) #{sday}-#{eday}", __LINE__, __method__, __FILE__)
      Array(sday..eday)
    end
  
    def get_invoice(id)
      fname = "#{@cache.path}/inv-#{id}.json"
      creds = "#{@config['username']}:#{@config['password']}"
      if !File.exists?(fname)
        msg = "Need to fetch invoice=#{id}"
        url = "https://#{hv['subdomain']}.harvestapp.com/invoices/#{id}"
        cmd = "curl -u #{creds} -XGET '#{url}' -H 'Accept: application/json' -L 2> /dev/null > #{fname}"
        %x{ #{cmd} }
      else
        msg = "Found invoice=#{id} in cache"
      end
      self._log("#{msg}", __LINE__, __method__, __FILE__)
      inv = {}
      File.open(fname, 'r') do |data|
        inv = JSON.load(data)
      end
      inv
    end
  
    def get_csv_line_items(invoice, unwind=true)
      inv = invoice
      inv = invoice['invoice'] if unwind
    
      cli = CSV.parse(inv['csv_line_items'])
      cols = cli.shift
      self._log("Found #{cli.size} line items on invoice", __LINE__, __method__, __FILE__)
      csv_line_items = Array.new
      cli.each do |line|
        i = 0
        c = {}
        line.each do |col|
          c[cols[i]] = col
          i += 1
        end
        c = self.denormalise_doc(c)
        #pp c
        csv_line_items.push(c)
      end

      #pp csv_line_items
      csv_line_items
    end

    def merge_csv_into_invoice(id, hv)
      self._log("Merge invoice csv_line_items, id=#{id}", __LINE__, __method__, __FILE__)
      invoice = self.get_invoice(id)
      csv_line_items = self.get_csv_line_items(invoice) 
      invoice['invoice']['csv_line_items'] = csv_line_items
      #pp invoice
      self._log("  Now denormalise invoice fields", __LINE__, __method__, __FILE__)
      invoice['invoice'] = self.denormalise_doc(invoice['invoice'])
      self._log("Completed merge invoice csv_line_items", __LINE__, __method__, __FILE__)
      invoice
    end

    def denormalise_doc(element)
      keys = element.keys
      #pp keys
      self._log("Denormalising doc", __LINE__, __method__, __FILE__)
      keys.each do |key|
        if key == 'client_id' or key == 'user_id' or key == 'project_id' or key == 'expense_category_id'
          #puts key
          #pp element[key]
          ekey = element[key] # e.g. ekey = element['user_id']
          self._log("  Denormalising #{key} = #{ekey}", __LINE__, __method__, __FILE__)
          if !ekey.nil?
            typid = {}
            if key == 'project_id' and ekey == 'false'
              self._log("    project_id = #{ekey}", __LINE__, __method__, __FILE__)
              typid = {'id' => 0, 'name' => "Unknown"}
            else
              typid['id'] = ekey # e.g. element['user_id]
              #puts @project_id[ekey.to_i]
              cmd = "typid['name'] = @#{key}[#{ekey}.to_i]['name']" # @user_id[ekey]
              puts cmd if @verbose
              eval(cmd)
              self._log("    #{cmd} => '#{typid['name']}'", __LINE__, __method__, __FILE__)
              #pp typid
              puts "#{key} = '#{ekey}'" if @verbose
              puts "@#{key}['#{ekey}'] => '#{typid['name']}'" if @debug # e.g. "user_id['854934'] => 'Baykal, Ali'"
            end
            pp typid if @debug
            element[key] = typid
          else
            self._log("  Unable to denormalise #{key}", __LINE__, __method__, __FILE__)
          end
        end
        pp element if @debug
      end
      self._log("Completed", __LINE__, __method__, __FILE__)
      element
    end
  
    def pull_type(type)
      cmd = "@client.#{type}s.all"
      cmd = "@client.expense_categories.all" if type == 'expense_category'
      msg = "Reading #{type} records from Harvest - #{cmd}"
      puts msg if @verbose
      self._log(msg, __LINE__, __method__, __FILE__)
      data = eval(cmd)
      self._log("Read #{data.size} records", __LINE__, __method__, __FILE__)
      @cache.write_cache(type, data)
      @logfile.flush
      data
    end
  
    def pull_invoices(year=nil, month=nil)
      self.initialize_reverse_indexes if @user_id.empty?
      type = "invoice"
      invoice = []

      puts "\npull_invoices: " if @verbose
      self._log("pull_type_for_month('#{type}', #{year}, #{month})", __LINE__, __method__, __FILE__)
      days = self.get_days_for_month(year, month)

      from = Date.ordinal(year, days[0])
      to = Date.ordinal(year, days[-1])
      options = {:timeframe => {:from => from.strftime("%Y%m%d"), :to => to.strftime("%Y%m%d")}}
      self._log("  Getting #{type} data for #{from.year}/#{from.mon}/#{from.day}-#{to.year}/#{to.mon}/#{to.day}", __LINE__, __method__, __FILE__)
      ui = @client.invoices.all(options)
      if ui.size > 0
        #puts "    Concat'ing data"
        invoice.concat(ui)
      else
        puts "      Empty data" if @debug
      end
        
      msg = "Found #{invoice.size} #{type} records"
      puts msg if @verbose
      self._log(msg, __LINE__, __method__, __FILE__)
      invoice.each_index do |i|
        invoice[i] = self.merge_csv_into_invoice(invoice[i]['id'], @hv)
      end

      postfix = sprintf("%d-%02d", year, month)
      self.cache.write_time_cache(type, invoice, postfix)    
      @logfile.flush

      self._log("Completed", __LINE__, __method__, __FILE__)
      @logfile.flush
    end
  
  end
  
  class MyElastic
  end

  attr_accessor :hinst
  attr_reader :elastic, :index

  include MyLogger

  def initialize(opt)
    @debug = false
    @verbose = false
    @logfile = File.open("#{File.dirname($0)}/logs/out.log", "a")
    
    @hinst = MyHarvest.new(@logfile)
    @hinst.set_cache_path(opt.index)
    @hinst.init_harvest_client
    
    @index = nil
    
    @client_id = {}
    @expense_category_id = {}
    @project_id = {}
    @task_id = {}
    @user_id = {}
    
    self.set_opt(opt)
    @hinst.initialize_reverse_indexes
    
    self._log("Completed", __LINE__, __method__, __FILE__)
  end
  
=begin
    def initialize_reverse_indexes(index)
    self._log("Entering", __LINE__, __method__, __FILE__)
    self.setup_client_ids(index)            if @client_id.empty?
    self.setup_expense_category_ids(index)  if @expense_category_id.empty?
    self.setup_project_ids(index)           if @project_id.empty?
    self.setup_task_ids(index)              if @task_id.empty?
    self.setup_user_ids(index)              if @user_id.empty?
    self._log("Completed", __LINE__, __method__, __FILE__)
    self
  end
=end
  def set_opt(opt)
    @debug = opt.debug
    @verbose = opt.verbose
    @hinst.set_verbose(opt.verbose, opt.debug)
  end

  def set_harvest(hv)
    @harvest = hv[0]
    @hv = hv[1]
  end
  
  def set_elasticsearch(idx, es)
    @elastic = es
    @index = idx
    puts "Setting default index as '#{@index}'" if @verbose
  end
  
  def method_missing(*args)
    m = args.shift
    self.log("Method called = #{m}", __method__, __LINE__)
    
    if type = m[/^setup_([a-z_]+)_ids$/,1] and @hinst.stype?(type)

      puts "method_missing: #{m}, #{type}" if @debug
      self.log("  Extracted type = #{type}", __method__, __LINE__)
      index = args.shift
      puts "Make call to setup_id_index('#{index}', '#{type}')" if @verbose
      self.log("  Handing off to setup_id_index('#{index}', '#{type}')", __method__, __LINE__)
      self.setup_id_index(index, type)
      eval("pp @#{type}_id if @debug")
      
    elsif type = m[/^pull_([a-z_]+)s$/,1] and @hinst.stype?(type)
      
      puts "method_missing: #{m}, #{type}" if @debug
      self.log("  Extracted type = #{type}", __method__, __LINE__)
      index = args.shift
      puts "Make call to pull_type('#{index}', '#{type}')" if @verbose
      self.log("  Handing off to pull_type('#{index}', '#{type}')", __method__, __LINE__)
      @hinst.pull_type(type)
      
    elsif type = m[/^pull_([a-z_]+)s$/,1] and @hinst.ctype?(type)
      
      puts "method_missing: #{m}, #{type}" if @debug
      self.log("  Extracted type = #{type}", __method__, __LINE__)
      index = args.shift
      year = args.shift.to_i
      month = args.shift.to_i
      if type.eql?('invoice')
        puts "Make call to pull_invoices(#{year}, #{month})" if @verbose
        self.log("  Handing off to pull_invoices(#{year}, #{month})", __method__, __LINE__)
        @hinst.pull_invoices(year, month)
      else
        puts "Make call to pull_type_for_month('#{index}', '#{type}', #{year}, #{month})" if @verbose
        self.log("  Handing off to pull_type_for_month('#{index}', '#{type}', #{year}, #{month})", __method__, __LINE__)
        self.pull_type_for_month(index, type, year, month)
      end
      
    elsif type = m[/^index_outdated_([a-z_]+)s$/,1] and @hinst.stype?(type)
      
      puts "method_missing: #{m}, #{type}" if @debug
      self.log("  Extracted type = #{type}", __method__, __LINE__)
      index = args.shift
      puts "Make call to index_outdated('#{index}', '#{type}')" if @verbose
      self.log("  Handing off to index_outdated('#{index}', '#{type}')", __method__, __LINE__)
      self.index_outdated(index, type)

    elsif type = m[/^index_outdated_([a-z_]+)s$/,1] and @hinst.ctype?(type)
      
      puts "method_missing: #{m}, #{type}" if @debug
      self.log("  Extracted type = #{type}", __method__, __LINE__)
      index = args.shift
      year = args.shift.to_i
      month = args.shift.to_i
      puts "Make call to index_outdated('#{index}', '#{type}', '#{year}', '#{month}')" if @verbose
      if type == 'invoice'
        self.log("  Handing off to index_invoices('#{index}', '#{type}')", __method__, __LINE__)
        self.index_invoices(index, type, year, month)
      else
        self.log("  Handing off to index_outdated_for_month('#{index}', '#{type}', #{year}, #{month})", __method__, __LINE__)
        self.index_outdated_for_month(index, type, year, month)
      end

=begin      
    elsif type = m[/^index_([a-z_]+)s$/,1] and @hinst.stype?(type)
      # Not absolutely sure that this option is required
      puts "method_missing: #{m}, #{type}" if @debug
      self.log("  Extracted type = #{type}", __method__, __LINE__)
      index = args.shift
      puts "Make call to index_type('#{index}', '#{type}')" if @verbose
      self.log("  Handing off to index_type('#{index}', '#{type}')", __method__, __LINE__)
      self.index_type(index, type)
=end
      
    else
      super
    end
    self.log("Completed", __method__, __LINE__)
  end
  
  # Other methods
  def get_type_fields(type)
    map = @elastic.indices.get_mapping(index: @index, type: type)
    fields = map[index]['mappings'][type]['properties'].keys
    #pp fields
    fields.delete(type)
    fields
  end
  
  def sort_on_field(arr, field)
    skeys = {}
    arr.each do |element|
      skeys[element[field]] = element
    end
    sarr = []
    skeys.keys.sort.each do |sfield|
      sarr.push(skeys[sfield])
    end
    sarr
  end
  
  def strip_body_fields(o, fields)
    self.log("Stripping fields", __method__, __LINE__)
    body = o.clone
    body.keys.each do |key|
      if !fields.include?(key)
        #self.log("  Stripping field #{key}", __method__, __LINE__)
        body.delete(key)
      end
    end
    body.delete('id')
    pp body if @debug
    self.log("Completed", __method__, __LINE__)
    body
  end
  
  def is_harvest_doc_newer?(doc, field, hdoc)
    # Want to compare the field and see if what we have from Harvest is newer
    if doc['_source'][field] < hdoc[field]
      true
    else
      false
    end
  end
  
  def users_by_date(index)
    #users = self.read_cache(index, 'user', true)
    users = @hinst.cache.read_cache('user', true)
    # Sort them on created_at date
    self.sort_on_field(users, 'created_at')
  end
  
  def user_valid_for_date(u, day, year, month)
    valid_user = false
    if Time.parse(u['created_at']).year < day.year
      valid_user = true
      #puts "      #{u['last_name']} was created before, #{Time.parse(u['created_at']).year} <= #{day.year}"
    elsif Time.parse(u['created_at']).year == day.year
      if Time.parse(u['created_at']).mon < day.mon
        valid_user = true
        #puts "      #{u['last_name']} was created before, #{Time.parse(u['created_at']).mon} <= #{day.mon}"
      elsif Time.parse(u['created_at']).mon == day.mon
        if Time.parse(u['created_at']).day <= day.day
          valid_user = true
          #puts "      #{u['last_name']} was created before, #{Time.parse(u['created_at']).day} <= #{day.day}"
        #else
          #puts "  Checking #{u['last_name']} - #{Time.parse(u['created_at'])}"
          #puts "    #{Time.parse(u['created_at']).year}"
        end
      end
    end
    valid_user
  end
  
  def get_days_for_month(year, month)
    # Need to build an array with the right day numbers for the specified month
    mon = Date.new(year, month, 1)
    sday = mon.yday
    eday = mon.next_month.prev_day.yday
    puts "Array(#{sday}..#{eday})" if @debug
    self.log("Range: (#{mon}) #{sday}-#{eday}", __method__, __LINE__)
    Array(sday..eday)
  end
  
  # Harvest specific
  def pull_type_for_month(index, type, year, month)
    #self.initialize_reverse_indexes(index) if @user_id.empty?
    data = []

    puts "\npull_#{type}s:" if @verbose
    self.log("pull_type_for_month('#{index}', '#{type}', #{year}, #{month})", __method__, __LINE__)
    susers = self.users_by_date(index)
    days = self.get_days_for_month(year, month)
    
    days.each do |d|
      day = Date.ordinal(year, d)
      puts "  Getting #{type} data for #{day.year}/#{day.mon}/#{day.day}" if @verbose
      self.log("  Getting #{type} data for #{day.year}/#{day.mon}/#{day.day}", __method__, __LINE__)

      # Iterate over [active] users
      susers.each do |u|
        valid_user = user_valid_for_date(u, day, year, month)
        if valid_user
          self.log("    Get #{type}s for #{u['last_name']}", __method__, __LINE__)
          ut = @harvest.expenses.all(day, u['id']) if type == "expense"
          ut = @harvest.time.all(day, u['id']) if type == "time"
          if ut.size > 0
            puts "---- #{u['email']} ----"
            pp ut
            #puts "    Concat'ing data"
            data.concat(ut)
          else
            puts "      Empty data" if @debug
          end
          @logfile.flush
        else
          break
        end
      end
    end
    
    puts "Found #{data.size} #{type} records" if @verbose
    self.log("Found #{data.size} #{type} records", __method__, __LINE__)
    postfix = sprintf("%d-%02d", year, month)
    @hinst.cache.write_time_cache(type, data, postfix)
    @logfile.flush
  end
  
=begin    
  # Harvest specific
  def pull_invoices(index, year=nil, month=nil)
    @hinst.pull_invoices(year, month)

    #self.initialize_reverse_indexes(index) if @user_id.empty?
    type = "invoice"
    invoice = []

    puts "\npull_invoices: " if @verbose
    self.log("pull_type_for_month('#{index}', '#{type}', #{year}, #{month})", __method__, __LINE__)
    days = self.get_days_for_month(year, month)

    from = Date.ordinal(year, days[0])
    to = Date.ordinal(year, days[-1])
    options = {:timeframe => {:from => from.strftime("%Y%m%d"), :to => to.strftime("%Y%m%d")}}
    self.log("  Getting #{type} data for #{from.year}/#{from.mon}/#{from.day}-#{to.year}/#{to.mon}/#{to.day}", __method__, __LINE__)
    ui = @harvest.invoices.all(options)
    if ui.size > 0
      #puts "    Concat'ing data"
      invoice.concat(ui)
    else
      puts "      Empty data" if @debug
    end
        
    puts "Found #{invoice.size} #{type} records" if @verbose
    self.log("Found #{invoice.size} #{type} records", __method__, __LINE__)
    invoice.each_index do |i|
      invoice[i] = self.merge_csv_into_invoice(index, invoice[i]['id'], @hv)
    end

    postfix = sprintf("%d-%02d", year, month)
    @hinst.cache.write_time_cache(type, invoice, postfix)    
    @logfile.flush

    self.log("Completed", __method__, __LINE__)
    @logfile.flush
  end
=end
  
=begin
  def sync_es_clients()
    type = 'client'
    fields = self.get_type_fields(type)
    pp fields if @debug

    arr = read_cache(index, type, true)
    arr.each do |o|
      pp o if @debug
      puts "#{o['id']}: #{o['name']}"
    end

    _id = o['id']
    body = self.strip_body_fields(o, fields)
    body = self.denormalise_doc(body)

    # Probably the way to do this is to query ES and get the doc modification times only
    # => So, that is get 'client' docs with _id and updated_at fields

    # Compare these with the updated_at times from the cache data and ...
    # Probably need a Hash version of arr. Delete items that don't require update.
    # Then the update list if just the "values" from this Hash
    
    # figure out which docs need to be updated
    # Finally we can update just those docs that require it - hopefully there won't be many
    
  end
=end
    
  def sync_es_expense_categorys()
    type = 'expense_category'
  end
  
  def sync_es_projects()
    type = 'project'
  end
  
  def sync_es_tasks()
    type = 'task'
  end
  
  def sync_es_users()
    type = 'user'
  end
  
  def delete_index(index)
    exist = @elastic.indices.exists?(index: index)
    puts "Check if index '#{index}' exists returns #{exist}" if @verbose
    if exist
      puts "So delete index #{index}" if @verbose
      stat = elastic.indices.delete(index: index)
      exist = !elastic.indices.exists?(index: index)
    else
      deleted = !exist
    end
    deleted
  end
  
  def create_index(index, indexf, force=false)
    if force
      if @elastic.indices.exists?(index: index)
        self.delete_index(index)
      end
    end
    #idx_def = self.read_cache_(indexf)
    idx_def = @hinst.cache.read_cache_(indexf)
    puts "Creating index #{index}"
    #puts idx_def
    created = @elastic.indices.create(index: index, body: idx_def)['acknowledged']
    #puts created
  end
    
  def create_es_index(index, idx_f, force=false)
    indexf = "#{idx_f}.json" if FileTest.exist?("#{idx_f}.json")
    indexf = "#{index}/#{idx_f}.json" if FileTest.exist?("#{index}/#{idx_f}.json")
    #idx_def = self.read_cache_(indexf)
    idx_def = @hinst.cache.read_cache_(indexf)
    if @elastic.indices.exists?(index: index)
      if force
        self.delete_index(index)
        puts "Creating index #{index}"
        #created = @elastic.indices.create(index: index, body: idx_def)['acknowledged']
        created = self.create_index(index, "#{index}/#{idx_f}.json")
      else
        puts "Index #{index} already exists so backing off"
        created = false
      end
    else
      puts "Creating index #{index}"
      #created = @elastic.indices.create(index: index, body: idx_def)['acknowledged']
      created = self.create_index(index, "#{index}/#{idx_f}.json")
    end
    #puts "Created = #{created}"
    created
  end
  
  def index_type(index, type, docs=nil)
    fields = self.get_type_fields(type)
    pp fields if @debug

    self.log("Need to get full #{type} records from cache", __method__, __LINE__)
    #docs = self.read_cache(index, type, true) if docs.nil?
    docs = @hinst.cache.read_cache(type, true) if docs.nil?
    docs.each do |o|
      #pp o
      _id = o[0]
      if type == 'user'
        puts ">>: #{o[1]['id']}: #{o[1]['last_name']}, #{o[1]['first_name']}"
      elsif o[1].keys.include?('name')
        puts ">>: #{o[1]['id']}: #{o[1]['name']}"
      end

      body = self.strip_body_fields(o[1], fields)
      body = self.denormalise_doc(body)

      action = 'skip'
      doc = {}
      rc = {}
  
      # See if this doc alreasy exists
      if @elastic.exists(index: index, type: type, id: _id) 
        #puts "Doc exists"
        # Get the doc from ES
        doc = @elastic.get(index: index, type: type, id: _id)
        action = 'index' if self.is_harvest_doc_newer?(doc, 'updated_at', o[1])
      else
        #puts "Doc doesn't exist"
        action = 'create'
      end
  
      if action != 'skip'
        self.log("  Need to #{action} #{type} doc in ES, id=#{_id}", __method__, __LINE__)
        rc = @elastic.index(index: index, type: type, id: _id, body: body, op_type: action)
      end

      msg = "  Doc operation (#{action})"
      msg = "  Doc operation (#{action}, version=#{rc['_version']})" if action == 'index'
      if @verbose
        puts msg
        log = true if action == 'create' and !rc['created']
        if action == 'index' and rc['_version'] <= doc['_version']
          log = true 
          pp doc
        end
        puts msg if log
        pp rc if log
      end
      @logfile.flush
    end

  end
  
  def index_outdated(index, type)
    #self.initialize_reverse_indexes(index) if @user_id.empty?
    # Need to query ES for all the items that we are being asked to index
    self.log("Updating ES #{type} records", __method__, __LINE__)
    search_results = self.get_type_updated_at(index, type)
    
    hashed_results = self.setup_id_index2(index, type)

    # Then we need to compare the updated_at fields with the corresponding fields from the JSON
    search_results.each do |res|
      #pp res
      id = res.keys.first.to_i
      #pp tcache[id]
      if res.values.first >= hashed_results[id]['updated_at']
        puts "Cached version (#{id}) isn't newer" if @vdebug
        hashed_results.delete(id)
      else
        self.log("  #{type}['#{id}'] is out-of-date in ES")
      end
    end
    # Whatever is left in hashed_results is what needs to be sent to elasticsearch
    puts "Got #{hashed_results.keys.size} docs to update"
    self.log("  Got #{hashed_results.keys.size} docs to update", __method__, __LINE__)
    index_type(index, type, hashed_results) if hashed_results.keys.size > 0
    self.log("Completed", __method__, __LINE__)
  end
  
  def index_outdated_for_month(index, type, year, month)
    #self.initialize_reverse_indexes(index) if @user_id.empty?
    pp @project_id
    # Need to query ES for all the items that we are being asked to index
    self.log("Updating ES #{type} records", __method__, __LINE__)
    search_results = self.get_type_updated_at(index, type, year.to_i, month.to_i)
    
    #puts "------------------------"
    hashed_results = self.setup_id_index2(index, type, year, month)
    #pp search_results
    #pp hashed_results

    # Then we need to compare the updated_at fields with the corresponding fields from the JSON
    search_results.each do |res|
      #pp res
      id = res.keys.first.to_i
      #pp tcache[id]
      if res.values.first >= hashed_results[id]['updated_at']
        puts "Cached version (#{id}) isn't newer" if @vdebug
        hashed_results.delete(id)
      else
        self.log("  #{type}['#{id}'] is out-of-date in ES")
      end
    end
    # Whatever is left in hashed_results is what needs to be sent to elasticsearch
    puts "Got #{hashed_results.keys.size} docs to update"
    self.log("  Got #{hashed_results.keys.size} docs to update", __method__, __LINE__)
    index_type(index, type, hashed_results) if hashed_results.keys.size > 0
    self.log("Completed", __method__, __LINE__)
  end
  
  def get_month_forex(year, month)
    # Need to get relevant usforex data
    mth = Date.new(year, month, 1).strftime('%Y-%m-%d')
    qq = { "filter" => { "match" => { "month" => "#{mth}" }}}
    self.log("Getting usforex data for #{mth}", __method__, __LINE__)
    #pp qq
    res = @elastic.search(index: 'usforex', type: 'currency_rate', body: qq)
    #pp res['hits']['hits']
    forex = []
    res['hits']['hits'].each do |h|
      forex.push(h['_source'])
    end
    #pp forex
    forex
  end
  
  def currency_conv(forex, from, amt)
    conv = {}
    conv['gbp_amount'] = amt if from.eql?('GBP')
    forex.each do |rate|
      if rate['from'].eql?(from) and rate['to'].eql?('GBP')
        conv['gbp_amount'] = amt * rate['rate'].to_f 
      elsif rate['from'].eql?(from) and rate['to'].eql?('USD')
        conv['usd_amount'] = amt * rate['rate'].to_f
      end
    end
    conv
  end
  
  def index_invoices(index, type, year, month)
    #self.initialize_reverse_indexes(index) if @user_id.empty?
    fields = self.get_type_fields(type)
    pp fields if @debug

    # Need to get relevant usforex data
    forex = self.get_month_forex(year, month)

    self.log("Need to get full #{type} records from cache", __method__, __LINE__)
    mth = Date.new(year, month, 1).strftime('%Y-%m')
    iname = "invoice-#{mth}.json"
    self.log("  Reading invoices from file #{index}/#{iname}", __method__, __LINE__)

    #invoices = self.read_time_cache(index, type, "#{mth}")
    invoices = @hinst.cache.read_time_cache(type, "#{mth}")
    invoices.each_index do |i|
      inv = invoices[i]
      _id = inv['id']
      self.log("  Invoice id=#{_id}", __method__, __LINE__)
      inv = self.strip_body_fields(inv, fields)
      currency = 'GBP' if inv['currency'].include?('GBP')
      currency = 'EUR' if inv['currency'].include?('EUR')
      currency = 'DKK' if inv['currency'].include?('DKK')
      inv['csv_line_items'].each do |item|
        amt = item['amount'].to_f
        conv = self.currency_conv(forex, currency, amt)
        item['gbp_amount'] = conv['gbp_amount']
        item['usd_amount'] = conv['usd_amount']
        self.log("    Converted #{currency} as required", __method__, __LINE__)
        self.log("      amount=#{amt}, gbp_amount=#{item['gbp_amount']}, usd_amount=#{item['usd_amount']}", __method__, __LINE__)
      end
      body = self.denormalise_doc(inv)
    
      action = 'skip'
      doc = {}
      rc = {}

      # See if this doc alreasy exists
      if @elastic.exists(index: index, type: type, id: _id) 
        #puts "Doc exists"
        # Get the doc from ES
        doc = @elastic.get(index: index, type: type, id: _id)
        action = 'index' if self.is_harvest_doc_newer?(doc, 'updated_at', body)
      else
        #puts "Doc doesn't exist"
        action = 'create'
      end

      if action != 'skip'
        self.log("  Need to #{action} #{type} doc in ES, id=#{_id}", __method__, __LINE__)
        rc = @elastic.index(index: index, type: type, id: _id, body: body, op_type: action)
      end
      
      @logfile.flush
    end
  end
  
  def get_type_updated_at(index, type, year=nil, month=nil)
    # We only NEED the 'updated_at' field, but 'name' will be useful when debugging
    flds = ["name", "updated_at"]
    
    q = {"match_all" => {}}
    if !year.nil? and !month.nil?
      # We have the option to limit the range if we need it - but we don't for now
      start = Date.new(year, month, 1)
      finish = start.next_month.prev_day
      f = {"range" => {"updated_at" => {"gte" => "#{start}", "le" => "#{finish}"}}}
      f = {"range" => {"spent_at" => {"gte" => "#{start}", "le" => "#{finish}"}}} if type.eql?('expense')
      #pp f
      #qq = {"fields" => flds, "query" => {"filtered" => {"query" => q, "filter" => f}}}
      qq = {"query" => {"filtered" => {"query" => q, "filter" => f}}}
    else
      qq = {"query" => q}
    end
    
    pp qq if @debug
    
    size = 100
    puts "Get hits from 0" if @debug
    self.log("Search ES /#{index}/#{type} with query: #{qq}", __method__, __LINE__)
    res = @elastic.search(index: index, body: qq, type: type, fields: flds, size: size)
    # This first result tells us how many hits there were altogether
    hcount = res['hits']['total']
    self.log("  Got #{hcount} #{type} docs", __method__, __LINE__)

    # Somewhere to put the results
    result = []
    self.log("  Build map of 'updated_at' time for first #{size} docs", __method__, __LINE__)
    res['hits']['hits'].each do |hit|
      result.push({hit['_id'] => hit['fields']['updated_at'].first})
    end
    
    hcount -= size
    from = size
    until hcount < 0 do
      #sleep(1)
      puts "Get hits from #{from}" if @debug
      self.log("  Get next #{size} docs of #{hcount}", __method__, __LINE__)
      res = @elastic.search(index: index, body: qq, type: type, fields: flds, size: size, from: from)
      res['hits']['hits'].each do |hit|
        result.push({hit['_id'] => hit['fields']['updated_at'].first})
      end
      hcount -= size
      from += size
    end
    pp result if @debug
    @logfile.flush
    result
  end
  
  def create_or_update_doc(type, id, doc)
    action = 'create'
    # See if this doc alreasy exists
    if @elastic.exists(index: @index, type: type, id: id) 
      #puts "Doc exists"
      # Get the doc from ES
      esdoc = @elastic.get(index: index, type: type, id: id)
      action = 'index' 
    end

    #if action != 'skip'
    rc = elastic.index(index: @index, type: type, id: id, body: doc, op_type: action)
    #end

    msg = "  Doc operation (#{action})"
    msg = "  Doc operation (#{action}, version=#{rc['_version']})" if action == 'index'
    if @verbose
      puts msg
      log = true if action == 'create' and !rc['created']
      if action == 'index' and rc['_version'] <= esdoc['_version']
        log = true 
        pp doc
      end
      puts msg if log
      pp rc if log
    end
  end

=begin  
  # Deprecated
  def sync_es(type, arr)
    fields = self.get_type_fields(type)
    pp fields if @debug

    arr.each do |o|
      pp o if @debug
      if type == 'user'
        puts "#{o['id']}: #{o['last_name']}"
      else
        puts "#{o['id']}: #{o['name']}"
      end

      _id = o['id']
      body = self.strip_body_fields(o, fields)
      body = self.denormalise_doc(body)

      action = 'skip'
      doc = {}
      rc = {}
  
      # See if this doc alreasy exists
      if @elastic.exists(index: @index, type: type, id: _id) 
        #puts "Doc exists"
        # Get the doc from ES
        doc = @elastic.get(index: @index, type: type, id: _id)
        action = 'index' if self.is_harvest_doc_newer?(doc, 'updated_at', o)
      else
        #puts "Doc doesn't exist"
        action = 'create'
      end
  
      if action != 'skip'
        rc = @elastic.index(index: @index, type: type, id: _id, body: body, op_type: action)
      end

      msg = "  Doc operation (#{action})"
      msg = "  Doc operation (#{action}, version=#{rc['_version']})" if action == 'index'
      if @verbose
        puts msg
        log = true if action == 'create' and !rc['created']
        if action == 'index' and rc['_version'] <= doc['_version']
          log = true 
          pp doc
        end
        puts msg if log
        pp rc if log
      end
    end
  end
=end
end

def get_harvest_handle(opt)
  hinst = ESUtils::MyHarvest.new(File.open("#{File.dirname($0)}/logs/harvest.log", "a"))
  hv = hinst.get_harvest_config(opt.harvest)
  pp hv if opt.debug
  harvest = hinst.init_harvest_client
  return harvest, hv, hinst
end

def get_esu_handle(opt)
  esu = ESUtils.new(opt)
end

def init_elasticsearch(opt, esu)
  es = YAML.load_file(opt.es_config)
  pp es if opt.debug
  # Not exporting, so must be importing into Elasticsearch
  elastic = Elasticsearch::Client.new(host: es['host'], user: es['user'], password: es['pass'])
  esu.set_elasticsearch(opt.index, elastic)
  esu
end


if __FILE__ == $0
  stypes = ESUtils::MyHarvest.class_eval("@@stypes")
  ctypes = ESUtils::MyHarvest.class_eval("@@ctypes")

  opt = ESOptions.parse(ARGV)
  esu = get_esu_handle(opt)

  if opt.export and (opt.rebuild_index or opt.index)
    puts "Can't specify options for export and elasticsearch at the same time"
    puts
    exit(1)
  elsif opt.export
    harvest = get_harvest_handle(opt)
    esu.set_harvest(harvest[0], harvest[1])

    puts "Export data from Harvest"
    # Generally we only want to export one type at a time
    if opt.type.nil?
      puts "Exporting all simple types"
      stypes.each do |type|
        esu.instance_eval("pull_#{type}s")
      end
    else
      puts "Exporting #{opt.type}s"
      if stypes.include?(opt.type)
        esu.instance_eval("pull_#{opt.type}s")
      elsif ctypes.include?(opt.type)
        if opt.year.nil? or opt.month.nil?
          puts "Must specify --year and --month-num"
          #exit
        else
          puts "Have to get some complex data"
          if opt.type == 'expense'
            esu.pull_expenses(opt.year, opt.month)
          elsif opt.type == 'invoice'
            esu.pull_invoices(opt.year, opt.month)
          elsif opt.type == 'time'
            esu.pull_times(opt.year, opt.month)
          end
        end
      end
    end
  elsif opt.index
    esu = init_elasticsearch(opt, esu)

    # Likely want the force parameter driven from the command-line but will do for now
    force = true if opt.rebuild_index
    #force = true # Temporary
    esu.create_es_index(opt.index, force)
    
    puts "Synchronise data with Elasticsearch"
    if opt.type.nil?
      puts "Syncing all simple types"
      stypes.each do |type|
        if opt.rebuild_index
          esu.instance_eval("index_type('#{opt.type}', '#{index}')")
        else
          esu.instance_eval("index_outdated_#{opt.type}s('#{index}')")
        end
      end
    else
      puts "Syncing #{opt.type}s"
      if @hinst.stypes?(opt.type)
        puts "index_outdated_#{opt.type}s('#{index}')"
        esu.instance_eval("index_outdated_#{opt.type}s('#{index}')")
      elsif @hinst.ctypes?(opt.type)
        if opt.year.nil? or opt.month.nil?
          puts "Must specify --year and --month-num"
          #exit
        else
          esu.instance_eval("index_outdated_#{opt.type}s('#{opt.index}', '#{opt.year}', '#{opt.month}')")
        end
      end
    end
      
  end
end
puts


