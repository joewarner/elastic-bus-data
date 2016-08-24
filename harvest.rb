#!/usr/bin/env ruby

$LOAD_PATH.unshift File.expand_path($0)

load 'esoptions.rb'

require 'elasticsearch'
require 'harvested'
require 'json'
require 'pp'
require 'yaml'
require 'csv'

class ESUtils
  
  attr_reader :elastic, :index
  
  @@stypes = %w{ client expense_category project task user }
  @@ctypes = %w{ expense invoice time }

  #class Harvest
  #end
  
  def log(msg, method=nil, lineno=nil)
    now = DateTime.now.strftime("%Y-%m-%d %H:%M:%S.%L")    
    logmsg = ""
    if method.nil?
      logmsg = "#{msg}"
    else
      mthd = method
      mthd = "#{method}[#{lineno}]" if !lineno.nil?
      logmsg = "#{mthd}: #{msg}"
    end
    @logfile.puts("#{now}: #{logmsg}")
  end
  
  def initialize(opt)
    @debug = false
    @verbose = false
    @logfile = File.open("#{File.dirname($0)}/logs/out.log", "w+")
    
    @index = nil
    
    @client_id = {}
    @expense_category_id = {}
    @project_id = {}
    @task_id = {}
    @user_id = {}
    
    self.log("Completed", __method__, __LINE__)
    self.set_opt(opt)
  end
  
  def initialize_reverse_indexes(index)
    self.log("Entering", __method__, __LINE__)
    self.setup_client_ids(index)            if @client_id.empty?
    self.setup_expense_category_ids(index)  if @expense_category_id.empty?
    self.setup_project_ids(index)           if @project_id.empty?
    self.setup_task_ids(index)              if @task_id.empty?
    self.setup_user_ids(index)              if @user_id.empty?
    self.log("Completed", __method__, __LINE__)
    self
  end
  
  def set_opt(opt)
    @debug = opt.debug
    @verbose = opt.verbose
  end

  def set_harvest(harvest, hv=nil)
    @harvest = harvest
    @hv = hv
  end
  
  def set_elasticsearch(idx, es)
    @elastic = es
    @index = idx
    puts "Setting default index as '#{@index}'" if @verbose
  end
  
  def method_missing(*args)
    m = args.shift
    self.log("Method called = #{m}", __method__, __LINE__)
    
    if type = m[/^setup_([a-z_]+)_ids$/,1] and @@stypes.include?(type)

      puts "method_missing: #{m}, #{type}" if @debug
      self.log("  Extracted type = #{type}", __method__, __LINE__)
      index = args.shift
      puts "Make call to setup_id_index('#{index}', '#{type}')" if @verbose
      self.log("  Handing off to setup_id_index('#{index}', '#{type}')", __method__, __LINE__)
      self.setup_id_index(index, type)
      eval("pp @#{type}_id if @debug")
      
    elsif type = m[/^pull_([a-z_]+)s$/,1] and @@stypes.include?(type)
      
      puts "method_missing: #{m}, #{type}" if @debug
      self.log("  Extracted type = #{type}", __method__, __LINE__)
      index = args.shift
      puts "Make call to pull_type('#{index}', '#{type}')" if @verbose
      self.log("  Handing off to pull_type('#{index}', '#{type}')", __method__, __LINE__)
      self.pull_type(index, type)
      
    elsif type = m[/^pull_([a-z_]+)s$/,1] and @@ctypes.include?(type)
      
      puts "method_missing: #{m}, #{type}" if @debug
      self.log("  Extracted type = #{type}", __method__, __LINE__)
      index = args.shift
      year = args.shift.to_i
      month = args.shift.to_i
      puts "Make call to pull_type_for_month('#{index}', '#{type}', #{year}, #{month})" if @verbose
      self.log("  Handing off to pull_type_for_month('#{index}', '#{type}', #{year}, #{month})", __method__, __LINE__)
      self.pull_type_for_month(index, type, year, month)
      
    elsif type = m[/^index_outdated_([a-z_]+)s$/,1] and @@stypes.include?(type)
      
      puts "method_missing: #{m}, #{type}" if @debug
      self.log("  Extracted type = #{type}", __method__, __LINE__)
      index = args.shift
      puts "Make call to index_outdated('#{index}', '#{type}')" if @verbose
      self.log("  Handing off to index_outdated('#{index}', '#{type}')", __method__, __LINE__)
      self.index_outdated(index, type)

    elsif type = m[/^index_outdated_([a-z_]+)s$/,1] and @@ctypes.include?(type)
      
      puts "method_missing: #{m}, #{type}" if @debug
      self.log("  Extracted type = #{type}", __method__, __LINE__)
      index = args.shift
      year = args.shift.to_i
      month = args.shift.to_i
      puts "Make call to index_outdated('#{index}', '#{type}', '#{year}', '#{month}')" if @verbose
      if type == 'invoice'
        self.log("  Handing off to index_invoices('#{index}', '#{type}')", __method__, __LINE__)
        self.index_invoices(index, type)
      else
        self.log("  Handing off to index_outdated_for_month('#{index}', '#{type}', #{year}, #{month})", __method__, __LINE__)
        self.index_outdated_for_month(index, type, year, month)
      end

=begin      
    elsif type = m[/^index_([a-z_]+)s$/,1] and @@stypes.include?(type)
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
  
  # Methods for caching Harvest data locally
  # The read & write cache methods are probably generally useful
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

  def read_cache(index, type, unwind=false, postfix=nil)
    fname = "#{index}/#{type}"
    fname = "#{fname}-#{postfix}" if postfix
    puts "Reading #{type} records from cache file #{fname}.json" if @verbose
    self.log("Reading #{type} records from cache file #{fname}.json", __method__, __LINE__)
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
  
  def write_cache(index, type, data, postfix=nil)
    fname = "#{index}/#{type}"
    fname = "#{fname}-#{postfix}" if postfix
    puts ".. and writing cache file #{fname}.json" if @verbose
    self.log("Writing to file #{fname}", __method__, __FILE__)
    self.write_cache_("#{fname}.json", data)
    # If we have just cached the data then make sure that we read it back from the file
    self.read_cache(index, type, true)
  end
  
  def read_time_cache(index, type, postfix=nil)
    self.log("Reading from time cache", __method__, __LINE__)
    data = self.read_cache(index, type, true, postfix)
    self.log("Reading #{data.size} records from time cache", __method__, __LINE__)
    data
  end

  def write_time_cache(index, type, data, postfix=nil)
    fname = "#{index}/#{type}"
    fname = "#{fname}-#{postfix}" if postfix
    puts "Writing cache file #{fname}.json" if @verbose
    self.log("Writing to file #{fname}.json", __method__, __LINE__)
    self.write_cache_("#{fname}.json", data)
    puts ".. and reading cache file #{fname}.json" if @verbose
    self.log(".. and reading cache file #{fname}.json", __method__, __LINE__)
    self.read_time_cache(index, type, postfix)
  end
  
  def get_invoice(index, id, hv)
    fname = "#{index}/inv-#{id}.json"
    creds = "#{hv['username']}:#{hv['password']}"
    if !File.exists?(fname)
      msg = "Need to fetch invoice=#{id}"
      url = "https://#{hv['subdomain']}.harvestapp.com/invoices/#{id}"
      cmd = "curl -u #{creds} -XGET -i '#{url}' -H 'Accept: application/json' -L 2> /dev/null | tail -r | head -n 1 > #{fname}"
      %x{ #{cmd} }
    else
      msg = "Found invoice=#{id} in cache"
    end
    self.log("#{msg}", __method__, __LINE__)
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
    self.log("Found #{cli.size} line items on invoice", __method__, __LINE__)
    csv_line_items = Array.new
    cli.each do |line|
      i = 0
      c = {}
      line.each do |col|
        c[cols[i]] = col
        i += 1
      end
      c = self.denormalise_doc(c)
      csv_line_items.push(c)
    end

    #pp csv_line_items
    csv_line_items
  end

  def merge_csv_into_invoice(index, id, hv)
    self.log("Merge invoice csv_line_items, id=#{id}", __method__, __LINE__)
    invoice = self.get_invoice(index, id, hv)
    csv_line_items = self.get_csv_line_items(invoice) 
    invoice['invoice']['csv_line_items'] = csv_line_items
    self.log("Completed merge invoice csv_line_items", __method__, __LINE__)
    invoice
  end

  # This method is probably Harvest specific
  def setup_id_index(index, type)
    # The @<type>_id maps are helpful for de-normalising docs before indexing them
    puts "Setup #{type}_id index" if @verbose
    self.log("Setup #{type}_id index", __method__, __LINE__)
    cmd = "self.read_cache('#{index}', '#{type}', true)"
    puts cmd if @debug
    arr = eval(cmd)
    arr.each do |element|
      name = "#{element['name']}"
      name = "#{element['last_name']}, #{element['first_name']}" if type == 'user'
      cmd = "@#{type}_id[element['id']] = name"
      eval(cmd)
    end
  end

  def setup_id_index2(index, type, year=nil, month=nil)
    puts "Setup #{type}_id2 index" if @verbose
    self.log("Setup #{type}_id2 index", __method__, __LINE__)
    if !year.nil? and !month.nil?
      postfix = sprintf("%d-%02d", year, month)
      cmd = "self.read_time_cache(index, type, postfix)"
    else
      cmd = "self.read_cache('#{index}', '#{type}', true)"
    end
    puts cmd if @debug
    arr = eval(cmd)
    # Want a map of objects indexed by their id
    idmap = {}
    arr.each do |element|
      idmap[element['id']] = element
    end
    idmap = augment_clients(index, type, idmap) if type.eql?('client')
    self.log("Completed", __method__, __LINE__)
    idmap
  end
  
  def augment_clients(index, type, idmap)
    # A bit of a hack for now
    self.log("Adding location data", __method__, __LINE__)
    loc = self.read_cache_("#{index}/#{type}-location.json")
    loc.each do |locn|
      id = locn['client']['id']
      #pp id
      idmap[id]['location'] = locn['client']['location']
      #pp idmap[id]
    end
    #pp loc
    idmap
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
  
  def denormalise_doc(element)
    keys = element.keys
    #pp keys
    self.log("Denormalising doc", __method__, __LINE__)
    keys.each do |key|
      if key == 'client_id' or key == 'user_id' or key == 'project_id' or key == 'expense_category_id'
        ekey = eval("element['#{key}']") # e.g. ekey = element['user_id']
        self.log("  Denormalising #{key} = #{ekey}", __method__, __LINE__)
        if !ekey.nil?
          typid = {}
          if key == 'project_id' and ekey == 'false'
            self.log("    project_id = #{ekey}", __method__, __LINE__)
            typid = {'id' => 0, 'name' => "Unknown"}
          else
            typid['id'] = ekey # e.g. element['user_id]
            cmd = "typid['name'] = @#{key}[#{ekey}]" # @user_id[ekey]
            puts cmd if @verbose
            eval(cmd)
            self.log("    #{cmd} => '#{typid['name']}'", __method__, __LINE__)
            puts "#{key} = '#{ekey}'" if @verbose
            puts "@#{key}['#{ekey}'] => '#{typid['name']}'" if @debug # e.g. "user_id['854934'] => 'Baykal, Ali'"
          end
          pp typid if @debug
          element[key] = typid
        else
          self.log("  Unable to denormalise #{key}", __method__, __LINE__)
        end
      end
      pp element if @debug
    end
    self.log("Completed", __method__, __LINE__)
    element
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
    users = self.read_cache(index, 'user', true)
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
  def pull_type(index, type)
    cmd = "@harvest.#{type}s.all"
    cmd = "@harvest.expense_categories.all" if type == 'expense_category'
    puts "Reading #{type} records from Harvest - #{cmd}" if @verbose
    self.log("Reading #{type} records from Harvest - #{cmd}", __method__, __LINE__)
    data = eval(cmd)
    self.log("Read #{data.size} records", __method__, __LINE__)
    self.write_cache(index, type, data)
    @logfile.flush
    data
  end
  
  # Harvest specific
  def pull_type_for_month(index, type, year, month)
    self.initialize_reverse_indexes(index) if @user_id.empty?
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
    self.write_time_cache(index, type, data, postfix)
    @logfile.flush
  end
  
  # Harvest specific
  def pull_invoices(index, year=nil, month=nil)
    self.initialize_reverse_indexes(index) if @user_id.empty?
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
    postfix = sprintf("%d-%02d", year, month)
    self.write_time_cache(index, type, invoice, postfix)    
    @logfile.flush
    invoice.each do |inv|
      self.get_invoice(index, inv['id'], @hv)
      #self.merge_csv_into_invoice(index, inv['id'], @hv)
    end
    self.log("Completed", __method__, __LINE__)
    @logfile.flush
  end
  
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
  
  def delete_index()
    exist = @elastic.indices.exists?(index: @index)
    puts "Check if index '#{@index}' exists returns #{exist}" if @verbose
    if exist
      puts "So delete index #{@index}" if @verbose
      stat = elastic.indices.delete(index: @index)
      exist = !elastic.indices.exists?(index: @index)
    else
      deleted = !exist
    end
    deleted
  end
  
  def create_es_index(index, force=false)
    idx_def = self.read_cache_("#{index}/#{index}.json")
    if @elastic.indices.exists?(index: index)
      if force
        self.delete_index()
        puts "Creating index #{index}"
        created = @elastic.indices.create(index: index, body: idx_def)['acknowledged']
      else
        puts "Index #{index} already exists so backing off"
        created = false
      end
    else
      puts "Creating index #{index}"
      created = @elastic.indices.create(index: index, body: idx_def)['acknowledged']
    end
    #puts "Created = #{created}"
    created
  end
  
  def index_type(index, type, docs=nil)
    fields = self.get_type_fields(type)
    pp fields if @debug

    self.log("Need to get full #{type} records from cache", __method__, __LINE__)
    docs = self.read_cache(index, type, true) if docs.nil?
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
    self.initialize_reverse_indexes(index) if @user_id.empty?
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
    self.initialize_reverse_indexes(index) if @user_id.empty?
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
  
  def index_invoices(index, type)
    self.initialize_reverse_indexes(index) if @user_id.empty?
    fields = self.get_type_fields(type)
    pp fields if @debug

    self.log("Need to get full #{type} records from cache", __method__, __LINE__)
    
    Dir.new(index).each do |fname|
      iname = fname[/^inv-[0-9]{5,}.json$/]
      if !iname.nil?
        inv = {}
        self.log("  Reading invoice from file #{index}/#{iname}", __method__, __LINE__)
        File.open("#{index}/#{iname}", 'r') do |data|
          inv = JSON.load(data)
        end
        _id = inv[type]['id']
        self.log("  Invoice id=#{_id}", __method__, __LINE__)
        inv[type] = self.strip_body_fields(inv[type], fields)
        inv[type]['csv_line_items'] = self.get_csv_line_items(inv)
        body = self.denormalise_doc(inv[type])
        
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
  hv = YAML.load_file(opt.harvest)
  pp hv if opt.debug
  harvest = Harvest.client(subdomain: hv['subdomain'], 
                            username: hv['username'], 
                            password: hv['password'])
  return harvest, hv
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
  stypes = ESUtils.class_eval("@@stypes")
  ctypes = ESUtils.class_eval("@@ctypes")

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
      if stypes.include?(opt.type)
        puts "index_outdated_#{opt.type}s('#{index}')"
        esu.instance_eval("index_outdated_#{opt.type}s('#{index}')")
      elsif ctypes.include?(opt.type)
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


