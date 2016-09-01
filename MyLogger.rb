module MyLogger

  def log(msg, method=nil, lineno=nil)
    _log(msg, lineno, method)
  end
  
  def _log(msg, lineno=nil, method=nil, file=nil)
    now = DateTime.now.strftime("%Y-%m-%d %H:%M:%S.%L")    
    prefix = "[#{lineno}]"              if !lineno.nil?
    prefix = "#{method}#{prefix}"       if !method.nil?
    prefix = "(#{file}) #{prefix}"      if !file.nil?
    logmsg = "#{prefix}: #{msg}"
    @logfile.puts("#{now}: #{logmsg}")  if !@logfile.nil?
  end

end

