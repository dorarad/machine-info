#!/u/bin/ruby

require 'time'

DELTA = 2 * 60 # seconds between updates
DOWN_THRESH = 6 * 60 # seconds until we've decided the machine is dead

IGNORE_USERS = %w(root pdm apache mysql nobody gdm rpc xfs dbus rpcuser smmsp ntp)

## occasionally (actually seems to happen with a strangely high
## frequency) we read between the cp and the mv for the nfs and
## nfs.old files. these parameters control how we delay and retry.
RETRIES = 3
RETRY_DELAY = 1 # seconds

class Numeric
  def min0; self > 0 ? self : 0; end # quickie
  def to_timestr
    days = self / (60 * 60 * 24)
    # don't modulo as days is for > 2 days; was: hours = (self / (60 * 60)) % 24
    hours = self / (60 * 60)
    # don't modulo as hours is for > 2 hours; was mins = (self / 60) % 60
    mins = self / 60
    # don't modulo as mins is for > 2 mins; was secs = self % 60
    secs = self

    case
    when days > 2
      "#{days}d"
    when hours > 2
      "#{hours}h"
    when mins > 2
      "#{mins}m"
    else
      "#{secs}s"
    end
  end
end

class SysInfo
  attr_reader :name

  TRIVIAL_CPU_THRESH = 2.0
  TRIVIAL_MEM_THRESH = 4.0

  def initialize name, hash
    @name = name
    @hash = hash
    @hash[:memused] = @hash[:memtot] - @hash[:memfree]
    ## CDM 2015: Sometimes swapfree is bigger than swaptot. I don't understand why, but then say that used is zero. Known CentOS bug.
    swapused = @hash[:swaptot] - @hash[:swapfree]
    if swapused < 0 then swapused = 0 end
    @hash[:swapused] = swapused
  end

  def cpu; lusers.inject(0) { |tot, (u, h)| tot + h[:cpu] }; end
  def maxcpu; [cpu, load1 * 100].max ; end
## CDM 2007: Using free memory is ineffective because modern linux OSes appear
## to just not free up no longer used memory until someone else needs it....
## && memused < (memtot * 0.9)  ;;  && memused < (memtot * 0.975)
## Try using swap....
  def down?
    require 'time'
    return (Time.now - Time.parse(date.to_s)) > DOWN_THRESH
  end
  def free?; !down? && maxcpu < 25.0 && swapused < (swaptot * 0.5) && (pswapin + pswapout) < 20.0 && ! (name == "jamie") && ! (name == "jacob") && ! (name == "nlp") && ! (name == "jay") && ! (name == "jerome"); end
  def freeish?; !down? && !free? && maxcpu < (cores - 1) * 65.0 && swapused < (swaptot * 0.7) && (pswapin + pswapout) < 150.0; end
  def busy?; !down? && !free? && !freeish? && (maxcpu / 100.0) < (cores + 1.0) && swapused < (swaptot * 0.925) && (pswapin + pswapout) < 600.0; end
  def overloaded?; !down? && !free? && !freeish? && !busy?; end
  def server?; server != false; end
  def qualitative_load
    load5.to_s + " (" +
      case load5
      when 0.0 .. 1.5
        "low"
      when 1.5 .. 3.0
        "moderate"
      when 3.0 .. 4.5
        "high"
      when 4.5 .. 6.0
        "very high"
      else
        "extremely high"
      end + ")"
  end
  def colored_qualitative_load
    load5.to_s + " (" +
      case load5
      when 0.0 .. 1.5
        "low"
      when 1.5 .. 3.0
        "moderate"
      when 3.0 .. 4.5
        "<span style=\"color: orange;\">high</span>"
      when 4.5 .. 6.0
        "<span style=\"color: red;\">very high</span>"
      else
        "<span style=\"color: red; font-weight: bold;\">extremely high</span>"
      end + ")"
  end

  def nontrivial_lusers; lusers.select { |name, h| h[:cpu] > TRIVIAL_CPU_THRESH || h[:mem] > TRIVIAL_MEM_THRESH }.to_h; end

  def method_missing m; @hash[m]; end
  def to_yaml *a; @hash.to_yaml(*a); end
end

class Array
  def to_h; Hash[*self.flatten]; end # should be in ruby core
  def sum; inject { |s, e| s + e }; end # ditto
end

###### EVERYTHING BELOW HERE IGNORED IF LOADED AS A LIBRARY #######
if __FILE__ == $0

require 'yaml'
require 'time'

FILES = %w(meminfo loadavg cpuinfo uptime uname-rp ps-axuwww date vmstat vmstat.old)
CLIENT_FILES = %w(nfs nfs.old)
SERVER_FILES = %w(nfsd nfsd.old fsbusy)
BASE_DIR = "/u/linux/status/"

class String
  def value_of regex
    if self =~ regex then $1 end
  end
end

machine_name = ARGV.shift or raise "expecting one argument: the machine name"
server = ARGV.shift || false
dir = File.join BASE_DIR, machine_name
File.exists?(dir) or raise "#{dir} does not exist"
File.directory?(dir) or raise "#{dir} is not a directory"

f = (FILES + (server ? SERVER_FILES : CLIENT_FILES)).map do |fn|
  tries = RETRIES
  data =
    begin
      File.readlines(File.join(dir, fn)).join
    rescue Errno::ENOENT => e
      unless tries == 0
        tries -= 1
        $stderr.puts "warning: can't read warning: can't read #{File.join(dir,fn)}, sleeping #{RETRY_DELAY}"
        sleep RETRY_DELAY
        retry
      end
      $stderr.puts "error: can't read #{File.join(dir,fn)}; abandoning hope."
      # raise e # die
    end
  [fn, data]
end.to_h

# Read the nvidia-smi-a file separately; this doesn't exist on some systems which
# didn't get the record-system-status update..

begin
  f["nvidia-smi-a"] = File.readlines(File.join(dir, "nvidia-smi-a")).join
rescue
  f["nvidia-smi-a"] = nil
end

# NO LONGER IN USE: read the nvidia-smi file
# begin
#   f["nvidia-smi"] = File.readlines(File.join(dir, "nvidia-smi")).join
# rescue
#   f["nvidia-smi"] = nil
# end

f["loadavg"] =~ /^(\S+) (\S+) (\S+)/
load1 = $1.to_f
load5 = $2.to_f
load15 = $3.to_f

kernver, arch = f["uname-rp"].split
date = Time.parse f["date"]

## parse weird nfs format---pull out v3 numbers (line starting with "proc3", columns 3
## through 24). sadly no labels but you can correlate the numbers with
## the output of nfsstat -cn.
nfs, nfs_old =
  if server
    %w(nfsd nfsd.old)
  else
    %w(nfs nfs.old)
  end.map { |n| f[n].split(/\n/).find { |e| /^proc3/ =~ e }.split(/\s+/)[3 ... 24].map { |x| x.to_i.min0 } }

fsystems = {}
if server
  fsbusy = f["fsbusy"].split(/\n/) # kinda lame since we just joined it, but...
  fsbusy.shift # drop blank line
  fsbusy.shift # drop header
  fsbusy.each do |l|
    fsystem, tps, blkreads, blkwrtns = l.split(" ")
    # $stderr.puts "fsbusy line |#{fsystem}|#{tps}|#{blkreads}|#{blkwrtns}|\n"
    fsystem = fsystem.gsub(/^\/[a-z]+\//, "")
    tps = tps.to_i
    blkreads = blkreads.to_i
    blkwrtns = blkwrtns.to_i
    value = { :tps => tps, :blkreads => blkreads, :blkwrtns => blkwrtns }
    fsystems[fsystem] = value
  end
end

memfree = f["meminfo"].nil? ? 0: f["meminfo"].value_of(/MemFree:\s+(\d+) kB/).to_i
membuffers = f["meminfo"].nil? ? 0: f["meminfo"].value_of(/Buffers:\s+(\d+) kB/).to_i
memcached = f["meminfo"].nil? ? 0: f["meminfo"].value_of(/Cached:\s+(\d+) kB/).to_i
coresperchip = f["cpuinfo"].lines.grep(/core id\s*:\s*\d/).uniq.length
chips = f["cpuinfo"].lines.grep(/physical id\s*:\s*\d/).uniq.length


# Prepare basic high-level GPU information
gpus = []
if not f["nvidia-smi-a"].nil?
   gpus = f["nvidia-smi-a"].scan(/Product Name[ :]*([A-Za-z0-9 ]+).*?FB Memory Usage[ \n]+Total[ :]+(\d+) MiB[\n ]+Used[ :]+(\d+) MiB.*?Utilization[ \n]+Gpu[ :]+(\d+) \%/m).map { |data|
     %i(name memtot memused utilization).zip(data).to_h
   }
end

# NO LONGER IN USE: reading the nvidia-smi file
# gpus = []
# if not f["nvidia-smi"].nil?
#   gpus = f["nvidia-smi"].scan(/[ \|]+\d+  ([A-Za-z0-9]+) ([A-Za-z0-9]+).+\|.+\|.+\|\n.*(\d+)MiB \/ (\d+)MiB[ |]*(\d+)%/).map { |data|
#     %i(name perf memused memtot utilization).zip(data).to_h
#   }
# end

status = {
  :memtot => f["meminfo"].nil? ? 0: f["meminfo"].value_of(/MemTotal:\s+(\d+) kB/).to_i / 1024,
  :memfree => (memfree + membuffers + memcached) / 1024,
  :swaptot => f["meminfo"].nil? ? 0: f["meminfo"].value_of(/SwapTotal:\s+(\d+) kB/).to_i / 1024,
  :swapfree => f["meminfo"].nil? ? 0: f["meminfo"].value_of(/SwapFree:\s+(\d+) kB/).to_i / 1024,

  :kernver => kernver,
  :arch => arch,

  :cputype => f["cpuinfo"].value_of(/model name\s*:\s*(.*)$/),
  # cpunum is number of execution contexts: counts 2 for each hyperthreaded core
  :cpunum => f["cpuinfo"].lines.grep(/processor\s*:\s*\d/).length,
  :chips => chips,
  :cores => coresperchip * chips,

  :uptime => (f["uptime"].nil? ? "0" : f["uptime"].value_of(/^(\d+)/)).to_i,
  :load1 => load1,
  :load5 => load5,
  :load15 => load15,

  :pswapin => ((f["vmstat"].nil? ? 0.0 : f["vmstat"].value_of(/pswpin\s+(\d+)/)).to_f -
    (f["vmstat.old"].nil? ? 0.0 : f["vmstat.old"].value_of(/pswpin\s+(\d+)/)).to_f).min0 / DELTA.to_f,

  :pswapout => ((f["vmstat"].nil? ? 0.0 : f["vmstat"].value_of(/pswpout\s+(\d+)/)).to_f -
    (f["vmstat.old"].nil? ? 0.0 : f["vmstat.old"].value_of(/pswpout\s+(\d+)/)).to_f).min0 / DELTA.to_f,

  :date => date,

  :gpus => gpus,

  :nfs => {
    :total => (nfs.sum - nfs_old.sum).to_f.min0 / DELTA.to_f,
    :getattr => (nfs[0] - nfs_old[0]).to_f.min0 / DELTA.to_f,
    :read => (nfs[5] - nfs_old[5]).to_f.min0 / DELTA.to_f,
    :write => (nfs[6] - nfs_old[6]).to_f.min0 / DELTA.to_f,
  },

  :server => server,
  :fsystems => fsystems,

}

lusers = {}
jobs = f["ps-axuwww"].split(/\n/) # kinda lame since we just joined it, but...
jobs.shift # drop header
jobs.each do |l|
  u, pid, cpu, mem, vsz, rss, tty, stat, start, time, cmd, *args = l.split
  cpu = cpu.to_f
  mem = mem.to_f
  pid = pid.to_i

  next if IGNORE_USERS.member? u

  time =~ /(\d+):(\d+)/
  time = ($1.to_i * 60) + $2.to_i
  lusers[u] ||= Hash.new 0
  lusers[u][:cpu] += cpu
  lusers[u][:mem] += mem
  lusers[u][:time] = [lusers[u][:time], time].max
  ## deal with people with absolute path
  cmd = cmd.gsub(/^\/juic[a-z]+\/(?:u|scr)\d+\/(u|scr)\//, '/\1/')
  if cmd =~ /^(?:\/bin)?\/?bash$/
    cmd = args[0]
    args = args[1, args.length]
  end
  full_cmd = cmd
  cmd =
    case cmd
    ## java doesn't have to be initial to allow for aliases in people's dirs
    when /java\d?(-\d\d)?$/
      if cmd =~ /^\/u\/nlp\/packages\/java\/(.*)$/
        name = $1
      elsif cmd =~ /jdk1.\d-current-x86_64\/bin\/(java)/
        name = $1
      else
        name = cmd
      end
      origname = name
      ## try to treat scala specially
      ## otherwise just look for something with a . in it or something after a -jar
      args.each_with_index do |a, i|
        a = a.gsub(/^\/juic[a-z]+\/(?:u|scr)\d+\/(u|scr)\//, '/\1/')
        if a =~ /scala\.tools\.nsc\.MainGenericRunner/
          name = a
          # but keep on looking for a scala class
        elsif i > 0 && a =~ /\.jar$/
          name = a
          name = name.gsub(/^\/u\/nlp\/packages\//, "")
          # but keep looking in case they're not using -jar
        elsif a =~ /^([\w\d_]+[.\/])*([\w\d$_]+)$/ &&
          (i == 0 || args[i - 1] !~ /^-(cp|classpath|i)$/)
          name = a
          break
        end
      end
      if name.length > 300 || origname == name
        origname + " {with an obscenely long classpath so the class name was lost}"
      else
        name.gsub(/^edu\.stanford\.nlp\./, "")
      end
    when /scala$/
      name = cmd
      ## try to treat scala specially
      ## otherwise just look for something with a . in it or something after a -jar
      args.each_with_index do |a, i|
        a = a.gsub(/^\/juic[a-z]+\/(?:u|scr)\d+\/(u|scr)\//, '/\1/')
        if a =~ /scala\.tools\.nsc\.MainGenericRunner/
          name = a
          # but keep on looking for a scala class
        elsif i > 0 && a =~ /\.jar$/
          name = a
          name = name.gsub(/^\/u\/nlp\/packages\//, "")
          # but keep looking in case they're not using -jar
        elsif a =~ /^([\w\d_]+[.\/])*([\w\d$_]+)$/ &&
          (i == 0 || args[i - 1] !~ /^-(cp|classpath|i)$/)
          name = a
          break
        end
      end
      name.gsub(/^edu\.stanford\.nlp\./, "")
    when /(perl|ruby|python)$/
      name = cmd
      ## look for the first thing that doesn't start with a - and
      ## isn't proceeded by a -I
      args.each_with_index do |a, i|
        a = a.gsub(/^\/juic[a-z]+\/(?:u|scr)\d+\/(u|scr)\//, '/\1/')
        if a =~ /^[^-]/ &&
          (args.length == 0 || args[i - 1] !~ /^-I$/)
          name = a
          break
        end
      end
      name.gsub(/^\/u\/nlp\/packages\//, "")
    when /^\/u\/nlp\/packages\/(.*)$/
      ## Things in /u/nlp/packages
      $1
    when /\/([^\/]+)$/
      $1
    else
      cmd
    end

  lusers[u][:cmd] = [] if lusers[u][:cmd] == 0
  lusers[u][:cmd].push [cmd, cpu, mem, { :pid => pid, :time => time, :full_cmd => full_cmd } ]
end

status[:lusers] = lusers
puts SysInfo.new(machine_name, status).to_yaml

end
