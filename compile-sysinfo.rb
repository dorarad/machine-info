#!/usr/bin/ruby

require 'yaml'
require 'ostruct'
require 'sysinfo'
require 'timeout'

## where the machine info yaml pages are
YAML_DIR = "/u/nlp/machine-info"

## for user "claims" and notes
USER_DIR = "/user"
NOTE_FN = ".javanlp-note"
CLAIM_FN = ".javanlp-claims"
SYSTEM_DIR = "/u/nlp/machine-info"
CLAIM_TOO_FN = "claims.txt"
NOTE_MAX_AGE = 5 * 24 * 3600 # (seconds) ignore files older than this

## known lusers. even if these guys aren't running anything we will
## poll their claims.  this list needs updated when new people come.
LUSERS = %w(jrfinkel grenager wtm manning pichuan dramage htseng
            jbrenier jmichels jurafsky rlsnow natec mgalley
            surabhig yhsung wcmac anenkova varung cerd mcdm acvogel)

## total cpu percentage usage threshold for whether a luser is
## "impressive" or not. 500 means 5 cpus.
IMPRESSIVE_THRESH = 500

# UNIMPRESSIVE_THRESH = 150


## ok now we start!
raise "expecting a sequence of machine names as arguments" if ARGV.empty?

## get machine info
machines = ARGV.map { |name| SysInfo.new name, YAML::load_file(File.join(YAML_DIR, "#{name}.yaml")) }.sort_by { |m| m.name }

## get user notes and claims
lusers = {}
machines.each do |m|
  m.lusers.each do |u, h|
    lusers[u] ||= {}
    lusers[u][:total_cpu] ||= 0
    lusers[u][:total_cpu] += h[:cpu]
  end unless m.down?
end

begin
  Timeout::timeout(3) do
    (lusers.keys + LUSERS).uniq.each do |u|
      lusers[u] ||= {}
      note_fn = File.join USER_DIR, u, NOTE_FN
      claim_fn = File.join USER_DIR, u, CLAIM_FN
      if File.exists?(note_fn) && (Time.now - File.mtime(note_fn)) < NOTE_MAX_AGE
        lusers[u][:note] = File.readlines(note_fn).join("<br>").chomp rescue "(can't read #{note_fn})"
      end
      if File.exists?(claim_fn) && (Time.now - File.mtime(claim_fn)) < NOTE_MAX_AGE
        lusers[u][:claims] = Hash[*File.readlines(claim_fn).map { |l| l =~ /^(\S+): (.*)$/ && [$1, $2] }.flatten.compact] rescue {"unknown machine (can't read #{claim_fn})" => ""}
      end
    end
  end
rescue Timeout::Error
  $stderr.puts "timeout reading user notes"
end

claim_too_fn = File.join SYSTEM_DIR, CLAIM_TOO_FN
if File.exists?(claim_too_fn)
  lusers["Use policy"] ||= {}
  lusers["Use policy"][:claims] = Hash[*File.readlines(claim_too_fn).map { |l| l =~ /^(\S+): (.*)$/ && [$1, $2] }.flatten.compact] rescue {"unknown machine (can't read #{claim_too_fn})" => ""}
end

claims = {}
lusers.each do |u, h|
  next unless h[:claims]
  h[:claims].each do |m, t|
    claims[m] ||= []
    claims[m] << [u, t]
  end
end

impressive = lusers.keys.select { |u| (lusers[u][:total_cpu] || 0) >= IMPRESSIVE_THRESH }
# unimpressive = lusers.keys.select { |u| (lusers[u][:total_cpu] || 200 < UNIMPRESSIVE_THRESH }
down = machines.select { |m| m.down? }.map { |m| m.name }
busy = machines.select { |m| !m.server? && m.busy? }.map { |m| m.name }
overloaded = machines.select { |m| !m.server? && m.overloaded? }.map { |m| m.name }
free = machines.select { |m| !m.server? && m.free? && !claims[m] }.map { |m| m.name }
freeish = machines.select { |m| !m.server? && m.freeish? && !claims[m] }.map { |m| m.name }
servers = machines.select { |m| m.server? }.map { |m| m.name }

puts({
  :impressive => impressive,
  :down => down,
  :busy => busy,
  :overloaded => overloaded,
  :free => free,
  :freeish => freeish,
  :claims => claims,
  :lusers => lusers,
  :servers => servers,
  :info => machines.map { |m| [m.name, m] }.to_h,
}.to_yaml)

## old ssh code
#   $stderr.puts "polling #{name}..."
#   h = nil
#   tries = 0
#   begin
#     Timeout::timeout(DELAY) do
#       h = YAML.load `ssh -oPasswordAuthentication=no -x -q wtm@#{name} "ruby -w sysinfo.rb 2> /dev/null"`
#     end
#   rescue Timeout::Error
#     if tries < MAX_TRIES
#       $stderr.puts "down? retrying"
#       tries += 1
#       retry
#     else
#       $stderr.puts "down"
#     end
#   end
#   SysInfo.new name, h
