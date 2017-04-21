#!/usr/bin/ruby

require 'yaml'
require 'sysinfo'

PROGRESS_CELLS = 10 # number of cells for the progress bars

PSWAP_S_THRESH = 1000 # pages per second that is a reasonable maximum.
                      # experimental evidence suggests 1000 or more means
                      # you're running some fucked up shit.
NFS_CALLS_S_THRESH = 1000 # nfs calls per second that is a reasonable maximum.
                          # just a guess

BLAME_NFS_THRESH = 200  # nfs calls before a machine is blamed

## add some utility methods
class SysInfo
  def fsystem_html
    if fsystems.empty? then "" else  " [" + fsystems.map { |fsystem, val| "#{fsystem}: #{val[:tps].to_s}t, #{val[:blkreads].to_s}r, #{val[:blkwrtns].to_s}w" }.listify(" and ", "; ") + "]"  end
  end

  def fsystem_long_html
    if fsystems.empty? then
      "&nbsp;"
    else
      head = <<EOS
<table bgcolor="#F5EEC6" cellpadding="4" cellspacing="0">
<tr><th align="left">filesystem</th><th align="left">tps</th><th align="left">blkreads/s</th><th align="left">blkwrtns/s</th></tr>
EOS
      body =  fsystems.map { |fsystem, val| "<tr><td>#{fsystem}</td><td> #{val[:tps].to_s} </td><td> #{val[:blkreads].to_s} </td><td> #{val[:blkwrtns].to_s} </td></tr>" }.listify("\n", "\n")
      tail = "</table>"
      [head, body, tail].join
    end
  end

  def statustab_html gpu_claim_list, gpu_to_user_list
    ret = <<EOS
<table cellpadding="2" cellspacing="0" bgcolor="#cccccc">
<tr><td align="left" bgcolor="#941414"><font color='white'><b>machine info</b></font></td></tr>
<tr><td align="right">load:</td> <td align="left">#{load1} #{load5} #{load15}</td>
    #{load5.to_bar 8.0, PROGRESS_CELLS, '#5D8896'}</tr>
<tr><td align="right">memory:</td> <td align="left">#{memfree}mb (#{memfree.to_pct memtot}) free</td>
    #{memused.to_bar memtot, PROGRESS_CELLS, '#A3CEDC'}</tr>
<tr><td align="right">swap:</td> <td align="left">#{swapused}mb (#{swapused.to_pct swaptot}) used</td>
    #{swapused.to_bar swaptot, PROGRESS_CELLS, '#CBF6FF'}</tr>
<tr><td align="right">swappage:</td> <td align="left">#{pswapin.nice 1} pg/s in, #{pswapout.nice 1} out</td>
    #{([pswapin + pswapout, PSWAP_S_THRESH].min).to_bar PSWAP_S_THRESH, PROGRESS_CELLS, 'cyan'}</tr>
<tr><td align="right">nfs:</td> <td align="left">#{nfs[:total].nice} calls/s (#{nfs[:getattr].nice}/#{nfs[:read].nice}/#{nfs[:write].nice}) </td>
    #{([nfs[:total], NFS_CALLS_S_THRESH].min).to_bar NFS_CALLS_S_THRESH, PROGRESS_CELLS, '#FFFFA0'}</tr>
</table>
EOS

    ret
  end

def gpu_usage_tab gpu_claim_list, gpu_to_user_list

  ret = ""

    if gpus.length > 0

      gpu_strs = gpus.each_with_index.map { |gpu, i|
        <<EOS
          <tr>
            <td align="left">gpu#{i}: #{gpu[:name]}</td>
            <td align="right">util:</td> <td align="left">#{gpu[:utilization]}%</td>
              #{gpu[:utilization].to_i.to_bar 100, PROGRESS_CELLS, '#5D8896'}
            <td align="right">mem:</td> <td align="left">#{gpu[:memused]} MiB (#{gpu[:memused].to_i.to_pct gpu[:memtot]}) used</td>
              #{gpu[:memused].to_i.to_bar gpu[:memtot], PROGRESS_CELLS, '#A3CEDC'}
            <td align="right">users:</td> <td align="left">#{gpu_to_user_list[i]}</td>
            <td align="right">&nbsp&nbsp&nbspclaim:</td> <td align="left">#{gpu_claim_list[i]}</td>
          </tr>
EOS
      }
      
      ret = 
        <<EOS
        <tr>
          <td colspan="2" bgcolor="#f6eeee">
            <table>
              <tr>
                <th>
                  <td align="left" bgcolor="#941414"><font color='white'><b>GPU info</b></font></td>
                </th>
              </tr>
EOS
      ret += gpu_strs.join "\n"
      ret += "</table>"
      ret += "</td></tr>"
    end

  ret
  end



  def usertab_html luser_info
    even = false

    # simple test to see if any of the jobs on this machine are PBS jobs
    using_pbs = false
    body = nontrivial_lusers.sort_by { |name, h| h[:cpu] }.map do |name, h|
      selected = h[:cmd].select { |what, cpu, mem, extra| (cpu > 0 || mem > 0) && extra[:pbs_job_id] != nil }
      using_pbs = using_pbs || selected.length() > 0
      if using_pbs
          break
      end
    end

    if using_pbs
        pbs_columns = '<th align="left">pbs id</th><th align="left">pbs name</th><th align="left">queue</th><th align="left">priority</th>'
    else
        pbs_columns = ""
    end

    head = <<EOS
<table bgcolor="#F5EEC6" cellpadding="4" cellspacing="0">
<tr><th align="left">luser</th><th align="left">% cpu</th><th align="left">% mem</th><th align="left">time</th><th>cmd</th>#{pbs_columns}</tr>
EOS

    body = nontrivial_lusers.sort_by { |name, h| h[:cpu] }.map do |name, h|
      #cmd = h[:cmd].select { |what, cpu, mem| cpu > SysInfo::TRIVIAL_CPU_THRESH || mem > SysInfo::TRIVIAL_MEM_THRESH }.map { |what, cpu, mem| what }
      # skip PBS MOM jobs
      selected = h[:cmd].select { |what, cpu, mem, extra| (cpu > 0 || mem > 0) and not
        (extra[:full_cmd] =~ /pbs_mom/ or extra[:full_cmd] =~ /mom_priv/ or what =~ /pbs_mom/ or what =~ /mom_priv/) }

      # TODO this could be more efficient
      cmd = selected.map { |what, cpu, mem, extra| what }
      time = selected.map { |what, cpu, mem, extra| extra[:time].to_timestr }
      all_cpu = selected.map { |what, cpu, mem, extra| cpu.to_s }
      all_mem = selected.map { |what, cpu, mem, extra| mem.to_s }
      # TODO full_cmd really isn't presented well currently -- need to split each command into a separate row
      # TODO also, full_cmd is really not the full command (args get lost in sysinfo.rb)
      full_cmd_div = selected.map { |what, cpu, mem, extra| "<div title=\"#{extra[:full_cmd]}\">#{what}</div>" }
      full_cmd = selected.map { |what, cpu, mem, extra| extra[:full_cmd] }
      pbs_job_ids = selected.map { |what, cpu, mem, extra| extra[:pbs_job_id] }
      pbs_job_names = selected.map { |what, cpu, mem, extra| extra[:pbs_job_name] }
      pbs_queues = selected.map { |what, cpu, mem, extra| extra[:pbs_queue] }
      pbs_priority = selected.map { |what, cpu, mem, extra| extra[:pbs_priority] }
        # <td title='#{full_cmd * '   '}' align='left'>#{cmd * '<br>'}</td>

      even = !even
      <<EOS
<tr>
<td align='left'> #{name}</td>
<td align='right'>#{all_cpu * '<br>'}</td>
<td align='right'>#{all_mem * '<br>'}</td>
<td align='right'>#{time * '<br>'}</td>
<td align='left'>#{full_cmd_div * ''}</td>
<td align='left'>#{pbs_job_ids * '<br>'}</td>
<td align='left'>#{pbs_job_names * '<br>'}</td>
<td align='left'>#{pbs_queues * '<br>'}</td>
<td align='left'>#{pbs_priority * '<br>'}</td>
<td align='left'> <font color='#555555'>#{luser_info.nil? ? "": luser_info[name].nil? ? "" : luser_info[name][:note]}</font></td>
</tr>
EOS
    end

    tail = "</table>"
    [head, body, tail].join
  end

# This method isn't used and would need rewriting
  def spectab_html
    <<EOS
<table cellpadding="2" cellspacing="0">
<tr><td align="right">cpu:</td> <td align="left">#{cpunum} x #{cputype}</td></tr>
<tr><td align="right">arch:</td> <td align="left">#{arch}</td></tr>
<!-- <tr><td align="right">kernel:</td> <td align="left">#{kernver}</td></tr> -->
<tr><td align="right">memory:</td> <td align="left">#{memtot}mb total</td></tr>
<tr><td align="right">swap:</td> <td align="left">#{swaptot}mb total</td></tr>
</table>
EOS
  end

  def spectab_short_html
    ## todo: replace this by more cpuinfo decoding -- either flags contains "ht" or siblings != cpu cores in cpuinfo
    cpudesc =
      if (cpunum == cores)
        "#{chips} x #{cputype} (#{cpunum} cores in total)"
      else
        "#{chips} x #{cputype} (#{cores} cores in total, #{cpunum} with hyperthreading)"
      end
    cpudesc = cpudesc.gsub(/\((?:tm|TM|R)\)/, "")
    '<font size="-2">' +
      [cpudesc,
       arch,
       kernver,
       "#{memtot}mb mem",
       "#{swaptot}mb swap",
       "up #{uptime.to_timestr}",
      ].join("&nbsp;&middot;&nbsp;") +
      '</font>'
  end
end

class Numeric
  def to_bar max, num_cells, col="green"
    max = 1 if max == 0
    filled = ([self.to_f / max.to_f, 1.0].min * num_cells).to_i
    filled = num_cells if filled >= num_cells
    ("<td bgcolor='#{col}'>&nbsp;</td>" * filled) +
      ("<td>&nbsp;</td>" * (num_cells - filled))
  end

  def to_pct max
    sprintf "%.0f%%", 100.0 * self.to_f / max.to_f
  end

  def nice digits=0; sprintf "%.#{digits}f", self; end
end

class Array
  ## join elements with 'sep' except for the last two, which are
  ## joined with 'last_sep'.
  def listify last_sep = " and ", sep = ", "
    ret = ""
    each_with_index do |e, i|
      ret += e.to_s +
        case i
        when self.size - 1
          ""
        when self.size - 2
          last_sep
        else
          sep
        end
    end
    ret
  end
end

class String
  def to_aref; "<a href=\"##{self}\">#{self}</a>"; end
  def to_anchor; "<a name=\"#{self}\">#{self}</a>"; end
  def wrap_if tag, cond
    if cond
      "<#{tag}>#{self}</#{tag}>"
    else
      self
    end
  end
end

## start here
h = YAML.load STDIN.read

impressive, down, busy, overloaded, free, freeish, claims, gpu_claims, lusers, servers, info, gpu_to_user, codalab_to_user = h[:impressive], h[:down], h[:busy], h[:overloaded], h[:free], h[:freeish], h[:claims], h[:gpu_claims], h[:lusers], h[:servers], h[:info], h[:gpu_to_user], (h[:codalab_to_user] || {})

gpu_users_flat = []
for user_str in gpu_to_user.values().flatten()
    if not user_str.is_a?(String)
        next
    end
    for user in user_str.split(",")
        gpu_users_flat.push user
    end
end
gpu_user_counts = gpu_users_flat.each_with_object(Hash.new(0)) do |name, h|
    h[name] += 1
end

puts <<EOS
<!--#include virtual="/header.html" -->

<h1>NLP machine status&nbsp;<span style="color: black; font-size: smaller; font-weight: normal;">
&mdash; polled <span id="timer">at #{Time.now}.</span></span></h1>

<script type="text/javascript">
function updateTimer() {
  var start = #{Time.now.to_f * 1000};
  var elapsed = ((new Date).getTime() - start) / 1000;

  var mins = Math.floor(elapsed / 60);
  var secs = Math.floor(elapsed % 60);

  var x = document.getElementById('timer').firstChild;
  if (mins > 0) {
    x.nodeValue = mins + "m " + secs + "s ago";
  }
  else {
    x.nodeValue = secs + "s ago";
  }
}

updateTimer();
setInterval('updateTimer()',1000);
</script>


<h2>Summary</h2>
EOS

if free.empty? && freeish.empty?
  puts "<p>All machines are either busy, down, or claimed.</p>"
else
  puts "<p><b>Free:</b> #{free.map { |name| name.to_aref.wrap_if('del', claims.member?(name)) }.listify}.</p>" unless free.empty?
  puts "<p><b>Spare capacity:</b> #{freeish.map { |name| name.to_aref.wrap_if('del', claims.member?(name)) }.listify}.</p>" unless freeish.empty?
end

unless busy.empty?
  busylist = busy.map do |name|
    ntl = info[name].nontrivial_lusers
    name.to_aref.wrap_if('del', claims.member?(name)) + (ntl.empty? ? "" : " (#{ntl.keys * ", "})")
  end.listify

  puts "<p><b>In use:</b> #{busylist}.</p>"
end

unless overloaded.empty?
  overloadedlist = overloaded.map do |name|
    ntl = info[name].nontrivial_lusers
    name.to_aref.wrap_if('del', claims.member?(name)) + (ntl.empty? ? "" : " (#{ntl.keys * ", "})")
  end.listify

  puts "<p><span style=\"color: red; font-weight: bold;\">Possibly overloaded</span>: #{overloadedlist}.</p>"
end

puts "<p><span style=\"color: red; font-weight: bold;\">No response</span>: #{down.map { |name| name.to_aref }.listify}.</p>" unless down.empty?
puts "<font color='red'>Claimed</font>: " + claims.map { |m, rs| "#{m.to_aref} (" + rs.map { |u, t| u }.listify + ")" }.listify unless claims.empty?

## cdm jun 2007: add fileserver name, now we have two (thanks to William for code help!)
## todo: Look for stat 'D' -- device wait processes for other clues to who's causing NFS load
# fsload = servers.map { |name| info[name].qualitative_load }.listify
fsload = servers.map { |name| "#{name.to_aref}: #{info[name].colored_qualitative_load}#{info[name].fsystem_html}"}.listify + "."

culprits = h[:info].select { |name, m| m.nfs[:total] > BLAME_NFS_THRESH && ! m.server? }.sort_by { |name, m| -m.nfs[:total] }
if ! culprits.empty?
  culpritsdesc = culprits.map do |name, m|
    "#{name.to_aref} making #{m.nfs[:total].nice 1} nfs calls/sec, " +
    if m.nontrivial_lusers.empty?
      "even though no one is really using the CPU - weird"
    else
      "for which we might blame " + m.nontrivial_lusers.map { |name, u| "<b>#{name}</b> (#{'%.1f' % u[:cpu]}% cpu)" }.listify(" or ")
    end
  end
end

puts "<p><b>Fileserver load:</b> #{fsload}"
if ! culprits.empty?
  puts " [<a href=\"#culprits\">Culprits</a>]"
end
puts "</p>"

puts "<p><b>Special machines</b> <i>(not for heavy compute!):</i> #{"jamie".to_aref} &amp; #{"jacob".to_aref} <i>(remote access);</i> #{"nlp".to_aref} <i>(webserver, tomcat);</i> #{"jerome".to_aref} <i>(Jenkins CI);</i> #{"jack".to_aref} <i>(mysql);</i> #{"jay".to_aref} <i>(tape backup).</i></p>"

puts "<p><b>Impressive:</b> " + impressive.map { |u| "#{u} #{lusers[u].nil? ? "": lusers[u][:note].nil? ? "" : "<i>[#{lusers[u][:note]}]</i>"}#{lusers[u].nil? ? "": " (#{'%.1f' % lusers[u][:total_cpu]}% cpu)" }" }.listify + ".</p>" unless impressive.empty?

puts "<p><b>GPUs used:</b> " + gpu_user_counts.sort_by {|k, v| -v}.map {|u, n| "#{u}: #{n}"}.listify + ".</p>"

# CDM Nov 2008: This script runs on juice as users pdm. This bit needs juicy mounted
# if ! impressive.empty?
#   people = `export JAVA_HOME=/u/nlp/packages/java/jdk1.6-current-i686 ; /u/nlp/packages/hadoop/hadoop-current/bin/hadoop job -list | sed '1,2d' | cut -f 4 | sort | uniq -c`
#   people = `/u/nlp/machine-info/hadoop-info.sh`
#   puts "<p>The hadoopers are: #{people}</p>"
# end

puts "<p><b>Documentation:</b> <a href=\"machine-info.shtml\">NLP computer help &amp; rules</a> &middot; <a href=\"machine-info.shtml\#machinespage\">machines page</a>.</p>"
# <a href=\"https://cs.stanford.edu/wiki/nlp-cluster/\">PBS (jude* machines)</a> &middot;

puts "<p><b>For info on CodaLab users go</b> <a href=\"https://codalab.stanford.edu/worksheets/0x8ea918daaabc4a4e92c080a91da6552d/\">here</a></p>"

if !codalab_to_user[:success]
  #puts "<p><b style=\"color: red;\">Warning: CodaLab scraping code failed! Please contact Ice.</b></p>"
end

puts "<h2>Machines</h2>"

#puts "<br>"
#puts "<div class='dropdown'>"
#puts "  <button style='background-color:#800000' class='btn btn-primary dropdown-toggle' type='button' data-toggle='dropdown'>Filter Machines by"
#puts "  <span class='caret'></span></button>"
#puts "  <ul class='dropdown-menu'>"
#puts "    <li><a href='#'>all</a></li>"
#puts "    <li><a href='#'>free</a></li>"
#puts "    <li><a href='#'>spare</a></li>"
#puts "    <li><a href='#'>gpu</a></li>"
#puts  "   </ul>"
#puts " </div>"
#puts "<br>"

#info.sort_by do |name, m|
  #[m.server? ? 1 : 0,
  # m.down? ? 1 : 0,
  # claims.member?(name) ? 1 : 0,
  # m.free? ? 0 : 1,
  # m.freeish? ? 0 : 1,
  # m.cpunum,
  # m.cpu,
  # -m.memfree]

info.each do |name, m|
  puts "<h3>#{name.to_anchor}#{m.server? ? " (fileserver)" : ""}</h3>"
  claims[name].each do |u, t|
    puts "<p><font color='red'>#{u}: #{t}</font></p>"
  end if claims[name]
  if m.down?
    puts "<p><font color='red'>Down? Last update #{m.date}</font></p>"
  end
  gpu_claims_list = {}
  gpu_claims_list.default = "free"
  gpu_users_list = {}
  gpu_users_list.default = "none"
  if gpu_claims[name]
    gpu_claims[name].each do |u, t|
      gpus_claimed = t.split(",")
      gpus_claimed.each do |gpu_name|
        gpu_index = gpu_name[-1, 1]
        gpu_claims_list[gpu_index.to_i] = u
      end
    end
  end
  if gpu_to_user[name]
    gpu_to_user[name].each do |gpu_index,user_list|
      gpu_users_list[gpu_index.to_i] = user_list
    end
  end
  puts <<EOS
#{m.spectab_short_html}
<table>
<tr><td valign='top'>#{m.statustab_html(gpu_claims_list, gpu_users_list)}</td>
    <td valign='top'>#{m.server? ? m.fsystem_long_html : m.nontrivial_lusers.empty? ? "" : m.usertab_html(lusers)}</td>
</tr>
#{m.gpu_usage_tab(gpu_claims_list, gpu_users_list)}
</table>
EOS
  if codalab_to_user[name]
    puts '<b>CodaLab users:</b>'
    puts '<ul>'
    codalab_to_user[name].each do |user, uuids|
      uuids.map! { |x| "<a target=_blank href=https://codalab.stanford.edu/bundles/#{x[0]}/>#{x[0][0,8]}</a> (#{x[1].gsub('failed', 'zombie')})" }
      puts "<li><b>#{user}</b>: #{uuids.join(', ')}</li>"
    end
    puts '</ul>'
  end
end

if ! culprits.empty?
  puts <<EOS
<h3><a name=\"culprits\">Fileserver load culprits</a></h3>
<p>#{culpritsdesc.listify(", and <br> ", ",<br>")}.</p>
EOS
end

puts <<EOS
<!--#include virtual="/footer.html" -->
EOS
