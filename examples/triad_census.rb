require 'dcell'

# CORRECT ANSWER FOR erdos-renyi-N_1000-E_0.2.egonets
# 0: 85449930
# 1: 63619591
# 2: 15793662
# 3: 1303817


load File.expand_path("./triad_census_vertex.rb")

DCell.setup
DCell.run!

graph_to_load = ARGV[0]

graph = []

File.open(graph_to_load).lines do |line|
  a = line.strip.split

  vertex = {}

  vertex[:id] = "n_#{a.shift}".to_sym
  vertex[:klazz] = TriadCensusVertex
  vertex[:outedges] = a.map { |v| "n_#{v}".to_sym}
  vertex[:value] = {}

  graph << vertex
end

puts "There are #{graph.size} nodes"

puts "Loading graph into coordinator node"

coordinator_node = DCell::Node["triad.census.elaine.coordinator"]
coordinator_node[:coordinator].graph = graph

puts "Running job"
coordinator_node[:coordinator].run_job

# zipcodes = coordinator_node[:coordinator].zipcodes

n = graph.size

total_triads_possible = ((1 / 6.0) * n * (n - 1) * (n - 2)).to_i
total_dyads_possible = ((1 / 2) * n * (n - 1)).to_i

out_val = { type0: 0, type1: 0, type2: 0, type3: 0 }
vertex_values = coordinator_node[:coordinator].vertex_values
vertex_values.each do |v|
  puts "node #{v[:id]} reports #{v[:value][:type2]} type 2 triads"
  puts "node #{v[:id]} reports #{v[:value][:type3]} type 3 triads"
  out_val[:type2] += v[:value][:type2]
  out_val[:type3] += v[:value][:type3]
  # if (total_dyads_possible < (v[:value][:type1_local]))
  #   puts "node #{v[:id]} reporting more type1 local involvement than possible! (#{v[:value][:type1_local]})"
  # end

  if v[:value][:type1_local] < 0
    if v[:value][:type1_local].abs > n
      puts "node #{v[:id]} reporting more type 1 local involvement than possible!"
      out_val[:type1] += (n - v[:value][:type1_local])
    end
  end
  
  # out_val[:type1] += (n - (v[:value][:type2] + v[:value][:type3]))
  # out_val[:type2] += v[:type2]
  # out_val[:type3] += v[:type3]
end

out_val[:type0] =  total_triads_possible - (out_val[:type1] + out_val[:type2] + out_val[:type3])

puts "*"*20
puts "Results:"
puts out_val
puts "*"*20



