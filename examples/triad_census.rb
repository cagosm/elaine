require 'dcell'

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

out_val = { type2: 0, type3: 0 }

coordinator_node[:coordinator].vertex_values.each do |v|
  out_val[:type2] += v[:type2]
  out_val[:type3] += v[:type3]
end

puts "*"*20
puts "Results:"
puts out_val
puts "*"*20



