require 'elaine'
require 'elaine/distributed'
require 'dcell'
# require 'test_add_vertex'

puts "__FILE__: #{__FILE__}"
load File.expand_path("./triad_census_vertex.rb")

DCell.start id: "triad.census.elaine.coordinator", addr: "tcp://127.0.0.1:8100"

Elaine::Distributed::Coordinator.supervise_as :coordinator
sleep
