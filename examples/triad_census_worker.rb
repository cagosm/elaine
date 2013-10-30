require 'elaine'
require 'elaine/distributed'
require 'dcell'

load File.expand_path("./triad_census_vertex.rb")

node_id = ARGV[0]
port = ARGV[1].to_i
coordinator_node = ARGV[2]



DCell.start id: node_id, addr: "tcp://127.0.0.1:#{port}"

Elaine::Distributed::Worker.supervise_as :worker, coordinator_node: coordinator_node
sleep
