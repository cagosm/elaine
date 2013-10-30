# bundle exec ruby triad_census_worker.rb triad.census.elaine.worker1 8101 triad.census.elaine.coordinator




require 'elaine'
require 'elaine/distributed'
require 'dcell'

load File.expand_path("./triad_census_vertex.rb")

node_id = ARGV[0]
port = ARGV[1].to_i
coordinator_node = ARGV[2]

redis_host = ARGV[3] || "slavenode20.cse.usf.edu"



DCell.start id: node_id, addr: "tcp://127.0.0.1:#{port}", registry: {adapter: 'redis', host: redis_host}

Elaine::Distributed::Worker.supervise_as :worker, coordinator_node: coordinator_node
sleep
