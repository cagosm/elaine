# bundle exec ruby triad_census_worker.rb triad.census.elaine.worker1 192.168.0.21 8101 triad.census.elaine.coordinator




require 'elaine'
require 'elaine/distributed'
require 'dcell'

load File.expand_path("./triad_census_vertex.rb")

node_id = ARGV[0]
node_ip = ARGV[1] || "127.0.0.1"
port = ARGV[2].to_i
coordinator_node = ARGV[3]
redis_host = ARGV[4] || "slavenode20.cse.usf.edu"



DCell.start id: node_id, addr: "tcp://#{host_ip}:#{port}", registry: {adapter: 'redis', host: redis_host}

Elaine::Distributed::Worker.supervise_as :worker, coordinator_node: coordinator_node
sleep
