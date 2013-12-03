require 'elaine'
require 'elaine/distributed'
require 'dcell'
# require 'test_add_vertex'

load File.expand_path("../test_add_vertex.rb", __FILE__)
load File.expand_path("../distributed_page_rank_vertex.rb", __FILE__)

DCell.start id: "test.elaine.coordinator", addr: "tcp://127.0.0.1:8090"

Elaine::Distributed::Coordinator.supervise_as :coordinator, partitioner: Elaine::Distributed::MD5Partitioner
sleep
