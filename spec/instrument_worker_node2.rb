require 'elaine'
require 'elaine/distributed'
require 'dcell'

# load File.expand_path("../test_add_vertex.rb", __FILE__)
# load File.expand_path("../distributed_page_rank_vertex.rb", __FILE__)
load File.expand_path("../instrument_vertex.rb", __FILE__)

DCell.start id: "test.elaine.worker2", addr: "tcp://127.0.0.1:8092"

# Celluloid.logger.level = ::Logger::WARN
Elaine::Distributed::PostOffice.supervise_as :postoffice, partitioner: Elaine::Distributed::MD5Partitioner
Elaine::Distributed::Worker.supervise_as :worker, coordinator_node: "test.elaine.coordinator"

sleep
