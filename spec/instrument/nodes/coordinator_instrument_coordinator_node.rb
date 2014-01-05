require 'elaine'
require 'elaine/distributed'
require 'dcell'



load File.expand_path("../../coordinators/coordinator_instrument_coordinator.rb", __FILE__)
# require 'test_add_vertex'

load File.expand_path("../../vertices/instrument_vertex.rb", __FILE__)
# load File.expand_path("../instrument_page_rank_vertex.rb", __FILE__)


DCell.start id: "test.elaine.coordinator", addr: "tcp://127.0.0.1:8090"

Celluloid.logger.level = ::Logger::WARN
Elaine::Spec::CoordinatorInstrumentCoordinator.supervise_as :coordinator, partitioner: Elaine::Distributed::MD5Partitioner
sleep

