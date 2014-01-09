require 'elaine'
require 'elaine/distributed'
require 'dcell'

load File.expand_path("../../vertices/instrument_vertex.rb", __FILE__)
load File.expand_path("../../vertices/message_sender_vertex.rb", __FILE__)
load File.expand_path("../../workers/worker_instrument_worker.rb", __FILE__)
load File.expand_path("../../post_offices/post_office_instrument_post_office.rb", __FILE__)

DCell.start id: "test.elaine.worker1", addr: "tcp://127.0.0.1:8091"
Celluloid.logger.level = ::Logger::WARN
Elaine::Spec::PostOfficeInstrumentPostOffice.supervise_as :postoffice, partitioner: Elaine::Distributed::MD5Partitioner
Elaine::Spec::WorkerInstrumentWorker.supervise_as :worker, coordinator_node: "test.elaine.coordinator"

sleep
