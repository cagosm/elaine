require 'elaine/distributed/worker'
require 'elaine/instrument/worker_instrument'

module Elaine
  module Distributed
    class InstrumentedWorker < Elaine::Distributed::Worker
      include Elaine::Instrument::WorkerInstrument
    end # class InstrumentedWorker
  end # module Distributed
end # module Elaine
