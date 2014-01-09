require 'elaine/distributed/coordinator'
require 'elaine/instrument/coordinator_instrument'

module Elaine
  module Distributed
    class InstrumentedCoordinator < Elaine::Distributed::Coordinator
      include Elaine::Instrument::CoordinatorInstrument
    end # class InstrumentedCoordinator
  end # module Distributed
end # module Elaine
