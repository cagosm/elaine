require 'elaine/distributed'
require 'elaine/instrument'
require 'elaine/instrument/worker_instrument'

module Elaine
  module Spec

    class WorkerInstrumentWorker < Elaine::Distributed::Worker      
      include Elaine::Instrument::WorkerInstrument

      # default_state :waiting
      # state :waiting, to: [:running] do
      #   puts "in waiting?"
      # end
      # state :running, to: [:finished]
      # state :finished


      def initialize(*args)
        super(*args)
        puts "instrumented? #{instrumented?}"
        enable_measurement(:superstep)
      end
    end # class WorkerInstrumentWorker
  end # module Spec
end # module Elaine
