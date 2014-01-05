require 'elaine/distributed'
require 'elaine/instrument'
require 'elaine/instrument/coordinator_instrument'

module Elaine
  module Spec

    class InstrumentCoordinator < Elaine::Distributed::Coordinator
      
      include Elaine::Instrument

      def run_job

        super
        puts "benchmark: #{instrument_measurements}"

      end

      # def initialize(graph: nil, num_partitions: 1, stop_condition: Celluloid::Condition.new, partitioner: Elaine::Distributed::MD5Partitioner)
      # def initialize(*args)
      #   # super(graph: graph, num_partitions, stop_condition, partitioner)
      #   super(*args)
      #   # add_to_method(:measure, :run_job)
      #   # puts "instance methods:"
      #   # puts  self.methods
      #   # puts "*" * 10
      #   # puts "class methods:"
      #   # add_to_method "run_job", "measure_run_job"
      #   # add_to_method "superstep", "measure_superstep"
      #   enable_measurement(:run_job)
      #   enable_measurement(:superstep)
      # end
    end # class InstrumentCoordinator
  end # module Spec
end # module Elaine
