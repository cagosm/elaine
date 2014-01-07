require 'benchmark'
require 'elaine/instrument'

module Elaine
  module Instrument
    module CoordinatorInstrument
      include Elaine::Instrument
      # extend Elaine::Instruments::Instrument

      class << self
        def included(klass)
          klass.send :include, InstanceMethods
        end
      end

      module InstanceMethods

        def measure_superstep(method_name)
          measure("superstep") do
            send(method_name)
          end
        end
        
        def measure_run_job(method_name)
          measure("run_job") do
            send(method_name)
          end
        end

        protected


        def measure(label="")
          instrument_measurements[label] ||= []


          v = nil
          measurement = Benchmark::measure { v = yield}
          instrument_measurements[label] << measurement.real
          v
        end
    
      end
      # end
    end # module CoordinatorInstrument
  end # module Instrument
end # module Elaine
