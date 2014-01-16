require 'benchmark'
require 'elaine/instrument'

module Elaine
  module Instrument
    module WorkerInstrument

      include Elaine::Instrument

      class << self
        def included(klass)
          klass.send :include, InstanceMethods
        end
      end

      module InstanceMethods

        def measure_superstep(method_name)
          v, measurement = measure("superstep") do 
            send(method_name)
          end

          instrument_measurements["superstep"] ||= {total_time: 0.0, count: 0, min_time: Float::INFINITY, max_time: 0.0, avg_time: 0.0, max_step: 0, min_step: 0}
          history = instrument_measurements["superstep"]
          history[:total_time] += measurement.real

          history[:count] += 1

          if measurement.real < history[:min_time]
            history[:min_time] = measurement.real
            history[:min_step] = history[:count]
          end

          if measurement.real > history[:max_time]
            history[:max_time] = measurement.real
            history[:max_step] = history[:count]
          end

          history[:avg_time] = history[:total_time] / history[:count]

          v
        end
        

        protected
        def measure(label="")
          v = nil
          measurement = Benchmark::measure { v = yield }
          return [v, measurement]
        end

      end # module InstanceMethods
      
    end # module WorkerInstrument
  end # module Instrument
end # module Elaine
