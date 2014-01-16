require 'benchmark'
require 'elaine/instrument'

module Elaine
  module Instrument
    module PostOfficeInstrument

      include Elaine::Instrument

      class << self
        def included(klass)
          klass.send :include, InstanceMethods
        end
      end

      module InstanceMethods

        def measure_deliver(method_name, *args)
          basic_measurement(method_name, "deliver", *args)
        end

        def measure_deliver_local(method_name, *args)
          basic_measurement(method_name, "deliver_local", *args)
        end

        def measure_receive_bulk(method_name, *args)
          basic_measurement(method_name, "receive_bulk", *args)
        end


        def measure_deliver_bulk(method_name, *args)
          basic_measurement(method_name, "deliver_bulk", *args)
        end

        def measure_deliver_remote(method_name, *args)
          basic_measurement(method_name, "deliver_remote", *args)
        end

        def measure_address(method_name, *args)
          basic_measurement(method_name, "address", *args)
        end
        
        protected

        def basic_measurement(method_name, label, *args)

          v, measurement = measure(label) do 
            send(method_name, *args)
          end

          instrument_measurements[label] ||= {total_time: 0.0, count: 0, min_time: Float::INFINITY, max_time: 0.0, avg_time: 0.0}

          history = instrument_measurements[label]
          history[:total_time] += measurement.real

          history[:count] += 1

          if measurement.real < history[:min_time]
            history[:min_time] = measurement.real
          end

          if measurement.real > history[:max_time]
            history[:max_time] = measurement.real
          end

          history[:avg_time] = history[:total_time] / history[:count]

          v

        end

        def measure(label="")
          v = nil
          measurement = Benchmark::measure { v = yield }
          return [v, measurement]
        end

      end # module InstanceMethods
      
    end # module PostOfficeInstrument
  end # module Instrument
end # module Elaine
