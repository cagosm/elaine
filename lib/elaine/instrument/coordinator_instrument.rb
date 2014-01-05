require 'benchmark'

module Elaine
  module Instrument
    module CoordinatorInstrument
      include Elaine::Instrument
      # extend Elaine::Instruments::Instrument

      class << self
        def included(klass)
          # puts "coordinator instrument included"
          klass.send :include, InstanceMethods
          # "puts extending class methods"
          # klass.send :extend, ClassMethods
          # add_to_method "blah"

          # klass.add_to_method :run_job, :measure_run_job
        end
      # puts "wtf?"
      # class << self
        # def included(klass)
        #   # @instrument_measurements = {}
        #   puts "benchmark instrument included"
        #   klass.send :include, InstanceMethods
        # end
      end

      module InstanceMethods
        def i_exist?
          return true
        end

        def measure_superstep(method_name)
          measure("superstep") do
            send(method_name)
          end
        end
        
        def measure_run_job(method_name)
          measure("run_job") do
            send(method_name)
          end
          # puts "waddup: #{instrument_measurements}"
          # puts "measure_run_job"
          # puts "measure run job called"
          # measure("run_job") do
          #   puts "running job"
          #   run_job
          # end
        end

        protected


        def measure(label="")
          # raise "no method to measure!" if method.nil?

          # puts "what's up?"

          instrument_measurements[label] ||= []

          # puts "measures: #{instrument_measurements}"
          # return
          v = nil
          measurement = Benchmark::measure { v = yield}
          # puts "measurement: #{measurement}"
          # instrument_measurements[label] << Benchmark::measure { v = yield }
          instrument_measurements[label] << measurement.real
          # puts "#{instrument_measurements}"
          # puts "v: #{v}"
          v
        end



        # alias_method :run_job, :measure_run_job
        # alias_method :measure_run_job, :run_job

    
      end
      # end
    end # module CoordinatorInstrument
  end # module Instrument
end # module Elaine
