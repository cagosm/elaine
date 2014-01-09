
module Elaine
  module Instrument

   class << self
      def included(klass)
        # @instrument_measurements = {}
        # puts "instrument included"
        # klass.send :extend, ClassMethods
        klass.send :include, InstanceMethods

        # @instrument_measurements = {}
      end
    end

     

    



    # def initialize
    #   @measurements = {}
    # end
    module InstanceMethods

      attr_accessor :instrument_measurements
      def instrument_measurements
        @instrument_measurements ||= {}
      end

      def enable_measurement(method)
        puts "enabling measurement for: #{method}"
        new_method = :"measure_#{method}"
        # puts "responds to #{method}: #{respond_to?(method)}, measure_#{method}: #{respond_to?(new_method)}"
        raise "Unknown method to instrument: #{method}" unless respond_to? new_method 
        add_to_method(method, "measure_#{method}")
      end

      def instrumented?
        true
      end

      def add_to_method(original_method, new_method, &block)
        
        uninstrumented_method_name = :"#{original_method}_without_instrumentation"
        instrumented_method_name = :"#{original_method}_with_#{new_method}"
        # instrumented_method_name = :"#{original_method}_with_"

        raise ArgumentError, "already instrumented #{original_method} for #{name}" if respond_to? instrumented_method_name
        raise ArgumentError, "could not find method #{original_method} for #{name}" unless self.respond_to?(original_method) || self.singleton_class.protected_method_defined?(original_method) || self.singleton_class.private_method_defined?(original_method)

      
        self.singleton_class.send :alias_method, uninstrumented_method_name, original_method
        self.singleton_class.send(:define_method, instrumented_method_name) do |*args, &block|
          send(new_method, uninstrumented_method_name, *args, &block)
        end
        
        self.singleton_class.send :alias_method, original_method, instrumented_method_name
        true
      end

      def remove_from_method(original_method, new_method)
        uninstrumented_method_name = :"#{original_method}_without_instrumentation"
        instrumented_method_name = :"#{original_method}_with_#{new_method}"
        self.singleton_class.send(:remove_method, instrumented_method_name)
        self.singleton_class.send(:alias_method, original_method, uninstrumented_method_name)
        self.singleton_class.send(:remove_method, uninstrumented_method_name)
      end
    end # module InstanceMethods
      

    # module ClassMethods
    #   def add_to_method(original_method, new_method, &block)
    #     return puts("fart")
    #     uninstrumented_method_name = :"#{original_method}_without_#{new_method}"
    #     instrumented_method_name = :"#{original_method}_with_#{new_method}"

    #     raise ArgumentError, "already instrumented #{original_method} for #{name}" if respond_to? instrumented_method_name
    #     raise ArgumentError, "could not find method #{original_method} for #{name}" unless respond_to?(original_method) || private_method_defined?(original_method)

    #     alias_method uninstrumented_method_name, original_method
    #     yield uninstrumented_method_name, instrumented_method_name #, metric_name
    #     alias_method original_method, instrumented_method_name
    #   end          
    # end

    protected
    def measure(label="")

    end

  end # module Instrument
end # module Elaine
