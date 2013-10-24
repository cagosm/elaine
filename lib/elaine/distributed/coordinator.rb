module Elaine
  module Distributed
    class Coordinator
      attr_reader :workers
      attr_reader :partitions

      def initialize(graph, partitions: 1, options = {})
        raise "empty graph" if graph.empty?

        @partitions = partitions

        @workers = []

        partition(graph) do |subgraph|
          @workers << Worker.new(subgraph)
        end
      end

      def partition(graph)
        size = (graph.size.to_f / partitions).ceil
        graph.each_slice(size) { |slice| yield slice }
      end

      def run
        loop do
          # execute a superstep and wait for workers to complete
          step = @workers.select {|w| w.active > 0}.collect {|w| w.superstep }
          step.each {|t| t.join}

          break if @workers.select {|w| w.active > 0}.size.zero?
        end
      end

    end # class Coordinator
  end # module Distributed
end # module Elaine
