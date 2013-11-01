# require 'dnssd'
# require 'celluloid/io'
require 'dcell'

module Elaine
  module Distributed
    class Coordinator
      include Celluloid
      include Celluloid::Logger
      # finalizer :shutdown

      attr_reader :workers
      attr_reader :partitions
      attr_reader :num_partitions
      # attr_reader :graph
      # attr_reader :zipcodes

      # attr_reader :server
      # def initialize(host, port, graph: nil, partitions: 1)
      def initialize(graph: nil, num_partitions: 1)
        @workers = []
        @num_partitions = num_partitions
        @graph = graph
        info "GOT GRAPH: #{graph}"
        @partitions = Hash.new

      end

      def fart
        puts "FART"
      end

      def graph=(g)
        debug "Setting graph"
        @graph = g
        debug "done setting graph"
      end

      def zipcodes
        zips = {}
        @partitions.each_pair do |zip, vertices|
          vertices.each do |vertex|
            zips[vertex[:id]] = zip
          end
        end
        zips
      end

      def partition
        # not sure if we should re-initialize or not
        # @partitions = Hash.new

        # size = (graph.size.to_f / num_partitions).ceil
        size = (@graph.size.to_f / workers.size).ceil
        # graph.each_slice(size).with_index do |slice, index|
        #   slice.each do |vertex_json|
        #     zipcodes[vertex_json[:id]] = workers[index]
        #   end
        # end

        @graph.each_slice(size).with_index do |slice, index|
          @partitions[@workers[index]] = slice
        end
        
        @partitions
      end

      def register_worker(worker_node)
        # we could, in theory, have multiple workers in the same node, however
        # i think it makes more sense to just have multiple nodes running on the
        # same machine instead of multiple workers in a single node
        # This should be re-evaluated at some point in the future.
        info "Registering worker: #{worker_node}"
        unless @workers.include? worker_node
          @workers << worker_node
        end
      end

      def run_job
        # zipcodes = {}
        debug "partitioning"
        partition
        debug "Partitions: #{@partitions}"

        # distribute the zipcodes
        debug "building zipcodes"
        zips = zipcodes
        debug "distributing zipcodes"
        @workers.each do |worker_node|
          DCell::Node[worker_node][:postoffice].zipcodes = zips
        end

        # now send the graph
        debug "distributing graph"
        @partitions.each_pair do |worker_node, vertices|
          DCell::Node[worker_node][:worker].init_graph vertices
        end


        debug "Running job"
        step_num = 0
        loop do
          step_num += 1
          # execute a superstep and wait for workers to complete
          debug "Initializing superstep #{step_num}"
          step = @workers.select do |w|
            DCell::Node[w][:worker].active > 0
          end.map {|w| DCell::Node[w][:worker].future(:init_superstep)}
          step.map { |f| f.value }

          debug "Running superstep #{step_num}"
          step = @workers.select do |w|
            DCell::Node[w][:worker].active > 0
          end.map {|w| DCell::Node[w][:worker].future(:superstep)}

          step.map { |f| f.value }

          # @workers.select { |w| DCell::Node[w][:worker].active > 0 }.each do |w|
          #   DCell::Node[w][:worker].superstep
          # end



          break if @workers.select { |w| DCell::Node[w][:worker].active > 0 }.size.zero?
        end
        debug "Job finished!"
      end

      # def each_vertex(&block)
      #   @workers.each do |w|
      #     worker_node = DCell::Node[w]
      #     worker_node[:worker].vertices2.each do |v|
      #       # yield worker_node[v]
      #       yield v
      #     end
      #   end
      # end

      def vertex_values(&block)
        @workers.map do |w|
          worker_node = DCell::Node[w]
          worker_node[:worker].vertex_values
          # puts "ret.class: #{ret.class}, size: #{ret.size}, ret: #{ret}"
          # ret
        end.flatten
      end

    end # class Coordinator
  end # module Distributed
end # module Elaine
