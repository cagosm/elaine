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

      def initialize(graph: nil, num_partitions: 1, stop_condition: Celluloid::Condition.new, partitioner: Elaine::Distributed::MD5Partitioner)
        @workers = []
        @num_partitions = num_partitions
        @graph = graph
        info "GOT GRAPH: #{graph}"
        @partitions = Hash.new
        @stop_condition = stop_condition
        @partitioner = partitioner
      end

      def graph=(g)
        debug "Setting graph"
        @graph = g
        debug "done setting graph"
      end

      def zipcodes
        zips = {}
        @partitions.each_pair do |zip, range|
          # vertices.each do |vertex|
            # zips[vertex[:id]] = zip
          # end
          zips[range] = zip
        end
        zips
      end

      def partition
        # # not sure if we should re-initialize or not
        # @partitions = Hash.new

        # # size = (@graph.size.to_f / workers.size).ceil

        # # @graph.each_slice(size).with_index do |slice, index|
        # #   @partitions[@workers[index]] = slice
        # # end

        # # trying a slow partitioning to check a bug...
        # debug "running slow partitioner..."

        # @graph.each_with_index do |v, idx|
        #   worker_node = idx % @workers.size
        #   @partitions[@workers[worker_node]] ||= []
        #   @partitions[@workers[worker_node]] << v
        # end

        # debug "done running slow partitioner:"
        # @partitions.each_pair do |k, v|
        #   debug "#{k}: #{v.size}"
        # end
        debug "running partitioner: #{@partitioner}"
        tmp_partitions = @partitioner.partition(@graph.map {|v| v[:id]}, @workers.size)
        debug "done partitioner: #{@partitioner}"

        debug "tmp_partititions: #{tmp_partitions}"
        
        # now we need to map the partitions back to the workers.
        tmp_partitions.each_with_index do |p, idx|
          @partitions[@workers[idx]] = p
        end

        debug "@partitions: #{@partitions}"

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

      def run_until_finished
        # zipcodes = {}
        debug "partitioning"
        partition
        # debug "Partitions: #{@partitions}"

        # distribute the zipcodes
        debug "building zipcodes"
        zips = zipcodes
        debug "distributing zipcodes"
        @workers.each do |worker_node|
          debug "Sending zipcodes to: #{worker_node}"
          DCell::Node[worker_node][:postoffice].zipcodes = zips
        end

        # now send the graph
        debug "distributing graph"
        # @partitions.each_pair do |worker_node, vertices|
        #   debug "Sending vertex #{v} to: #{worker_node}"
        #   # DCell::Node[worker_node][:worker].init_graph vertices
        # end

        # TODO This needs to be dealt with differently if we are loading
        # the graph from a remote location (i.e., not sending the graph to each
        # worker)
        @graph.each do |vertex|
          vertex_key = @partitioner.key(vertex[:id])
          # zips.each_pair do |k, v|

          # end
          debug "zips: #{zips}"
          worker_node = zips.select { |k, v| k.include? vertex_key }

          if worker_node.size != 1
            raise "Bad worker node size: #{worker_node.size}"
          end

          worker_node = worker_node.first[1]

          DCell::Node[worker_node][:worker].add_vertex vertex
        end


        debug "Running job"
        step_num = 0
        loop do
          step_num += 1
          # execute a superstep and wait for workers to complete
          debug "Initializing superstep #{step_num}"
          step = @workers.select do |w|
            debug "Checking for active vertices on node #{w}"
            DCell::Node[w][:worker].active > 0
          end.map {|w| DCell::Node[w][:worker].future(:init_superstep)}
          step.map { |f| f.value }

          debug "Running superstep #{step_num}"
          step = @workers.select do |w|
            DCell::Node[w][:worker].active > 0
          end.map {|w| DCell::Node[w][:worker].future(:superstep)}

          step.map { |f| f.value }

          break if @workers.select { |w| DCell::Node[w][:worker].active > 0 }.size.zero?
        end
        debug "Job finished!"
      end

      def run_job
        run_until_finished
      end

      def stop
        @workers.each do |w|
          DCell::Node[w][:worker].async.stop
        end
        @stop_condition.signal(true)
      end

      def run_and_stop
        run_until_finished
        @stop_condition.signal(true)
      end

      def vertex_values(&block)
        @workers.map do |w|
          worker_node = DCell::Node[w]
          worker_node[:worker].vertex_values
        end.flatten
      end

    end # class Coordinator
  end # module Distributed
end # module Elaine
