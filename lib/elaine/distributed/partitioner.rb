# require 'dnssd'
# require 'celluloid/io'
# require 'dcell'

module Elaine
  module Distributed
    # this might serve better as a module that gets included?
    class Partitioner
      def self.partition(vals, num_partitions)
                # not sure if we should re-initialize or not
        partitions = Array.new(num_partitions)

        # size = (@graph.size.to_f / workers.size).ceil

        # @graph.each_slice(size).with_index do |slice, index|
        #   @partitions[@workers[index]] = slice
        # end

        # trying a slow partitioning to check a bug...
        # debug "running slow partitioner..."

        vals.each_with_index do |v, idx|
          partition = idx % num_partitions
          # @partitions[@workers[worker_node]] ||= []
          # @partitions[@workers[worker_node]] << v
          partitions[partition] << v
        end

        # debug "done running slow partitioner:"
        # @partitions.each_pair do |k, v|
        #   debug "#{k}: #{v.size}"
        # end
        
        partitions
      end
    end # class Partitioner
  end # module Distributed
end # module Elaine
