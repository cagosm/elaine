require 'digest/md5'

require 'logger'

module Elaine
  module Distributed
    # this might serve better as a module that gets included?
    class MD5Partitioner

      def self.key(v)
        d = Digest::MD5.hexdigest(v.to_s)
        [d].pack("H*").unpack("l>").first
      end

      def self.partition(vals, num_partitions)
        logger = Logger.new(STDERR)

        partitions = Array.new(num_partitions)


        # this is a really poor implementation....
        logger.debug "Finding partition sizes..."
        vals.each_with_index do |v, idx|
          partition = idx % num_partitions
          
          partitions[partition] ||= 0
          partitions[partition] += 1
        end

        # now we know how many vertices should be in each partition...

        # HACK - again not very efficient...
        # TODO should really look into bloom filters or something
        partition_ranges = Array.new(num_partitions)
        local_count = 0
        partition_num = 0
        min_val = nil
        max_val = nil
        # vals.map { |t| key(t)}.sort.each do |v|
        tmp_vals = []
        logger.debug "Building keys..."
        vals.each do |t|
          tmp_vals << key(t)
        end

        tmp_vals.sort!
        logger.debug "Adding to partitions"
        tmp_vals.each do |v|
          # logger.debug "local_count: #{local_count}"
          # logger.debug "size of partition for partition number #{partition_num}: #{partitions[partition_num]}"


          # logger.debug "key for #{v} = #{key(v)}"

          # min_val ||= key(v)
          min_val ||= v
          min_val = v if v < min_val

          # HACK this can get ugly if there is only one vertex in a given
          # partition...
          max_val ||= v
          max_val = v if v > max_val
          local_count += 1

          if partitions[partition_num] <= local_count
            # we need to move to the next partition
            # logger.debug "moving to next partition?"
            partition_ranges[partition_num] = (min_val..max_val)
            min_val = nil
            max_val = nil
            partition_num += 1
            local_count = 0
          end

        end
        tmp_vals.clear
        
        partition_ranges
      end
    end # class Partitioner
  end # module Distributed
end # module Elaine
