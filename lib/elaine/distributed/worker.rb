require 'dcell'
require 'digest/md5'

module Elaine
  module Distributed
    class Worker
      include Celluloid
      include Celluloid::Logger

      attr_reader :vertices, :active, :vertices2
      attr_accessor :graph_size
      
      def initialize(coordinator_node: "elaine.coordinator", g: [], zipcodes: {}, stop_condition: Celluloid::Condition.new, partitioner: Elaine::Distributed::MD5Partitioner, out_box_buffer_size: 10_000, num_partitions: 3)

        # @coordinator_node = DCell::Node["elaine.coordinator"]
        @coordinator_node = coordinator_node
        DCell::Node[@coordinator_node][:coordinator].register_worker DCell.me.id

        @vertices = []
        @superstep_num = 0
        @stop_condition = stop_condition
        # @postoffice = Celluloid::Actor[:postoffice]
        @partitioner = partitioner

        @out_box_buffer_size = out_box_buffer_size

        @outbox = []

        @address_cache = {}
        @num_partitions = num_partitions
        @active_count = 0
        @active = true
        # @postoffice = Elaine::Distributed::PostOffice.new
      end

      def active!
        debug "active! called"
        @active = true
      end

      def active?
        @active
      end

      def set_zipcodes(zips)
        @zipcodes = zips
      end

      def address(to)
        start_time = Time.now.to_i
      # if !@address_cache[to]
        dest = @zipcodes.keys.select { |k| k.include?(@partitioner.key(to)) }
        if dest.size != 1
          if dest.size > 1
            raise "There were multiple destinations (#{dest.size}) found for node: #{to}"
          else
            raise "There was no destination found for node: #{to}"
          end
        end
        # @address_cache[to] = dest.first
          end_time = Time.now.to_i
          debug "address(#{to}) took #{end_time - start_time} seconds."
          d = dest.first
          # debug "Addres cache size: #{@address_cache.size}"
        # end


        # node = DCell::Node[@zipcodes[@address_cache[to]]]
        node = DCell::Node[@zipcodes[d]]
        # debug "Address is: #{zipcodes[dest.first]}"
        raise "Destination node (key: #{d}, node: #{@zipcodes[d]}) was nil!" if node.nil?
        node
      end

      def deliver_all
        # debug "outbox buffer size reached #{@outbox2.size}/#{@out_box_buffer_size}"
        # msgs = @outbox2.shift(@out_box_buffer_size)
        # deliver_bulk2 to_deliver

        # msgs = Array.new(@outbox)

        to_deliver = {}
        @outbox.each do |m|
          raise "Message was nil!" if m.nil?
          node = address(m[:to])
          debug "to: #{m[:to]}"
          debug "destination node: #{node.id}"
          to_deliver[node.id] ||= []
          to_deliver[node.id] << m
        end

        to_deliver.each_pair do |k, v|
          Celluloid::Actor[:postoffice].deliver_bulk(k, v)
        end
        @outbox.clear
      end

      def deliver(to, msg)
        debug "worker#deliver(#{to}, #{msg})"
        @outbox.push({to: to, msg: msg})
      end

      def add_vertices(vs)
        debug "Adding #{vs.size} vertices..."
        counter = 0
        vs.each do |v|
          counter += 1
          debug "Adding vertex ##{counter}" if counter % 1_000 == 0
          add_vertex v
        end
      end

      def add_vertex(v)
        @vertices2 ||= []

        # vertex = v[:klazz].new v[:id], v[:value], self, v[:outedges]
        vertex = v[:klazz].new v[:id], v[:value], Celluloid::Actor[:postoffice], v[:outedges]

        @vertices2 << vertex
        # @active_count = @vertices2.select { |v| v.active? }.size
        @active_count += 1

      end

      def init_graph(g=[])
        raise 'empty worker graph' if g.empty?
        if @vertices.size > 0
          @vertices.each do |v|
            # Celluloid::Actor[v].terminate
          end
        end
        @vertices = []
        # raise "Graph already initialized!" if @vertices.size > 0

        # we are going to assume that graphs come in as json documents
        # *describing* the graph.
        # @vertices = graph
        # @active_count   = graph.size

        # HACK the local vertices should be dealt with differently than
        # the @vertices2 member
        @vertices2 = []
        g.each do |n|
          # n[:klazz].supervise_as n[:id], n[:id], n[:value], Celluloid::Actor[:postoffice], n[:outedges]
          @vertices << n[:id]
          # v = n[:klazz].new n[:id], n[:value], Celluloid::Actor[:postoffice], n[:outedges]
          v = n[:klazz].new n[:id], n[:value], Celluloid::Actor[:postoffice], n[:outedges]
          # v = n[:klazz].new n[:id], n[:value], self, n[:outedges]
          @vertices2 << v
        end
        @active_count = @vertices.size

        debug "There are #{@vertices.size} vertices in this worker."

      end

      # HACK this should be handled better...
      def init_superstep
        debug "Starting init_superstep"
        @active = false
        counter = 0
        @vertices2.each_slice(5_000) do |slice|

          counter += 1
          debug "Loading messages for slice ##{counter}" if counter % 10 == 0
          # v.messages = Celluloid::Actor[:postoffice].read_all(v.id)
          slice_ids = []
          slice.each do |v|
            slice_ids << v.id
          end
          # boxes = Celluloid::Actor[:postoffice].bulk_read_all(slice_ids)
          boxes = Celluloid::Actor[:postoffice].bulk_read_all(slice_ids)
          slice.each do |v|
            v.messages = boxes[v.id]
            v.active! if v.messages.size > 0
            active! if v.active?
          end
        end
        Celluloid::Actor[:postoffice].init_superstep
        debug "#{DCell.me.id} finished init_superstep"
      end

      def pmap(enum, &block)
        futures = enum.map { |elem| Celluloid::Future.new(elem, &block) }
        futures.map { |future| future.value }
      end

      def stop
        @stop_condition.signal(true)
      end

      def superstep
        @active = false
        # might improve performance not to bother with this active check
        # here and just loop through all vertices and execute if they are active
        # not sure how it will interact with threads, could end up being a
        # thread that has no active vertices...
        active = @vertices2.select {|v| v.active?}
        debug "There are #{active.size} active vertices in this step"

        # we are going to make 4 slices and run them each in a thread
        debug "active.size: #{active.size}"
        slice_size = (active.size / @num_partitions).to_i
        slice_size = 1 if slice_size < 1
        debug "slice size: #{slice_size}"
        slices = active.each_slice(slice_size)

        pmap(slices) do |s|
          s.each do |v|
            v.step
            active! if v.active?
          end
        end
        
        # pmap(active) do |v|
        #   v.step
        # end
        
        # active.each do |v|
        #   v.step
        # end

        # info "Delivering all messages from end of super step."
        # debug "Finishing vertex deliveries"
        # @vertices2.each do |v|
        #   v.deliver_all
        # end
        info "Delivering all messages from end of super step."
        # Celluloid::Actor[:postoffice].deliver_all
        Celluloid::Actor[:postoffice].deliver_all
        # deliver_all
        # info "Done delivering"

        info "Finished super step"

        @active_count = active.select {|v| v.active?}.size    
      end

      # def deliver_all
      #   # @outbox.each_pair do |k, v|
      #   #   unless v.empty?
      #   #     deliver_bulk k, Array.new(v)
      #   #   end
      #   #   v.clear
      #   # end
      #   # @outbox.each_value do |v|clear

      #   # msgs = Array.new(@outbox2)
      #   # return

      #   warn "deliver_all @outbox2.size; #{@outbox2.size}"
      #    #.shift(@out_box_buffer_size)
      #     # deliver_bulk2 to_deliver

      #   my_id = DCell.me.id
      #   to_deliver = {}
      #   @outbox2.each do |m|
      #     # debug "deliver_all m: #{m}"
      #     # node = self.address(m[:to])
      #     # debug "deliver_all node: #{node}"

      #     # d = Digest::MD5.hexdigest(m[:to].to_s)
      #     # hid = [d].pack("H*").unpack("l>").first
      #     hid = @partitioner.key(m[:to])
      #     dest = @zipcodes.keys.select { |k| k.include?(hid) }.first
      #     # debug "destination node: #{dest}"
      #     # to_deliver[node.id] ||= []
      #     # to_deliver[node.id] << m
      #     to_deliver[@zipcodes[dest]] ||= []
      #     to_deliver[@zipcodes[dest]] << m
      #     # to_deliver[my_id] ||= []
      #     # to_deliver[my_id] << m
          
      #   end

      #   to_deliver.each_pair do |k, v|
      #     deliver_bulk(k, v)
      #   end

      #   @outbox2.clear
      # end

      def vertex_values
        @vertices2.map { |v| {id: v.id, value: v.value} }
      end

      protected


      # def address(to)
      #   # if !@address_cache[to]
      #   # if @address_cache[to].nil?
      #   d = Digest::MD5.hexdigest(to.to_s)
      #   key = @partitioner.key(to)
      #   dest = @zipcodes.keys.select { |k| k.include?(key) }
      #   if dest.size != 1
      #     if dest.size > 1
      #       raise "There were multiple destinations (#{dest.size}) found for node: #{to}"
      #     else
      #       raise "There was no destination found for node: #{to}"
      #     end
      #   end
      #     # @address_cache[to] = dest.first
      #     # d = dest.first
      #     # debug "Addres cache size: #{@address_cache.size}"
      #   # end


      #   # node = DCell::Node[@zipcodes[@address_cache[to]]]
      #   debug "Address is: #{@zipcodes[dest.first]}"
      #   # raise "Destination node (key: #{d}, node: #{@zipcodes[d]}) was nil!" if node.nil?

      #   # DCell::Node[DCell.me.id]
      #   DCell::Node[@zipcodes[dest.first]]
      # end
    end # class Worker
  end # module Distributed
end # module Elaine
