require 'dcell'

module Elaine
  module Distributed
    class PostOffice
      include Celluloid
      include Celluloid::Logger

      attr_reader :mailboxes
      attr_reader :zipcodes

      def initialize(partitioner: Elaine::Distributed::MD5Partitioner, out_box_buffer_size: 10_000 )
        @mailboxes = Hash.new
        @zipcodes = Hash.new
        @partitioner = partitioner
        @outbox = {}
        @outbox2 = []
        @out_box_buffer_size = out_box_buffer_size
        @address_cache = {}
      end

      def zipcodes=(zipcodes)
        @zipcodes = zipcodes

        # do we need to initialize all the mailboxes here?
        # might be smart?
        @mailboxes = Hash.new
        # @message_queue = []
        @outbox = Hash.new
        @outbox2 = []
        zipcodes.each_value do |v|
          debug "Initializing outbox for #{v}"
          @outbox[v] = []
        end

        Celluloid::Actor[:worker].set_zipcodes(zipcodes)


        # @message_queue = []
        # my_id = DCell.me.id
        # @zipcodes.each_pair do |k, v|
        #   if v == my_id
        #     debug "Creating mailbox for: #{k}"
        #     @mailboxes[k] = []
        #   end
        # end

      end


      def deliver_multiple(msgs)
        msgs.each do |msg|
          deliver(msg[:to], msg[:msg])
        end
        msgs.size
      end


      def deliver(to, msg)
        # return nil
        node = address(to)

        # if node.id.eql?(DCell.me.id)
        #   # debug "Delivering to mailbox: #{to}"
        #   # @mailboxes[to] ||= []
        #   # @mailboxes[to].push msg
        #   # @message_queue << {to: to, msg: msg}

        #   # debug "Done delivering to mailbox: #{to}"
        #   nil
        # else
        #   # debug "Delivering message to remote mailbox: #{msg}"
        #   node[:postoffice].async.deliver(to, msg)
        #   # debug "Finished delivery remnote box: to #{node.id}"
        #   nil
        # end

        # trying bulk delivery with a buffer...
        # debug "to: #{to}"
        # debug "destination node: #{node.id}"

        if node.id == DCell.me.id
          debug "Delivering local message to #{to}"
          @mailboxes[to].push(msg)
          Celluloid::Actor[:worker].active!
          return nil
        end

        @outbox[node.id] ||= []
        @outbox[node.id].push({to: to, msg: msg})
        # @outbox2.push({to: to, msg: msg})
        # @outbox2.push({to: to, msg: msg})

        # check to see if we should do a bulk delivery...
        if @outbox[node.id].size >= @out_box_buffer_size
          to_deliver = @outbox[node.id].shift @out_box_buffer_size
          deliver_bulk node.id, to_deliver
        end

        # if @outbox2.size >= @out_box_buffer_size
        #   debug "outbox buffer size reached #{@outbox2.size}/#{@out_box_buffer_size}"
        #   msgs = @outbox2.shift(@out_box_buffer_size)
        #   # deliver_bulk2 to_deliver

        #   to_deliver = {}
        #   msgs.each do |m|
        #     node = address(m[:to])
        #     debug "to: #{m[:to]}"
        #     debug "destination node: #{node.id}"
        #     to_deliver[node.id] ||= []
        #     to_deliver[node.id] << m
        #   end

        #   to_deliver.each_pair do |k, v|
        #     deliver_bulk(k, v)
        #   end

        # end

        nil
      end

      def deliver_all
        @outbox.each_pair do |k, v|
          unless v.empty?
            deliver_bulk k, Array.new(v)
          end
          v.clear
        end
        @outbox.each_value { |v| v.clear }

        # msgs = Array.new(@outbox2)
        #  #.shift(@out_box_buffer_size)
        #   # deliver_bulk2 to_deliver

        # to_deliver = {}
        # msgs.each do |m|
        #   debug "deliver_all m: #{m}"
        #   node = address(m[:to])
        #   debug "destination node: #{node.id}"
        #   to_deliver[node.id] ||= []
        #   to_deliver[node.id] << m
        # end

        # to_deliver.each_pair do |k, v|
        #   deliver_bulk(k, v)
        # end

        # @outbox2.clear
      end

      def deliver_bulk(destination_node_address, msgs)
        debug "delivering bulk to: #{destination_node_address}"
        n = DCell::Node[destination_node_address]

        debug "node: #{n}"
        s = n[:postoffice]
        debug "postoffice: #{s}"
        raise "No postoffice service for #{destination_node_address}" if s.nil?
        # s.async.receive_bulk(msgs)
        s.receive_bulk(msgs)
        msgs.size
      end



      def receive_bulk(msgs)
        debug "Post office receiving bulk (#{msgs.size} messages)"
        Celluloid::Actor[:worker].active! if msgs.size > 0
        msgs.each do |msg|
          # @mailboxes[msg[:to]] ||= []
          @mailboxes[msg[:to]].push msg[:msg]
        end
        msgs.size
      end

      def read(mailbox)
        node = address(mailbox)
        if node.id.eql?(Dcell.me.id)
          @mailboxes[mailbox].map { |v| v }
        else
          node[:postoffice].read mailbox
        end
      end

      def bulk_read_all(mailboxes)
        debug "Bulk reading #{mailboxes.size} mailboxes"
        ret = {}
        mailboxes.each do |mailbox|
          # node = address(mailbox)
          # if node.id.eql?(DCell.me.id)
            @mailboxes[mailbox] ||= []
            msgs = Array.new(@mailboxes[mailbox])
            @mailboxes[mailbox].clear # .shift(@mailboxes[mailbox].size)
            ret[mailbox] = msgs
          # end
        end

        ret

      end

      def read_all(mailbox)
        node = address(mailbox)
        # debug "node: #{node}"
        # debug "node.id: '#{node.id}'"
        # debug "DCell.me.id: '#{DCell.me.id}'"
        if node.id.eql?(DCell.me.id)
          @mailboxes[mailbox] ||= []
          msgs = @mailboxes[mailbox].shift(@mailboxes[mailbox].size) #.map { |v| v }
          # @mailboxes[mailbox].clear
          msgs
          # msgs = @message_queue.select { |v| v[:to] == mailbox }
          # @message_queue.delete_if { |v| v[:to] == mailbox }
          # msgs
        else
          raise "Can't destructively read a non-local mailbox! (#{mailbox} on #{DCell.me.id}"
        end
      end

      def messages?(mailbox)
        @mailboxes[mailbox].size > 0
      end

      protected

      def address(to)
        # start_time = Time.now.to_i
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
          # end_time = Time.now.to_i
          # debug "address(#{to}) took #{end_time - start_time} seconds."
          d = dest.first
          # debug "Addres cache size: #{@address_cache.size}"
        # end


        # node = DCell::Node[@zipcodes[@address_cache[to]]]
        node = DCell::Node[@zipcodes[d]]
        # debug "Address is: #{zipcodes[dest.first]}"
        raise "Destination node (key: #{d}, node: #{@zipcodes[d]}) was nil!" if node.nil?
        node
      end

    end # class PostOffice
  end # module Distributed
end # module Elaine
