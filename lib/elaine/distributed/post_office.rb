require 'dcell'

module Elaine
  module Distributed
    class PostOffice
      include Celluloid
      include Celluloid::Logger

      attr_reader :mailboxes
      attr_reader :zipcodes

      def initialize(partitioner: Elaine::Distributed::MD5Partitioner, out_box_buffer_size: 1_000 )
        @mailboxes = Hash.new
        @zipcodes = Hash.new
        @partitioner = partitioner
        @outbox = {}
        @out_box_buffer_size = out_box_buffer_size
      end

      def zipcodes=(zipcodes)
        @zipcodes = zipcodes

        # do we need to initialize all the mailboxes here?
        # might be smart?
        @mailboxes = Hash.new
        # @message_queue = []
        @outbox = Hash.new
        # @message_queue = []
        # my_id = DCell.me.id
        # @zipcodes.each_pair do |k, v|
        #   if v == my_id
        #     debug "Creating mailbox for: #{k}"
        #     @mailboxes[k] = []
        #   end
        # end

      end

      def address(to)
        dest = @zipcodes.keys.select { |k| k.include?(@partitioner.key(to)) }
        if dest.size != 1
          if dest.size > 1
            raise "There were multiple destinations (#{dest.size}) found for node: #{to}"
          else
            raise "There was no destination found for node: #{to}"
          end
        end
        node = DCell::Node[@zipcodes[dest.first]]
      end


      def deliver(to, msg)
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
        @outbox[node.id] ||= []
        @outbox[node.id].push({to: to, msg: msg})

        # check to see if we should do a bulk delivery...
        if @outbox[node.id].size >= @out_box_buffer_size
          to_deliver = @outbox[node.id].shift @out_box_buffer_size
          deliver_bulk node.id, to_deliver
        end

        nil
      end

      def deliver_all
        @outbox.each_pair do |k, v|
          unless v.empty?
            deliver_bulk k, v
          end
        end
        @outbox.clear
      end

      def deliver_bulk(destination_node_address, msgs)
        debug "delivering bulk to: #{destination_node_address}"
        DCell::Node[destination_node_address][:postoffice].async.receive_bulk(msgs)
        msgs.size
      end

      def receive_bulk(msgs)
        msgs.each do |msg|
          @mailboxes[msg[:to]] ||= []
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

      def read_all(mailbox)
        node = address(mailbox)
        # debug "node: #{node}"
        # debug "node.id: '#{node.id}'"
        # debug "DCell.me.id: '#{DCell.me.id}'"
        if node.id.eql?(DCell.me.id)
          @mailboxes[mailbox] ||= []
          msgs = @mailboxes[mailbox].map { |v| v }
          @mailboxes[mailbox].clear
          msgs
          # msgs = @message_queue.select { |v| v[:to] == mailbox }
          # @message_queue.delete_if { |v| v[:to] == mailbox }
          # msgs
        else
          raise "Can't destructively read a non-local mailbox!"
        end
      end

    end # class PostOffice
  end # module Distributed
end # module Elaine
