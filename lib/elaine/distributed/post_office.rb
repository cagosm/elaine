require 'dcell'
require 'zlib'

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
        @out_box_buffer_size = out_box_buffer_size
        @address_cache = {}
        @node_cache = {}
        @post_office_cache = {}
        @sent_active = false
        @active = true
        @my_address = DCell.me.id
      end

      def active?
        @active
      end

      def active!
        @active = true
      end

      def instrumented?
        false
      end

      def init_superstep
        @sent_active = false
        @active = false
      end

      def zipcodes=(zipcodes)
        @zipcodes = zipcodes

        # do we need to initialize all the mailboxes here?
        # might be smart?
        @mailboxes = Hash.new
        @outbox = Hash.new
        zipcodes.each_value do |v|
          debug "Initializing outbox for #{v}"
          @outbox[v] = []
        end

        Celluloid::Actor[:worker].set_zipcodes(zipcodes)

      end


      def deliver_multiple(msgs)
        msgs.each do |msg|
          deliver(msg[:to], msg[:msg])
        end
        msgs.size
      end


      def deliver(to, msg)

        node_address = address(to)

        if node_address == @my_address
          deliver_local(to, msg)
        else
          deliver_remote(node_address, to, msg)
        end
        
      end

      def deliver_all
        @outbox.each_pair do |k, v|
          unless v.empty?
            deliver_bulk k, Array.new(v)
          end
          v.clear
        end
        @outbox.each_value { |v| v.clear }

      end

      def deliver_bulk(destination_node_address, msgs)
        debug "delivering bulk to: #{destination_node_address}"
       
        s = DCell::Node[destination_node_address][:postoffice]
        debug "postoffice: #{s}"
        raise "No postoffice service for #{destination_node_address}" if s.nil?
        compressed_messages = Zlib::Deflate.deflate(Marshal.dump(msgs))
        s.active!
        s.async.receive_bulk(compressed_messages)
        msgs.size
      end



      def receive_bulk(compressed_msgs)
        msgs = Marshal.load(Zlib::Inflate.inflate(compressed_msgs))
        debug "Post office receiving bulk (#{msgs.size} messages)"
        
        if msgs.size > 0
          active!
        end
        
        msgs.each do |msg|
          @mailboxes[msg[:to]] << msg[:msg]
        end
        msgs.size
      end

      def read(mailbox)
        node = address(mailbox)
        if node.id.eql?(@my_address)
          @mailboxes[mailbox].map { |v| v }
        else
          node[:postoffice].read mailbox
        end
      end

      def bulk_read_all(mailboxes)
        debug "Bulk reading #{mailboxes.size} mailboxes"
        ret = {}

        mailboxes.each do |mailbox|
          @mailboxes[mailbox] ||= []
          msgs = Array.new(@mailboxes[mailbox])
          @mailboxes[mailbox].clear # .shift(@mailboxes[mailbox].size)
          ret[mailbox] = msgs
        end

        ret

      end

      def read_all(mailbox)
        node = address(mailbox)
       
        if node.id.eql?(@my_address)
          @mailboxes[mailbox] ||= []
          msgs = @mailboxes[mailbox].shift(@mailboxes[mailbox].size) #.map { |v| v }
          
          msgs
          
        else
          raise "Can't destructively read a non-local mailbox! (#{mailbox} on #{@my_address}"
        end
      end

      def messages?(mailbox)
        @mailboxes[mailbox].size > 0
      end

      protected

      def deliver_local(to, msg)
        debug "Delivering local message to #{to}"
        @mailboxes[to] << msg
        active!
        nil
      end

      def deliver_remote(node_address, to, msg)
        @outbox[node_address] ||= []
        @outbox[node_address] << {to: to, msg: msg}
        
        # check to see if we should do a bulk delivery...
        if @outbox[node_address].size >= @out_box_buffer_size
          to_deliver = @outbox[node_address].shift @out_box_buffer_size
          deliver_bulk node_address, to_deliver
        end

        nil
      end

      def address(to)
        if @address_cache[to].nil?
          dest = @zipcodes.keys.select { |k| k.include?(@partitioner.key(to)) }
          if dest.size != 1
            if dest.size > 1
              raise "There were multiple destinations (#{dest.size}) found for node: #{to}"
            else
              raise "There was no destination found for node: #{to}"
            end
          end
          d = dest.first
          node_address = @zipcodes[d]
          raise "Destination node (key: #{d}, node: #{@zipcodes[d]}) was nil!" if node_address.nil?

          @address_cache[to] = node_address
        end
        @address_cache[to]
       
      end

    end # class PostOffice
  end # module Distributed
end # module Elaine
