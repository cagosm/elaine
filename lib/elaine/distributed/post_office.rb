require 'dcell'

module Elaine
  module Distributed
    class PostOffice
      include Celluloid
      include Celluloid::Logger

      attr_reader :mailboxes
      attr_reader :zipcodes

      def initialize(partitioner: Elaine::Distributed::MD5Partitioner)
        @mailboxes = Hash.new
        @zipcodes = Hash.new
        @partitioner = partitioner
      end

      def zipcodes=(zipcodes)
        @zipcodes = zipcodes

        # do we need to initialize all the mailboxes here?
        # might be smart?
        @mailboxes = Hash.new
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

        if node.id.eql?(DCell.me.id)
          # debug "Delivering to mailbox: #{to}"
          @mailboxes[to] ||= []
          @mailboxes[to].push msg
          # debug "Done delivering to mailbox: #{to}"
          nil
        else
          # debug "Delivering message to remote mailbox: #{msg}"
          node[:postoffice].async.deliver(to, msg)
          # debug "Finished delivery remnote box: to #{node.id}"
          nil
        end
      end

      def read(mailbox)
        node = address(mailbox)
        if node.id.eql?(Dcell.me.id)
          @mailboxes[mailbox]
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
        else
          raise "Can't destructively read a non-local mailbox!"
        end
      end

    end # class PostOffice
  end # module Distributed
end # module Elaine
