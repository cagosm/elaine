require 'dcell'

module Elaine
  module Distributed
    class PostOffice
      include Celluloid
      include Celluloid::Logger

      attr_reader :mailboxes
      attr_reader :zipcodes

      def initialize
        @mailboxes = Hash.new
        @zipcodes = Hash.new
      end

      def zipcodes=(zipcodes)
        @zipcodes = zipcodes

        # do we need to initialize all the mailboxes here?
        # might be smart?
        # @mailboxes = Hash.new

      end

      def address(to)
        # debug "There are: #{zipcodes.size} zipcodes"
        # debug "Looking up address for #{to}"
        # debug "Post office for #{to} is #{@zipcodes[to]}"
        node = DCell::Node[@zipcodes[to]]
      end


      def deliver(to, msg)
        node = address(to)

        if node.id.eql?(DCell.me.id)
          @mailboxes[to] ||= []
          @mailboxes[to].push msg
        else

          remote_post_office = node[:postoffice]
          remote_post_office.deliver(to, msg)
        end
      end

      def read(mailbox)
        node = address(to)
        if node.eql?(Dcell.me.id)
          @mailboxes[to]
        else
          node[:postoffice].read to
        end
      end

      def read!(mailbox)
        node = address(mailbox)
        # debug "node: #{node}"
        # debug "node.id: '#{node.id}'"
        # debug "DCell.me.id: '#{DCell.me.id}'"
        if node.id.eql?(DCell.me.id)
          @mailboxes.delete(mailbox) || []
        else
          raise "Can't destructively read a non-local mailbox!"
        end
      end

    end # class PostOffice
  end # module Distributed
end # module Elaine
