require 'dcell'

module Elaine
  module Distributed
    class PostOffice
      include Celluloid

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

      end

      def address(to)
        node = DCell::Nodes[@zipcodes[to]][:node]
      end


      def deliver(to, msg)
        node = address(to)

        if node.eql?(DCell.me)
          @mailboxes[to] ||= []
          @mailboxes[to].push msg
        else
          remote_post_office = node[@zipcodes[to][:service]]
          remote_post_office.deliver(to, msg)
        end
      end

      def read(mailbox)
        node = address(to)
        if node.eql?(Dcell.me)
          @mailboxes[to]
        else
          node[zipcodes[to][:service]].read to
        end
      end

      def read!(mailbox)
        node = address(to)
        if node.eql?(DCell.me)
          @mailboxes.delete(to) || []
        else
          raise "Can't destructively read a non-local mailbox!"
        end
      end

    end # class PostOffice
  end # module Distributed
end # module Elaine
