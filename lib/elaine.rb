require 'elaine/vertex'
require 'elaine/worker'
require 'elaine/coordinator'

require 'singleton'

class PostOffice
  include Singleton

  def initialize
    @mailboxes = Hash.new
    @mutex = Mutex.new
  end

  def deliver(to, msg)
    @mutex.synchronize do
      if @mailboxes[to]
        @mailboxes[to].push msg
      else
        @mailboxes[to] = [msg]
      end
    end
  end

  def read(box)
    @mutex.synchronize do
      @mailboxes.delete(box) || []
    end
  end
end
