
require 'elaine/distributed'
class MessageSenderVertex < Elaine::Distributed::Vertex
  def compute
    # puts "sup?"
    # @value += 1
    # sleep(0.1)
    # deliver_to_all_neighbors(@value)
    # vote_to_stop if @value > 5
    # puts "@value: #{@value}"
    if @value >= 5
      vote_to_stop
    else
      
      @value += 1
      # puts "sending message ##{@count} (#{@value}"
      deliver_to_all_neighbors(@value)
    end
  end
end
