require 'elaine'
require 'elaine/distributed'
require 'dcell'
require 'distributed_helper'

class InstrumentVertex; end
class MessageSenderVertex; end


# DCell.start

# class DistributedAddVertex < Elaine::Distributed::Vertex
#   def compute
#     @value += 1
#     halt if @value >= 5
#   end
# end

# class DistributedPageRankVertex < Elaine::Distributed::Vertex
#   def compute
#     if superstep >= 1
#       sum = messages.inject(0) {|total,msg| total += msg; total }
#       @value = (0.15 / 3) + 0.85 * sum
#     end

#     if superstep < 30
#       deliver_to_all_neighbors(@value / neighbors.size)
#     else
#       halt
#     end
#   end
# end


module InstrumentCoordinator
  PORT = 8090
  def self.start
    @@pid = Process.spawn Gem.ruby, File.expand_path("../../nodes/instrument_coordinator_node.rb", __FILE__)
    puts "Coordinator pid: #{@@pid}"
    unless @@pid
      STDERR.print "ERROR: Couldn't start test coordinator node"
      exit 1
    end
  end

  def self.wait_until_ready
    STDERR.print "Waiting for test coordinator node to start up..."

    socket = nil
    30.times do
      begin
        socket = TCPSocket.open("127.0.0.1", PORT)
        break if socket
      rescue Errno::ECONNREFUSED
        STDERR.print "."
        sleep 1
      end
    end

    if socket
      STDERR.puts " done!"
      socket.close
    else
      STDERR.puts " FAILED!"
      raise "couldn't connect to test node!"
    end
  end

  def self.stop
    puts "@@pid: #{@@pid}"
    unless @@pid
      STDERR.print "ERROR: Test coordinator node was never started!"
      exit 1
    end
    Process.kill 9, @@pid
  rescue Errno::ESRCH
  ensure
    Process.wait @@pid rescue nil
  end
end

module InstrumentWorker1
  PORT = 8091
  def self.start
    @pid = Process.spawn Gem.ruby, File.expand_path("../../nodes/instrument_worker_node1.rb", __FILE__)

    unless @pid
      STDERR.print "ERROR: Couldn't start test worker node 1"
      exit 1
    end
  end

  def self.wait_until_ready
    STDERR.print "Waiting for test worker node 1 to start up..."

    socket = nil
    30.times do
      begin
        socket = TCPSocket.open("127.0.0.1", PORT)
        break if socket
      rescue Errno::ECONNREFUSED
        STDERR.print "."
        sleep 1
      end
    end

    if socket
      STDERR.puts " done!"
      socket.close
    else
      STDERR.puts " FAILED!"
      raise "couldn't connect to test node!"
    end
  end

  def self.stop
    unless @pid
      STDERR.print "ERROR: Test worker node 1 was never started!"
      exit 1
    end
    Process.kill 9, @pid
  rescue Errno::ESRCH
  ensure
    Process.wait @pid rescue nil
  end


end


module InstrumentWorker2
  PORT = 8092
  def self.start
    @pid = Process.spawn Gem.ruby, File.expand_path("../../nodes/instrument_worker_node2.rb", __FILE__)

    unless @pid
      STDERR.print "ERROR: Couldn't start test worker node 2"
      exit 1
    end
  end

  def self.wait_until_ready
    STDERR.print "Waiting for test worker node 2 to start up..."

    socket = nil
    30.times do
      begin
        socket = TCPSocket.open("127.0.0.1", PORT)
        break if socket
      rescue Errno::ECONNREFUSED
        STDERR.print "."
        sleep 1
      end
    end

    if socket
      STDERR.puts " done!"
      socket.close
    else
      STDERR.puts " FAILED!"
      raise "couldn't connect to test node!"
    end
  end

  def self.stop
    unless @pid
      STDERR.print "ERROR: Test worker node 2 was never started!"
      exit 1
    end
    Process.kill 9, @pid
  rescue Errno::ESRCH
  ensure
    Process.wait @pid rescue nil
  end


end


# RSpec.configure do |config|
#   config.before(:suite) do
#     DCell.setup
#     DCell.run!    
#   end

#   # config.before(:all) do

#   #   TestCoordinator.start
#   #   TestCoordinator.wait_until_ready
#   #   TestWorker1.start
#   #   TestWorker1.wait_until_ready
#   #   TestWorker2.start
#   #   TestWorker2.wait_until_ready
#   # end

#   # config.after(:all) do
#   #   TestWorker1.stop
#   #   TestWorker2.stop
#   #   TestCoordinator.stop
#   # end
# end
