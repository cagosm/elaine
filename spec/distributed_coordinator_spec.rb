require 'distributed_helper'

describe Elaine::Distributed::Coordinator do
  # it 'should not allow empty graphs' do
  #   lambda { Coordinator.new([]) }.should raise_error
  # end

  let(:graph) do
    [
      {
        klazz: DistributedAddVertex,
        id: :igvita,
        value: 1,
        outedges: :wikipedia
      },
      {
        klazz: DistributedAddVertex,
        id: :wikipedia,
        value: 2,
        outedges: :google
      },
      {
        klazz: DistributedAddVertex,
        id: :google,
        value: 1,
        outedges: :wikipedia
      }
    ]
  end


  # let(:c_superviser) do
  #   DCell.start id: "test.elaine.coordinator", addr: "tcp://127.0.0.1:8090"
  # end

  # let(:w1_superviser) do
  #   DCell.start id: "test.elaine.worker1", addr: "tcp://127.0.0.1:8091"
  # end

  # let(:w2_superviser) do
  #   DCell.start id: "test.elaine.worker2", addr: "tcp://127.0.0.1:8092"
  # end

  # before(:all) do
  #   @c_superviser = DCell.start id: "test.elaine.coordinator", addr: "tcp://127.0.0.1:8090"
  #   @w1_superviser = DCell.start id: "test.elaine.worker1", addr: "tcp://127.0.0.1:8091"
  #   @w2_superviser = DCell.start id: "test.elaine.worker2", addr: "tcp://127.0.0.1:8092"
  # end

  it 'should partition graphs with variable worker sizes'
  # do

   # TestCoordinator.start


    # puts "starting coordinator node"
    
    # puts "supervising coordinator"
    # @c_superviser.supervise_as(:coordinator, Elaine::Distributed::Coordinator, graph: graph)
    # # c_superviser = Elaine::Distributed::Coordinator.supervise_as :coordinator, graph: graph

    # # puts "starting worker node 1"
    
    # # puts "supervising worker node 1"
    # # w1_superviser = Elaine::Distributed::Worker.supervise_as :worker, coordinator_node: "test.elaine.coordinator"

    # @w1_superviser.supervise_as :worker, Elaine::Distributed::Worker, coordinator_node: "test.elaine.coordinator"

    # DCell::Node["test.elaine.coordinator"][:coordinator].workers.size.should == 1

    # # puts "starting worker node 2"
    
    # # puts "supervising worker node 2"
    # @w2_superviser.supervise_as :worker, Elaine::Distributed::Worker, coordinator_node: "test.elaine.coordinator"

    # DCell::Node["test.elaine.coordinator"][:coordinator].workers.size.should == 2

    # w1_superviser.finalize
    # w2_superviser.finalize
    # c_superviser.finalize

    # puts "Nodes: #{DCell::Node.all}"

  #end

  it "should schedule workers to run intil there are no active vertices" do
    # DCell.start id: "test.elaine.coordinator", addr: "tcp://127.0.0.1:8090"
    # c_superviser = Elaine::Distributed::Coordinator.supervise_as :coordinator, graph: graph
    # DCell.start id: "test.elaine.worker1", addr: "tcp://127.0.0.1:8091"
    # w1_superviser = Elaine::Distributed::Worker.supervise_as :worker, coordinator_node: "test.elaine.coordinator"

    # DCell.start id: "test.elaine.worker2", addr: "tcp://127.0.0.1:8092"
    # w2_superviser = Elaine::Distributed::Worker.supervise_as :worker, coordinator_node: "test.elaine.coordinator"


    # c_superviser.supervise_as(:coordinator, Elaine::Distributed::Coordinator, graph: graph)
    # w1_superviser.supervise_as :worker, Elaine::Distributed::Worker, coordinator_node: "test.elaine.coordinator"
    # w2_superviser.supervise_as :worker, Elaine::Distributed::Worker, coordinator_node: "test.elaine.coordinator"

    DCell::Node["test.elaine.coordinator"][:coordinator].graph = graph
    DCell::Node["test.elaine.coordinator"][:coordinator].partition
    DCell::Node["test.elaine.coordinator"][:coordinator].run_job

    DCell::Node["test.elaine.coordinator"][:coordinator].workers.each do |w|
      DCell::Node[w][:worker].vertices.each do |v|
        DCell::Node[w][v].value.should == 5
      end
    end
  end
  # it 'should schedule workers to run until there are no active vertices' do
  #   c = Coordinator.new(graph)
  #   c.run

  #   c.workers.each do |w|
  #     w.vertices.each do |v|
  #       v.value.should == 5
  #     end
  #   end
  # end

  # context 'PageRank' do
  #   class PageRankVertex < Vertex
  #     def compute
  #       if superstep >= 1
  #         sum = messages.inject(0) {|total,msg| total += msg; total }
  #         @value = (0.15 / 3) + 0.85 * sum
  #       end

  #       if superstep < 30
  #         deliver_to_all_neighbors(@value / neighbors.size)
  #       else
  #         halt
  #       end
  #     end
  #   end

  it "should calculate PageRank of a circular graph" do
    g =[
      {
        klazz: DistributedPageRankVertex,
        id: :igvita,
        value: 1,
        outedges: :wikipedia
      },
      {
        klazz: DistributedPageRankVertex,
        id: :wikipedia,
        value: 1,
        outedges: :google
      },
      {
        klazz: DistributedPageRankVertex,
        id: :google,
        value: 1,
        outedges: :igvita
      }
    ]
    
    DCell::Node["test.elaine.coordinator"][:coordinator].graph = g
    DCell::Node["test.elaine.coordinator"][:coordinator].partition
    DCell::Node["test.elaine.coordinator"][:coordinator].run_job

    DCell::Node["test.elaine.coordinator"][:coordinator].workers.each do |w|
      DCell::Node[w][:worker].vertices.each do |v|
        (DCell::Node[w][v].value * 100).to_i.should == 33
      end
    end
  end
  #   it 'should calculate PageRank of a circular graph' do
  #     graph = [
  #       #                   name     value  out-edges
  #       PageRankVertex.new(:igvita,     1,  :wikipedia),
  #       PageRankVertex.new(:wikipedia,  1,  :google),
  #       PageRankVertex.new(:google,     1,  :igvita)
  #     ]

  #     c = Coordinator.new(graph)
  #     c.run

  #     c.workers.each do |w|
  #       w.vertices.each do |v|
  #         (v.value * 100).to_i.should == 33
  #       end
  #     end
  #   end

  it "should calculate PageRank of an arbitrary graph"
  #   it 'should calculate PageRank of arbitrary graph' do
  #     graph = [
  #       # page 1 -> page 1, page 2  (0.18)
  #       # page 2 -> page 1, page 3  (0.13)
  #       # page 3 -> page 3          (0.69)

  #       #                   name     value  out-edges
  #       PageRankVertex.new(:igvita,     1,  :igvita, :wikipedia),
  #       PageRankVertex.new(:wikipedia,  1,  :igvita, :google),
  #       PageRankVertex.new(:google,     1,  :google)
  #     ]

  #     c = Coordinator.new(graph)
  #     c.run

  #     c.workers.each do |w|
  #       (w.vertices.find {|v| v.id == :igvita }.value * 100).ceil.to_i.should == 19
  #       (w.vertices.find {|v| v.id == :wikipedia }.value * 100).ceil.to_i.should == 13
  #       (w.vertices.find {|v| v.id == :google }.value * 100).to_i.should == 68
  #     end
  #   end
  # end

  it 'should parition nodes by hashing the node id'
  it 'should allow scheduling multiple partitions to a single worker'
end