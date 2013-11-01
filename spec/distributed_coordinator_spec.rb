require 'distributed_helper'

describe Elaine::Distributed::Coordinator do
  # it 'should not allow empty graphs' do
  #   lambda { Coordinator.new([]) }.should raise_error
  # end

  before(:each) do

    TestCoordinator.start
    TestCoordinator.wait_until_ready
    TestWorker1.start
    TestWorker1.wait_until_ready
    TestWorker2.start
    TestWorker2.wait_until_ready
  end

  after(:each) do
    TestWorker1.stop
    TestWorker2.stop
    TestCoordinator.stop
  end

  let(:graph) do
    [
      {
        klazz: DistributedAddVertex,
        id: :igvita,
        value: 1,
        outedges: [:wikipedia]
      },
      {
        klazz: DistributedAddVertex,
        id: :wikipedia,
        value: 2,
        outedges: [:google]
      },
      {
        klazz: DistributedAddVertex,
        id: :google,
        value: 1,
        outedges: [:wikipedia]
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

  # it 'should partition graphs with variable worker sizes'
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
    values = DCell::Node["test.elaine.coordinator"][:coordinator].vertex_values
    values.each do |v|
      v[:value].should == 5
    end

    # DCell::Node["test.elaine.coordinator"][:coordinator].workers.each do |w|
    #   DCell::Node[w][:worker].vertices.each do |v|
    #     DCell::Node[w][v].value.should == 5
    #   end
    # end
  end

  it "should calculate PageRank of a circular graph" do
    g =[
      {
        klazz: DistributedPageRankVertex,
        id: :igvita,
        value: 1,
        outedges: [:wikipedia]
      },
      {
        klazz: DistributedPageRankVertex,
        id: :wikipedia,
        value: 1,
        outedges: [:google]
      },
      {
        klazz: DistributedPageRankVertex,
        id: :google,
        value: 1,
        outedges: [:igvita]
      }
    ]

    DCell::Node["test.elaine.coordinator"][:coordinator].graph = g
    DCell::Node["test.elaine.coordinator"][:coordinator].partition
    DCell::Node["test.elaine.coordinator"][:coordinator].run_job

    values = DCell::Node["test.elaine.coordinator"][:coordinator].vertex_values
    values.each do |v|
      (v[:value] * 100).to_i.should == 33
    end

    # DCell::Node["test.elaine.coordinator"][:coordinator].workers.each do |w|
    #   DCell::Node[w][:worker].vertices.each do |v|
    #     (DCell::Node[w][v].value * 100).to_i.should == 33
    #   end
    # end
  end
  

  it "should calculate PageRank of an arbitrary graph" do
    g = [
      # page 1 -> page 1, page 2  (0.18)
      # page 2 -> page 1, page 3  (0.13)
      # page 3 -> page 3          (0.69)

      #                   name     value  out-edges
      {
        klazz: DistributedPageRankVertex,
        id: :igvita,
        value: 1,
        outedges: [:igvita, :wikipedia]
      },
      {
        klazz: DistributedPageRankVertex,
        id: :wikipedia,
        value: 1,
        outedges: [:igvita, :google]
      },
      {
        klazz: DistributedPageRankVertex,
        id: :google,
        value: 1,
        outedges: [:google]
      }
    ]
    DCell::Node["test.elaine.coordinator"][:coordinator].graph = g
    DCell::Node["test.elaine.coordinator"][:coordinator].partition
    DCell::Node["test.elaine.coordinator"][:coordinator].run_job

    vertex_values = DCell::Node["test.elaine.coordinator"][:coordinator].vertex_values
    vertex_values.each do |v|
      if v[:id] == :igvita
        (v[:value] * 100).ceil.to_i.should == 19
      elsif v[:id] == :wikipedia
        (v[:value] * 100).ceil.to_i.should == 13
      elsif v[:id] == :google
        (v[:value] * 100).to_i.should == 68
      else
        fail "Unexpected node id: #{v[:id]}"
      end
    end

    # (DCell::Node[zipcodes[:igvita]][:igvita].value * 100).ceil.to_i.should == 19
    # (DCell::Node[zipcodes[:wikipedia]][:wikipedia].value * 100).ceil.to_i.should == 13
    # (DCell::Node[zipcodes[:google]][:google].value * 100).to_i.should == 68
    
  end

  #     c = Coordinator.new(graph)
  #     c.run

  #     c.workers.each do |w|
  #       (w.vertices.find {|v| v.id == :igvita }.value * 100).ceil.to_i.should == 19
  #       (w.vertices.find {|v| v.id == :wikipedia }.value * 100).ceil.to_i.should == 13
  #       (w.vertices.find {|v| v.id == :google }.value * 100).to_i.should == 68
  #     end
  #   end
  # end

  # it 'should parition nodes by hashing the node id'
  # it 'should allow scheduling multiple partitions to a single worker'
end