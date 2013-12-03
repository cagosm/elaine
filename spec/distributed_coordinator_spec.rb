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
        # id: :igvita,
        id: 0,
        value: 1,
        outedges: [1]
      },
      {
        klazz: DistributedAddVertex,
        # id: :wikipedia,
        id: 1,
        value: 2,
        outedges: [2]
      },
      {
        klazz: DistributedAddVertex,
        # id: :google,
        id: 2,
        value: 1,
        outedges: [1]
      }
    ]
  end


 

  it "should schedule workers to run intil there are no active vertices" do
    
    DCell::Node["test.elaine.coordinator"][:coordinator].graph = graph
    DCell::Node["test.elaine.coordinator"][:coordinator].partition
    DCell::Node["test.elaine.coordinator"][:coordinator].run_job
    values = DCell::Node["test.elaine.coordinator"][:coordinator].vertex_values
    values.each do |v|
      v[:value].should == 5
    end

  end

  it "should calculate PageRank of a circular graph" do
    g =[
      {
        klazz: DistributedPageRankVertex,
        # id: :igvita,
        id: 0,
        value: 1,
        outedges: [1]
      },
      {
        klazz: DistributedPageRankVertex,
        # id: :wikipedia,
        id: 1,
        value: 1,
        outedges: [2]
      },
      {
        klazz: DistributedPageRankVertex,
        # id: :google,
        id: 2,
        value: 1,
        outedges: [0]
      }
    ]

    DCell::Node["test.elaine.coordinator"][:coordinator].graph = g
    DCell::Node["test.elaine.coordinator"][:coordinator].partition
    DCell::Node["test.elaine.coordinator"][:coordinator].run_job

    values = DCell::Node["test.elaine.coordinator"][:coordinator].vertex_values
    values.each do |v|
      (v[:value] * 100).to_i.should == 33
    end
  end
  

  it "should calculate PageRank of an arbitrary graph" do
    g = [
      # page 1 -> page 1, page 2  (0.18)
      # page 2 -> page 1, page 3  (0.13)
      # page 3 -> page 3          (0.69)

      #                   name     value  out-edges
      {
        klazz: DistributedPageRankVertex,
        # id: :igvita,
        id: 0,
        value: 1,
        outedges: [0, 1]
      },
      {
        klazz: DistributedPageRankVertex,
        # id: :wikipedia,
        id: 1,
        value: 1,
        outedges: [0, 2]
      },
      {
        klazz: DistributedPageRankVertex,
        # id: :google,
        id: 2,
        value: 1,
        outedges: [2]
      }
    ]
    DCell::Node["test.elaine.coordinator"][:coordinator].graph = g
    DCell::Node["test.elaine.coordinator"][:coordinator].partition
    DCell::Node["test.elaine.coordinator"][:coordinator].run_job

    vertex_values = DCell::Node["test.elaine.coordinator"][:coordinator].vertex_values
    vertex_values.each do |v|
      # if v[:id] == :igvita
      if v[:id] == 0
        (v[:value] * 100).ceil.to_i.should == 19
      # elsif v[:id] == :wikipedia
      elsif v[:id] == 1
        (v[:value] * 100).ceil.to_i.should == 13
      # elsif v[:id] == :google
      elsif v[:id] == 2
        (v[:value] * 100).to_i.should == 68
      else
        fail "Unexpected node id: #{v[:id]}"
      end
    end

    
  end

  it 'should parition nodes by hashing the node id'
  it 'should allow scheduling multiple partitions to a single worker'
end