require 'instruments_helper'
require 'elaine/instruments'

describe Elaine::Instruments::Instrument do

  before(:each) do
    InstrumentCoordinator.start
    InstrumentCoordinator.wait_until_ready
    InstrumentWorker1.start
    InstrumentWorker1.wait_until_ready
    InstrumentWorker2.start
    InstrumentWorker2.wait_until_ready
  end

  after(:each) do
    InstrumentWorker1.stop
    InstrumentWorker2.stop
    InstrumentCoordinator.stop
  end

  let(:graph) do
    [
      {
        klazz: InstrumentVertex,
        # id: :igvita,
        id: 0,
        value: 1,
        outedges: [1]
      },
      {
        klazz: InstrumentVertex,
        # id: :wikipedia,
        id: 1,
        value: 2,
        outedges: [2]
      },
      {
        klazz: InstrumentVertex,
        # id: :google,
        id: 2,
        value: 1,
        outedges: [1]
      }
    ]
  end

  it "should import the the measure method" do
    DCell::Node["test.elaine.coordinator"][:coordinator].instrumented?.should be_true
    # DCell::Node["test.elaine.coordinator"][:coordinator].i_exist?.should be_true

    puts "setting graph"
    DCell::Node["test.elaine.coordinator"][:coordinator].graph = graph
    puts "done setting graph"
    DCell::Node["test.elaine.coordinator"][:coordinator].partition
    DCell::Node["test.elaine.coordinator"][:coordinator].run_job



    # values = DCell::Node["test.elaine.coordinator"][:coordinator].vertex_values
    # values.each do |v|
    #   v[:value].should == 5
    # end

  end

  
end