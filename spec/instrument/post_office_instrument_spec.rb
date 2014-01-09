require 'instrument/helpers/coordinator_instruments_helper'
require 'elaine/instrument'
require 'elaine/instrument/post_office_instrument'



describe Elaine::Instrument::PostOfficeInstrument do

  before(:each) do
    CoordinatorInstrumentCoordinator.start
    CoordinatorInstrumentCoordinator.wait_until_ready
    CoordinatorInstrumentWorker1.start
    CoordinatorInstrumentWorker1.wait_until_ready
    CoordinatorInstrumentWorker2.start
    CoordinatorInstrumentWorker2.wait_until_ready
  end

  after(:each) do
    CoordinatorInstrumentWorker1.stop
    CoordinatorInstrumentWorker2.stop
    CoordinatorInstrumentCoordinator.stop
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

  let(:graph2) do
    [
      {
        klazz: MessageSenderVertex,
        id: 0,
        value: 0,
        outedges: [1]
      },
      {
        klazz: MessageSenderVertex,
        id: 1,
        value: 0,
        outedges: [0]
      }
    ]
  end

  let(:graph3) do
    [
      {
        klazz: MessageSenderVertex,
        id: 0,
        value: 0,
        outedges: [0]
      },
      {
        klazz: MessageSenderVertex,
        id: 1,
        value: 0,
        outedges: [1]
      }
    ]
  end

# enable_measurement(:deliver)
#         enable_measurement(:deliver_local)
#         enable_measurement(:deliver_bulk)
#         enable_measurement(:receive_bulk)

  it "should return true for #instrumeneted?" do
    
    DCell::Node["test.elaine.worker1"][:postoffice].instrumented?.should be_true
    DCell::Node["test.elaine.worker2"][:postoffice].instrumented?.should be_true
  end

  it "should record the number of messages delivered" do
    
    DCell::Node["test.elaine.worker1"][:postoffice].enable_measurement(:deliver)
    
    DCell::Node["test.elaine.worker2"][:postoffice].enable_measurement(:deliver)
    
    
    DCell::Node["test.elaine.coordinator"][:coordinator].graph = graph2
    
    DCell::Node["test.elaine.coordinator"][:coordinator].partition
    DCell::Node["test.elaine.coordinator"][:coordinator].run_job

    
    DCell::Node["test.elaine.worker1"][:postoffice].instrument_measurements["deliver"][:count].should == 5
    DCell::Node["test.elaine.worker2"][:postoffice].instrument_measurements["deliver"][:count].should == 5
  end

  it "should record the number of local deliveries" do

    DCell::Node["test.elaine.worker1"][:postoffice].enable_measurement(:deliver_local)
    
    DCell::Node["test.elaine.worker2"][:postoffice].enable_measurement(:deliver_local)
    
    
    DCell::Node["test.elaine.coordinator"][:coordinator].graph = graph3
    
    DCell::Node["test.elaine.coordinator"][:coordinator].partition
    DCell::Node["test.elaine.coordinator"][:coordinator].run_job

    
    DCell::Node["test.elaine.worker1"][:postoffice].instrument_measurements["deliver_local"][:count].should == 5
    DCell::Node["test.elaine.worker2"][:postoffice].instrument_measurements["deliver_local"][:count].should == 5
  end

  it "should not record local deliveries when none took place" do
    DCell::Node["test.elaine.worker1"][:postoffice].enable_measurement(:deliver_local)
    
    DCell::Node["test.elaine.worker2"][:postoffice].enable_measurement(:deliver_local)
    
    
    DCell::Node["test.elaine.coordinator"][:coordinator].graph = graph2
    
    DCell::Node["test.elaine.coordinator"][:coordinator].partition
    DCell::Node["test.elaine.coordinator"][:coordinator].run_job

    
    DCell::Node["test.elaine.worker1"][:postoffice].instrument_measurements["deliver_local"].should be_nil
    # [:count].should == 0
    DCell::Node["test.elaine.worker2"][:postoffice].instrument_measurements["deliver_local"].should be_nil
    # [:count].should == 0
  end
 
end