require 'elaine/distributed/post_office'
require 'elaine/instrument/post_office_instrument'

module Elaine
  module Distributed
    class InstrumentedPostOffice < Elaine::Distributed::PostOffice
      include Elaine::Instrument::PostOfficeInstrument
      # def initialize(*args)
      #   super(*args)
      # end
    end # class InstrumentedPostOffice
  end # module Distributed
end # module Elaine
