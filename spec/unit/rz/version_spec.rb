require 'spec_helper'

describe RZ,'VERSION' do
  subject { RZ::VERSION }
  it { should be_kind_of(String) }
  it { should =~ %r(\A\d+\.\d+\.\d+\Z) }
end
