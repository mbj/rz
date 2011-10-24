require 'rz/worker'

describe RZ::Worker, '.register' do
  let(:object) do
    mod = Module.new do
      include RZ::Worker

      class << self
        public :register
      end
    end
  end

  subject do
    object.register name,&block; object
  end

  let(:name) { :example }
  let(:block) { nil }


  context 'when block given' do
    let(:block) { proc {} }

    its(:registry) { should have_key(name.to_s) }
  end

  context 'when no block given' do
    its(:registry) { should have_key(name.to_s) }
  end

  context 'when name is already registred' do
    before do
      object.register name
    end

    it 'should raise ArgumentError' do
      expect { subject }.to raise_error(ArgumentError,'"example" is already registred')
    end
  end
end
