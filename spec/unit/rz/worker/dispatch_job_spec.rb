require 'spec_helper'

describe RZ::Worker,'#dispatch_job' do
  let(:object) do
    Class.new do
      include RZ::Worker

      register :job_that_raises do
        raise
      end

      register :interrupt do
        raise Interrupt
      end

      register :test_job do
        'result value'
      end
    end.new
  end

  subject { object.send :dispatch_job,job }

  context 'when job does not have "name"' do
    let(:job) { {} }

    it 'should raise ArgumentError' do
      expect { subject }.to raise_error(ArgumentError,'missing "name" in options')
    end
  end

  context 'when job does not have "arguments"' do
    let(:job) { { 'name' => 'test' } }

    it 'should raise ArgumentError' do
      expect { subject }.to raise_error(ArgumentError,'missing "arguments" in options')
    end
  end

  context 'when job is not registred' do
    let(:job) { { 'name' => 'test', 'arguments' => [] } }

    it 'should raise ArgumentError' do
      expect { subject }.to raise_error(ArgumentError,'job "test" is not registred')
    end
  end

  context 'when job returns some value' do
    let(:job) { { 'name' => 'test_job', 'arguments' => [] } }

    it { should == 'result value' }
  end

  context 'when job rasies any exception' do
    let(:job) { { 'name' => 'job_that_raises', 'arguments' => [] } }

    it 'should raise RZ::JobExecutionError' do
      expect { subject }.to raise_error(RZ::JobExecutionError,'job "job_that_raises" failed with RuntimeError')
    end
  end

  context 'when job raises an Interrupt' do
    let(:job) { { 'name' => 'interrupt', 'arguments' => [] } }

    it 'should raise Interrupt' do
      expect { subject }.to raise_error(Interrupt)
    end
  end
end
