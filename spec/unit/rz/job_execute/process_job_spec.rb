require 'spec_helper'
require 'rz/job_executor'

describe RZ::JobExecutor,'#process_job' do
  EXPECTED_EXCEPTION = RuntimeError.new
  let(:object) do
    Class.new do
      include RZ::JobExecutor


      register :job_that_raises do
        raise EXPECTED_EXCEPTION
      end

      register :interrupt do
        raise Interrupt
      end


      register :test_job do
        'result value'
      end

      register :signal_exception do
        raise SignalException.new('SIGTERM')
      end
    end.new
  end

  subject { object.send :process_job,job }

  context 'when job does not have "name"' do
    let(:job) { {} }

    it 'should raise ArgumentError' do
      expect { subject }.to raise_error(RZ::ClientError,'missing "name" in options')
    end
  end

  context 'when job does not have "arguments"' do
    let(:job) { { 'name' => 'test' } }

    it 'should raise ArgumentError' do
      expect { subject }.to raise_error(RZ::ClientError,'missing "arguments" in options')
    end
  end

  context 'when job is not registred' do
    let(:job) { { 'name' => 'test', 'arguments' => [] } }

    it 'should raise ArgumentError' do
      expect { subject }.to raise_error(RZ::ClientError,'job "test" is not registred')
    end
  end

  context 'when job returns some value' do
    let(:job) { { 'name' => 'test_job', 'arguments' => [] } }

    it { should == 'result value' }
  end

  context 'when job rasies any exception' do
    let(:job) { { 'name' => 'job_that_raises', 'arguments' => [] } }

    it 'should raise RZ::JobExecutionError' do
      expect { subject }.to(
        raise_error(RZ::ClientJobExecutionError,'job "job_that_raises" failed with RuntimeError') do |error|
          error.original_exception.should == EXPECTED_EXCEPTION
          error.job.should == job
        end
      )
    end
  end

  context 'when job raises an Interrupt' do
    let(:job) { { 'name' => 'interrupt', 'arguments' => [] } }

    it 'should raise Interrupt' do
      expect { subject }.to raise_error(Interrupt)
    end
  end

  context 'when job raises an SignalException' do
    let(:job) { { 'name' => 'signal_exception', 'arguments' => [] } }

    it 'should raise SignalException' do
      expect { subject }.to raise_error(SignalException)
    end
  end
end
