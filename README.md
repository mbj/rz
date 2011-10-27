# Ruby ZMQ job server with truly pulling workers.

This is a playground to build something that solves some of my messaging problems 
using ZMQ primitives. Maybe this all would be solved just by using rabbbitMQ 
and friends. But I wanna learn about this topic more in depth. I'm thinking about 
ZMQ as a messaging framework. So pls keep in mind my "Problems" below are not problems 
of zmq itself. 

The subject is task distribution in a classic multi worker central job-distributor setup.
(I'm also interested in the freelance patterns)

Problem 1: Peers cannot know how busy a worker is.

  This is a generic problem. Peers can only infer worker workload using connection counts 
  like haproxy or other strategies like round robin etc. 
  A "small message, big computation" job can easily block a worker for a serious amount of 
  time. So work can queue up for this worker raising the overall latency for the hole system.

  Background:
    A zmq PULL socket connecting a PUSH socket does not really pull from the server. 
    It pulls from the local mailbox.

Problem 2: Lost tasks in worker mailboxes on crash

  Task distribution can easily be done using zmqs unique ability to connect more 
  than one downstream per socket. Each downstream socket/worker has its own mailbox. 
  Unprocessed messages are lost when this worker crashes. When this mailbox is big or there 
  are many small messages this can hurt latency for the hole system too.

  Background:
    ZMQ pushes messages from mailbox to mailbox zmq_recv and zmq_send add or remove messages 
    from this mailbox. A background thread does the transport between mailboxes. You cannot know 
    by design in which mailbox your message currently is.

Problem 3: Adding workers while processing many jobs

  Imagine a PUSH - PULL zmq setup. 1 PUSH sockets sends 1000 small messages to 2 PULL sockets. 
  Each message takes 1 second to process. After 10 seconds you are connecting a 3rd worker. But 
  all 1000 messages are already send to one of the workers mailboxes. Bad. Your new worker is 
  waiting for jobs while the other two have to much.

Idea: Let the workers pull work from the server. 

  When workers are pulling the servers for work there is no worker side queue of unprocessed,
  lost-in-case-of-crash or present-in-mailbox where other workers do not have anything to do.

Implementation:

  To be documented. The current stage works but is far from well designed tested etc...

I really appreciate any input!

## Installation

With git and local working copy:

```bash
$ git clone git://github.com/mbj/rz.git
$ cd rz
$ gem install bundler
$ bundle install
examples/service &
examples/worker &
examples/client &
```

NOTE: This gem is currently only tested with 1.9 is likely to work with ruby-1.8 and backports.

## Usage

See examples directory for code.

## Note on Patches/Pull Requests

* If you want your code merged into the mainline, please discuss the proposed changes with me before doing any work on it. This library is still in early development, and it may not always be clear the direction it is going. Some features may not be appropriate yet, may need to be deferred until later when the foundation for them is laid, or may be more applicable in a plugin.
* Fork the project.
* Make your feature addition or bug fix.
* Add specs for it. This is important so I don't break it in a future version unintentionally. Tests must cover all branches within the code, and code must be fully covered.
* Commit, do not mess with Rakefile, version, or history.  (if you want to have your own version, that is fine but bump version in a commit by itself I can ignore when I pull)
* Run "rake ci". This must pass and not show any regressions in the
  metrics for the code to be merged.
* Send me a pull request. Bonus points for topic branches.

## Copyright

Copyright &copy; 2011 Markus Schirp. See LICENSE for details.
