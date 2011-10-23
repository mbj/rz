# RZ Ruby ZMQ job server

Simplifies writing job servers in ruby
And it is totally disfunctional ATM!

## Installation

With git and local working copy:

```bash
$ git clone git://github.com/mbj/rz.git
$ cd rz
$ gem install bundler
$ bundle install
>> require 'rb/client'
=> true
```

NOTE: This gem is currently only tested with 1.9 

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
