(asdf:defsystem #:tsqueue
  :depends-on (#:bordeaux-threads)
  :name "tsqueue"
  :author "plkrueger <plkrueger@comcast.net>"
  :maintainer "plkrueger"
  :licence "MIT"
  :description "Thread Safe Queue"
  :components ((:file "thread-safe-queue")))